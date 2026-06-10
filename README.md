# Juzu ── Type-Safe, 3-Phase Interceptor Pipeline Engine for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/nedcg/juzu.svg)](https://pkg.go.dev/github.com/nedcg/juzu)
[![Go Report Card](https://goreportcard.com/badge/github.com/nedcg/juzu)](https://goreportcard.com/report/github.com/nedcg/juzu)

**Juzu** is an elegant, generic pipeline engine written in Go. Inspired by the **Interceptor Pattern** (famously used in Clojure's Pedestal framework), Juzu enables you to coordinate complex, multi-stage workflows on top of any arbitrary state struct in a type-safe manner.

Juzu is designed for building middleware-heavy applications, event processing systems, data-ingestion pipelines, and transaction coordinator chains where state separation, error propagation/recovery, and dynamic scheduling are critical.

---

## Key Features

*   **100% Type-Safe (Go Generics):** Operates on any custom state type `T` via `Execution[T]`, eliminating `interface{}` cast overhead and type-assertions.
*   **Three-Phase Interceptor Lifecycle:** Every interceptor implements three distinct phases: `Enter` (FIFO), `Leave` (LIFO), and `Error` (LIFO).
*   **Error Recovery & Resolution:** Interceptors can catch, modify, propagate, or completely suppress (resolve) errors. Resolving an error resumes the `Leave` execution phase down the remaining stack.
*   **Dynamic Queue Control:** Interceptors can append new stages mid-flight via `Enqueue` or halt downstream stages entirely via `Terminate`.
*   **Context-Aware Execution:** Built-in checks for `context.Context` cancellation or deadline expiration between stages.
*   **High Performance / Zero Allocations:** Includes an in-place `Reset` method allowing `Execution[T]` structures to be pooled using `sync.Pool` to avoid memory allocations in high-throughput pipelines.

---

## The 3-Phase Execution Lifecycle

Juzu runs execution pipelines in a stack-based double-pass traversal:

```mermaid
graph TD
    %% Define styles
    classDef enter fill:#d4edda,stroke:#28a745,color:#155724;
    classDef leave fill:#cce5ff,stroke:#004085,color:#004085;
    classDef err fill:#f8d7da,stroke:#721c24,color:#721c24;

    Start([Start Execute]) --> E1

    %% Enter Phase
    subgraph Enter Phase (FIFO)
        E1["Interceptor A: Enter()"]
        E2["Interceptor B: Enter()"]
        E3["Interceptor C: Enter()"]
    end
    class E1,E2,E3 enter;

    E1 -- Success --> E2
    E2 -- Success --> E3
    E3 -- Success --> LeavePhase

    %% Leave Phase
    subgraph Leave Phase (LIFO)
        L3["Interceptor C: Leave()"]
        L2["Interceptor B: Leave()"]
        L1["Interceptor A: Leave()"]
    end
    class L3,L2,L1 leave;

    LeavePhase --> L3
    L3 -- Success --> L2
    L2 -- Success --> L1
    L1 -- Success --> End([Pipeline Completed])

    %% Error Phase
    subgraph Error Phase (LIFO)
        Err3["Interceptor C: Error()"]
        Err2["Interceptor B: Error()"]
        Err1["Interceptor A: Error()"]
    end
    class Err3,Err2,Err1 err;

    %% Enter Errors
    E1 -- Error --> Err1
    E2 -- Error --> Err2
    E3 -- Error --> Err3

    %% Leave Errors
    L3 -- Error --> Err3
    L2 -- Error --> Err2
    L1 -- Error --> Err1

    %% Error Traversal
    Err3 -- "Still Active" --> Err2
    Err2 -- "Still Active" --> Err1
    Err1 -- "Unresolved" --> ReturnErr([Return Error])

    %% Recovery
    Err2 -- "Error Resolved (returns nil)" --> L1
    Err3 -- "Error Resolved (returns nil)" --> L2
```

1.  **Enter Phase (Forward / FIFO):** Executes `Enter` hooks sequentially down the queue. Each executed interceptor is pushed onto a LIFO execution stack.
2.  **Leave Phase (Reverse / LIFO):** Once all `Enter` hooks succeed, Juzu pops interceptors from the stack one by one, executing their `Leave` hooks in reverse order.
3.  **Error Phase (Reverse / LIFO):** If an error occurs in any `Enter` or `Leave` hook (or if the `context.Context` is cancelled), execution immediately halts. Juzu pops interceptors from the stack, routing the error through their `Error` hooks.
    *   **Error Recovery:** If an `Error` hook handles/suppresses the error by returning `nil`, the pipeline recovers! It halts the error propagation and resumes executing the `Leave` phase for all remaining interceptors left on the stack.

---

## Installation

```bash
go get github.com/nedcg/juzu
```

---

## Core Interfaces & Structs

### 1. The Interceptor Interface

An interceptor represents a single processing module. You can implement the interface directly:

```go
type Interceptor[T any] interface {
	Name() string
	Enter(exec *Execution[T]) error
	Leave(exec *Execution[T]) error
	Error(exec *Execution[T], err error) error
}
```

Or you can use `juzu.Func[T]` or helper functions (`juzu.Enter`, `juzu.Leave`, `juzu.Error`) to create ad-hoc interceptors.

### 2. Execution State Manager

The `Execution[T]` struct maintains the execution state, the pipeline queue, and your custom mutable data `T`:

```go
type Execution[T any] struct {
	Data T // Your custom state
    // private fields...
}
```

---

## Usage Examples

### Example 1: Basic Pipeline

In this example, we process a HTTP-like request context, modifying the request headers in `Enter` and logging/auditing the status in `Leave`.

```go
package main

import (
	"context"
	"fmt"
	"github.com/nedcg/juzu"
)

// Define your pipeline state
type RequestState struct {
	Headers map[string]string
	Body    string
	Status  int
}

func main() {
	// 1. Create interceptors
	authInterceptor := juzu.Enter("Auth", func(e *juzu.Execution[*RequestState]) error {
		if e.Data.Headers["Authorization"] == "" {
			e.Data.Status = 401
			e.Terminate() // Halt downstream pipeline execution
		}
		return nil
	})

	businessLogic := juzu.Enter("Process", func(e *juzu.Execution[*RequestState]) error {
		fmt.Println("Processing payload:", e.Data.Body)
		e.Data.Status = 200
		return nil
	})

	auditLogger := juzu.Leave("AuditLog", func(e *juzu.Execution[*RequestState]) error {
		fmt.Printf("[AuditLog] Handled request with status: %d\n", e.Data.Status)
		return nil
	})

	// 2. Assemble the queue
	pipeline := []juzu.Interceptor[*RequestState]{
		auditLogger, // Enter: skipped (has no Enter hook), Leave: runs last
		authInterceptor,
		businessLogic,
	}

	// 3. Prepare state and run execution
	state := &RequestState{
		Headers: map[string]string{"Authorization": "Bearer token123"},
		Body:    `{"userID": "12345"}`,
	}

	exec := juzu.NewExecution(context.Background(), pipeline, state)
	if err := exec.Execute(); err != nil {
		fmt.Printf("Pipeline failed: %v\n", err)
	}
}
```

### Example 2: Error Recovery

Juzu makes error recovery simple. Any stage's `Error` hook can catch a downstream failure and resolve it:

```go
recoveryInterceptor := &juzu.Func[MyState]{
	Id: "Recovery",
	ErrorFunc: func(exec *juzu.Execution[MyState], err error) error {
		fmt.Printf("Caught downstream error: %v. Recovering...\n", err)
		// Perform recovery actions...
		return nil // Returning nil marks the error as resolved!
	},
}
```

If a downstream step fails, `Recovery.Error()` intercepts the error. Because it returns `nil`, Juzu will resume running the `Leave` hooks of all upstream interceptors on the stack.

### Example 3: Dynamic Scheduling

Interceptors can add steps to the queue dynamically based on runtime conditions:

```go
injector := juzu.Enter("DynamicStep", func(e *juzu.Execution[*MyState]) error {
	if e.Data.NeedsCompacting {
		// Enqueue the compaction step to run right after this hook finishes
		e.Enqueue(compactionInterceptor)
	}
	return nil
})
```

---

## Memory Reuse & Pooling (Zero Allocation)

For high-throughput systems (such as Kafka consumer loops processing millions of events per second), allocation overhead can be a bottleneck. 

Juzu's `Execution[T]` can be pooled using `sync.Pool` by resetting its state in-place:

```go
var executionPool = sync.Pool{
	New: func() any {
		return &juzu.Execution[MyState]{}
	},
}

func HandleBatch(ctx context.Context, pipeline []juzu.Interceptor[MyState], data MyState) error {
	// 1. Get execution manager from pool
	exec := executionPool.Get().(*juzu.Execution[MyState])
	
	// 2. Reset in-place
	exec.Reset(ctx, pipeline, data)
	
	// 3. Run execution
	err := exec.Execute()
	
	// 4. Put back to pool
	executionPool.Put(exec)
	return err
}
```

---

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
