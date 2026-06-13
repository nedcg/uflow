<p align="center">
  <img src="assets/logo.png" alt="uflow Logo" width="250" height="250" />
</p>

# uflow ── Type-Safe Interceptor Engine for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/nedcg/uflow.svg)](https://pkg.go.dev/github.com/nedcg/uflow)
[![Go Report Card](https://goreportcard.com/badge/github.com/nedcg/uflow)](https://goreportcard.com/report/github.com/nedcg/uflow)

**uflow** is an elegant, generic interceptor engine written in Go. It enables you to coordinate complex, multi-stage middleware flows on top of any arbitrary state struct in a completely type-safe manner. 

`uflow` is designed for building robust HTTP applications, event processing systems (like Kafka/RabbitMQ consumers), and transaction coordinator chains where state isolation, guaranteed cleanup, and dynamic error recovery are critical.

---

## 🌊 The Interceptor Concept (U-Shape Execution)

Unlike standard linear pipelines (`A -> B -> C`), `uflow` implements the **Interceptor Pattern** (often known as the Onion or Step pattern, popularized by Clojure's Pedestal framework).

When you execute a group of interceptor steps, `uflow` traverses them in a **U-Shape**:

1. **Dive In**: It executes all `In` hooks sequentially (e.g., `A -> B -> C`).
2. **Surface Out**: Once the bottom is reached, it traverses back *Out* in reverse order (`C -> B -> A`).
3. **Catch Recovery**: If an error occurs anywhere, execution immediately halts and bubbles backwards through the `Catch` hooks, allowing outer steps to gracefully recover or rollback.

```text
          [ Start ]
              │
      ┌───────▼───────┐
      │ Step A: In()  │
      └───────┬───────┘
      ┌───────▼───────┐
      │ Step B: In()  │
      └───────┬───────┘
              │             <-- [ Bottom of the U ]
      ┌───────▼───────┐
      │ Step B: Out() │
      └───────┬───────┘
      ┌───────▼───────┐
      │ Step A: Out() │
      └───────┬───────┘
              │
          [ Done ]
```

This ensures that if an interceptor allocates resources or opens a transaction during the `In` phase, it is mathematically guaranteed the opportunity to clean it up during the `Out` or `Catch` phase, regardless of what downstream steps do.

---

## 🚀 Installation

```bash
go get github.com/nedcg/uflow
```

---

## 🧩 Building Pipelines (Step-by-Step)

uflow revolves around the `Step[T]` interface, but provides incredibly ergonomic wrappers to define logic exactly where you need it.

### 1. In/Out Steps (The Full Interceptor)
The most powerful way to define a step is to use `uflow.StepFunc`. This allows you to hook into both the `In` and `Out` phases in a single module.
```go
telemetryStep := &uflow.StepFunc[*Context]{
    Id: "Telemetry",
    InFunc: func(r *uflow.Runner[*Context]) error {
        r.Data.StartTime = time.Now()
        return nil
    },
    OutFunc: func(r *uflow.Runner[*Context]) error {
        duration := time.Since(r.Data.StartTime)
        fmt.Println("Request took:", duration)
        return nil
    },
}
```

### 2. In-Only Steps
Perfect for pre-processing, validation, or authentication.
```go
authStep := uflow.In("Auth", func(r *uflow.Runner[*Context]) error {
    if r.Data.Token == "" {
        return errors.New("unauthorized")
    }
    return nil
})
```

### 3. Out-Only Steps
Perfect for post-processing or auditing that must happen *after* everything else has run.
```go
auditStep := uflow.Out("Audit", func(r *uflow.Runner[*Context]) error {
    fmt.Println("Pipeline finished with status:", r.Data.Status)
    return nil
})
```

### 4. Group (Pipeline Flattening)
You can bundle multiple steps into a cohesive, reusable module using `uflow.NewGroup`. 

Groups are natively flattened by the `Runner`. This guarantees the U-shape "onion" ordering is perfectly preserved across boundaries.

```go
// Group multiple steps into a cohesive block
securityModule := uflow.NewGroup(
    rateLimitStep,
    authStep,
    corsStep,
)

// You can nest Groups inside other Groups infinitely!
mainFlow := uflow.NewGroup(
    telemetryStep,
    securityModule, // Flattens seamlessly
    businessLogicStep,
)
```

### 5. Nested Isolation (`NestedIn` / `NestedOut`)
Sometimes you *don't* want to flatten steps. If you want an entire sub-pipeline to execute from start-to-finish entirely within the parent's `In` or `Out` phase, use `NestedIn`.

```go
isolatedTx := uflow.NestedIn("TxBlock", 
    uflow.In("BeginTx", begin),
    uflow.Out("CommitTx", commit),
    uflow.Catch("RollbackTx", rollback),
)

// The parent pipeline will pause at `isolatedTx`, wait for the entire 
// Begin -> Commit/Rollback sub-execution to finish, and then continue.
```

### 6. Short-Circuiting (`Terminate`)
If an interceptor decides that no further downstream processing is needed (e.g., returning a cached HTTP response), it can call `Terminate()`.

```go
cacheStep := uflow.In("CacheCheck", func(r *uflow.Runner[*Context]) error {
    if cached := getCache(r.Data.Request); cached != nil {
        r.Data.Response = cached
        
        // Stops any deeper 'In' hooks from running.
        // Execution immediately turns around and begins the 'Out' phase!
        r.Terminate() 
    }
    return nil
})
```

---

## 💡 Real-World Applications

### 🌐 HTTP Middleware (Tracing)
The Interceptor pattern is the ultimate model for HTTP requests. You can inject a Trace ID on the way `In`, and write the final response headers on the way `Out`.

```go
type RequestState struct {
    Req       *http.Request
    Res       http.ResponseWriter
    TraceID   string
}

tracingStep := &uflow.StepFunc[*RequestState]{
    Id: "Tracing",
    InFunc: func(r *uflow.Runner[*RequestState]) error {
        r.Data.TraceID = generateUUID()
        r.Data.Req.Header.Set("X-Trace-ID", r.Data.TraceID)
        return nil
    },
    OutFunc: func(r *uflow.Runner[*RequestState]) error {
        r.Data.Res.Header().Set("X-Trace-ID", r.Data.TraceID)
        return nil
    },
}
```

### 📩 Kafka Consumers (Transactions & DLQ)
When processing event streams, you want to guarantee that a message is acknowledged *only* if the pipeline succeeds, and DLQ'd (Dead-Letter Queue) if it fails.

```go
kafkaAckStep := &uflow.StepFunc[*MessageState]{
    Id: "KafkaAck",
    // Ack the message on the way OUT (Success)
    OutFunc: func(r *uflow.Runner[*MessageState]) error {
        r.Data.Message.Ack()
        return nil
    },
    // Nack or DLQ the message on CATCH (Failure)
    CatchFunc: func(r *uflow.Runner[*MessageState], err error) error {
        sendToDLQ(r.Data.Message, err)
        r.Data.Message.Nack()
        
        // Return nil to tell uflow we've handled the error gracefully!
        return nil 
    },
}
```

### 🪵 Contextual Logging
You can inject a highly-contextual logger into the pipeline state, use it deep within your business logic, and effortlessly tear it down or sync it at the end.

```go
loggingStep := &uflow.StepFunc[*AppState]{
    Id: "Logger",
    InFunc: func(r *uflow.Runner[*AppState]) error {
        // Create a scoped logger
        r.Data.Logger = baseLogger.With("trace", r.Data.TraceID)
        return nil
    },
    OutFunc: func(r *uflow.Runner[*AppState]) error {
        // Guarantee logs are flushed to disk before the pipeline fully exits
        r.Data.Logger.Sync()
        return nil
    },
}
```

---

## ⚡ Memory Reuse & Pooling (Zero Allocation)

For high-throughput systems (such as consuming millions of events per second), allocation overhead can be a bottleneck. 

uflow's `Runner[T]` can be pooled using standard `sync.Pool` by resetting its state in-place:

```go
var runnerPool = sync.Pool{
	New: func() any {
		return &uflow.Runner[*MyState]{}
	},
}

func ProcessEvent(ctx context.Context, flow uflow.Step[*MyState], data *MyState) error {
	// 1. Get runner from pool
	r := runnerPool.Get().(*uflow.Runner[*MyState])
	
	// 2. Reset in-place (Flattens the flow automatically)
	r.Reset(ctx, []uflow.Step[*MyState]{flow}, data)
	
	// 3. Run execution
	err := r.Execute()
	
	// 4. Put back to pool
	runnerPool.Put(r)
	return err
}
```

---

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
