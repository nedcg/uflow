# Juzu Kafka Package

This package provides a production-grade, stateful Kafka consumer integration built on top of **juzu**. It implements **Exactly-Once Semantics (EOS)** with transactional commits, batch message processing, and **Stateful Poisoned Group Routing** to handle message failures without breaking ordering guarantees.

---

## Key Features

1. **Stateful Poisoned Group Routing:** If processing fails for a message belonging to a specific business group (e.g. `tenant_id`, `user_id`, or `order_id`), that group is flagged as "poisoned". All subsequent messages belonging to the same group bypass normal processing and route directly to the DLQ, ensuring failures do not block the partition.
2. **Order-Preserving DLQ Reprocessing:** Reprocesses poisoned messages directly from the DLQ while keeping the group blocked. This prevents out-of-order execution if new messages arrive on the main topic during reprocessing.
3. **Batch Processing:** Consumers process messages in configurable batches for higher throughput (e.g. bulk database inserts), while committing offset transactions for the entire batch.
4. **Decoupled Architecture:** Business logic and pipeline interceptors are completely decoupled from `sarama` types, allowing for trivial unit testing without mock producers or servers.

---

## Architecture & Causality Guarantee

When a message fails, preserving strict FIFO ordering per Group ID is critical. To achieve this, the system isolates failures and replays them safely.

### 1. The Causality Chain
A new message `M4` on the main topic can **never** be processed until the main consumer is notified that the group is clean. And the group can **never** be clean until the reprocessor has finished and committed the last message in the DLQ (`M3`).

```
[M3 (in DLQ)]  ====Must Finish First====>  [Unpoison State Update]  ====Must Propagate First====>  [M4 (in Main Topic) processed]
```

### 2. Message Life Cycle under Failure

```
                  +--------------------------------+
                  |  Message Batch (Main Topic)    |
                  +--------------------------------+
                                  |
                                  v
                      +------------------------+
                      | PoisonFilter (Enter)   |
                      +------------------------+
                       /                      \
             [Group Is Poisoned]         [Group Is Healthy]
                     /                          \
                    v                            v
         +--------------------+        +--------------------+
         |   To Failed List   |        |  Business Logic    |
         +--------------------+        +--------------------+
                    |                    /                \
                    |               [Success]           [Failure]
                    |                 /                    \
                    |                v                      v
                    |       +-----------------+     +-----------------+
                    |       | Outgoing Msgs   |     | Fail Message /  |
                    |       +-----------------+     | Poison Group    |
                    |                |              +-----------------+
                    \                |                      /
                     \_______________|_____________________/
                                     |
                                     v
                       +---------------------------+
                       |   PoisonFilter (Leave)    |
                       |  (Cascade-poison batch)   |
                       +---------------------------+
                                     |
                                     v
                       +---------------------------+
                       |   Transactional Commit    |
                       |  Write Outgoing & DLQ     |
                       |  & Compacted State & Offs |
                       +---------------------------+
```

---

## Configured Topics & Recommended Settings

This design relies on three Kafka topics. Below are the recommended production configurations to ensure durability, strict ordering, and high performance.

### 1. Main Input Topic
The source topic containing your incoming business message stream.
* **Partitioning Key:** Must be partitioned by **Business Group ID** (e.g. `user_id` or `tenant_id`). This guarantees that all messages for a specific group land in the same partition and are processed sequentially.

---

### 2. DLQ (Dead Letter Queue) Topic
Stores the full raw payload of failed and skipped messages.
* **`cleanup.policy`**: `delete`
  * *Reason:* Must preserve the exact arrival order and multiple failures of the same group.
* **`retention.ms`**: `-1` (infinite) or at least **`1209600000` to `2592000000`** (14 to 30 days).
  * *Reason:* Failed payloads must not expire before engineers can deploy bug fixes and trigger the reprocess worker.
* **Partitions**: **Must match the partition count of the Main Input Topic**.
* **Partitioning Key**: **Must use the same partitioning key (Business Group ID)**.
  * *Reason:* This ensures that messages in the DLQ land in partitions that map 1:1 with the main topic, keeping order-preserving DLQ consumption simple and predictable.
* **`min.insync.replicas`**: `2` (with a replication factor of `3`) to guarantee data durability.

---

### 3. Compacted State Topic
A key-value state store recording the poisoned status of business groups (`GroupID -> ErrorMessage` or empty string to unpoison).
* **`cleanup.policy`**: `compact`
  * *Reason:* Kafka will automatically delete older state updates for a group ID, keeping only the most recent status.
* **`retention.ms`**: `-1` (infinite).
  * *Reason:* Poisoned groups must remain poisoned indefinitely until resolved.
* **Partitions**: `1` (Recommended).
  * *Reason:* Having 1 partition guarantees total ordering of state updates and makes it trivial for background consumers to populate their local in-memory caches. If scaling is required, partition by **GroupID**.
* **`min.cleanable.dirty.ratio`**: `0.01` (1%)
  * *Reason:* Tells Kafka to compact the log quickly after writes, reducing disk usage and startup tail-reading times.
* **`segment.ms`**: `600000` (10 minutes) or lower.
  * *Reason:* Forces Kafka to roll log segments quickly, which triggers compaction much sooner.

### 4. Derived Topic Naming Conventions
Derived topics (DLQ and Compacted State) should be named relative to your existing main topic to maintain clarity and structure. 

The DLQ topic name is **automatically derived** per message as `<original_topic_name>-dlq`.

| Topic Role | Naming Pattern | Example (Given Main Topic: `prod.orders`) |
| :--- | :--- | :--- |
| **DLQ (Derived)** | `<main_topic_name>-dlq` | `prod.orders-dlq` |
| **Compacted State** | `<main_topic_name>.poison-state` | `prod.orders.poison-state` |

---

## Operations & Reprocessing Guide

When a bug or database issue is resolved, follow these steps to safely reprocess messages:

### Step 1: Run the Reprocessor
Start the `Reprocessor` consumer on the DLQ topic. While the reprocessor runs, the groups remain marked as poisoned in the state store. Any new messages arriving on the main topic for those groups will continue to be safely routed to the DLQ.

### Step 2: Unpoison the Groups
To start reprocessing a group, publish an unpoison tombstone to the compacted state topic (or call `store.Unpoison(groupID)`):
* **Key:** `<groupID>`
* **Value:** `""` (empty string)

### Step 3: Self-Cleaning Reprocess Loop
The reprocessor will see the group is now unpoisoned/healthy, consume its messages from the DLQ, and execute them. 
* **If it succeeds:** The reprocessor commits the business outputs and commits the DLQ offset.
* **If it fails again:** The reprocessor re-poisons the group, cycles the failed message back to the tail of the DLQ, and commits the offset.
* **For other groups:** If a message belongs to a group that is *still* poisoned (unrelated to the one you are fixing), the reprocessor automatically writes it back to the tail of the DLQ and commits the offset, avoiding head-of-line blocking.

### Step 4: Resume Normal Processing
Once the DLQ is fully drained for a group, the reprocessor finishes. Any new messages arriving on the main topic will find the group clean and process normally.

---

## Configuration & Usage Example

### 1. Define the Business Handler
```go
package main

import (
	"encoding/json"
	"github.com/nedcg/juzu"
	"github.com/nedcg/juzu/kafka"
)

type UserEvent struct {
	UserID string `json:"user_id"`
	Action string `json:"action"`
}

var ProcessUserEvent = juzu.Enter("ProcessUserEvent", func(e *juzu.Execution[*kafka.BatchContext]) error {
	for _, msg := range e.Data.Messages {
		var event UserEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			e.Data.Fail(msg, err)
			continue
		}

		// Perform business logic ...
		
		// Send output message transactionally
		e.Data.Send("user-activity-log", msg.Key, []byte("processed: "+event.Action))
	}
	return nil
})
```

### 2. Run the Consumer
```go
func main() {
	config := sarama.NewConfig()
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.ID = "my-transactional-producer"

	producer, _ := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	saramaProducer := saramax.NewSaramaProducer(producer)
	
	// Get Group ID extractor (optional, defaults to string(msg.Key))
	getGroupID := func(msg kafka.Message) string {
		var event UserEvent
		_ = json.Unmarshal(msg.Value, &event)
		return event.UserID
	}

	stateStore := kafka.NewMemoryStateStore() // Or CompactedTopicStore

	pipeline := []juzu.Interceptor[*kafka.BatchContext]{
		kafka.PoisonFilter(stateStore, getGroupID),
		ProcessUserEvent,
	}

	genericConsumer := kafka.NewBatchConsumer(kafka.BatchConsumerConfig{
		GroupID:         "my-group",
		Producer:        saramaProducer,
		Pipeline:        pipeline,
		StateTopic:      "my-compacted-state-topic",
		GetGroupID:      getGroupID,
		UseTransactions: true, // Use Transactions (EOS)
	})

	handler := saramax.NewConsumerGroupHandler(
		genericConsumer,
		100, // Max Batch Size
		50*time.Millisecond,
	)

	// Consume...
}
```
