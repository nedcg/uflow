package kafka

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/nedcg/juzu"
)

// Reprocessor runs as a consumer group on the DLQ topic to safely reprocess messages.
type Reprocessor struct {
	groupID      string
	dlqTopic     string
	stateStore   GroupStateStore
	getGroupID   func(Message) string
	producer     sarama.SyncProducer
	pipeline     []juzu.Interceptor[*BatchContext]
	maxBatchSize int
	maxBatchWait time.Duration
}

// NewReprocessor creates a new Reprocessor instance.
func NewReprocessor(
	groupID string,
	dlqTopic string,
	stateStore GroupStateStore,
	getGroupID func(Message) string,
	producer sarama.SyncProducer,
	pipeline []juzu.Interceptor[*BatchContext],
	maxBatchSize int,
	maxBatchWait time.Duration,
) *Reprocessor {
	if maxBatchSize <= 0 {
		maxBatchSize = 10 // Reprocessing typically runs in smaller batches
	}
	if maxBatchWait <= 0 {
		maxBatchWait = 100 * time.Millisecond
	}
	return &Reprocessor{
		groupID:      groupID,
		dlqTopic:     dlqTopic,
		stateStore:   stateStore,
		getGroupID:   getGroupID,
		producer:     producer,
		pipeline:     pipeline,
		maxBatchSize: maxBatchSize,
		maxBatchWait: maxBatchWait,
	}
}

// Setup implements sarama.ConsumerGroupHandler.
func (r *Reprocessor) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler.
func (r *Reprocessor) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim reads messages from the DLQ and processes them or cycles them back to the tail.
func (r *Reprocessor) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var batch []Message
	var rawMsgs []*sarama.ConsumerMessage

	ticker := time.NewTicker(r.maxBatchWait)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				if len(batch) > 0 {
					r.processDLQBatch(session, batch, rawMsgs)
				}
				return nil
			}

			// Map to domain message
			headers := make(map[string][]byte)
			for _, hdr := range msg.Headers {
				headers[string(hdr.Key)] = hdr.Value
			}

			domainMsg := Message{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       msg.Key,
				Value:     msg.Value,
				Headers:   headers,
				Timestamp: msg.Timestamp,
			}

			batch = append(batch, domainMsg)
			rawMsgs = append(rawMsgs, msg)

			if len(batch) >= r.maxBatchSize {
				r.processDLQBatch(session, batch, rawMsgs)
				batch = nil
				rawMsgs = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				r.processDLQBatch(session, batch, rawMsgs)
				batch = nil
				rawMsgs = nil
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

func (r *Reprocessor) processDLQBatch(session sarama.ConsumerGroupSession, batch []Message, rawMsgs []*sarama.ConsumerMessage) {
	ctx := &BatchContext{}
	var cycleMsgs []Message

	// 1. Separate messages belonging to poisoned groups from healthy groups
	for _, msg := range batch {
		groupID := r.getGroupID(msg)
		if r.stateStore.IsPoisoned(groupID) {
			// Group is still poisoned; cycle the message back to the tail of the DLQ
			cycleMsgs = append(cycleMsgs, msg)
		} else {
			// Group has been unpoisoned; add to active batch for reprocessing
			ctx.Messages = append(ctx.Messages, msg)
		}
	}

	// 2. Process healthy messages through the pipeline
	var err error
	if len(ctx.Messages) > 0 {
		exec := juzu.NewExecution(session.Context(), r.pipeline, ctx)
		err = exec.Execute()
	}

	if err != nil {
		// Batch failed with unrecovered error (e.g. database still offline).
		// We do not commit offsets and panic to retry.
		log.Printf("DLQ reprocessing batch execution failed: %v", err)
		panic(err)
	}

	// 3. Atomically write changes (outgoing messages, DLQ cycles, state updates, offsets)
	maxOffsets := make(map[string]map[int32]int64)
	for _, msg := range rawMsgs {
		if _, ok := maxOffsets[msg.Topic]; !ok {
			maxOffsets[msg.Topic] = make(map[int32]int64)
		}
		if currentMax, ok := maxOffsets[msg.Topic][msg.Partition]; !ok || msg.Offset > currentMax {
			maxOffsets[msg.Topic][msg.Partition] = msg.Offset
		}
	}

	if err := r.commitReprocessing(ctx, cycleMsgs, maxOffsets); err != nil {
		log.Printf("Failed to commit reprocessing batch: %v", err)
		panic(err)
	}

	// 4. Mark consumer offsets on the DLQ topic
	for topic, partitions := range maxOffsets {
		for partition, offset := range partitions {
			session.MarkOffset(topic, partition, offset+1, "")
		}
	}
}

func (r *Reprocessor) commitReprocessing(ctx *BatchContext, cycleMsgs []Message, maxOffsets map[string]map[int32]int64) error {
	// Start transaction on producer
	if err := r.producer.BeginTxn(); err != nil {
		return err
	}

	var saramaMsgs []*sarama.ProducerMessage

	// 1. Send new outgoing business messages from successful runs
	for _, m := range ctx.OutgoingMessages {
		saramaMsgs = append(saramaMsgs, &sarama.ProducerMessage{
			Topic: m.Topic,
			Key:   sarama.ByteEncoder(m.Key),
			Value: sarama.ByteEncoder(m.Value),
		})
	}

	// 2. Re-enqueue (cycle) messages for groups that are still poisoned
	for _, msg := range cycleMsgs {
		saramaMsgs = append(saramaMsgs, &sarama.ProducerMessage{
			Topic:   r.dlqTopic,
			Key:     sarama.ByteEncoder(msg.Key),
			Value:   sarama.ByteEncoder(msg.Value),
			Headers: mapToSaramaHeaders(msg.Headers),
		})
	}

	// 3. Handle messages that failed processing *again*
	for _, failed := range ctx.FailedMessages {
		groupID := r.getGroupID(failed.Message)
		
		// Poison the group again in the state store
		_ = r.stateStore.MarkPoisoned(groupID, failed.Error)

		// Re-enqueue to DLQ tail with the new error header
		headers := mapToSaramaHeaders(failed.Message.Headers)
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("x-reprocess-error"),
			Value: []byte(failed.Error.Error()),
		})

		saramaMsgs = append(saramaMsgs, &sarama.ProducerMessage{
			Topic:   r.dlqTopic,
			Key:     sarama.ByteEncoder(failed.Message.Key),
			Value:   sarama.ByteEncoder(failed.Message.Value),
			Headers: headers,
		})
	}

	// Send everything to Kafka
	if len(saramaMsgs) > 0 {
		if err := r.producer.SendMessages(saramaMsgs); err != nil {
			_ = r.producer.AbortTxn()
			return err
		}
	}

	// Commit DLQ offsets inside the transaction
	txOffsets := make(map[string][]*sarama.PartitionOffsetMetadata)
	for topic, partitions := range maxOffsets {
		for partition, offset := range partitions {
			txOffsets[topic] = append(txOffsets[topic], &sarama.PartitionOffsetMetadata{
				Partition: partition,
				Offset:    offset + 1,
			})
		}
	}

	if err := r.producer.AddOffsetsToTxn(txOffsets, r.groupID); err != nil {
		_ = r.producer.AbortTxn()
		return err
	}

	if err := r.producer.CommitTxn(); err != nil {
		_ = r.producer.AbortTxn()
		return err
	}

	return nil
}

func mapToSaramaHeaders(h map[string][]byte) []sarama.RecordHeader {
	var headers []sarama.RecordHeader
	for k, v := range h {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: v,
		})
	}
	return headers
}
