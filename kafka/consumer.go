package kafka

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/nedcg/juzu"
)

// ConsumerGroupHandler wraps Sarama ConsumerGroupHandler to manage batching and transactions.
type ConsumerGroupHandler struct {
	groupID         string
	producer        sarama.SyncProducer
	pipeline        []juzu.Interceptor[*BatchContext]
	dlqTopic        string
	stateTopic      string
	getGroupID      func(Message) string
	useTransactions bool
	maxBatchSize    int
	maxBatchWait    time.Duration
}

// NewConsumerGroupHandler creates a new ConsumerGroupHandler.
func NewConsumerGroupHandler(
	groupID string,
	producer sarama.SyncProducer,
	pipeline []juzu.Interceptor[*BatchContext],
	dlqTopic string,
	stateTopic string,
	getGroupID func(Message) string,
	useTransactions bool,
	maxBatchSize int,
	maxBatchWait time.Duration,
) *ConsumerGroupHandler {
	if maxBatchSize <= 0 {
		maxBatchSize = 100
	}
	if maxBatchWait <= 0 {
		maxBatchWait = 50 * time.Millisecond
	}
	return &ConsumerGroupHandler{
		groupID:         groupID,
		producer:        producer,
		pipeline:        pipeline,
		dlqTopic:        dlqTopic,
		stateTopic:      stateTopic,
		getGroupID:      getGroupID,
		useTransactions: useTransactions,
		maxBatchSize:    maxBatchSize,
		maxBatchWait:    maxBatchWait,
	}
}

// Setup is run at the beginning of a new session, before Messages are consumed.
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages.
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var batch []Message
	var rawMsgs []*sarama.ConsumerMessage

	ticker := time.NewTicker(h.maxBatchWait)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// Claim channel closed, flush any remaining messages in the buffer
				if len(batch) > 0 {
					h.processBatch(session, batch, rawMsgs)
				}
				return nil
			}

			// Map Sarama headers
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

			if len(batch) >= h.maxBatchSize {
				h.processBatch(session, batch, rawMsgs)
				batch = nil
				rawMsgs = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				h.processBatch(session, batch, rawMsgs)
				batch = nil
				rawMsgs = nil
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *ConsumerGroupHandler) processBatch(session sarama.ConsumerGroupSession, batch []Message, rawMsgs []*sarama.ConsumerMessage) {
	ctx := &BatchContext{
		Messages: batch,
	}

	exec := juzu.NewExecution(session.Context(), h.pipeline, ctx)
	err := exec.Execute()

	if err != nil {
		// Whole batch execution failed with an unrecovered error (e.g. database offline).
		// We do not commit offsets and panic to restart the consumer container.
		log.Printf("Batch execution failed with unrecovered error: %v. Restarting consumer.", err)
		panic(err)
	}

	// Calculate highest offset consumed per partition in this batch
	maxOffsets := make(map[string]map[int32]int64)
	for _, msg := range rawMsgs {
		if _, ok := maxOffsets[msg.Topic]; !ok {
			maxOffsets[msg.Topic] = make(map[int32]int64)
		}
		if currentMax, ok := maxOffsets[msg.Topic][msg.Partition]; !ok || msg.Offset > currentMax {
			maxOffsets[msg.Topic][msg.Partition] = msg.Offset
		}
	}

	if h.useTransactions && h.producer != nil {
		if err := h.commitTransaction(ctx, maxOffsets); err != nil {
			log.Printf("Transactional commit failed: %v. Restarting consumer.", err)
			panic(err)
		}
	} else {
		h.commitNonTransactional(ctx, session, maxOffsets)
	}
}

func (h *ConsumerGroupHandler) buildProducerMessages(ctx *BatchContext) []*sarama.ProducerMessage {
	var saramaMsgs []*sarama.ProducerMessage

	// 1. Add outgoing business messages
	for _, m := range ctx.OutgoingMessages {
		saramaMsgs = append(saramaMsgs, &sarama.ProducerMessage{
			Topic: m.Topic,
			Key:   sarama.ByteEncoder(m.Key),
			Value: sarama.ByteEncoder(m.Value),
		})
	}

	// 2. Add DLQ and state updates for failed/poisoned messages
	for _, failed := range ctx.FailedMessages {
		// Send to DLQ if configured
		if h.dlqTopic != "" {
			headers := []sarama.RecordHeader{
				{Key: []byte("x-original-topic"), Value: []byte(failed.Message.Topic)},
				{Key: []byte("x-original-partition"), Value: []byte(fmt.Sprintf("%d", failed.Message.Partition))},
				{Key: []byte("x-original-offset"), Value: []byte(fmt.Sprintf("%d", failed.Message.Offset))},
				{Key: []byte("x-error"), Value: []byte(failed.Error.Error())},
			}

			saramaMsgs = append(saramaMsgs, &sarama.ProducerMessage{
				Topic:   h.dlqTopic,
				Key:     sarama.ByteEncoder(failed.Message.Key),
				Value:   sarama.ByteEncoder(failed.Message.Value),
				Headers: headers,
			})
		}

		// Update compacted state topic if configured
		if h.stateTopic != "" && h.getGroupID != nil {
			groupID := h.getGroupID(failed.Message)
			saramaMsgs = append(saramaMsgs, &sarama.ProducerMessage{
				Topic: h.stateTopic,
				Key:   sarama.StringEncoder(groupID),
				Value: sarama.StringEncoder(failed.Error.Error()),
			})
		}
	}

	return saramaMsgs
}

func (h *ConsumerGroupHandler) commitTransaction(ctx *BatchContext, maxOffsets map[string]map[int32]int64) error {
	if err := h.producer.BeginTxn(); err != nil {
		return err
	}

	saramaMsgs := h.buildProducerMessages(ctx)

	if len(saramaMsgs) > 0 {
		if err := h.producer.SendMessages(saramaMsgs); err != nil {
			_ = h.producer.AbortTxn()
			return err
		}
	}

	// Add group offsets to transaction
	txOffsets := make(map[string][]*sarama.PartitionOffsetMetadata)
	for topic, partitions := range maxOffsets {
		for partition, offset := range partitions {
			txOffsets[topic] = append(txOffsets[topic], &sarama.PartitionOffsetMetadata{
				Partition: partition,
				Offset:    offset + 1, // Commit next offset to consume
			})
		}
	}

	if err := h.producer.AddOffsetsToTxn(txOffsets, h.groupID); err != nil {
		_ = h.producer.AbortTxn()
		return err
	}

	if err := h.producer.CommitTxn(); err != nil {
		_ = h.producer.AbortTxn()
		return err
	}

	return nil
}

func (h *ConsumerGroupHandler) commitNonTransactional(ctx *BatchContext, session sarama.ConsumerGroupSession, maxOffsets map[string]map[int32]int64) {
	saramaMsgs := h.buildProducerMessages(ctx)

	if len(saramaMsgs) > 0 && h.producer != nil {
		if err := h.producer.SendMessages(saramaMsgs); err != nil {
			log.Printf("Failed to produce outgoing/DLQ messages: %v", err)
		}
	}

	// Mark offsets directly on consumer group session
	for topic, partitions := range maxOffsets {
		for partition, offset := range partitions {
			session.MarkOffset(topic, partition, offset+1, "")
		}
	}
}
