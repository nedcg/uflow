package saramax

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/nedcg/juzu/kafka"
)

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler by delegating to a generic BatchConsumer.
type ConsumerGroupHandler struct {
	consumer     *kafka.BatchConsumer
	maxBatchSize int
	maxBatchWait time.Duration
}

// NewConsumerGroupHandler creates a new ConsumerGroupHandler adapter.
func NewConsumerGroupHandler(
	consumer *kafka.BatchConsumer,
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
		consumer:     consumer,
		maxBatchSize: maxBatchSize,
		maxBatchWait: maxBatchWait,
	}
}

// Setup implements sarama.ConsumerGroupHandler.
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler.
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim buffers claims and routes them to the BatchConsumer.
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var batch []kafka.Message
	var rawMsgs []*sarama.ConsumerMessage

	ticker := time.NewTicker(h.maxBatchWait)
	defer ticker.Stop()

	sessAdapter := NewSaramaSession(session)

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				if len(batch) > 0 {
					h.processBatch(sessAdapter, batch, rawMsgs)
				}
				return nil
			}

			headers := make(map[string][]byte)
			for _, hdr := range msg.Headers {
				headers[string(hdr.Key)] = hdr.Value
			}

			domainMsg := kafka.Message{
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
				h.processBatch(sessAdapter, batch, rawMsgs)
				batch = nil
				rawMsgs = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				h.processBatch(sessAdapter, batch, rawMsgs)
				batch = nil
				rawMsgs = nil
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *ConsumerGroupHandler) processBatch(session *SaramaSession, batch []kafka.Message, rawMsgs []*sarama.ConsumerMessage) {
	maxOffsets := make(map[string]map[int32]int64)
	for _, msg := range rawMsgs {
		if _, ok := maxOffsets[msg.Topic]; !ok {
			maxOffsets[msg.Topic] = make(map[int32]int64)
		}
		if currentMax, ok := maxOffsets[msg.Topic][msg.Partition]; !ok || msg.Offset > currentMax {
			maxOffsets[msg.Topic][msg.Partition] = msg.Offset
		}
	}

	err := h.consumer.Process(session.Context(), session, batch, maxOffsets)
	if err != nil {
		log.Printf("Sarama consumer processing batch failed: %v. Restarting.", err)
		panic(err)
	}
}
