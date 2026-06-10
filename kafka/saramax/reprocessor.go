package saramax

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/nedcg/juzu/kafka"
)

// Reprocessor implements sarama.ConsumerGroupHandler by delegating to a generic Reprocessor.
type Reprocessor struct {
	reprocessor  *kafka.Reprocessor
	maxBatchSize int
	maxBatchWait time.Duration
}

// NewReprocessor creates a new Reprocessor adapter.
func NewReprocessor(
	reprocessor *kafka.Reprocessor,
	maxBatchSize int,
	maxBatchWait time.Duration,
) *Reprocessor {
	if maxBatchSize <= 0 {
		maxBatchSize = 10
	}
	if maxBatchWait <= 0 {
		maxBatchWait = 100 * time.Millisecond
	}
	return &Reprocessor{
		reprocessor:  reprocessor,
		maxBatchSize: maxBatchSize,
		maxBatchWait: maxBatchWait,
	}
}

// Setup implements sarama.ConsumerGroupHandler.
func (r *Reprocessor) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler.
func (r *Reprocessor) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim buffers claims and routes them to the generic Reprocessor.
func (r *Reprocessor) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var batch []kafka.Message
	var rawMsgs []*sarama.ConsumerMessage

	ticker := time.NewTicker(r.maxBatchWait)
	defer ticker.Stop()

	sessAdapter := NewSaramaSession(session)

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				if len(batch) > 0 {
					r.processBatch(sessAdapter, batch, rawMsgs)
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

			if len(batch) >= r.maxBatchSize {
				r.processBatch(sessAdapter, batch, rawMsgs)
				batch = nil
				rawMsgs = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				r.processBatch(sessAdapter, batch, rawMsgs)
				batch = nil
				rawMsgs = nil
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

func (r *Reprocessor) processBatch(session *SaramaSession, batch []kafka.Message, rawMsgs []*sarama.ConsumerMessage) {
	maxOffsets := make(map[string]map[int32]int64)
	for _, msg := range rawMsgs {
		if _, ok := maxOffsets[msg.Topic]; !ok {
			maxOffsets[msg.Topic] = make(map[int32]int64)
		}
		if currentMax, ok := maxOffsets[msg.Topic][msg.Partition]; !ok || msg.Offset > currentMax {
			maxOffsets[msg.Topic][msg.Partition] = msg.Offset
		}
	}

	err := r.reprocessor.Process(session.Context(), session, batch, maxOffsets)
	if err != nil {
		log.Printf("Sarama reprocessor processing batch failed: %v. Restarting.", err)
		panic(err)
	}
}
