package saramax

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	"github.com/nedcg/uflow/kafka"
)

// CompactedTopicStore implements kafka.GroupStateStore, backing the in-memory cache with a Kafka compacted topic.
type CompactedTopicStore struct {
	*kafka.MemoryStateStore
	producer   sarama.SyncProducer
	stateTopic string
}

// NewCompactedTopicStore creates a new CompactedTopicStore.
func NewCompactedTopicStore(producer sarama.SyncProducer, stateTopic string) *CompactedTopicStore {
	return &CompactedTopicStore{
		MemoryStateStore: kafka.NewMemoryStateStore(),
		producer:         producer,
		stateTopic:       stateTopic,
	}
}

// MarkPoisoned publishes the poisoned status to Kafka and updates the local cache.
func (s *CompactedTopicStore) MarkPoisoned(groupID string, err error) error {
	_, _, writeErr := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: s.stateTopic,
		Key:   sarama.StringEncoder(groupID),
		Value: sarama.StringEncoder(err.Error()),
	})
	if writeErr != nil {
		return writeErr
	}

	// Update local cache immediately for intra-batch ordering safety
	s.MemoryStateStore.MarkPoisoned(groupID, err)
	return nil
}

// Unpoison publishes a tombstone/clear message to Kafka and updates the local cache.
func (s *CompactedTopicStore) Unpoison(groupID string) error {
	_, _, writeErr := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: s.stateTopic,
		Key:   sarama.StringEncoder(groupID),
		Value: sarama.StringEncoder(""), // Empty string represents a clean/tombstone state
	})
	if writeErr != nil {
		return writeErr
	}

	s.MemoryStateStore.Unpoison(groupID)
	return nil
}

// StartReader runs a background consumer to stream compaction state updates and populate the local cache.
func (s *CompactedTopicStore) StartReader(ctx context.Context, consumerClient sarama.Consumer) error {
	partitions, err := consumerClient.Partitions(s.stateTopic)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		pc, err := consumerClient.ConsumePartition(s.stateTopic, partition, sarama.OffsetOldest)
		if err != nil {
			return err
		}

		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					key := string(msg.Key)
					val := string(msg.Value)
					if val == "" {
						_ = s.MemoryStateStore.Unpoison(key)
					} else {
						_ = s.MemoryStateStore.MarkPoisoned(key, errors.New(val))
					}
				case <-ctx.Done():
					return
				}
			}
		}(pc)
	}

	return nil
}
