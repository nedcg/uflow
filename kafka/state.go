package kafka

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
)

// GroupStateStore defines the interface to check and modify the poisoned status of business groups.
type GroupStateStore interface {
	IsPoisoned(groupID string) bool
	MarkPoisoned(groupID string, err error) error
	Unpoison(groupID string) error
}

// MemoryStateStore is a thread-safe in-memory implementation of GroupStateStore.
type MemoryStateStore struct {
	mu     sync.RWMutex
	states map[string]string // groupID -> errorMsg
}

// NewMemoryStateStore creates a new MemoryStateStore.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		states: make(map[string]string),
	}
}

// IsPoisoned returns true if the group is poisoned.
func (s *MemoryStateStore) IsPoisoned(groupID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	errStr, ok := s.states[groupID]
	return ok && errStr != ""
}

// MarkPoisoned registers a group as poisoned with the associated error.
func (s *MemoryStateStore) MarkPoisoned(groupID string, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[groupID] = err.Error()
	return nil
}

// Unpoison clears the poisoned status of a group.
func (s *MemoryStateStore) Unpoison(groupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.states, groupID)
	return nil
}

// CompactedTopicStore implements GroupStateStore, backing the in-memory cache with a Kafka compacted topic.
type CompactedTopicStore struct {
	*MemoryStateStore
	producer   sarama.SyncProducer
	stateTopic string
}

// NewCompactedTopicStore creates a new CompactedTopicStore.
func NewCompactedTopicStore(producer sarama.SyncProducer, stateTopic string) *CompactedTopicStore {
	return &CompactedTopicStore{
		MemoryStateStore: NewMemoryStateStore(),
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
						s.MemoryStateStore.Unpoison(key)
					} else {
						// Lock memory state store map writes
						s.MemoryStateStore.mu.Lock()
						s.MemoryStateStore.states[key] = val
						s.MemoryStateStore.mu.Unlock()
					}
				case <-ctx.Done():
					return
				}
			}
		}(pc)
	}

	return nil
}
