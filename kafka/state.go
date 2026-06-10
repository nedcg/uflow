package kafka

import (
	"sync"
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
