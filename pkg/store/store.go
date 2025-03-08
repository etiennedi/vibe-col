package store

import (
	"errors"
	"sync"
)

// MemTable represents an in-memory key-value store
type MemTable struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewMemTable creates a new MemTable
func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string][]byte),
	}
}

// Set stores a key-value pair in the MemTable
func (m *MemTable) Set(key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.data[key] = value
	return nil
}

// Get retrieves a value for the given key from the MemTable
func (m *MemTable) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	value, ok := m.data[key]
	if !ok {
		return nil, errors.New("key not found")
	}
	
	return value, nil
}

// Delete removes a key from the MemTable
func (m *MemTable) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	delete(m.data, key)
	return nil
}