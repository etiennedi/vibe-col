// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/weaviate/sroar"
)

const (
	// DefaultNumStripes is the default number of stripes for the memtable
	// Reduced from 256 to 8 based on performance analysis which showed that
	// fewer stripes often perform better due to reduced lock management overhead.
	// See STRIPING_ANALYSIS.md for details.
	DefaultNumStripes = 8

	// DefaultMaxHeight is the default maximum height of the skip list
	DefaultMaxHeight = 24

	// DefaultProbability is the default probability of increasing the height of a node
	DefaultProbability = 0.25

	// DefaultBatchSize is the default batch size for flushing
	DefaultBatchSize = 10000
)

// Iterator is an interface for iterating over memtable entries
type Iterator interface {
	// Next advances the iterator to the next entry
	// Returns false if there are no more entries
	Next() bool

	// HasNext returns true if there are more entries to read
	HasNext() bool

	// Entry returns the current ID-value pair
	// Only valid after Next() returns true
	Entry() (uint64, int64)

	// EntryWithDeleted returns the current ID-value pair and whether it's deleted
	// Only valid after Next() returns true
	EntryWithDeleted() (uint64, int64, bool)

	// Close releases any resources used by the iterator
	Close()
}

// Memtable is an in-memory data structure that stores key-value pairs
// and supports efficient writes, reads, and aggregations
type Memtable interface {
	// Add adds a single ID-value pair
	Add(id uint64, value int64) error

	// BatchAdd adds multiple ID-value pairs
	BatchAdd(ids []uint64, values []int64) error

	// Delete marks an entry as deleted
	Delete(id uint64) bool

	// BatchDelete marks multiple entries as deleted
	BatchDelete(ids []uint64) int

	// Get returns the value for a specific ID (respects deletion)
	Get(id uint64) (int64, bool)

	// Scan returns key-value pairs within a range (skipping deleted entries)
	Scan(startID, endID uint64) ([]uint64, []int64)

	// ScanIterator returns an iterator for iterating over entries in a range
	// If both startID and endID are 0, it returns an iterator over all entries
	ScanIterator(startID, endID uint64) Iterator

	// Aggregate performs unfiltered aggregation operations
	// Returns: min ID, max ID, min value, max value, sum of values, count
	Aggregate() (uint64, uint64, int64, int64, int64, int)

	// FilteredAggregate performs aggregation on IDs present in the filter
	// Returns: min ID, max ID, min value, max value, sum of values, count
	FilteredAggregate(filter *sroar.Bitmap) (uint64, uint64, int64, int64, int64, int)

	// Flush writes the non-deleted contents to the specified path
	// Returns the number of entries written and any error
	Flush(path string) (uint64, error)

	// ActiveCount returns the number of non-deleted entries
	ActiveCount() int64

	// IsEmpty checks if the memtable has any non-deleted entries
	IsEmpty() bool
}

// skipNode represents a node in the skip list
type skipNode struct {
	key     uint64
	value   int64
	next    []*skipNode
	mu      sync.Mutex  // For node-level locking during updates
	deleted atomic.Bool // Atomic flag for logical deletion
}

// MemtableImpl is the implementation of the Memtable interface
// It uses a fully concurrent skip list that allows multiple writers
// to operate simultaneously without global locks
type MemtableImpl struct {
	skipList           *ConcurrentSkipList
	entryCount         atomic.Int64 // Total entries added
	logicalDeleteCount atomic.Int64 // Total entries logically deleted
}

// MemtableOptions defines configuration options for creating a memtable
type MemtableOptions struct {
	MaxLevel int   // Maximum level of the skip list
	Seed     int64 // Seed for the random number generator
}

// DefaultMemtableOptions returns the default options for creating a memtable
func DefaultMemtableOptions() *MemtableOptions {
	return &MemtableOptions{
		MaxLevel: 24,
		Seed:     0, // Will use time-based seed if 0
	}
}

// NewMemtable creates a new memtable instance
func NewMemtable(opts *MemtableOptions) Memtable {
	if opts == nil {
		opts = DefaultMemtableOptions()
	}

	return &MemtableImpl{
		skipList: NewConcurrentSkipList(),
	}
}

// Add adds a single ID-value pair to the memtable
// This implementation uses a skip list with fine-grained locking
// that allows multiple writers to operate simultaneously
func (m *MemtableImpl) Add(id uint64, value int64) error {
	m.skipList.Insert(id, value)
	m.entryCount.Add(1)
	return nil
}

// BatchAdd adds multiple ID-value pairs to the memtable
// Each insertion is fully concurrent
func (m *MemtableImpl) BatchAdd(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return nil // Nothing to do
	}

	// Process each key-value pair
	for i := 0; i < len(ids); i++ {
		m.skipList.Insert(ids[i], values[i])
	}

	// Update total count
	m.entryCount.Add(int64(len(ids)))
	return nil
}

// Delete marks an entry as deleted
func (m *MemtableImpl) Delete(id uint64) bool {
	// The skipList.Delete method handles the atomic compare-and-swap
	// to ensure thread safety
	if m.skipList.Delete(id) {
		m.logicalDeleteCount.Add(1)
		return true
	}
	return false
}

// BatchDelete marks multiple entries as deleted
func (m *MemtableImpl) BatchDelete(ids []uint64) int {
	if len(ids) == 0 {
		return 0
	}

	// Process each ID for deletion
	deletedCount := 0
	for _, id := range ids {
		if m.skipList.Delete(id) {
			deletedCount++
		}
	}

	// Update the logical delete count
	if deletedCount > 0 {
		m.logicalDeleteCount.Add(int64(deletedCount))
	}

	return deletedCount
}

// Get returns the value for a specific ID (respects deletion)
func (m *MemtableImpl) Get(id uint64) (int64, bool) {
	return m.skipList.Get(id)
}

// Scan returns key-value pairs within a range (skipping deleted entries)
func (m *MemtableImpl) Scan(startID, endID uint64) ([]uint64, []int64) {
	// Get an iterator for the range
	it := m.skipList.RangeIterator(startID, endID)

	// Collect non-deleted entries
	var ids []uint64
	var values []int64

	for it.Next() {
		if !it.IsDeleted() {
			ids = append(ids, it.Key())
			values = append(values, it.Value())
		}
	}

	return ids, values
}

// memtableIterator implements the Iterator interface for memtable entries
type memtableIterator struct {
	rangeIt *RangeIterator
	hasNext bool
}

// Next advances the iterator to the next entry
func (it *memtableIterator) Next() bool {
	// Skip deleted entries
	for {
		if !it.rangeIt.Next() {
			it.hasNext = false
			return false
		}

		if !it.rangeIt.IsDeleted() {
			it.hasNext = true
			return true
		}
	}
}

// HasNext returns whether there are more entries
func (it *memtableIterator) HasNext() bool {
	return it.hasNext
}

// Entry returns the current ID-value pair
func (it *memtableIterator) Entry() (uint64, int64) {
	return it.rangeIt.Key(), it.rangeIt.Value()
}

// EntryWithDeleted returns the current ID-value pair and deletion status
func (it *memtableIterator) EntryWithDeleted() (uint64, int64, bool) {
	return it.rangeIt.Key(), it.rangeIt.Value(), it.rangeIt.IsDeleted()
}

// Close releases any resources
func (it *memtableIterator) Close() {
	// No resources to release
}

// ScanIterator returns an iterator for iterating over entries in a range
func (m *MemtableImpl) ScanIterator(startID, endID uint64) Iterator {
	rangeIt := m.skipList.RangeIterator(startID, endID)
	return &memtableIterator{
		rangeIt: rangeIt,
		hasNext: true, // Optimistic assumption, will be corrected on first Next() call
	}
}

// Aggregate performs unfiltered aggregation operations
// Returns: min ID, max ID, min value, max value, sum of values, count
func (m *MemtableImpl) Aggregate() (uint64, uint64, int64, int64, int64, int) {
	// Use an iterator to scan the entire list
	it := m.skipList.Iterator()

	// Initialize result variables
	var minID, maxID uint64
	var minValue, maxValue, sum int64
	var count int
	var initialized bool

	// Process each non-deleted node
	for it.Next() {
		if !it.IsDeleted() {
			id := it.Key()
			value := it.Value()

			// Initialize on first entry
			if !initialized {
				minID = id
				maxID = id
				minValue = value
				maxValue = value
				initialized = true
			} else {
				// Update min/max IDs
				if id < minID {
					minID = id
				}
				if id > maxID {
					maxID = id
				}

				// Update min/max values
				if value < minValue {
					minValue = value
				}
				if value > maxValue {
					maxValue = value
				}
			}

			// Update sum and count
			sum += value
			count++
		}
	}

	return minID, maxID, minValue, maxValue, sum, count
}

// FilteredAggregate performs aggregation on IDs present in the filter
// Returns: min ID, max ID, min value, max value, sum of values, count
func (m *MemtableImpl) FilteredAggregate(filter *sroar.Bitmap) (uint64, uint64, int64, int64, int64, int) {
	// Use an iterator to scan the entire list
	it := m.skipList.Iterator()

	// Initialize result variables
	var minID, maxID uint64
	var minValue, maxValue, sum int64
	var count int
	var initialized bool

	// Process each non-deleted node that matches the filter
	for it.Next() {
		if !it.IsDeleted() {
			id := it.Key()

			// Check if ID is in the filter
			if filter.Contains(id) {
				value := it.Value()

				// Initialize on first entry
				if !initialized {
					minID = id
					maxID = id
					minValue = value
					maxValue = value
					initialized = true
				} else {
					// Update min/max IDs
					if id < minID {
						minID = id
					}
					if id > maxID {
						maxID = id
					}

					// Update min/max values
					if value < minValue {
						minValue = value
					}
					if value > maxValue {
						maxValue = value
					}
				}

				// Update sum and count
				sum += value
				count++
			}
		}
	}

	return minID, maxID, minValue, maxValue, sum, count
}

// ActiveCount returns the number of non-deleted entries
func (m *MemtableImpl) ActiveCount() int64 {
	return m.entryCount.Load() - m.logicalDeleteCount.Load()
}

// IsEmpty checks if the memtable has any non-deleted entries
func (m *MemtableImpl) IsEmpty() bool {
	return m.ActiveCount() == 0
}
