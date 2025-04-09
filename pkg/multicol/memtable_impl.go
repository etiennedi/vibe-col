package multicol

import (
	"fmt"
	"sync"
	"sync/atomic"

	"vibe-lsm/pkg/col"

	"github.com/weaviate/sroar"
)

// MemtableImpl is the implementation of the Memtable interface
// that uses a sync.Map internally for thread safety
type MemtableImpl struct {
	data       sync.Map
	addCount   atomic.Int64
	delCount   atomic.Int64
	wal        WALManager
	walEnabled bool
}

// DurableMemtable extends the Memtable interface to add WAL support
type DurableMemtable interface {
	Memtable

	// EnableWAL enables WAL for durability at the specified path
	EnableWAL(path string) error

	// Sync ensures all operations are durably persisted
	Sync() error

	// DisableWAL disables the WAL
	DisableWAL() error
}

// MemtableOptions defines configuration options for the memtable
type MemtableOptions struct {
	// Reserved for future use
	WalPath       string // Path to the WAL file (empty disables WAL)
	WalBufferSize int    // Buffer size for the WAL (0 means default)
}

// DefaultMemtableOptions returns default options for the memtable
func DefaultMemtableOptions() *MemtableOptions {
	return &MemtableOptions{
		WalPath:       "", // WAL disabled by default
		WalBufferSize: 0,  // Use default buffer size
	}
}

// NewMemtable creates a new memtable instance
func NewMemtable(opts *MemtableOptions) Memtable {
	mt := &MemtableImpl{}

	// If options are provided and WAL path is set, enable WAL
	if opts != nil && opts.WalPath != "" {
		// Ignore errors in constructor - caller should check Memtable type
		// and call EnableWAL explicitly if WAL is required
		_ = mt.EnableWAL(opts.WalPath)
	}

	return mt
}

// NewDurableMemtable creates a new memtable with WAL support
func NewDurableMemtable(opts *MemtableOptions) (DurableMemtable, error) {
	mt := &MemtableImpl{}

	// If options are provided and WAL path is set, enable WAL
	if opts != nil && opts.WalPath != "" {
		if err := mt.EnableWAL(opts.WalPath); err != nil {
			return nil, err
		}
	}

	return mt, nil
}

// EnableWAL enables the write-ahead log for durability
func (m *MemtableImpl) EnableWAL(path string) error {
	// Create a new WAL
	wal, err := NewWAL(path, 0) // Use default buffer size
	if err != nil {
		return fmt.Errorf("failed to create WAL: %w", err)
	}

	// Recover from the WAL if it exists
	if err := wal.Recover(m); err != nil {
		return fmt.Errorf("failed to recover from WAL: %w", err)
	}

	// Set the WAL and mark as enabled
	m.wal = wal
	m.walEnabled = true

	return nil
}

// Sync ensures all operations are durably persisted
func (m *MemtableImpl) Sync() error {
	if !m.walEnabled || m.wal == nil {
		return fmt.Errorf("WAL not enabled")
	}

	return m.wal.Sync()
}

// DisableWAL disables the WAL
func (m *MemtableImpl) DisableWAL() error {
	if !m.walEnabled || m.wal == nil {
		return nil // Nothing to do
	}

	// Close the WAL
	var closeErr error
	if err := m.wal.Close(); err != nil {
		closeErr = fmt.Errorf("failed to close WAL: %w", err)
	}

	// Clear WAL and mark as disabled regardless of close error
	// This makes the method idempotent and avoids resource leaks
	m.wal = nil
	m.walEnabled = false

	return closeErr
}

// Add adds a single ID-value pair
func (m *MemtableImpl) Add(id uint64, value int64) error {
	// If WAL is enabled, log the operation first
	if m.walEnabled && m.wal != nil {
		if err := m.wal.LogAdd(id, value); err != nil {
			return fmt.Errorf("failed to log add operation: %w", err)
		}
	}

	// Add to in-memory map
	m.data.Store(id, value)
	m.addCount.Add(1)
	return nil
}

// BatchAdd adds multiple ID-value pairs
func (m *MemtableImpl) BatchAdd(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	// If WAL is enabled, log the batch operation first
	if m.walEnabled && m.wal != nil {
		if err := m.wal.LogBatchAdd(ids, values); err != nil {
			return fmt.Errorf("failed to log batch add operation: %w", err)
		}
	}

	for i := 0; i < len(ids); i++ {
		m.data.Store(ids[i], values[i])
	}

	m.addCount.Add(int64(len(ids)))
	return nil
}

// Delete marks an entry as deleted
func (m *MemtableImpl) Delete(id uint64) bool {
	// Check if the entry exists first
	_, exists := m.data.Load(id)
	if !exists {
		return false
	}

	// If WAL is enabled, log the delete operation
	if m.walEnabled && m.wal != nil {
		if err := m.wal.LogDelete(id); err != nil {
			// Log the error but continue with the operation
			fmt.Printf("failed to log delete operation: %v\n", err)
		}
	}

	m.data.Delete(id)
	m.delCount.Add(1)
	return true
}

// BatchDelete marks multiple entries as deleted
func (m *MemtableImpl) BatchDelete(ids []uint64) int {
	// For batch operations, we first collect which IDs actually exist
	var existingIds []uint64
	for _, id := range ids {
		if _, exists := m.data.Load(id); exists {
			existingIds = append(existingIds, id)
		}
	}

	// If none exist, return early
	if len(existingIds) == 0 {
		return 0
	}

	// If WAL is enabled, log the batch delete operation
	if m.walEnabled && m.wal != nil {
		if err := m.wal.LogBatchDelete(existingIds); err != nil {
			// Log the error but continue with the operation
			fmt.Printf("failed to log batch delete operation: %v\n", err)
		}
	}

	// Apply the deletes
	for _, id := range existingIds {
		m.data.Delete(id)
	}

	m.delCount.Add(int64(len(existingIds)))
	return len(existingIds)
}

// Get returns the value for a specific ID
func (m *MemtableImpl) Get(id uint64) (int64, bool) {
	if value, ok := m.data.Load(id); ok {
		return value.(int64), true
	}
	return 0, false
}

// Scan returns key-value pairs within a range
func (m *MemtableImpl) Scan(startID, endID uint64) ([]uint64, []int64) {
	var ids []uint64
	var values []int64

	m.data.Range(func(key, value interface{}) bool {
		id := key.(uint64)
		if id >= startID && id <= endID {
			ids = append(ids, id)
			values = append(values, value.(int64))
		}
		return true
	})

	return ids, values
}

// memtableIterator implements the Iterator interface for memtable entries
type memtableIterator struct {
	keys    []uint64
	values  []int64
	pos     int
	hasNext bool
}

// Next advances the iterator to the next entry
func (it *memtableIterator) Next() bool {
	it.pos++
	if it.pos < len(it.keys) {
		it.hasNext = true
		return true
	}
	it.hasNext = false
	return false
}

// HasNext returns true if there are more entries
func (it *memtableIterator) HasNext() bool {
	return it.hasNext
}

// Entry returns the current entry
func (it *memtableIterator) Entry() (uint64, int64) {
	return it.keys[it.pos], it.values[it.pos]
}

// EntryWithDeleted returns the current entry and deleted status
func (it *memtableIterator) EntryWithDeleted() (uint64, int64, bool) {
	return it.keys[it.pos], it.values[it.pos], false
}

// Close does nothing for this implementation
func (it *memtableIterator) Close() {
	// Nothing to do
}

// ScanIterator returns an iterator for traversing entries
func (m *MemtableImpl) ScanIterator(startID, endID uint64) Iterator {
	ids, values := m.Scan(startID, endID)
	return &memtableIterator{
		keys:    ids,
		values:  values,
		pos:     -1, // Start before first element
		hasNext: len(ids) > 0,
	}
}

// Aggregate returns statistics about the entries
func (m *MemtableImpl) Aggregate() (uint64, uint64, int64, int64, int64, int) {
	var minID uint64 = ^uint64(0)
	var maxID uint64 = 0
	var minValue int64 = 1<<63 - 1
	var maxValue int64 = -1 << 63
	var sum int64 = 0
	count := 0

	m.data.Range(func(key, value interface{}) bool {
		id := key.(uint64)
		val := value.(int64)

		if id < minID {
			minID = id
		}
		if id > maxID {
			maxID = id
		}
		if val < minValue {
			minValue = val
		}
		if val > maxValue {
			maxValue = val
		}

		sum += val
		count++

		return true
	})

	if count == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	return minID, maxID, minValue, maxValue, sum, count
}

// FilteredAggregate applies a filter to the aggregation
func (m *MemtableImpl) FilteredAggregate(filter *sroar.Bitmap) (uint64, uint64, int64, int64, int64, int) {
	var minID uint64 = ^uint64(0)
	var maxID uint64 = 0
	var minValue int64 = 1<<63 - 1
	var maxValue int64 = -1 << 63
	var sum int64 = 0
	count := 0

	m.data.Range(func(key, value interface{}) bool {
		id := key.(uint64)

		if !filter.Contains(id) {
			return true
		}

		val := value.(int64)

		if id < minID {
			minID = id
		}
		if id > maxID {
			maxID = id
		}
		if val < minValue {
			minValue = val
		}
		if val > maxValue {
			maxValue = val
		}

		sum += val
		count++

		return true
	})

	if count == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	return minID, maxID, minValue, maxValue, sum, count
}

// ActiveCount returns the number of active entries
func (m *MemtableImpl) ActiveCount() int64 {
	var count int64 = 0
	m.data.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// IsEmpty returns true if the memtable is empty
func (m *MemtableImpl) IsEmpty() bool {
	empty := true
	m.data.Range(func(key, value interface{}) bool {
		empty = false
		return false // Stop ranging once we find one entry
	})
	return empty
}

// Flush writes the non-deleted contents to the specified path
// Returns the number of entries written and any error
func (m *MemtableImpl) Flush(path string) (uint64, error) {
	// Create a BufferedWriter with default options
	writer, err := col.NewBufferedWriter(path)
	if err != nil {
		return 0, err
	}
	defer writer.Close()

	writeCount := uint64(0)

	// Iterate through all entries and add them to the writer
	m.data.Range(func(key, value interface{}) bool {
		id := key.(uint64)
		val := value.(int64)

		if err := writer.Add(id, val); err != nil {
			// Capture error but continue ranging
			err = fmt.Errorf("failed to write entry ID %d: %w", id, err)
			return false
		}

		writeCount++
		return true
	})

	if err != nil {
		return 0, err
	}

	return writeCount, nil
}

// Close implements the AggregateSource interface
func (m *MemtableImpl) Close() error {
	// If WAL is enabled, close it
	if m.walEnabled && m.wal != nil {
		return m.DisableWAL()
	}
	return nil
}

// AggregateWithOptions implements the AggregateSource interface
func (m *MemtableImpl) AggregateWithOptions(opts col.AggregateOptions) col.AggregateResult {
	// If memtable is empty, return zeros
	if m.IsEmpty() {
		return col.AggregateResult{
			Count: 0,
			Min:   0,
			Max:   0,
			Sum:   0,
			Avg:   0,
		}
	}

	// Use existing FilteredAggregate for filtered aggregation
	if opts.Filter != nil || opts.DenyFilter != nil {
		// Get the effective filter by combining Filter and DenyFilter
		var effectiveFilter *sroar.Bitmap
		if opts.Filter != nil {
			effectiveFilter = opts.Filter.Clone()
			if opts.DenyFilter != nil {
				// Remove denied IDs from the filter
				effectiveFilter = effectiveFilter.AndNot(opts.DenyFilter)
			}
		} else if opts.DenyFilter != nil {
			// Create a bitmap of all IDs and remove denied ones
			effectiveFilter = sroar.NewBitmap()
			m.data.Range(func(key, _ interface{}) bool {
				id := key.(uint64)
				if !opts.DenyFilter.Contains(id) {
					effectiveFilter.Set(id)
				}
				return true
			})
		}

		_, _, minValue, maxValue, sum, count := m.FilteredAggregate(effectiveFilter)

		result := col.AggregateResult{
			Count: count,
			Min:   minValue,
			Max:   maxValue,
			Sum:   sum,
		}

		if count > 0 {
			result.Avg = float64(sum) / float64(count)
		}

		return result
	}

	// Use existing Aggregate for unfiltered aggregation
	_, _, minValue, maxValue, sum, count := m.Aggregate()

	result := col.AggregateResult{
		Count: count,
		Min:   minValue,
		Max:   maxValue,
		Sum:   sum,
	}

	if count > 0 {
		result.Avg = float64(sum) / float64(count)
	}

	return result
}

// GetGlobalIDBitmap implements the AggregateSource interface
func (m *MemtableImpl) GetGlobalIDBitmap() (*sroar.Bitmap, error) {
	bitmap := sroar.NewBitmap()

	// Add all IDs to the bitmap
	m.data.Range(func(key, _ interface{}) bool {
		id := key.(uint64)
		bitmap.Set(id)
		return true
	})

	return bitmap, nil
}
