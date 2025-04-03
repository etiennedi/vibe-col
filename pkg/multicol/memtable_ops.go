package multicol

import (
	"fmt"
	"sort"
	"vibe-lsm/pkg/col"

	"github.com/weaviate/sroar"
)

// Add adds a single ID-value pair to the memtable
func (m *MemtableImpl) Add(id uint64, value int64) error {
	// Fast path for strictly increasing IDs
	lastID := m.lastID.Load()
	if lastID < id {
		// Try to update lastID atomically
		if m.lastID.CompareAndSwap(lastID, id) {
			// Got the fast path, use a specialized lock
			m.sequentialLock.Lock()
			defer m.sequentialLock.Unlock()

			m.skipList.Insert(id, value)
			m.entryCount.Add(1)
			return nil
		}
	}

	// Fall back to regular striped locking for random access
	stripeIdx := id % m.numStripes
	m.stripeLocks[stripeIdx].Lock()
	defer m.stripeLocks[stripeIdx].Unlock()

	m.skipList.Insert(id, value)
	m.entryCount.Add(1)
	return nil
}

// BatchAdd adds multiple ID-value pairs to the memtable
func (m *MemtableImpl) BatchAdd(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return nil // Nothing to do
	}

	// Make a copy of the arrays to ensure they don't change during processing
	idsCopy := make([]uint64, len(ids))
	valuesCopy := make([]int64, len(values))
	copy(idsCopy, ids)
	copy(valuesCopy, values)

	// Check if data is sorted for fast path
	isSorted := true
	lastID := m.lastID.Load()
	for i := 0; i < len(idsCopy); i++ {
		if i > 0 && idsCopy[i] <= idsCopy[i-1] {
			isSorted = false
			break
		}
		if i == 0 && idsCopy[i] <= lastID {
			isSorted = false
			break
		}
	}

	// Use fast path for sorted data
	if isSorted {
		m.sequentialLock.Lock()
		defer m.sequentialLock.Unlock()

		for i := 0; i < len(idsCopy); i++ {
			m.skipList.Insert(idsCopy[i], valuesCopy[i])
		}

		// Update lastID
		m.lastID.Store(idsCopy[len(idsCopy)-1])
		m.entryCount.Add(int64(len(idsCopy)))
		return nil
	}

	// Group entries by stripe for regular path
	entriesByStripe := make(map[uint64][][2]uint64)
	for i := 0; i < len(idsCopy); i++ {
		stripe := idsCopy[i] % m.numStripes
		entriesByStripe[stripe] = append(entriesByStripe[stripe], [2]uint64{idsCopy[i], uint64(i)})
	}

	// Process each stripe sequentially
	for stripe, entries := range entriesByStripe {
		m.stripeLocks[stripe].Lock()

		for _, entry := range entries {
			id := entry[0]
			index := entry[1]
			m.skipList.Insert(id, valuesCopy[index])
		}

		m.stripeLocks[stripe].Unlock()
	}

	// Update entry count
	m.entryCount.Add(int64(len(idsCopy)))
	return nil
}

// Delete marks an entry as deleted
func (m *MemtableImpl) Delete(id uint64) bool {
	stripeIdx := id % m.numStripes
	m.stripeLocks[stripeIdx].RLock()
	defer m.stripeLocks[stripeIdx].RUnlock()

	// Find the node using the standard search algorithm
	current := m.skipList.head

	// Traverse from top to bottom level
	for level := m.skipList.height - 1; level >= 0; level-- {
		for current.next[level] != nil && current.next[level].key < id {
			current = current.next[level]
		}
	}

	// Check if the node exists at bottom level
	candidate := current.next[0]
	if candidate == nil || candidate.key != id {
		// Node doesn't exist, nothing to delete
		return false
	}

	// Node exists, mark it as deleted
	if candidate.deleted.CompareAndSwap(false, true) {
		// First time this node was deleted
		m.logicalDeleteCount.Add(1)
		return true
	}

	// Node was already marked as deleted
	return false
}

// BatchDelete marks multiple entries as deleted
func (m *MemtableImpl) BatchDelete(ids []uint64) int {
	if len(ids) == 0 {
		return 0
	}

	// Sort IDs for more efficient deletion
	sortedIDs := make([]uint64, len(ids))
	copy(sortedIDs, ids)
	sort.Slice(sortedIDs, func(i, j int) bool {
		return sortedIDs[i] < sortedIDs[j]
	})

	// Group by stripe for efficient locking
	entriesByStripe := make(map[uint64][]uint64)
	for _, id := range sortedIDs {
		stripe := id % m.numStripes
		entriesByStripe[stripe] = append(entriesByStripe[stripe], id)
	}

	deletedCount := 0

	// Process each stripe
	for stripe, stripeIDs := range entriesByStripe {
		// Acquire read lock for this stripe
		m.stripeLocks[stripe].RLock()

		for _, id := range stripeIDs {
			// Find the node
			current := m.skipList.head

			// Search for the node
			for level := m.skipList.height - 1; level >= 0; level-- {
				for current.next[level] != nil && current.next[level].key < id {
					current = current.next[level]
				}
			}

			// Check bottom level
			candidate := current.next[0]
			if candidate != nil && candidate.key == id {
				// Found the node, mark as deleted
				if candidate.deleted.CompareAndSwap(false, true) {
					deletedCount++
				}
			}
		}

		m.stripeLocks[stripe].RUnlock()
	}

	// Update the logical delete count
	if deletedCount > 0 {
		m.logicalDeleteCount.Add(int64(deletedCount))
	}

	return deletedCount
}

// Get returns the value for a specific ID
func (m *MemtableImpl) Get(id uint64) (int64, bool) {
	return m.skipList.Get(id)
}

// Scan returns key-value pairs within a range
func (m *MemtableImpl) Scan(startID, endID uint64) ([]uint64, []int64) {
	var ids []uint64
	var values []int64

	// Create an iterator for the range
	iter := m.skipList.RangeIterator(startID, endID)

	// Collect non-deleted entries
	for iter.Next() {
		if !iter.IsDeleted() {
			ids = append(ids, iter.Key())
			values = append(values, iter.Value())
		}
	}

	return ids, values
}

// Aggregate performs unfiltered aggregation operations
func (m *MemtableImpl) Aggregate() (uint64, uint64, int64, int64, int64, int) {
	var sum int64
	var count int
	var minID, maxID uint64
	var minValue, maxValue int64
	var firstEntry bool = true

	// Compute aggregates on demand by iterating through the skiplist
	iter := m.skipList.Iterator()
	for iter.Next() {
		// Skip deleted entries
		if iter.IsDeleted() {
			continue
		}

		id := iter.Key()
		value := iter.Value()

		// Update aggregates
		sum += value
		count++

		// Handle first entry
		if firstEntry {
			minID = id
			maxID = id
			minValue = value
			maxValue = value
			firstEntry = false
			continue
		}

		// Update min/max
		if id < minID {
			minID = id
		}
		if id > maxID {
			maxID = id
		}
		if value < minValue {
			minValue = value
		}
		if value > maxValue {
			maxValue = value
		}
	}

	// Special case for empty result
	if count == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	return minID, maxID, minValue, maxValue, sum, count
}

// FilteredAggregate performs aggregation on IDs present in the filter
func (m *MemtableImpl) FilteredAggregate(filter *sroar.Bitmap) (uint64, uint64, int64, int64, int64, int) {
	var sum int64
	var count int
	var minID, maxID uint64
	var minValue, maxValue int64
	var firstEntry bool = true

	// Iterate through the skiplist and apply filter
	iter := m.skipList.Iterator()
	for iter.Next() {
		id := iter.Key()

		// Skip deleted entries and non-matching entries
		if iter.IsDeleted() || !filter.Contains(id) {
			continue
		}

		value := iter.Value()

		// Update aggregates
		sum += value
		count++

		// Handle first entry
		if firstEntry {
			minID = id
			maxID = id
			minValue = value
			maxValue = value
			firstEntry = false
			continue
		}

		// Update min/max
		if id < minID {
			minID = id
		}
		if id > maxID {
			maxID = id
		}
		if value < minValue {
			minValue = value
		}
		if value > maxValue {
			maxValue = value
		}
	}

	// Special case for empty result
	if count == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	return minID, maxID, minValue, maxValue, sum, count
}

// Flush writes the non-deleted contents to the specified path
func (m *MemtableImpl) Flush(path string) error {
	// Create a BufferedWriter
	writer, err := col.NewBufferedWriter(path)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Temporary buffers for batch processing
	const batchSize = DefaultBatchSize
	ids := make([]uint64, 0, batchSize)
	values := make([]int64, 0, batchSize)

	// Iterate through the skiplist
	iter := m.skipList.Iterator()
	for iter.Next() {
		// Skip deleted entries during flush
		if iter.IsDeleted() {
			continue
		}

		// Add to current batch
		ids = append(ids, iter.Key())
		values = append(values, iter.Value())

		// Write batch when it reaches size limit
		if len(ids) >= batchSize {
			if err := writer.BatchAdd(ids, values); err != nil {
				return err
			}

			// Reset buffers
			ids = ids[:0]
			values = values[:0]
		}
	}

	// Write any remaining entries
	if len(ids) > 0 {
		if err := writer.BatchAdd(ids, values); err != nil {
			return err
		}
	}

	// Close the writer explicitly to ensure data is flushed
	return writer.Close()
}

// ActiveCount returns the number of non-deleted entries
func (m *MemtableImpl) ActiveCount() int64 {
	return m.entryCount.Load() - m.logicalDeleteCount.Load()
}

// IsEmpty checks if the memtable has any non-deleted entries
func (m *MemtableImpl) IsEmpty() bool {
	return m.ActiveCount() == 0
}
