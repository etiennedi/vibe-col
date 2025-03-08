package store

import (
	"fmt"
	"sort"
	"sync"
)

// AggregationType represents the type of aggregation to perform
type AggregationType string

const (
	// Common aggregation types
	Min    AggregationType = "min"
	Max    AggregationType = "max"
	Mean   AggregationType = "mean"
	Median AggregationType = "median"
	Count  AggregationType = "count"
	Sum    AggregationType = "sum"
)

// ColumnStore represents a column-oriented storage engine
type ColumnStore struct {
	memTable     *ColumnMemTable
	segments     []*Segment
	mu           sync.Mutex      // Global mutex for the store
	flushTrigger int             // Number of entries that trigger a flush
}

// ColumnMemTable represents the in-memory component of the column store
type ColumnMemTable struct {
	ids    []uint64
	values []int64
}

// Segment represents a persisted, immutable data segment
type Segment struct {
	blocks []*Block
}

// Block represents a block of data within a segment
type Block struct {
	ids    []uint64
	values []int64
	// Statistics for the block
	minValue int64
	maxValue int64
	count    int
	sum      int64
}

// NewColumnStore creates a new column-oriented store
func NewColumnStore(flushTrigger int) *ColumnStore {
	return &ColumnStore{
		memTable:     &ColumnMemTable{ids: make([]uint64, 0), values: make([]int64, 0)},
		segments:     make([]*Segment, 0),
		flushTrigger: flushTrigger,
	}
}

// Put adds a new value to the store with the given ID
func (cs *ColumnStore) Put(id uint64, value int64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	
	// Add to memtable
	cs.memTable.ids = append(cs.memTable.ids, id)
	cs.memTable.values = append(cs.memTable.values, value)
	
	// Auto-flush if needed
	if len(cs.memTable.ids) >= cs.flushTrigger {
		// Create a new segment with this memtable
		if err := cs.flushMemTableLocked(cs.memTable); err != nil {
			return fmt.Errorf("error during auto-flush: %w", err)
		}
		
		// Reset the memtable
		cs.memTable = &ColumnMemTable{ids: make([]uint64, 0), values: make([]int64, 0)}
	}

	return nil
}

// flushMemTableLocked flushes the given memtable to a segment
// Assumes cs.mu is already locked
func (cs *ColumnStore) flushMemTableLocked(memTable *ColumnMemTable) error {
	if len(memTable.ids) == 0 {
		return nil // Nothing to flush
	}
	
	// For now, we'll create a single block for the entire segment
	block := &Block{
		ids:    make([]uint64, len(memTable.ids)),
		values: make([]int64, len(memTable.values)),
	}
	
	// Copy data to the block
	copy(block.ids, memTable.ids)
	copy(block.values, memTable.values)
	
	// Calculate block statistics
	block.count = len(block.values)
	if block.count > 0 {
		block.minValue = block.values[0]
		block.maxValue = block.values[0]
		block.sum = 0
		
		for _, v := range block.values {
			if v < block.minValue {
				block.minValue = v
			}
			if v > block.maxValue {
				block.maxValue = v
			}
			block.sum += v
		}
	}
	
	// Create a new segment with this block
	segment := &Segment{
		blocks: []*Block{block},
	}
	
	// Add the segment to our list
	cs.segments = append(cs.segments, segment)
	
	return nil
}

// Flush forces the current memtable to be flushed to a new segment
func (cs *ColumnStore) Flush() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	
	// Flush the current memtable
	if err := cs.flushMemTableLocked(cs.memTable); err != nil {
		return err
	}
	
	// Reset the memtable
	cs.memTable = &ColumnMemTable{ids: make([]uint64, 0), values: make([]int64, 0)}
	
	return nil
}

// Aggregate performs the specified aggregation on all data in the store
func (cs *ColumnStore) Aggregate(aggType AggregationType) (float64, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Gather all values from both memtable and segments
	var allValues []int64
	
	// First add memtable values
	allValues = append(allValues, cs.memTable.values...)
	
	// Then add segment values
	for _, segment := range cs.segments {
		for _, block := range segment.blocks {
			allValues = append(allValues, block.values...)
		}
	}

	// If no data, return error
	if len(allValues) == 0 {
		return 0, fmt.Errorf("no data available for aggregation")
	}

	// Perform the requested aggregation
	switch aggType {
	case Min:
		min := allValues[0]
		for _, v := range allValues {
			if v < min {
				min = v
			}
		}
		return float64(min), nil

	case Max:
		max := allValues[0]
		for _, v := range allValues {
			if v > max {
				max = v
			}
		}
		return float64(max), nil

	case Mean:
		sum := int64(0)
		for _, v := range allValues {
			sum += v
		}
		return float64(sum) / float64(len(allValues)), nil

	case Sum:
		sum := int64(0)
		for _, v := range allValues {
			sum += v
		}
		return float64(sum), nil

	case Count:
		return float64(len(allValues)), nil

	case Median:
		// Sort values to find median
		sort.Slice(allValues, func(i, j int) bool {
			return allValues[i] < allValues[j]
		})
		mid := len(allValues) / 2
		if len(allValues)%2 == 0 {
			// Even number of elements - average of two middle values
			return float64(allValues[mid-1]+allValues[mid]) / 2.0, nil
		}
		// Odd number of elements - return middle value
		return float64(allValues[mid]), nil

	default:
		return 0, fmt.Errorf("unsupported aggregation type: %s", aggType)
	}
}