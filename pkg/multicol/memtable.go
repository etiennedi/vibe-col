// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"vibe-lsm/pkg/col"

	"github.com/weaviate/sroar"
)

// Iterator defines the interface for iterating over entries
type Iterator interface {
	// Next advances the iterator to the next entry
	// Returns false if there are no more entries
	Next() bool

	// HasNext returns true if there are more entries
	HasNext() bool

	// Entry returns the current entry
	Entry() (uint64, int64)

	// EntryWithDeleted returns the current entry and deleted status
	EntryWithDeleted() (uint64, int64, bool)

	// Close releases any resources used by the iterator
	Close()
}

// Memtable defines the interface for an in-memory table
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

	// Additional methods for MultiReader compatibility
	AggregateWithOptions(opts col.AggregateOptions) col.AggregateResult
	GetGlobalIDBitmap() (*sroar.Bitmap, error)
	GetDeletedIDBitmap() (*sroar.Bitmap, error)
	Close() error
}
