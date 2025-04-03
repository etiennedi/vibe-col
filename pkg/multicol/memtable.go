// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/weaviate/sroar"
)

const (
	// DefaultNumStripes is the default number of stripes for the memtable
	DefaultNumStripes = 256

	// DefaultMaxHeight is the default maximum height of the skip list
	DefaultMaxHeight = 24

	// DefaultProbability is the default probability of increasing the height of a node
	DefaultProbability = 0.25

	// DefaultBatchSize is the default batch size for flushing
	DefaultBatchSize = 10000
)

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

	// Aggregate performs unfiltered aggregation operations
	// Returns: min ID, max ID, min value, max value, sum of values, count
	Aggregate() (uint64, uint64, int64, int64, int64, int)

	// FilteredAggregate performs aggregation on IDs present in the filter
	// Returns: min ID, max ID, min value, max value, sum of values, count
	FilteredAggregate(filter *sroar.Bitmap) (uint64, uint64, int64, int64, int64, int)

	// Flush writes the non-deleted contents to the specified path
	Flush(path string) error

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

// ConcurrentSkipList is a concurrent skip list implementation
type ConcurrentSkipList struct {
	head      *skipNode    // Head of the list
	maxHeight int          // Maximum height
	height    int          // Current height
	rng       *rand.Rand   // Random number generator for level determination
	size      atomic.Int64 // Number of elements
}

// MemtableImpl is the implementation of the Memtable interface
type MemtableImpl struct {
	skipList           *ConcurrentSkipList
	stripeLocks        []sync.RWMutex
	numStripes         uint64
	entryCount         atomic.Int64  // Total entries added
	logicalDeleteCount atomic.Int64  // Total entries marked as deleted
	lastID             atomic.Uint64 // Last ID added (for sequential optimization)
	sequentialLock     sync.Mutex    // Lock for sequential inserts
}

// MemtableOptions defines configuration options for creating a memtable
type MemtableOptions struct {
	NumStripes int   // Number of stripes for concurrent access
	MaxHeight  int   // Maximum height of the skip list
	Seed       int64 // Seed for the random number generator
}

// DefaultMemtableOptions returns the default options for creating a memtable
func DefaultMemtableOptions() *MemtableOptions {
	return &MemtableOptions{
		NumStripes: DefaultNumStripes,
		MaxHeight:  DefaultMaxHeight,
		Seed:       0, // Will use time-based seed if 0
	}
}

// NewMemtable creates a new memtable with the specified options
func NewMemtable(opts *MemtableOptions) Memtable {
	if opts == nil {
		opts = DefaultMemtableOptions()
	}

	numStripes := opts.NumStripes
	if numStripes <= 0 {
		numStripes = DefaultNumStripes
	}

	maxHeight := opts.MaxHeight
	if maxHeight <= 0 {
		maxHeight = DefaultMaxHeight
	}

	// Initialize the skip list
	sl := &ConcurrentSkipList{
		maxHeight: maxHeight,
		height:    1,
		rng:       rand.New(rand.NewSource(opts.Seed)),
	}

	// Create the head node with the maximum height
	sl.head = &skipNode{
		next: make([]*skipNode, maxHeight),
	}

	// Create the memtable
	return &MemtableImpl{
		skipList:    sl,
		stripeLocks: make([]sync.RWMutex, numStripes),
		numStripes:  uint64(numStripes),
	}
}
