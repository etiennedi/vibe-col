// Package store implements an LSM tree storage engine built on top of the multicol package.
package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"vibe-lsm/pkg/col"
	"vibe-lsm/pkg/multicol"

	"github.com/weaviate/sroar"
)

// VibeStoreOptions defines configuration for the LSM store
type VibeStoreOptions struct {
	// Directory for segment files
	DataDir string

	// Memtable options
	MemtableSize     int64
	MemtableMaxAgeMs int64 // Max age of memtable before flush (in milliseconds)
	MemtableOptions  *multicol.MemtableOptions

	// Segment options
	CompactionOptions multicol.CompactionOptions

	// Background tasks
	MaxSegmentsBeforeCompaction int
}

// DefaultOptions returns the default store options
func DefaultOptions(dataDir string) VibeStoreOptions {
	return VibeStoreOptions{
		DataDir:                     dataDir,
		MemtableSize:                10000, // Flush after 10,000 entries
		MemtableMaxAgeMs:            60000, // Flush after 60 seconds
		MemtableOptions:             multicol.DefaultMemtableOptions(),
		CompactionOptions:           multicol.DefaultCompactionOptions(),
		MaxSegmentsBeforeCompaction: 10, // Trigger compaction at 10 segments
	}
}

// taskType defines the types of background tasks
type taskType int

const (
	taskFlush taskType = iota
	taskCompaction
	taskCleanup
)

// task represents a unit of work for the background worker
type task struct {
	taskType taskType
	memtable multicol.Memtable // For flush tasks
	segments []*col.Reader     // For compaction tasks
	oldState *VibeStoreState   // For cleanup tasks
}

// VibeStoreState represents a consistent snapshot of the store
type VibeStoreState struct {
	activeMemtable    multicol.Memtable
	activeSince       time.Time
	flushingMemtables []multicol.Memtable
	segments          []*col.Reader
	readerCount       atomic.Int32

	// Cached MultiReader for efficient queries
	multiReader     *multicol.MultiReader
	multiReaderLock sync.RWMutex
}

// getMultiReader returns the cached MultiReader for the current state
// or creates a new one if needed
func (state *VibeStoreState) getMultiReader() *multicol.MultiReader {
	state.multiReaderLock.RLock()
	if state.multiReader != nil {
		reader := state.multiReader
		state.multiReaderLock.RUnlock()
		return reader
	}
	state.multiReaderLock.RUnlock()

	// Need to create a new MultiReader
	state.multiReaderLock.Lock()
	defer state.multiReaderLock.Unlock()

	// Double-check to avoid race conditions
	if state.multiReader != nil {
		return state.multiReader
	}

	// Prepare sources for MultiReader (oldest to newest as required by MultiReader)
	sources := make([]multicol.AggregateSource, 0,
		len(state.segments)+len(state.flushingMemtables)+1)

	// Add segments (oldest first)
	for _, segment := range state.segments {
		sources = append(sources, segment)
	}

	// Add flushing memtables (oldest first)
	for _, memtable := range state.flushingMemtables {
		sources = append(sources, memtable)
	}

	// Add active memtable (newest)
	sources = append(sources, state.activeMemtable)

	// Create a MultiReader with all sources
	state.multiReader = multicol.NewMultiReader(sources)
	return state.multiReader
}

// VibeStore represents a complete LSM store
type VibeStore struct {
	// Atomic reference to current store state
	state atomic.Value // Holds *VibeStoreState

	// Lock for state transitions
	stateLock sync.RWMutex

	// Background task coordination
	taskQueue chan task
	shutdown  chan struct{}

	// Configuration
	options VibeStoreOptions
}

// NewVibeStore creates a new LSM store with the given options
func NewVibeStore(options VibeStoreOptions) (*VibeStore, error) {
	// Create the data directory if it doesn't exist
	err := ensureDirectoryExists(options.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create a new store
	store := &VibeStore{
		options:   options,
		taskQueue: make(chan task, 100), // Buffer for task queueing
		shutdown:  make(chan struct{}),
	}

	// Create initial memtable
	initialMemtable := multicol.NewMemtable(options.MemtableOptions)

	// Initialize the state
	initialState := &VibeStoreState{
		activeMemtable:    initialMemtable,
		activeSince:       time.Now(),
		flushingMemtables: make([]multicol.Memtable, 0),
		segments:          make([]*col.Reader, 0),
	}
	store.state.Store(initialState)

	// Start the background worker
	go store.backgroundWorker()

	// Start the memtable age check timer
	go store.memtableAgeChecker()

	return store, nil
}

// ensureDirectoryExists creates a directory if it doesn't exist
func ensureDirectoryExists(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if info.IsDir() {
			return nil // Directory already exists
		}
		return fmt.Errorf("%s exists but is not a directory", dir)
	}

	if os.IsNotExist(err) {
		// Create the directory with permissions rwxr-xr-x
		return os.MkdirAll(dir, 0755)
	}

	return err // Some other error occurred
}

// backgroundWorker processes tasks from the task queue
func (vs *VibeStore) backgroundWorker() {
	for {
		select {
		case task := <-vs.taskQueue:
			vs.processTask(task)
		case <-vs.shutdown:
			return
		}
	}
}

// processTask handles a background task
func (vs *VibeStore) processTask(t task) {
	switch t.taskType {
	case taskFlush:
		vs.doFlushMemtable(t.memtable)
	case taskCompaction:
		// Compaction is a no-op for now
	case taskCleanup:
		vs.doCleanup(t.oldState)
	}

	// For now, we won't automatically trigger compaction
}

// memtableAgeChecker periodically checks if the active memtable is too old
func (vs *VibeStore) memtableAgeChecker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			vs.checkMemtableAge()
		case <-vs.shutdown:
			return
		}
	}
}

// checkMemtableAge checks if the active memtable is too old and should be flushed
func (vs *VibeStore) checkMemtableAge() {
	vs.stateLock.RLock()
	currentState := vs.state.Load().(*VibeStoreState)
	activeSince := currentState.activeSince
	vs.stateLock.RUnlock()

	ageMs := time.Since(activeSince).Milliseconds()
	if ageMs >= vs.options.MemtableMaxAgeMs {
		vs.triggerFlush()
	}
}

// Add adds a key-value pair to the store
func (vs *VibeStore) Add(id uint64, value int64) error {
	vs.stateLock.RLock()
	currentState := vs.state.Load().(*VibeStoreState)
	memtable := currentState.activeMemtable
	vs.stateLock.RUnlock()

	err := memtable.Add(id, value)
	if err != nil {
		return err
	}

	// Check if memtable size exceeds threshold
	if memtable.ActiveCount() >= vs.options.MemtableSize {
		vs.triggerFlush()
	}

	return nil
}

// Delete marks a key as deleted in the store
func (vs *VibeStore) Delete(id uint64) error {
	vs.stateLock.RLock()
	currentState := vs.state.Load().(*VibeStoreState)
	memtable := currentState.activeMemtable
	vs.stateLock.RUnlock()

	deleted := memtable.Delete(id)
	if !deleted {
		// Even if the ID wasn't in the active memtable, we've still
		// recorded the deletion tombstone, so this isn't an error
	}

	return nil
}

// BatchAdd adds multiple key-value pairs to the store
func (vs *VibeStore) BatchAdd(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	vs.stateLock.RLock()
	currentState := vs.state.Load().(*VibeStoreState)
	memtable := currentState.activeMemtable
	vs.stateLock.RUnlock()

	err := memtable.BatchAdd(ids, values)
	if err != nil {
		return err
	}

	// Check if memtable size exceeds threshold
	if memtable.ActiveCount() >= vs.options.MemtableSize {
		vs.triggerFlush()
	}

	return nil
}

// GetValue retrieves a value for a key using the aggregate operation
// This is consistent with the OLAP nature of the store
func (vs *VibeStore) GetValue(id uint64) (int64, bool) {
	// Create a filter with just this ID
	filter := sroar.NewBitmap()
	filter.Set(id)

	// Use aggregate to get value
	result, err := vs.Aggregate(col.AggregateOptions{
		Filter: filter,
	})

	if err != nil || result.Count == 0 {
		return 0, false
	}

	// If the count is 1, we found it (we're guaranteed to get the newest value)
	return result.Sum, true
}

// Aggregate performs aggregation across all data sources
func (vs *VibeStore) Aggregate(opts col.AggregateOptions) (col.AggregateResult, error) {
	vs.stateLock.RLock()
	currentState := vs.state.Load().(*VibeStoreState)
	currentState.readerCount.Add(1)
	vs.stateLock.RUnlock()

	defer currentState.readerCount.Add(-1)

	// Get the cached MultiReader for the current state
	multiReader := currentState.getMultiReader()

	// Perform the aggregation
	return multiReader.Aggregate(multicol.AggregateOptions{
		SkipPreCalculated: opts.SkipPreCalculated,
		Filter:            opts.Filter,
	})
}

// triggerFlush schedules a flush task for the active memtable
func (vs *VibeStore) triggerFlush() {
	// Get current state and swap memtables under lock
	vs.stateLock.Lock()

	// Double-check if we actually need to flush
	currentState := vs.state.Load().(*VibeStoreState)
	if currentState.activeMemtable.ActiveCount() == 0 {
		vs.stateLock.Unlock()
		return // Nothing to flush
	}

	oldMemtable := currentState.activeMemtable

	// Create new memtable
	newMemtable := multicol.NewMemtable(vs.options.MemtableOptions)

	// Create new state
	newState := &VibeStoreState{
		activeMemtable:    newMemtable,
		activeSince:       time.Now(),
		flushingMemtables: append(currentState.flushingMemtables, oldMemtable),
		segments:          currentState.segments,
		multiReader:       nil, // Will be created on demand
	}

	// Store new state
	vs.state.Store(newState)
	vs.stateLock.Unlock()

	// Schedule flush task
	vs.taskQueue <- task{
		taskType: taskFlush,
		memtable: oldMemtable,
	}
}

// doFlushMemtable performs the actual memtable flush (called by coordinator)
func (vs *VibeStore) doFlushMemtable(memtable multicol.Memtable) {
	// Create segment path with proper directory handling
	timestamp := time.Now().UnixNano()
	filename := fmt.Sprintf("segment_%d.col", timestamp)
	segmentPath := filepath.Join(vs.options.DataDir, filename)

	// Debug: Print the path being used
	fmt.Printf("Flushing memtable to: %s\n", segmentPath)

	// Flush memtable to disk
	entriesWritten, err := memtable.Flush(segmentPath)
	if err != nil {
		// Log error and return
		fmt.Printf("Error flushing memtable: %v\n", err)
		return
	}

	if entriesWritten == 0 {
		// No entries were written, skip adding this segment
		// We don't need to update the state
		fmt.Printf("No entries written, skipping segment creation\n")
		return
	}

	// Verify the file exists before opening
	if _, err := os.Stat(segmentPath); os.IsNotExist(err) {
		fmt.Printf("Error: Segment file not found at %s after flush\n", segmentPath)
		return
	}

	// Open new segment
	newSegment, err := col.NewReader(segmentPath)
	if err != nil {
		fmt.Printf("Error opening new segment: %v\n", err)
		return
	}

	// Update state
	vs.stateLock.Lock()
	currentState := vs.state.Load().(*VibeStoreState)

	// Find and remove the flushed memtable
	newFlushingMemtables := make([]multicol.Memtable, 0, len(currentState.flushingMemtables))
	for _, m := range currentState.flushingMemtables {
		if m != memtable {
			newFlushingMemtables = append(newFlushingMemtables, m)
		}
	}

	// Add new segment (ensuring newest is last)
	newSegments := make([]*col.Reader, len(currentState.segments)+1)
	copy(newSegments, currentState.segments)
	newSegments[len(currentState.segments)] = newSegment

	// Create new state
	newState := &VibeStoreState{
		activeMemtable:    currentState.activeMemtable,
		activeSince:       currentState.activeSince,
		flushingMemtables: newFlushingMemtables,
		segments:          newSegments,
		multiReader:       nil, // Will be created on demand
	}

	// Store new state and create task to clean up old state
	oldState := currentState
	vs.state.Store(newState)
	vs.stateLock.Unlock()

	// Schedule old state cleanup
	vs.taskQueue <- task{
		taskType: taskCleanup,
		oldState: oldState,
	}
}

// doCleanup cleans up resources from an old state
func (vs *VibeStore) doCleanup(oldState *VibeStoreState) {
	// Wait until no readers are using the old state
	for oldState.readerCount.Load() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// Cleanup MultiReader if it exists
	oldState.multiReaderLock.Lock()
	if oldState.multiReader != nil {
		oldState.multiReader.Close()
		oldState.multiReader = nil
	}
	oldState.multiReaderLock.Unlock()
}

// Close shuts down the store gracefully
func (vs *VibeStore) Close() error {
	// Signal shutdown to background workers
	close(vs.shutdown)

	// Flush active memtable if not empty
	currentState := vs.state.Load().(*VibeStoreState)
	if !currentState.activeMemtable.IsEmpty() {
		memtable := currentState.activeMemtable
		timestamp := time.Now().UnixNano()
		segmentPath := filepath.Join(vs.options.DataDir, fmt.Sprintf("segment_%d.col", timestamp))
		_, err := memtable.Flush(segmentPath)
		if err != nil {
			return fmt.Errorf("error flushing final memtable: %w", err)
		}
	}

	// Close all resources
	for _, segment := range currentState.segments {
		segment.Close()
	}

	if currentState.multiReader != nil {
		currentState.multiReaderLock.Lock()
		currentState.multiReader.Close()
		currentState.multiReaderLock.Unlock()
	}

	return nil
}

// EmptyAggregateOptions returns empty aggregate options for convenience
func EmptyAggregateOptions() col.AggregateOptions {
	return col.AggregateOptions{}
}

// ForceFlush immediately triggers a flush of the active memtable
// This is primarily used for testing
func (vs *VibeStore) ForceFlush() {
	vs.triggerFlush()

	// Wait a bit for the flush to complete
	time.Sleep(100 * time.Millisecond)
}
