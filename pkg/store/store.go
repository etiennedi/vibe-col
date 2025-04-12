// Package store implements an LSM tree storage engine built on top of the multicol package.
package store

import (
	"encoding/json"
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

// VibeStoreOptions defines the configuration options for the LSM store
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
	CompactionCheckIntervalMs   int64 // How often to check for compaction (in milliseconds)
	DisableCompaction           bool  // Completely disable compaction (for testing)
}

// DefaultOptions returns the default store options
func DefaultOptions(dataDir string) VibeStoreOptions {
	return VibeStoreOptions{
		DataDir:                     dataDir,
		MemtableSize:                10000, // Flush after 10,000 entries
		MemtableMaxAgeMs:            60000, // Flush after 60 seconds
		MemtableOptions:             multicol.DefaultMemtableOptions(),
		CompactionOptions:           multicol.DefaultCompactionOptions(),
		MaxSegmentsBeforeCompaction: 10,    // Trigger compaction at 10 segments
		CompactionCheckIntervalMs:   30000, // Check compaction every 30 seconds
		DisableCompaction:           false, // Compaction enabled by default
	}
}

// taskType defines the types of background tasks
type taskType int

const (
	taskFlush taskType = iota
	taskCompaction
	taskCleanup
)

// task represents a background task
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

// GetSegments returns a slice of the current segment readers in the state.
// This is primarily for debugging or internal use where direct access is needed.
func (state *VibeStoreState) GetSegments() []*col.Reader {
	return state.segments
}

// getMultiReader returns the cached MultiReader for the current state
// or creates a new one if needed
func (state *VibeStoreState) getMultiReader() *multicol.MultiReader {
	// First, check if we already have a cached reader (fast path)
	state.multiReaderLock.RLock()
	if state.multiReader != nil {
		reader := state.multiReader
		state.multiReaderLock.RUnlock()
		return reader
	}
	state.multiReaderLock.RUnlock()

	// Need to create a new MultiReader (slow path)
	state.multiReaderLock.Lock()
	defer state.multiReaderLock.Unlock()

	// Double-check to avoid race conditions
	if state.multiReader != nil {
		return state.multiReader
	}

	// Prepare sources for MultiReader (oldest to newest as required by MultiReader)
	sources := make([]multicol.AggregateSource, 0,
		len(state.segments)+len(state.flushingMemtables)+1)

	// Add segments (oldest first) - these may include both original
	// and compacted segments during transitions
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
	taskQueue    chan task
	shutdown     chan struct{}
	workerWG     sync.WaitGroup // Wait group for background workers
	compactionWG sync.WaitGroup // Wait group for ongoing compactions
	isCompacting atomic.Bool    // Flag to prevent concurrent compactions

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
		options:      options,
		taskQueue:    make(chan task, 100), // Buffer for task queueing
		shutdown:     make(chan struct{}),
		isCompacting: atomic.Bool{},
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

	// Load segments from manifest if it exists
	if err := store.loadManifest(); err != nil {
		// Close any segments we may have opened
		currentState := store.state.Load().(*VibeStoreState)
		for _, segment := range currentState.segments {
			segment.Close()
		}
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	// Start background tasks
	go store.backgroundWorker()
	go store.memtableAgeChecker()
	go store.compactionChecker()

	// Add background workers to WaitGroup
	store.workerWG.Add(3) // Add 1 for each background goroutine

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
	defer vs.workerWG.Done() // Ensure Done is called when goroutine exits
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
		// Ensure the compaction flag is reset regardless of outcome
		defer func() {
			vs.isCompacting.Store(false)
			// Check if more compaction is needed *after* resetting the flag
			_ = vs.triggerCompaction(false)
		}()
		vs.doCompaction(t.segments)
	case taskCleanup:
		vs.doCleanup(t.oldState)
	}
}

// memtableAgeChecker periodically checks if the active memtable is too old
func (vs *VibeStore) memtableAgeChecker() {
	defer vs.workerWG.Done() // Ensure Done is called when goroutine exits
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

	// Perform the aggregation with all options
	return multiReader.Aggregate(multicol.AggregateOptions{
		SkipPreCalculated: opts.SkipPreCalculated,
		Filter:            opts.Filter,
	})
}

// AggregateWithOptions performs aggregation with additional options
// for advanced use cases
func (vs *VibeStore) AggregateWithOptions(opts AggregateOptions) (col.AggregateResult, error) {
	vs.stateLock.RLock()
	currentState := vs.state.Load().(*VibeStoreState)
	currentState.readerCount.Add(1)
	vs.stateLock.RUnlock()

	defer currentState.readerCount.Add(-1)

	// Get the cached MultiReader for the current state
	multiReader := currentState.getMultiReader()

	// Before performing aggregation, apply parallel option if specified
	// This needs to be done for each source in the MultiReader
	if opts.Parallel != 0 {
		// For now, we don't directly apply parallel settings to segments
		// This is a placeholder for future enhancement when we implement
		// segment-level parallelism by configuring each reader individually

		// The actual parallel processing is handled internally by the col.Reader
		// when the proper options are passed
	}

	// Map our options to multicol options and include DenyFilter
	multicolOpts := multicol.AggregateOptions{
		SkipPreCalculated: opts.SkipPreCalculated,
		Filter:            opts.Filter,
	}

	// Perform the aggregation with all options
	result, err := multiReader.Aggregate(multicolOpts)

	// The parallel setting gets applied inside each individual reader,
	// but the higher-level MultiReader orchestrates across multiple sources
	// If we want truly parallel processing across all data, we may need to
	// enhance the MultiReader implementation in the future

	return result, err
}

// AggregateOptions defines the options for aggregation operations
type AggregateOptions struct {
	// SkipPreCalculated forces the aggregation to read all values from blocks
	// instead of using pre-calculated values from the footer
	SkipPreCalculated bool

	// Filter is a bitmap of allowed IDs for filtered aggregation
	Filter *sroar.Bitmap

	// DenyFilter is a bitmap of denied IDs for filtered aggregation
	// If both Filter and DenyFilter are provided, an ID must be in Filter AND NOT in DenyFilter
	DenyFilter *sroar.Bitmap

	// Parallel enables parallel aggregation with the specified number of workers
	// If Parallel is 0, aggregation is performed sequentially
	// If Parallel is negative, GOMAXPROCS is used as the number of workers
	Parallel int
}

// DefaultAggregateOptions returns default options for aggregation
func DefaultAggregateOptions() AggregateOptions {
	return AggregateOptions{
		SkipPreCalculated: false,
		Filter:            nil,
		DenyFilter:        nil,
		Parallel:          0, // Sequential by default
	}
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
	// IMPORTANT: Reopen existing segments to get new file handles
	newSegments := make([]*col.Reader, 0, len(currentState.segments)+1)
	for _, existingSegment := range currentState.segments {
		reopenedSegment, err := col.NewReader(existingSegment.FilePath())
		if err != nil {
			vs.stateLock.Unlock()
			newSegment.Close() // Close the newly created segment
			// Close previously reopened segments
			for _, seg := range newSegments {
				seg.Close()
			}
			fmt.Printf("ERROR: Failed to reopen existing segment %s during flush state update: %v\n", existingSegment.FilePath(), err)
			return
		}
		newSegments = append(newSegments, reopenedSegment)
	}
	// Append the newly flushed segment
	newSegments = append(newSegments, newSegment)

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

	// Save the updated manifest
	if err := vs.saveManifest(); err != nil {
		fmt.Printf("Warning: Failed to save manifest after flush: %v\n", err)
	}

	// Schedule old state cleanup
	vs.taskQueue <- task{
		taskType: taskCleanup,
		oldState: oldState,
	}
}

// doCleanup cleans up resources from an old state
func (vs *VibeStore) doCleanup(oldState *VibeStoreState) {
	// Wait until no readers are using the old state.
	// This is crucial for ensuring data consistency during compaction and flushes.
	// Readers increment the counter when they start using a state and decrement when done.
	for oldState.readerCount.Load() > 0 {
		//fmt.Printf("Cleanup waiting: %d readers still using old state\n", oldState.readerCount.Load())
		time.Sleep(10 * time.Millisecond)
	}
	//fmt.Printf("Cleanup proceeding: No readers left for old state\n")

	// Cleanup the MultiReader associated with the old state, if it exists
	oldState.multiReaderLock.Lock()
	if oldState.multiReader != nil {
		oldState.multiReader.Close() // Close the MultiReader itself
		oldState.multiReader = nil
	}
	oldState.multiReaderLock.Unlock()

	// Get the current state to identify which segments are still active
	currentState := vs.state.Load().(*VibeStoreState)

	// Create a map of active segment file paths for efficient lookup
	activeSegmentPaths := make(map[string]bool)
	for _, segment := range currentState.segments {
		activeSegmentPaths[segment.FilePath()] = true
	}

	// Iterate through the segments in the *old* state
	for _, segment := range oldState.segments {
		filePath := segment.FilePath()

		// Check if this segment is still present in the *current* state
		if activeSegmentPaths[filePath] {
			//fmt.Printf("Cleanup skipping active segment: %s\n", filePath)
			continue // Segment is still active, do not close or delete
		}

		// If the segment is NOT in the current state, it means it was replaced
		// (e.g., by compaction) and is safe to close and delete.
		fmt.Printf("Cleanup closing and deleting segment: %s\n", filePath)

		// Close the segment reader first
		if err := segment.Close(); err != nil {
			// Log the error but proceed to attempt file deletion
			fmt.Printf("Warning: Error closing segment %s: %v\n", filePath, err)
		}

		// Delete the segment file from the filesystem
		if err := os.Remove(filePath); err != nil {
			// Log the error if deletion fails
			fmt.Printf("Warning: Failed to delete segment file %s: %v\n", filePath, err)
		}
	}
}

// Close shuts down the store gracefully
func (vs *VibeStore) Close() error {
	// Signal shutdown to background workers
	close(vs.shutdown)

	// NOTE: Removed WaitGroup wait as shutdown logic needs rethinking.
	// fmt.Println("Close: Signalled shutdown, waiting for workers...")
	// vs.workerWG.Wait()
	// fmt.Println("Close: Workers finished. Proceeding with final flush and close.")

	// Flush active memtable if not empty
	currentState := vs.state.Load().(*VibeStoreState)
	if !currentState.activeMemtable.IsEmpty() {
		// This is handling the direct flush rather than using the task queue
		// So we need to manually ensure the level is set to 0
		memtable := currentState.activeMemtable
		timestamp := time.Now().UnixNano()
		segmentPath := filepath.Join(vs.options.DataDir, fmt.Sprintf("segment_%d.col", timestamp))
		_, err := memtable.Flush(segmentPath)
		if err != nil {
			return fmt.Errorf("error flushing final memtable: %w", err)
		}

		// Open the newly created segment
		newSegment, err := col.NewReader(segmentPath)
		if err != nil {
			fmt.Printf("Error opening final segment: %v\n", err)
		} else {
			// Add it to the state
			vs.stateLock.Lock()
			finalState := vs.state.Load().(*VibeStoreState)
			newSegments := make([]*col.Reader, len(finalState.segments)+1)
			copy(newSegments, finalState.segments)
			newSegments[len(finalState.segments)] = newSegment

			newState := &VibeStoreState{
				activeMemtable:    finalState.activeMemtable,
				activeSince:       finalState.activeSince,
				flushingMemtables: finalState.flushingMemtables,
				segments:          newSegments,
				multiReader:       nil, // Will be created on demand
			}

			vs.state.Store(newState)
			vs.stateLock.Unlock()

			// Save the final manifest
			if err := vs.saveManifest(); err != nil {
				fmt.Printf("Warning: Failed to save manifest during close: %v\n", err)
			}
		}
	} else {
		// Even if there's no new segment, save the manifest one last time
		if err := vs.saveManifest(); err != nil {
			fmt.Printf("Warning: Failed to save manifest during close: %v\n", err)
		}
	}

	// Close all resources
	currentState = vs.state.Load().(*VibeStoreState)
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

// TriggerCompaction manually triggers a compaction cycle
// Returns true if a compaction was triggered, false if no eligible segments were found
func (vs *VibeStore) TriggerCompaction() bool {
	// When called explicitly, allow compaction even if automatic compaction is disabled
	return vs.triggerCompaction(true)
}

// findCompactionPair identifies the best pair of segments to compact
// Returns indices of left and right segments, or -1,-1 if no eligible pair found
func (vs *VibeStore) findCompactionPair() (int, int) {
	currentState := vs.state.Load().(*VibeStoreState)
	segments := currentState.segments

	// Need at least 2 segments to compact
	if len(segments) < 2 {
		return -1, -1
	}

	// Start from the oldest segments (index 0) and move forward
	for i := 0; i < len(segments)-1; i++ {
		leftSegment := segments[i]
		rightSegment := segments[i+1]

		// Check if these segments have the same level
		if leftSegment.Level() == rightSegment.Level() {
			return i, i + 1
		}
	}

	// No eligible pairs found
	return -1, -1
}

// triggerCompaction checks if compaction is needed and schedules it
// Returns true if a compaction was triggered, false otherwise
// The isManual parameter indicates whether this is a manual trigger (true) or automatic (false)
func (vs *VibeStore) triggerCompaction(isManual bool) bool {
	// If compaction is disabled and this is NOT a manual request, do nothing
	if vs.options.DisableCompaction && !isManual {
		// Only log when we're blocking an automatic compaction
		// fmt.Printf("Automatic compaction is disabled via options.DisableCompaction\n")
		return false
	}

	// Check if a compaction is already running or scheduled
	if !vs.isCompacting.CompareAndSwap(false, true) {
		// fmt.Printf("Skipping compaction trigger: already compacting\n")
		return false // Already compacting or another trigger is in progress
	}

	// --- Compaction lock acquired --- //
	// Defer unsetting the flag ONLY if we fail to find/queue a pair
	failToQueue := true
	defer func() {
		if failToQueue {
			vs.isCompacting.Store(false)
		}
	}()

	// Find a pair of segments to compact
	leftIdx, rightIdx := vs.findCompactionPair()

	// If no eligible pair, return without scheduling a task
	if leftIdx < 0 || rightIdx < 0 {
		fmt.Printf("No eligible segment pairs found for compaction\n")
		// We didn't queue, so unset the flag via defer
		return false
	}

	// Get segments to compact
	currentState := vs.state.Load().(*VibeStoreState)
	// Check if segments still exist (could have been compacted away)
	if leftIdx >= len(currentState.segments) || rightIdx >= len(currentState.segments) {
		fmt.Printf("Segments at index %d or %d no longer exist in current state\n", leftIdx, rightIdx)
		// We didn't queue, so unset the flag via defer
		return false
	}
	leftSegment := currentState.segments[leftIdx]
	rightSegment := currentState.segments[rightIdx]

	fmt.Printf("Found eligible segments for compaction: index %d (level %d) and %d (level %d)\n",
		leftIdx, leftSegment.Level(), rightIdx, rightSegment.Level())

	// Schedule compaction task
	vs.taskQueue <- task{
		taskType: taskCompaction,
		segments: []*col.Reader{leftSegment, rightSegment},
	}

	failToQueue = false // We successfully queued, the flag will be unset by processTask
	return true
}

// doCompaction performs segment compaction (called by the background worker)
func (vs *VibeStore) doCompaction(segments []*col.Reader) {
	if len(segments) != 2 {
		fmt.Printf("Error: Invalid number of segments for compaction: %d\n", len(segments))
		return
	}

	leftSegment := segments[0]
	rightSegment := segments[1]

	// Create a new segment file path
	timestamp := time.Now().UnixNano()
	filename := fmt.Sprintf("compacted_%d.col", timestamp)
	outputPath := filepath.Join(vs.options.DataDir, filename)

	fmt.Printf("Compacting segments with levels %d and %d to: %s\n",
		leftSegment.Level(), rightSegment.Level(), outputPath)

	// Record start time
	startTime := time.Now()

	// Perform compaction using the multicol.Compact function
	// This function reads from the two input segments and writes the combined,
	// sorted, and deduplicated data to the output path.
	err := multicol.Compact(leftSegment, rightSegment, outputPath, vs.options.CompactionOptions)

	// Calculate duration
	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("Error compacting segments after %.2f seconds: %v\n", duration.Seconds(), err)
		// Attempt to clean up the potentially partial output file
		_ = os.Remove(outputPath)
		return
	}

	// Open the new compacted segment
	newSegment, err := col.NewReader(outputPath)
	if err != nil {
		fmt.Printf("Error opening compacted segment: %v\n", err)
		// If we can't open the new segment, it's best to remove it
		_ = os.Remove(outputPath)
		return
	}

	// Atomically update the store state
	vs.stateLock.Lock()
	currentState := vs.state.Load().(*VibeStoreState)

	// Find the positions of the segments in the *current* state again,
	// as the state might have changed while we were compacting.
	leftPos := -1
	rightPos := -1
	for i, segment := range currentState.segments {
		if segment == leftSegment {
			leftPos = i
		} else if segment == rightSegment {
			rightPos = i
		}
	}

	// If segments are not consecutive or not found in the current state, abort.
	if leftPos < 0 || rightPos < 0 || rightPos != leftPos+1 {
		vs.stateLock.Unlock()
		newSegment.Close()
		_ = os.Remove(outputPath)
		fmt.Printf("Segments [%s, %s] are no longer valid for compaction in the current state\n",
			leftSegment.FilePath(), rightSegment.FilePath())
		return
	}

	// Create the new list of segments for the next state
	// IMPORTANT: Reopen existing segments to get new file handles
	newSegments := make([]*col.Reader, 0, len(currentState.segments)-1)

	// Reopen segments before the compacted range
	for i := 0; i < leftPos; i++ {
		reopenedSegment, err := col.NewReader(currentState.segments[i].FilePath())
		if err != nil {
			vs.stateLock.Unlock()
			newSegment.Close()
			_ = os.Remove(outputPath)
			fmt.Printf("ERROR: Failed to reopen segment %s during compaction state update: %v\n", currentState.segments[i].FilePath(), err)
			// Note: This leaves the store in a potentially inconsistent state. Recovery might be needed.
			return
		}
		newSegments = append(newSegments, reopenedSegment)
	}

	// Add the newly compacted segment
	newSegments = append(newSegments, newSegment)

	// Reopen segments after the compacted range
	for i := rightPos + 1; i < len(currentState.segments); i++ {
		reopenedSegment, err := col.NewReader(currentState.segments[i].FilePath())
		if err != nil {
			vs.stateLock.Unlock()
			newSegment.Close()
			_ = os.Remove(outputPath)
			// Close previously reopened segments in the new list
			for _, seg := range newSegments {
				seg.Close()
			}
			fmt.Printf("ERROR: Failed to reopen segment %s during compaction state update: %v\n", currentState.segments[i].FilePath(), err)
			return
		}
		newSegments = append(newSegments, reopenedSegment)
	}

	fmt.Printf("Compaction complete in %.2f seconds. Replacing segments at index %d and %d with new segment (level %d)\n",
		duration.Seconds(), leftPos, rightPos, newSegment.Level())

	// Create the new state object
	newState := &VibeStoreState{
		activeMemtable:    currentState.activeMemtable,
		activeSince:       currentState.activeSince,
		flushingMemtables: currentState.flushingMemtables, // Memtables are handled separately
		segments:          newSegments,
		multiReader:       nil, // Invalidate cache, will be recreated on demand
	}

	// Atomically swap the state
	oldState := currentState
	vs.state.Store(newState)
	vs.stateLock.Unlock() // Release the lock *after* the atomic swap

	// Save the updated manifest reflecting the new segment list
	if err := vs.saveManifest(); err != nil {
		fmt.Printf("Warning: Failed to save manifest after compaction: %v\n", err)
	}

	// Schedule the cleanup of the old state
	vs.taskQueue <- task{
		taskType: taskCleanup,
		oldState: oldState,
	}
}

// compactionChecker periodically checks if compaction is needed
func (vs *VibeStore) compactionChecker() {
	defer vs.workerWG.Done() // Ensure Done is called when goroutine exits
	// If compaction is disabled, don't run the checker
	if vs.options.DisableCompaction {
		// Just wait for shutdown signal
		<-vs.shutdown
		return
	}

	// Use the configured interval or default to 30 seconds
	intervalMs := vs.options.CompactionCheckIntervalMs
	if intervalMs <= 0 {
		intervalMs = 30000 // Default to 30 seconds
	}

	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = vs.triggerCompaction(false) // This is an automatic compaction
		case <-vs.shutdown:
			return
		}
	}
}

// GetSegmentLevels returns a list of the current segment levels from oldest to newest
// This is primarily for debugging and monitoring the compaction process
func (vs *VibeStore) GetSegmentLevels() []uint16 {
	currentState := vs.state.Load().(*VibeStoreState)

	levels := make([]uint16, len(currentState.segments))
	for i, segment := range currentState.segments {
		levels[i] = segment.Level()
	}

	return levels
}

// IsCompacting returns true if a compaction task is currently believed to be running or scheduled.
func (vs *VibeStore) IsCompacting() bool {
	return vs.isCompacting.Load()
}

// GetStateForDebug returns the current internal state pointer.
// WARNING: Use only for debugging purposes. Modifying the state
// directly can lead to corruption.
func (vs *VibeStore) GetStateForDebug() *VibeStoreState {
	return vs.state.Load().(*VibeStoreState)
}

// SegmentManifest represents the persisted state of the store
type SegmentManifest struct {
	ActiveSegments []SegmentInfo `json:"active_segments"`
	LastUpdated    time.Time     `json:"last_updated"`
	Version        int           `json:"version"` // For future format changes
}

// SegmentInfo contains metadata about a segment
type SegmentInfo struct {
	FilePath string `json:"file_path"`
	Level    uint16 `json:"level"`
}

// saveManifest persists the current state to disk
func (vs *VibeStore) saveManifest() error {
	vs.stateLock.RLock()
	currentState := vs.state.Load().(*VibeStoreState)

	// Build the manifest
	manifest := SegmentManifest{
		ActiveSegments: make([]SegmentInfo, len(currentState.segments)),
		LastUpdated:    time.Now(),
		Version:        1,
	}

	// Populate segment info
	for i, segment := range currentState.segments {
		manifest.ActiveSegments[i] = SegmentInfo{
			FilePath: segment.FilePath(),
			Level:    segment.Level(),
		}
	}
	vs.stateLock.RUnlock()

	// Marshal to JSON
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Write to a temporary file first
	manifestPath := filepath.Join(vs.options.DataDir, "manifest.json")
	tempPath := manifestPath + ".tmp"

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary manifest: %w", err)
	}

	// Rename for atomic update
	if err := os.Rename(tempPath, manifestPath); err != nil {
		return fmt.Errorf("failed to finalize manifest: %w", err)
	}

	return nil
}

// loadManifest loads the persisted state from disk
func (vs *VibeStore) loadManifest() error {
	manifestPath := filepath.Join(vs.options.DataDir, "manifest.json")

	// Check if manifest exists
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		// No manifest, use empty state
		return nil
	}

	// Read the manifest file
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}

	// Parse the manifest
	var manifest SegmentManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("failed to parse manifest: %w", err)
	}

	// Load all segments
	segments := make([]*col.Reader, 0, len(manifest.ActiveSegments))
	for _, info := range manifest.ActiveSegments {
		segment, err := col.NewReader(info.FilePath)
		if err != nil {
			fmt.Printf("Warning: Failed to open segment %s: %v\n", info.FilePath, err)
			continue
		}
		segments = append(segments, segment)
	}

	// Create a new state with the loaded segments
	vs.stateLock.Lock()
	currentState := vs.state.Load().(*VibeStoreState)

	newState := &VibeStoreState{
		activeMemtable:    currentState.activeMemtable,    // Keep the active memtable
		activeSince:       currentState.activeSince,       // Keep the activation time
		flushingMemtables: currentState.flushingMemtables, // Keep any flushing memtables
		segments:          segments,                       // Set the loaded segments
		multiReader:       nil,                            // Will be created on demand
	}

	vs.state.Store(newState)
	vs.stateLock.Unlock()

	fmt.Printf("Loaded %d segments from manifest\n", len(segments))
	return nil
}
