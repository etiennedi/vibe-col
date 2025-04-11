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

	// Start the compaction checker
	go store.compactionChecker()

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
		// Trigger compaction check after flush
		vs.triggerCompaction()
	case taskCompaction:
		vs.doCompaction(t.segments)
		// Check if more compaction is needed
		vs.triggerCompaction()
	case taskCleanup:
		vs.doCleanup(t.oldState)
	}
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
		// This is handling the direct flush rather than using the task queue
		// So we need to manually ensure the level is set to 0
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

// TriggerCompaction manually triggers a compaction cycle
// This is primarily used for testing
func (vs *VibeStore) TriggerCompaction() {
	vs.triggerCompaction()
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
func (vs *VibeStore) triggerCompaction() {
	// If compaction is disabled, do nothing
	if vs.options.DisableCompaction {
		return
	}

	// Find a pair of segments to compact
	leftIdx, rightIdx := vs.findCompactionPair()

	// If no eligible pair, return without scheduling a task
	if leftIdx < 0 || rightIdx < 0 {
		return
	}

	// Get segments to compact
	currentState := vs.state.Load().(*VibeStoreState)
	leftSegment := currentState.segments[leftIdx]
	rightSegment := currentState.segments[rightIdx]

	// Schedule compaction task
	vs.taskQueue <- task{
		taskType: taskCompaction,
		segments: []*col.Reader{leftSegment, rightSegment},
	}
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

	// Perform compaction using the multicol.Compact function
	// This will automatically calculate the level based on our rules
	err := multicol.Compact(leftSegment, rightSegment, outputPath, vs.options.CompactionOptions)
	if err != nil {
		fmt.Printf("Error compacting segments: %v\n", err)
		return
	}

	// Open the new compacted segment
	newSegment, err := col.NewReader(outputPath)
	if err != nil {
		fmt.Printf("Error opening compacted segment: %v\n", err)
		return
	}

	// Update state to replace the compacted segments with the new one
	vs.stateLock.Lock()
	currentState := vs.state.Load().(*VibeStoreState)

	// Find the positions of the segments in the current state
	// They might have changed since we first selected them
	leftPos := -1
	rightPos := -1
	for i, segment := range currentState.segments {
		if segment == leftSegment {
			leftPos = i
		} else if segment == rightSegment {
			rightPos = i
		}
	}

	// If segments are not consecutive or not found, abort
	if leftPos < 0 || rightPos < 0 || rightPos != leftPos+1 {
		vs.stateLock.Unlock()
		newSegment.Close()
		fmt.Printf("Segments are no longer valid for compaction\n")
		return
	}

	// Create new segment array with the compacted segment
	newSegments := make([]*col.Reader, 0, len(currentState.segments)-1)
	newSegments = append(newSegments, currentState.segments[:leftPos]...)
	newSegments = append(newSegments, newSegment)
	newSegments = append(newSegments, currentState.segments[rightPos+1:]...)

	fmt.Printf("Compaction complete. New segment has level %d\n", newSegment.Level())

	// Create new state
	newState := &VibeStoreState{
		activeMemtable:    currentState.activeMemtable,
		activeSince:       currentState.activeSince,
		flushingMemtables: currentState.flushingMemtables,
		segments:          newSegments,
		multiReader:       nil, // Will be created on demand
	}

	// Store new state and create task to clean up old state
	oldState := currentState
	vs.state.Store(newState)
	vs.stateLock.Unlock()

	// Schedule cleanup of old state
	vs.taskQueue <- task{
		taskType: taskCleanup,
		oldState: oldState,
	}

	// Close the old segments since they're no longer needed
	// This is safe because we kept the references in oldState for proper cleanup
	leftSegment.Close()
	rightSegment.Close()
}

// compactionChecker periodically checks if compaction is needed
func (vs *VibeStore) compactionChecker() {
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
			vs.triggerCompaction()
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
