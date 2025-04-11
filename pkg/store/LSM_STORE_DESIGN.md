# LSM Store Design for Vibe LSM

This document outlines the design of a Log-Structured Merge (LSM) tree storage engine built on the existing `multicol` package.

## Overview

The LSM store will manage:
1. An active memtable that accepts writes
2. Zero or more flushing memtables
3. Multiple on-disk segments organized in levels
4. A single background thread for coordinating maintenance tasks

## Core Components

### VibeStore

Main storage engine that coordinates all LSM components.

```go
// VibeStore represents a complete LSM store with multiple levels
type VibeStore struct {
    // Atomic reference to current store state
    state         atomic.Value // Holds *VibeStoreState
    
    // Lock for state transitions
    stateLock     sync.RWMutex
    
    // Background task coordination
    taskQueue     chan task
    shutdown      chan struct{}
    
    // Configuration
    options       VibeStoreOptions
    flushTrigger  int64
}

// VibeStoreOptions defines configuration for the LSM store
type VibeStoreOptions struct {
    // Directory for segment files
    DataDir string
    
    // Memtable options
    MemtableSize int64
    MemtableOptions *multicol.MemtableOptions
    
    // Segment options
    CompactionOptions multicol.CompactionOptions
    
    // Background tasks
    MaxSegmentsBeforeCompaction int
}
```

### VibeStoreState

Immutable snapshot of the store's state, allowing atomic transitions.

```go
// VibeStoreState represents a consistent snapshot of the store
type VibeStoreState struct {
    activeMemtable     multicol.Memtable
    flushingMemtables  []multicol.Memtable
    segments           []*col.Reader
    readerCount        atomic.Int32
    
    // Cached MultiReader for efficient queries
    multiReader        *multicol.MultiReader
}
```

### Background Coordinator

Single thread that handles maintenance tasks in sequence.

```go
// Task types for the background worker
type taskType int

const (
    taskFlush taskType = iota
    taskCompaction
    taskCleanup
)

// Task definition for coordinator
type task struct {
    taskType     taskType
    memtable     multicol.Memtable    // For flush tasks
    segments     []*col.Reader        // For compaction tasks
    oldState     *VibeStoreState      // For cleanup tasks
}
```

## Core Operations

### Write Path

1. Acquire read lock on `stateLock`
2. Get reference to active memtable from current state
3. Release read lock
4. Write to active memtable
5. Check if memtable size exceeds flush threshold
6. If threshold exceeded, trigger memtable flush

```go
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
    
    if memtable.ActiveCount() >= vs.flushTrigger {
        vs.triggerFlush()
    }
    
    return nil
}
```

### Read Path

1. Acquire read lock on `stateLock`
2. Get reference to current state and increment reader count
3. Release read lock
4. Get or create MultiReader from state
5. Perform query operations through MultiReader
6. Decrement reader count when done

```go
// Get retrieves a value for a key
func (vs *VibeStore) Get(id uint64) (int64, bool) {
    vs.stateLock.RLock()
    currentState := vs.state.Load().(*VibeStoreState)
    currentState.readerCount.Add(1)
    vs.stateLock.RUnlock()
    
    defer currentState.readerCount.Add(-1)
    
    // Use MultiReader for consistent lookups
    return currentState.getMultiReader().Get(id)
}
```

### Memtable Flush

1. Create new memtable
2. Swap active and flushing memtables under write lock
3. Schedule flush task for background worker
4. Background worker flushes memtable to disk
5. Create new segment from flushed memtable
6. Update state to add new segment and remove flushing memtable

```go
// triggerFlush schedules a flush task for the active memtable
func (vs *VibeStore) triggerFlush() {
    vs.stateLock.Lock()
    currentState := vs.state.Load().(*VibeStoreState)
    oldMemtable := currentState.activeMemtable
    
    newMemtable := multicol.NewMemtable(vs.options.MemtableOptions)
    
    newState := &VibeStoreState{
        activeMemtable: newMemtable,
        flushingMemtables: append(currentState.flushingMemtables, oldMemtable),
        segments: currentState.segments,
    }
    
    vs.state.Store(newState)
    vs.stateLock.Unlock()
    
    vs.taskQueue <- task{
        taskType: taskFlush,
        memtable: oldMemtable,
    }
}
```

### Compaction

1. Background worker selects segments for compaction
2. Compact segments into a new segment
3. Update state to replace old segments with new compacted segment
4. Clean up old segment files

```go
// doCompaction performs segment compaction (called by coordinator)
func (vs *VibeStore) doCompaction(segments []*col.Reader) {
    // Perform compaction to create a new segment
    // Update state to replace old segments with compacted one
    // Close and delete original segment files
}
```

## State Transitions

All state transitions follow the same pattern:
1. Create a new state object with desired changes
2. Atomically replace old state with new state
3. Schedule cleanup for old state when safe

This approach ensures:
- Reads see a consistent view of the store
- Writes proceed uninterrupted during maintenance
- Resources are safely cleaned up when no longer needed

## Background Coordinator Thread

```go
// startBackgroundWorker launches the single coordinator thread
func (vs *VibeStore) startBackgroundWorker() {
    vs.taskQueue = make(chan task, 100)
    vs.shutdown = make(chan struct{})
    
    go func() {
        for {
            select {
            case task := <-vs.taskQueue:
                vs.processTask(task)
            case <-vs.shutdown:
                return
            }
        }
    }()
}
```

The coordinator thread:
1. Processes one task at a time, in queue order
2. Handles all maintenance operations (flush, compaction, cleanup)
3. Checks for follow-up tasks after each task completes
4. Can prioritize critical operations (e.g., memtable flush)

## Benefits of Single-Coordinator Design

1. **Simplified Concurrency**: No coordination required between concurrent background operations.
2. **Reduced Lock Contention**: State lock only held briefly during transitions.
3. **Predictable Resource Usage**: Only one background task runs at a time.
4. **Better Control**: Clear sequencing and prioritization of maintenance tasks.
5. **Simpler Error Handling**: Centralized error handling for background operations.

## MultiReader Caching

To optimize query performance:
1. Each state snapshot maintains a cached MultiReader
2. MultiReader created on first use and reused for subsequent queries
3. Ensures consistent query results across a state snapshot
4. Eliminates overhead of repeatedly constructing MultiReaders

## Future Enhancements

1. Tiered compaction strategy
2. Bloom filters for faster negative lookups
3. Write-ahead logging for crash recovery
4. Memory-mapped I/O for improved performance
5. Range delete optimization 