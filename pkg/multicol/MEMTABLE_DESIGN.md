# Memtable Design for Vibe-LSM

This document outlines the design for a high-performance memtable to be implemented in the Vibe-LSM project.

## Design Goals

- High-throughput, concurrent write operations
- Support for reads and aggregations (though optimized for writes)
- Support for deletion
- Efficient flushing to column files
- Lock-free reading with concurrent modification support

## Core Data Structure

The memtable is based on a concurrent skip list with the following characteristics:

1. **Striped Locking**: Divides key space into multiple stripes to reduce lock contention
2. **Lock-Free Reading**: Readers can traverse the structure without acquiring locks
3. **Logical Deletion**: Deleted entries are marked with a flag but remain in the structure until flush

## Interface

```go
type Memtable interface {
    // Add a single ID-value pair
    Add(id uint64, value int64) error
    
    // BatchAdd multiple ID-value pairs
    BatchAdd(ids []uint64, values []int64) error
    
    // Delete marks an entry as deleted
    Delete(id uint64) bool
    
    // BatchDelete marks multiple entries as deleted
    BatchDelete(ids []uint64) int
    
    // Get the value for a specific ID (respects deletion)
    Get(id uint64) (int64, bool)
    
    // Scan returns key-value pairs within a range (skipping deleted entries)
    Scan(startID, endID uint64) ([]uint64, []int64)
    
    // Aggregate performs unfiltered aggregation operations
    // Returns: min ID, max ID, min value, max value, sum of values, count
    Aggregate() (uint64, uint64, int64, int64, int64, int)
    
    // FilteredAggregate performs aggregation on IDs present in the filter
    // Returns: min ID, max ID, min value, max value, sum of values, count
    FilteredAggregate(filter *bitmap.Bitmap) (uint64, uint64, int64, int64, int64, int)
    
    // Flush writes the non-deleted contents to the specified path
    Flush(path string) error
    
    // ActiveCount returns the number of non-deleted entries
    ActiveCount() int64
    
    // IsEmpty checks if the memtable has any non-deleted entries
    IsEmpty() bool
}
```

## Implementation Details

### Concurrent Skip List Node

```go
type skipNode struct {
    key        uint64
    value      int64
    next       []*skipNode 
    mu         sync.Mutex     // For node-level locking during updates
    deleted    atomic.Bool    // Atomic flag for logical deletion
}
```

### Main Memtable Structure

```go
type Memtable struct {
    skipList            *ConcurrentSkipList
    stripeLocks         [numStripes]sync.RWMutex
    entryCount          atomic.Int64   // Total entries added
    logicalDeleteCount  atomic.Int64   // Total entries marked as deleted
}
```

### Concurrent Skip List Structure

```go
type ConcurrentSkipList struct {
    head       *skipNode      // Head of the list
    maxHeight  int            // Maximum height
    height     int            // Current height
    rng        *rand.Rand     // Random number generator for level determination
    size       atomic.Int64   // Number of elements
}
```

## Core Operations Implementation

### Add Operation

```go
func (m *Memtable) Add(id uint64, value int64) error {
    stripeIdx := id % numStripes
    m.stripeLocks[stripeIdx].Lock()
    defer m.stripeLocks[stripeIdx].Unlock()
    
    // Add to skip list
    m.skipList.Insert(id, value)
    
    // Update entry count
    m.entryCount.Add(1)
    
    return nil
}
```

### BatchAdd Operation

```go
func (m *Memtable) BatchAdd(ids []uint64, values []int64) error {
    // Sort by stripe to minimize lock contention
    entriesByStripe := make(map[uint64][][2]interface{})
    
    // Group entries by stripe
    for i, id := range ids {
        stripe := id % numStripes
        entriesByStripe[stripe] = append(entriesByStripe[stripe], [2]interface{}{id, values[i]})
    }
    
    // Process each stripe sequentially with a single lock acquisition
    for stripe, entries := range entriesByStripe {
        m.stripeLocks[stripe].Lock()
        
        for _, entry := range entries {
            id, value := entry[0].(uint64), entry[1].(int64)
            m.skipList.Insert(id, value)
        }
        
        m.stripeLocks[stripe].Unlock()
    }
    
    // Update entry count
    m.entryCount.Add(int64(len(ids)))
    
    return nil
}
```

### Delete Operation

```go
func (m *Memtable) Delete(id uint64) bool {
    stripeIdx := id % numStripes
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
    // Use atomic operation to ensure visibility to readers
    if candidate.deleted.CompareAndSwap(false, true) {
        // First time this node was deleted
        m.logicalDeleteCount.Add(1)
        return true
    }
    
    // Node was already marked as deleted
    return false
}
```

### BatchDelete Operation

```go
func (m *Memtable) BatchDelete(ids []uint64) int {
    if len(ids) == 0 {
        return 0
    }
    
    // Sort IDs for more efficient deletion
    sort.Slice(ids, func(i, j int) bool {
        return ids[i] < ids[j]
    })
    
    // Group by stripe for efficient locking
    entriesByStripe := make(map[uint64][]uint64)
    for _, id := range ids {
        stripe := id % numStripes
        entriesByStripe[stripe] = append(entriesByStripe[stripe], id)
    }
    
    deletedCount := 0
    
    // Process each stripe
    for stripe, stripeIDs := range entriesByStripe {
        // Acquire read lock for this stripe (only need read lock to mark as deleted)
        m.stripeLocks[stripe].RLock()
        
        for _, id := range stripeIDs {
            // Find the node
            current := m.skipList.head
            
            // Search for the node (similar to Get)
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
```

### Get Operation

```go
func (m *Memtable) Get(id uint64) (int64, bool) {
    // No locks needed for reading
    current := m.skipList.head
    
    // Traverse from top to bottom level
    for level := m.skipList.height - 1; level >= 0; level-- {
        for current.next[level] != nil && current.next[level].key < id {
            current = current.next[level]
        }
    }
    
    // Check if node exists at bottom level
    current = current.next[0]
    if current != nil && current.key == id {
        // Check if the node is marked as deleted
        if current.deleted.Load() {
            // Node exists but is logically deleted
            return 0, false
        }
        return current.value, true
    }
    
    return 0, false
}
```

### Scan Operation

```go
func (m *Memtable) Scan(startID, endID uint64) ([]uint64, []int64) {
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
```

### Aggregate Operation

```go
func (m *Memtable) Aggregate() (uint64, uint64, int64, int64, int64, int) {
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
```

### FilteredAggregate Operation

```go
func (m *Memtable) FilteredAggregate(filter *bitmap.Bitmap) (uint64, uint64, int64, int64, int64, int) {
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
```

### Flush Operation

```go
func (m *Memtable) Flush(path string) error {
    // Create a BufferedWriter
    writer, err := col.NewBufferedWriter(path)
    if err != nil {
        return err
    }
    defer writer.Close()
    
    // Temporary buffers for batch processing
    const batchSize = 10000
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
```

### Utility Methods

```go
// Returns the number of non-deleted entries
func (m *Memtable) ActiveCount() int64 {
    return m.entryCount.Load() - m.logicalDeleteCount.Load()
}

// Checks if the memtable has any non-deleted entries
func (m *Memtable) IsEmpty() bool {
    return m.ActiveCount() == 0
}
```

## Skip List Implementation Details

### Insertion Algorithm

```go
func (sl *ConcurrentSkipList) Insert(key uint64, value int64) {
    // Maintain array of nodes to update at each level
    update := make([]*skipNode, sl.maxHeight)
    current := sl.head
    
    // Find position to insert without locking
    for level := sl.height - 1; level >= 0; level-- {
        for current.next[level] != nil && current.next[level].key < key {
            current = current.next[level]
        }
        update[level] = current
    }
    
    // Check if key already exists
    current = current.next[0]
    if current != nil && current.key == key {
        // Update value with lock
        current.mu.Lock()
        current.value = value
        current.mu.Unlock()
        return
    }
    
    // Generate random height for new node
    newLevel := sl.randomLevel()
    if newLevel > sl.height {
        // Extend height if needed
        for level := sl.height; level < newLevel; level++ {
            update[level] = sl.head
        }
        sl.height = newLevel
    }
    
    // Create new node
    newNode := &skipNode{
        key:   key,
        value: value,
        next:  make([]*skipNode, newLevel),
    }
    
    // Insert node at all levels
    for level := 0; level < newLevel; level++ {
        // Acquire lock on predecessor node to update its next pointer
        update[level].mu.Lock()
        newNode.next[level] = update[level].next[level]
        update[level].next[level] = newNode
        update[level].mu.Unlock()
    }
    
    // Update size
    sl.size.Add(1)
}
```

### Iterator Implementation

```go
type SkipListIterator struct {
    list     *ConcurrentSkipList
    current  *skipNode
}

func (it *SkipListIterator) Next() bool {
    // Advance to next node
    if it.current == nil {
        it.current = it.list.head.next[0]
    } else {
        it.current = it.current.next[0]
    }
    
    return it.current != nil
}

func (it *SkipListIterator) Key() uint64 {
    return it.current.key
}

func (it *SkipListIterator) Value() int64 {
    return it.current.value
}

func (it *SkipListIterator) IsDeleted() bool {
    return it.current.deleted.Load()
}
```

### Range Iterator Implementation

```go
type SkipListRangeIterator struct {
    iterator *SkipListIterator
    endID    uint64
}

func (sl *ConcurrentSkipList) RangeIterator(startID, endID uint64) *SkipListRangeIterator {
    // Find starting position
    current := sl.head
    for level := sl.height - 1; level >= 0; level-- {
        for current.next[level] != nil && current.next[level].key < startID {
            current = current.next[level]
        }
    }
    
    iterator := &SkipListIterator{
        list:    sl,
        current: current, // Start before the first node >= startID
    }
    
    return &SkipListRangeIterator{
        iterator: iterator,
        endID:    endID,
    }
}

func (it *SkipListRangeIterator) Next() bool {
    if !it.iterator.Next() {
        return false
    }
    
    if it.iterator.Key() > it.endID {
        return false
    }
    
    return true
}

func (it *SkipListRangeIterator) Key() uint64 {
    return it.iterator.Key()
}

func (it *SkipListRangeIterator) Value() int64 {
    return it.iterator.Value()
}

func (it *SkipListRangeIterator) IsDeleted() bool {
    return it.iterator.IsDeleted()
}
```

## Write Optimization Techniques

### Fast Path for Sequential Writes

```go
func (m *Memtable) Add(id uint64, value int64) error {
    // Fast path for strictly increasing IDs
    if atomic.LoadUint64(&m.lastID) < id {
        // Try to update lastID atomically
        if atomic.CompareAndSwapUint64(&m.lastID, m.lastID, id) {
            // Got the fast path, use a specialized lock
            m.sequentialLock.Lock()
            defer m.sequentialLock.Unlock()
            
            m.skipList.Insert(id, value)
            m.entryCount.Add(1)
            return nil
        }
    }
    
    // Fall back to regular striped locking for random access
    stripeIdx := id % numStripes
    m.stripeLocks[stripeIdx].Lock()
    defer m.stripeLocks[stripeIdx].Unlock()
    
    m.skipList.Insert(id, value)
    m.entryCount.Add(1)
    return nil
}
```

### Sorted Batch Insertion

```go
func (m *Memtable) BatchAddSorted(ids []uint64, values []int64) error {
    // For pre-sorted data, we can optimize by using a single lock
    // and bulk insertion technique
    if len(ids) == 0 {
        return nil
    }
    
    // Verify data is sorted
    isSorted := true
    for i := 1; i < len(ids); i++ {
        if ids[i] <= ids[i-1] {
            isSorted = false
            break
        }
    }
    
    if isSorted {
        // Fast path for sorted data
        m.sequentialLock.Lock()
        defer m.sequentialLock.Unlock()
        
        // Use bulk insertion
        m.skipList.BulkInsert(ids, values)
        
        // Update entry count
        m.entryCount.Add(int64(len(ids)))
        
        // Update last ID
        if len(ids) > 0 {
            atomic.StoreUint64(&m.lastID, ids[len(ids)-1])
        }
        
        return nil
    }
    
    // Fall back to regular batch add for unsorted data
    return m.BatchAdd(ids, values)
}
```

## Performance Characteristics

### Concurrency Model

1. **Reader/Writer Separation**:
   - Readers never block writers
   - Writers only block other writers affecting the same stripe
   - Deletion operations use read locks

2. **Lock Granularity**:
   - Striped locks (default: 256 stripes) for general operations
   - Node-level locks for skip list structure modifications
   - Specialized fast path for sequential writes

### Expected Performance

1. **Write Operations**:
   - Single writes: O(log n) complexity with minimal contention
   - Batch writes: Amortized O(log n) per entry with stripe optimization
   - Sequential writes: Near O(1) with fast path

2. **Read Operations**:
   - Get: O(log n) with zero contention (lock-free)
   - Scan: O(log n + k) where k is the number of entries in range
   - Aggregation: O(n) full scan required

3. **Deletion Operations**:
   - Single deletion: O(log n) with minimal contention (read lock only)
   - Batch deletion: Amortized O(log n) per entry with stripe optimization

### Memory Usage

1. **Per-Entry Overhead**:
   - ID: 8 bytes
   - Value: 8 bytes
   - Next pointers: ~2-3 pointers on average (16-24 bytes)
   - Deleted flag: 1 byte
   - Mutex: ~40 bytes
   - Total: ~73-81 bytes per entry

2. **Overall Efficiency**:
   - Expected ~100 bytes per entry including skip list overhead
   - For 1 million entries: ~100MB memory usage

## Implementation Recommendations

1. **Tuning Opportunities**:
   - Number of stripes: Adjust based on expected concurrency
   - Skip list max height: Adjust based on expected size
   - Batch sizes for flush: Adjust based on memory constraints

2. **Testing Strategies**:
   - Concurrent stress testing with mixed operations
   - Test with various key distributions (sequential, random, etc.)
   - Test with heavy deletion workloads

3. **Future Enhancements**:
   - Custom memory allocator for skip list nodes
   - Bloom filter for fast negative lookups
   - Adaptive stripe sizing based on contention monitoring

## Limitations and Constraints

1. **Not Durable**: Memtable is an in-memory structure and does not provide durability guarantees
2. **Limited Size**: Should be used until reaching a reasonable size, then flushed
3. **Deletion Overhead**: Logical deletion means space is not reclaimed until flush
4. **Aggregation Performance**: Unfiltered aggregation requires full scans

## Conclusion

This memtable design provides an excellent foundation for the Vibe-LSM storage engine, offering:

1. High concurrency for writes with minimal contention
2. Efficient support for reads and aggregations
3. Simple but effective deletion mechanism
4. Lock-free reading with serializable consistency
5. Efficient flushing to column files

When implemented properly, it should deliver the performance characteristics needed for high-throughput time-series data ingestion while supporting the required analytical capabilities. 