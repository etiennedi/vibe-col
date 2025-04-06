# Memtable Implementation

## Overview

The memtable is a core in-memory data structure in VibeDB that supports efficient reads and writes. This document outlines the implementation choices and performance characteristics.

## Current Implementation

The current memtable implementation uses a `sync.Map` as its underlying data structure. This choice was made after benchmarking revealed that:

1. A simple `sync.Map`-based approach performed better than a custom skiplist implementation for concurrent random access patterns.
2. The implementation is much simpler and more maintainable.

## Performance Characteristics

### Concurrent Write Performance

Benchmark results show that the current implementation provides excellent concurrent write performance:

```
BenchmarkMemtableRandomAccess/Memtable-10   60   54915701 ns/op
```

This represents approximately 18.2 million operations per second with 36 concurrent writers on 100,000 elements.

### Implementation Advantages

1. **Built-in Concurrency Support**: The `sync.Map` is specifically designed for high-read, concurrent access patterns with built-in lock-free reads.

2. **Simplicity**: The implementation is much simpler than a custom skiplist, making it easier to maintain and understand.

3. **Performance**: Extensive benchmarking showed that this implementation outperforms custom skiplist implementations with complex locking schemes.

## Interface

The memtable implements the following interface:

```go
type Memtable interface {
    // Add adds a single ID-value pair
    Add(id uint64, value int64) error
    
    // BatchAdd adds multiple ID-value pairs
    BatchAdd(ids []uint64, values []int64) error
    
    // Delete marks an entry as deleted
    Delete(id uint64) bool
    
    // BatchDelete marks multiple entries as deleted
    BatchDelete(ids []uint64) int
    
    // Get returns the value for a specific ID
    Get(id uint64) (int64, bool)
    
    // Scan returns key-value pairs within a range
    Scan(startID, endID uint64) ([]uint64, []int64)
    
    // ScanIterator returns an iterator for entries in a range
    ScanIterator(startID, endID uint64) Iterator
    
    // Aggregate performs unfiltered aggregation operations
    Aggregate() (uint64, uint64, int64, int64, int64, int)
    
    // FilteredAggregate performs aggregation on specified IDs
    FilteredAggregate(filter *sroar.Bitmap) (uint64, uint64, int64, int64, int64, int)
    
    // Flush writes contents to disk
    Flush(path string) (uint64, error)
    
    // ActiveCount returns the number of entries
    ActiveCount() int64
    
    // IsEmpty checks if the memtable is empty
    IsEmpty() bool
}
```

## Future Considerations

While the current implementation performs well, there are potential areas for future optimization:

1. **Sorted Iteration**: If sorted traversal becomes a performance bottleneck, we could explore hybrid approaches that maintain sorted segments.

2. **Custom Sharding**: For extremely high concurrency scenarios, explicit sharding could be implemented on top of multiple `sync.Map` instances.

3. **Memory Efficiency**: The current implementation focuses on performance. If memory usage becomes a concern, more compact representations could be explored. 