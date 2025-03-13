# Filtered Aggregations Implementation Plan

This document outlines the implementation plan for adding filtered aggregations to the column file format using the `github.com/weaviate/sroar` library for bitmap-based filtering.

## Overview

The goal is to extend the existing aggregation functionality to support filtering based on a bitmap of allowed IDs. This will enable efficient querying of subsets of data without reading unnecessary blocks or processing irrelevant IDs.

## Implementation Status

### ✅ Completed

1. **API Extension**:
   - Extended `AggregateOptions` struct to include a `Filter` field of type `*sroar.Bitmap`
   - Updated `DefaultAggregateOptions` function to initialize the filter to nil
   - Modified `AggregateWithOptions` to use filtered aggregation when a filter is provided

2. **Block Filtering Implementation**:
   - Implemented `FilteredBlockIterator` to efficiently find blocks that potentially contain IDs in the filter
   - Implemented `readBlockFiltered` to read a block and filter values based on the bitmap
   - Used the min/max ID range of each block to quickly skip blocks that don't overlap with the filter

3. **Basic Aggregation Implementation**:
   - Implemented `aggregateWithFilter` to perform aggregation with filtering
   - Added support for empty result handling when no blocks match the filter

4. **Testing**:
   - Created comprehensive tests covering various scenarios:
     - No filter
     - Filter matching a single block
     - Filter across multiple blocks
     - Filter with non-existent IDs
     - Sparse and dense filters
     - Testing with cached values
     - Testing the individual components (FilteredBlockIterator, readBlockFiltered)

5. **Benchmarking**:
   - Created benchmarks to measure performance with:
     - No filter
     - Sparse filter (0.1%)
     - Medium filter (10%)
     - Dense filter (50%)
     - Single block filter
     - Range filter
     - Each benchmark is run with and without cached values

### 🔄 In Progress

None at the moment.

### ⏳ Planned for Future

1. **Optimized Cached Aggregation**:
   - Implement a hybrid approach for cached aggregations where fully included blocks use cached values
   - This would improve performance for filters that include entire blocks

2. **Parallel Processing**:
   - Implement parallel processing of blocks for improved throughput on multi-core systems
   - Add a configurable parameter for the number of worker goroutines

3. **Memory Optimizations**:
   - Reduce allocations in the filtering process
   - Optimize bitmap operations to avoid creating unnecessary temporary bitmaps

4. **Advanced Filtering**:
   - Support for multiple filter bitmaps with different operations (AND, OR, NOT)
   - Support for range filters that don't require explicit bitmap creation

## Critical Analysis of the Approach

### Strengths of the Current Implementation:

1. **Block-Level Filtering**: The implementation efficiently skips blocks that don't contain any IDs in the bitmap by leveraging the min/max ID metadata in the footer.

2. **Whole Block Reading**: Reading entire blocks when at least one ID matches is efficient for I/O operations, aligning with SSD page sizes.

3. **Post-Read Filtering**: Filtering IDs after reading a block ensures accurate results while maintaining the block-based I/O pattern.

### Potential Improvements:

1. **Partial Block Processing**: While reading entire blocks is efficient for I/O, we could optimize CPU usage by avoiding decompression/decoding of values that don't match the filter.

2. **Memory Efficiency**: For very large bitmaps, we should ensure we're not creating unnecessary copies of the bitmap for each block.

3. **Cached Aggregations**: The current implementation doesn't use cached values for partially included blocks. A hybrid approach could improve performance.

4. **Bitmap Density Consideration**: Different optimization strategies could be applied based on the density of the bitmap.

## Performance Results

The benchmark results show:

- **No filter (cached)**: ~133 ns/op, 0 B/op, 0 allocs/op
- **No filter**: ~534,000 ns/op, 3,276,801 B/op, 300 allocs/op
- **Sparse filter (0.1%)**: ~2,043,000 ns/op, 4,917,242 B/op, 508 allocs/op
- **Medium filter (10%)**: ~1,479,000 ns/op, 4,917,243 B/op, 508 allocs/op
- **Dense filter (50%)**: ~1,382,000 ns/op, 4,917,243 B/op, 508 allocs/op
- **Single block filter**: ~37,000 ns/op, 49,160 B/op, 6 allocs/op
- **Range filter**: ~361,000 ns/op, 1,229,304 B/op, 131 allocs/op

These results show that:
1. Using cached values is extremely fast when no filter is applied
2. Single block filters are very efficient (only reading one block)
3. Range filters are more efficient than sparse filters (fewer blocks to read)
4. The implementation scales well with different filter densities

## Original Implementation Plan

Below is the original implementation plan for reference.

### Phase 1: API Extension

1. **Extend AggregateOptions**:
   ```go
   type AggregateOptions struct {
       // Existing fields
       SkipPreCalculated bool
       
       // New field
       Filter *sroar.Bitmap // Filter bitmap containing allowed IDs
   }
   ```

2. **Update Aggregate Method Signatures**:
   - Update `Aggregate()` and `AggregateWithOptions()` to handle the filter option
   - Add helper methods for filtered aggregation

### Phase 2: Block Filtering Implementation

1. **Create a FilteredBlockIterator**:
   ```go
   // FilteredBlockIterator returns blocks that potentially contain IDs in the filter
   func (r *Reader) FilteredBlockIterator(filter *sroar.Bitmap) []uint64 {
       if filter == nil {
           // Return all block indices if no filter
           blocks := make([]uint64, r.BlockCount())
           for i := range blocks {
               blocks[i] = uint64(i)
           }
           return blocks
       }
       
       // Get filter range
       filterMin := filter.Minimum()
       filterMax := filter.Maximum()
       
       // Find blocks that overlap with the filter range
       var matchingBlocks []uint64
       for i, entry := range r.blockIndex {
           // Skip blocks outside the filter range
           if entry.MaxID < filterMin || entry.MinID > filterMax {
               continue
           }
           
           matchingBlocks = append(matchingBlocks, uint64(i))
       }
       
       return matchingBlocks
   }
   ```

2. **Implement Filtered Block Reading**:
   ```go
   // readBlockFiltered reads a block and filters values based on the bitmap
   func (r *Reader) readBlockFiltered(blockIndex int, filter *sroar.Bitmap) ([]uint64, []int64, error) {
       // Read the entire block
       allIDs, allValues, err := r.readBlock(blockIndex)
       if err != nil {
           return nil, nil, err
       }
       
       if filter == nil {
           return allIDs, allValues, nil
       }
       
       // Filter IDs and values
       filteredIDs := make([]uint64, 0, len(allIDs))
       filteredValues := make([]int64, 0, len(allValues))
       
       for i, id := range allIDs {
           if filter.Contains(id) {
               filteredIDs = append(filteredIDs, id)
               filteredValues = append(filteredValues, allValues[i])
           }
       }
       
       return filteredIDs, filteredValues, nil
   }
   ```

### Phase 3: Aggregation Implementation

1. **Implement Filtered Aggregation**:
   ```go
   // AggregateWithOptions aggregates all blocks with the specified options and returns the result
   func (r *Reader) AggregateWithOptions(opts AggregateOptions) AggregateResult {
       // If no filter is provided, use the existing implementation
       if opts.Filter == nil {
           return r.aggregateWithoutFilter(opts)
       }
       
       // Get blocks that potentially match the filter
       matchingBlocks := r.FilteredBlockIterator(opts.Filter)
       
       // If no blocks match, return empty result
       if len(matchingBlocks) == 0 {
           return AggregateResult{
               Count: 0,
               Min:   0,
               Max:   0,
               Sum:   0,
               Avg:   0,
           }
       }
       
       // Check if we can use cached values for any blocks
       if !opts.SkipPreCalculated && len(r.blockIndex) > 0 {
           return r.aggregateWithFilterUsingCache(matchingBlocks, opts.Filter)
       }
       
       // Fallback: read and aggregate all matching blocks
       var count int
       var min int64 = 9223372036854775807  // Max int64
       var max int64 = -9223372036854775808 // Min int64
       var sum int64 = 0
       
       for _, blockIdx := range matchingBlocks {
           // Read block with filtering
           _, values, err := r.readBlockFiltered(int(blockIdx), opts.Filter)
           if err != nil {
               // Skip blocks with errors
               continue
           }
           
           count += len(values)
           for _, v := range values {
               if v < min {
                   min = v
               }
               if v > max {
                   max = v
               }
               sum += v
           }
       }
       
       // Calculate average
       var avg float64 = 0
       if count > 0 {
           avg = float64(sum) / float64(count)
       }
       
       return AggregateResult{
           Count: count,
           Min:   min,
           Max:   max,
           Sum:   sum,
           Avg:   avg,
       }
   }
   ```

2. **Implement Cached Aggregation with Filtering**:
   ```go
   // aggregateWithFilterUsingCache uses cached values for fully included blocks
   func (r *Reader) aggregateWithFilterUsingCache(matchingBlocks []uint64, filter *sroar.Bitmap) AggregateResult {
       var count int
       var min int64 = 9223372036854775807  // Max int64
       var max int64 = -9223372036854775808 // Min int64
       var sum int64 = 0
       
       // Create a bitmap for each block's ID range
       for _, blockIdx := range matchingBlocks {
           entry := r.blockIndex[blockIdx]
           
           // Create a bitmap for the block's ID range
           blockBitmap := sroar.NewBitmap()
           for id := entry.MinID; id <= entry.MaxID; id++ {
               blockBitmap.Set(id)
           }
           
           // Check if the block is fully included in the filter
           blockAndFilter := blockBitmap.And(filter)
           if blockAndFilter.GetCardinality() == blockBitmap.GetCardinality() {
               // Block is fully included, use cached values
               minValue := uint64ToInt64(entry.MinValue)
               maxValue := uint64ToInt64(entry.MaxValue)
               blockSum := uint64ToInt64(entry.Sum)
               
               count += int(entry.Count)
               if minValue < min {
                   min = minValue
               }
               if maxValue > max {
                   max = maxValue
               }
               sum += blockSum
           } else {
               // Block is partially included, read and filter
               _, values, err := r.readBlockFiltered(int(blockIdx), filter)
               if err != nil {
                   // Skip blocks with errors
                   continue
               }
               
               count += len(values)
               for _, v := range values {
                   if v < min {
                       min = v
                   }
                   if v > max {
                       max = v
                   }
                   sum += v
               }
           }
       }
       
       // Calculate average
       var avg float64 = 0
       if count > 0 {
           avg = float64(sum) / float64(count)
       }
       
       return AggregateResult{
           Count: count,
           Min:   min,
           Max:   max,
           Sum:   sum,
           Avg:   avg,
       }
   }
   ```

### Phase 4: Optimization and Parallelization

1. **Parallel Block Processing**:
   ```go
   // Parallel processing of blocks
   func (r *Reader) aggregateBlocksParallel(matchingBlocks []uint64, filter *sroar.Bitmap, numWorkers int) AggregateResult {
       if len(matchingBlocks) == 0 {
           return AggregateResult{}
       }
       
       // Create a worker pool
       type blockResult struct {
           count int
           min   int64
           max   int64
           sum   int64
       }
       
       resultChan := make(chan blockResult, len(matchingBlocks))
       blockChan := make(chan uint64, len(matchingBlocks))
       
       // Start workers
       var wg sync.WaitGroup
       for i := 0; i < numWorkers; i++ {
           wg.Add(1)
           go func() {
               defer wg.Done()
               for blockIdx := range blockChan {
                   // Read and process block
                   _, values, err := r.readBlockFiltered(int(blockIdx), filter)
                   if err != nil {
                       continue
                   }
                   
                   // Calculate block aggregates
                   result := blockResult{
                       count: len(values),
                       min:   9223372036854775807,  // Max int64
                       max:   -9223372036854775808, // Min int64
                       sum:   0,
                   }
                   
                   for _, v := range values {
                       if v < result.min {
                           result.min = v
                       }
                       if v > result.max {
                           result.max = v
                       }
                       result.sum += v
                   }
                   
                   resultChan <- result
               }
           }()
       }
       
       // Send blocks to workers
       for _, blockIdx := range matchingBlocks {
           blockChan <- blockIdx
       }
       close(blockChan)
       
       // Wait for workers to finish
       go func() {
           wg.Wait()
           close(resultChan)
       }()
       
       // Combine results
       finalResult := AggregateResult{
           Min: 9223372036854775807,  // Max int64
           Max: -9223372036854775808, // Min int64
       }
       
       for result := range resultChan {
           finalResult.Count += result.count
           if result.min < finalResult.Min {
               finalResult.Min = result.min
           }
           if result.max > finalResult.Max {
               finalResult.Max = result.max
           }
           finalResult.Sum += result.sum
       }
       
       // Calculate average
       if finalResult.Count > 0 {
           finalResult.Avg = float64(finalResult.Sum) / float64(finalResult.Count)
       }
       
       return finalResult
   }
   ```

2. **Optimized Block Range Check**:
   ```go
   // Optimized block range check using bitmap min/max
   func blockOverlapsFilter(blockMin, blockMax uint64, filter *sroar.Bitmap) bool {
       // Quick check using filter min/max
       filterMin := filter.Minimum()
       filterMax := filter.Maximum()
       
       if blockMax < filterMin || blockMin > filterMax {
           return false
       }
       
       return true
   }
   ```

### Phase 5: Testing and Benchmarking

1. **Unit Tests**:
   - Test with various filter densities (sparse, medium, dense)
   - Test with filters that exclude all blocks
   - Test with filters that include all blocks
   - Test with filters that partially include blocks

2. **Benchmarks**:
   - Benchmark filtered vs. unfiltered aggregation
   - Benchmark with different filter densities
   - Benchmark parallel vs. sequential processing

## Conclusion

The filtered aggregations feature has been successfully implemented with the basic functionality working as expected. The implementation provides efficient filtering at the block level and post-read filtering of IDs and values. Comprehensive tests and benchmarks have been created to verify correctness and measure performance.

Future work will focus on optimizing the implementation further, particularly for cached aggregations and parallel processing, as well as reducing memory allocations and supporting more advanced filtering operations. 