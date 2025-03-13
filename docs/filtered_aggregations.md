# Filtered Aggregations Implementation Plan

This document outlines the implementation plan for adding filtered aggregations to the column file format using the `github.com/weaviate/sroar` library for bitmap-based filtering.

## Overview

The goal is to extend the existing aggregation functionality to support filtering based on a bitmap of allowed IDs. This will enable efficient querying of subsets of data without reading unnecessary blocks or processing irrelevant IDs.

## Critical Analysis of the Proposed Approach

### Strengths of the Proposed Plan:

1. **Block-Level Filtering**: Skipping blocks that don't contain any IDs in the bitmap is an excellent optimization. This leverages the min/max ID metadata in the footer to avoid unnecessary I/O.

2. **Whole Block Reading**: Reading entire blocks when at least one ID matches is reasonable given the block size design (128kB). This aligns with SSD page sizes and minimizes random I/O.

3. **Post-Read Filtering**: Filtering IDs after reading a block ensures accurate results while maintaining the block-based I/O pattern.

### Potential Improvements and Considerations:

1. **Partial Block Processing**: While reading entire blocks is efficient for I/O, we could optimize CPU usage by avoiding decompression/decoding of values that don't match the filter. This would require changes to the decoding process.

2. **Bitmap Range Checks**: The sroar library provides `Minimum()` and `Maximum()` methods, which could be used to quickly determine if a block's ID range overlaps with the bitmap's range before checking individual IDs.

3. **Parallel Processing**: For large files with many blocks, we could process multiple blocks in parallel to improve throughput on multi-core systems.

4. **Memory Efficiency**: For very large bitmaps, we should ensure we're not creating unnecessary copies of the bitmap for each block.

5. **Cached Aggregations**: We should consider how to handle cached aggregations (like pre-calculated sums) when filters are applied. We might need a hybrid approach where we use cached values for fully included blocks and calculate on-the-fly for partially included blocks.

6. **Bitmap Density Consideration**: If the bitmap is very sparse or very dense, different optimization strategies might be appropriate. For example, with a very dense bitmap (most IDs included), we might just use the existing aggregation code with minimal filtering.

## Detailed Implementation Plan

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

3. **Integration with Load Test Application**:
   - Add filter generation to the load test application
   - Add filtered aggregation benchmarks

## Performance Considerations

1. **Memory Usage**:
   - Be mindful of creating large temporary bitmaps, especially when checking if a block is fully included in the filter
   - Consider using bitmap operations that don't create new bitmaps when possible

2. **I/O Efficiency**:
   - The current approach of reading entire blocks is good for I/O efficiency
   - Consider adding a prefetch mechanism for sequential block reads

3. **CPU Efficiency**:
   - The parallel processing approach should help utilize multiple cores
   - Profile the code to identify bottlenecks, especially in the bitmap operations

## Future Enhancements

1. **Advanced Filtering**:
   - Support for multiple filter bitmaps with different operations (AND, OR, NOT)
   - Support for range filters that don't require explicit bitmap creation

2. **Incremental Aggregation**:
   - Support for incremental aggregation where results are updated as new blocks are processed
   - This could be useful for streaming or progressive UI updates

3. **Custom Aggregations**:
   - Support for user-defined aggregation functions
   - Support for more complex aggregations like percentiles, histograms, etc.

## Conclusion

The proposed implementation plan provides a solid foundation for adding filtered aggregations to the column file format. By leveraging the sroar library for bitmap operations and the existing block-based structure of the file format, we can achieve efficient filtering with minimal changes to the codebase.

The key optimizations are:
1. Early block filtering using min/max ID ranges
2. Efficient ID filtering using the sroar bitmap
3. Optimized handling of fully included blocks using cached values
4. Parallel processing for improved throughput

This implementation should scale well with large datasets and provide good performance for both sparse and dense filters. 