package col

import (
	"runtime"
	"sync"

	"github.com/weaviate/sroar"
)

// AggregateOptions contains options for the aggregation process
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

// DefaultAggregateOptions returns the default options for aggregation
func DefaultAggregateOptions() AggregateOptions {
	return AggregateOptions{
		SkipPreCalculated: false,
		Filter:            nil,
		DenyFilter:        nil,
		Parallel:          0, // Default to sequential aggregation
	}
}

// Aggregate aggregates all blocks and returns the result using default options
func (r *Reader) Aggregate() AggregateResult {
	return r.AggregateWithOptions(DefaultAggregateOptions())
}

// AggregateWithOptions aggregates all blocks with the specified options and returns the result
func (r *Reader) AggregateWithOptions(opts AggregateOptions) AggregateResult {
	// If parallel aggregation is enabled, use it
	if opts.Parallel != 0 {
		return r.aggregateParallel(opts)
	}

	// If a filter or deny filter is provided, use filtered aggregation
	if opts.Filter != nil || opts.DenyFilter != nil {
		return r.aggregateWithFilter(opts)
	}

	// If we have a footer with block statistics and we're not skipping pre-calculated values, use it for efficient aggregation
	if len(r.blockIndex) > 0 && !opts.SkipPreCalculated {
		var count int
		var min int64 = 9223372036854775807  // Max int64
		var max int64 = -9223372036854775808 // Min int64
		var sum int64 = 0

		for _, entry := range r.blockIndex {
			// Convert stored uint64 values back to int64
			minValue := uint64ToInt64(entry.MinValue)
			maxValue := uint64ToInt64(entry.MaxValue)
			blockSum := uint64ToInt64(entry.Sum)

			// Update aggregates
			count += int(entry.Count)
			if minValue < min {
				min = minValue
			}
			if maxValue > max {
				max = maxValue
			}
			sum += blockSum
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

	// Fallback: read and aggregate all blocks
	var count int
	var min int64 = 9223372036854775807  // Max int64
	var max int64 = -9223372036854775808 // Min int64
	var sum int64 = 0

	for i := uint64(0); i < r.header.BlockCount; i++ {
		_, values, err := r.GetPairs(i)
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

// FilteredBlockIterator returns blocks that potentially contain IDs in the filter
func (r *Reader) FilteredBlockIterator(filter, denyFilter *sroar.Bitmap) []uint64 {
	// If no filters are provided, return all blocks
	if filter == nil && denyFilter == nil {
		blocks := make([]uint64, r.BlockCount())
		for i := range blocks {
			blocks[i] = uint64(i)
		}
		return blocks
	}

	var matchingBlocks []uint64

	// If only deny filter is provided, we need to check all blocks
	if filter == nil && denyFilter != nil {
		// We still need to check all blocks since we're only excluding IDs
		blocks := make([]uint64, r.BlockCount())
		for i := range blocks {
			blocks[i] = uint64(i)
		}
		return blocks
	}

	// If allow filter is provided, use it to find matching blocks
	if filter != nil {
		// Get filter range
		filterMin := filter.Minimum()
		filterMax := filter.Maximum()

		// Find blocks that overlap with the filter range
		for i, entry := range r.blockIndex {
			// Skip blocks outside the filter range
			if entry.MaxID < filterMin || entry.MinID > filterMax {
				continue
			}

			matchingBlocks = append(matchingBlocks, uint64(i))
		}
	}

	return matchingBlocks
}

// readBlockFiltered reads a block and filters values based on the allow and deny bitmaps
func (r *Reader) readBlockFiltered(blockIndex int, filter, denyFilter *sroar.Bitmap) ([]uint64, []int64, error) {
	// Read the entire block
	allIDs, allValues, err := r.readBlock(blockIndex)
	if err != nil {
		return nil, nil, err
	}

	// If no filters are provided, return all values
	if filter == nil && denyFilter == nil {
		return allIDs, allValues, nil
	}

	// Filter IDs and values
	filteredIDs := make([]uint64, 0, len(allIDs))
	filteredValues := make([]int64, 0, len(allValues))

	for i, id := range allIDs {
		// Check if ID is allowed (either no allow filter or ID is in allow filter)
		isAllowed := filter == nil || filter.Contains(id)

		// Check if ID is denied (ID is in deny filter)
		isDenied := denyFilter != nil && denyFilter.Contains(id)

		// Include ID if it's allowed and not denied
		if isAllowed && !isDenied {
			filteredIDs = append(filteredIDs, id)
			filteredValues = append(filteredValues, allValues[i])
		}
	}

	return filteredIDs, filteredValues, nil
}

// aggregateWithFilter performs aggregation with filtering
func (r *Reader) aggregateWithFilter(opts AggregateOptions) AggregateResult {
	// Get blocks that potentially match the filter
	matchingBlocks := r.FilteredBlockIterator(opts.Filter, opts.DenyFilter)

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

	// Read and aggregate all matching blocks
	var count int
	var min int64 = 9223372036854775807  // Max int64
	var max int64 = -9223372036854775808 // Min int64
	var sum int64 = 0

	for _, blockIdx := range matchingBlocks {
		// Read block with filtering
		_, values, err := r.readBlockFiltered(int(blockIdx), opts.Filter, opts.DenyFilter)
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

// aggregateParallel performs aggregation in parallel
func (r *Reader) aggregateParallel(opts AggregateOptions) AggregateResult {
	// Determine the number of workers
	numWorkers := opts.Parallel
	if numWorkers < 0 {
		// Use GOMAXPROCS if Parallel is negative
		numWorkers = runtime.GOMAXPROCS(0)
	}

	// Ensure we don't create more workers than blocks
	blockCount := int(r.header.BlockCount)
	if numWorkers > blockCount {
		numWorkers = blockCount
	}

	// If we have only one worker or one block, fall back to sequential aggregation
	if numWorkers <= 1 || blockCount <= 1 {
		// Remove the Parallel option to avoid recursion
		seqOpts := opts
		seqOpts.Parallel = 0
		return r.AggregateWithOptions(seqOpts)
	}

	// Get blocks that potentially match the filter
	var blockIndices []uint64
	if opts.Filter != nil || opts.DenyFilter != nil {
		blockIndices = r.FilteredBlockIterator(opts.Filter, opts.DenyFilter)
	} else {
		// Use all blocks if no filter is provided
		blockIndices = make([]uint64, blockCount)
		for i := range blockIndices {
			blockIndices[i] = uint64(i)
		}
	}

	// If no blocks match, return empty result
	if len(blockIndices) == 0 {
		return AggregateResult{
			Count: 0,
			Min:   0,
			Max:   0,
			Sum:   0,
			Avg:   0,
		}
	}

	// If we have a footer with block statistics and we're not skipping pre-calculated values,
	// we can use it for efficient parallel aggregation
	if len(r.blockIndex) > 0 && !opts.SkipPreCalculated && opts.Filter == nil && opts.DenyFilter == nil {
		return r.aggregateParallelWithFooter(blockIndices, numWorkers)
	}

	// Otherwise, we need to read and aggregate all blocks in parallel
	return r.aggregateParallelWithReading(blockIndices, opts, numWorkers)
}

// aggregateParallelWithFooter performs parallel aggregation using pre-calculated values from the footer
func (r *Reader) aggregateParallelWithFooter(blockIndices []uint64, numWorkers int) AggregateResult {
	// Create a channel for workers to send their results
	resultChan := make(chan AggregateResult, numWorkers)

	// Calculate how many blocks each worker should process
	blocksPerWorker := (len(blockIndices) + numWorkers - 1) / numWorkers

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Calculate the range of blocks this worker should process
			startIdx := workerID * blocksPerWorker
			endIdx := (workerID + 1) * blocksPerWorker
			if endIdx > len(blockIndices) {
				endIdx = len(blockIndices)
			}

			// Skip if this worker has no blocks to process
			if startIdx >= endIdx {
				return
			}

			// Process blocks assigned to this worker
			var count int
			var min int64 = 9223372036854775807  // Max int64
			var max int64 = -9223372036854775808 // Min int64
			var sum int64 = 0

			for i := startIdx; i < endIdx; i++ {
				blockIdx := blockIndices[i]
				entry := r.blockIndex[blockIdx]

				// Convert stored uint64 values back to int64
				minValue := uint64ToInt64(entry.MinValue)
				maxValue := uint64ToInt64(entry.MaxValue)
				blockSum := uint64ToInt64(entry.Sum)

				// Update aggregates
				count += int(entry.Count)
				if minValue < min {
					min = minValue
				}
				if maxValue > max {
					max = maxValue
				}
				sum += blockSum
			}

			// Calculate average
			var avg float64 = 0
			if count > 0 {
				avg = float64(sum) / float64(count)
			}

			// Send result to channel
			resultChan <- AggregateResult{
				Count: count,
				Min:   min,
				Max:   max,
				Sum:   sum,
				Avg:   avg,
			}
		}(w)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(resultChan)

	// Merge results
	var finalResult AggregateResult
	var totalCount int
	var totalSum int64

	for result := range resultChan {
		totalCount += result.Count
		totalSum += result.Sum

		if result.Min < finalResult.Min || finalResult.Count == 0 {
			finalResult.Min = result.Min
		}

		if result.Max > finalResult.Max || finalResult.Count == 0 {
			finalResult.Max = result.Max
		}

		finalResult.Count += result.Count
	}

	// Calculate final average
	if totalCount > 0 {
		finalResult.Avg = float64(totalSum) / float64(totalCount)
	}

	finalResult.Sum = totalSum

	return finalResult
}

// aggregateParallelWithReading performs parallel aggregation by reading blocks
func (r *Reader) aggregateParallelWithReading(blockIndices []uint64, opts AggregateOptions, numWorkers int) AggregateResult {
	// Create a channel for workers to send their results
	resultChan := make(chan AggregateResult, numWorkers)

	// Calculate how many blocks each worker should process
	blocksPerWorker := (len(blockIndices) + numWorkers - 1) / numWorkers

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Calculate the range of blocks this worker should process
			startIdx := workerID * blocksPerWorker
			endIdx := (workerID + 1) * blocksPerWorker
			if endIdx > len(blockIndices) {
				endIdx = len(blockIndices)
			}

			// Skip if this worker has no blocks to process
			if startIdx >= endIdx {
				return
			}

			// Process blocks assigned to this worker
			var count int
			var min int64 = 9223372036854775807  // Max int64
			var max int64 = -9223372036854775808 // Min int64
			var sum int64 = 0

			for i := startIdx; i < endIdx; i++ {
				blockIdx := blockIndices[i]

				// Read block with filtering if needed
				var values []int64
				var err error

				if opts.Filter != nil || opts.DenyFilter != nil {
					// Read block with filtering
					_, values, err = r.readBlockFiltered(int(blockIdx), opts.Filter, opts.DenyFilter)
				} else {
					// Read block without filtering
					_, values, err = r.GetPairs(blockIdx)
				}

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

			// Send result to channel
			resultChan <- AggregateResult{
				Count: count,
				Min:   min,
				Max:   max,
				Sum:   sum,
				Avg:   avg,
			}
		}(w)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(resultChan)

	// Merge results
	var finalResult AggregateResult
	var totalCount int
	var totalSum int64

	for result := range resultChan {
		totalCount += result.Count
		totalSum += result.Sum

		if result.Min < finalResult.Min || finalResult.Count == 0 {
			finalResult.Min = result.Min
		}

		if result.Max > finalResult.Max || finalResult.Count == 0 {
			finalResult.Max = result.Max
		}

		finalResult.Count += result.Count
	}

	// Calculate final average
	if totalCount > 0 {
		finalResult.Avg = float64(totalSum) / float64(totalCount)
	}

	finalResult.Sum = totalSum

	return finalResult
}
