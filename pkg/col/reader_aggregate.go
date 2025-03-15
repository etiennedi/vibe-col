package col

import (
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
}

// DefaultAggregateOptions returns the default options for aggregation
func DefaultAggregateOptions() AggregateOptions {
	return AggregateOptions{
		SkipPreCalculated: false,
		Filter:            nil,
		DenyFilter:        nil,
	}
}

// Aggregate aggregates all blocks and returns the result using default options
func (r *Reader) Aggregate() AggregateResult {
	return r.AggregateWithOptions(DefaultAggregateOptions())
}

// AggregateWithOptions aggregates all blocks with the specified options and returns the result
func (r *Reader) AggregateWithOptions(opts AggregateOptions) AggregateResult {
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
