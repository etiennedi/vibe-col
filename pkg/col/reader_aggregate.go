package col

// AggregateOptions contains options for the aggregation process
type AggregateOptions struct {
	// SkipPreCalculated forces the aggregation to read all values from blocks
	// instead of using pre-calculated values from the footer
	SkipPreCalculated bool
}

// DefaultAggregateOptions returns the default options for aggregation
func DefaultAggregateOptions() AggregateOptions {
	return AggregateOptions{
		SkipPreCalculated: false,
	}
}

// Aggregate aggregates all blocks and returns the result using default options
func (r *Reader) Aggregate() AggregateResult {
	return r.AggregateWithOptions(DefaultAggregateOptions())
}

// AggregateWithOptions aggregates all blocks with the specified options and returns the result
func (r *Reader) AggregateWithOptions(opts AggregateOptions) AggregateResult {
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
