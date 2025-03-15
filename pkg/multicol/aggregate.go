// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"fmt"

	"vibe-lsm/pkg/col"

	"github.com/weaviate/sroar"
)

// MultiReader represents a collection of column file readers
// ordered from oldest (index 0) to newest (last index).
type MultiReader struct {
	readers []*col.Reader
}

// NewMultiReader creates a new MultiReader from a slice of Readers.
// The readers should be ordered from oldest (index 0) to newest (last index).
func NewMultiReader(readers []*col.Reader) *MultiReader {
	return &MultiReader{
		readers: readers,
	}
}

// Close closes all readers.
func (mr *MultiReader) Close() error {
	var lastErr error
	for _, reader := range mr.readers {
		if err := reader.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// AggregateOptions contains options for the aggregation process
type AggregateOptions struct {
	// SkipPreCalculated forces the aggregation to read all values from blocks
	// instead of using pre-calculated values from the footer
	SkipPreCalculated bool

	// Filter is a bitmap of allowed IDs for filtered aggregation
	Filter *sroar.Bitmap
}

// Aggregate aggregates data across all readers, handling updates correctly.
// It processes readers from newest to oldest, using global ID bitmaps as deny lists
// to exclude updated values from older files.
func (mr *MultiReader) Aggregate(opts AggregateOptions) (col.AggregateResult, error) {
	if len(mr.readers) == 0 {
		return col.AggregateResult{}, nil
	}

	// Initialize the result with zero values
	result := col.AggregateResult{}

	// Initialize an empty deny bitmap to track processed IDs
	denyBitmap := sroar.NewBitmap()

	// Process readers from newest to oldest
	for i := len(mr.readers) - 1; i >= 0; i-- {
		reader := mr.readers[i]

		// Create aggregation options for this reader
		readerOpts := col.AggregateOptions{
			SkipPreCalculated: opts.SkipPreCalculated,
			Filter:            opts.Filter,
			DenyFilter:        denyBitmap,
		}

		// Aggregate this reader with the current deny filter
		readerResult := reader.AggregateWithOptions(readerOpts)

		// Get the global ID bitmap for this reader
		globalIDs, err := reader.GetGlobalIDBitmap()
		if err != nil {
			return col.AggregateResult{}, fmt.Errorf("failed to get global ID bitmap from reader %d: %w", i, err)
		}

		// Add all IDs from this reader to the deny bitmap for older readers
		denyBitmap = denyBitmap.Or(globalIDs)

		// Merge the results
		if result.Count == 0 {
			// First result, just copy it
			result = readerResult
		} else {
			// Merge with existing result
			result = mergeAggregateResults(result, readerResult)
		}
	}

	return result, nil
}

// mergeAggregateResults merges two AggregateResults into one.
func mergeAggregateResults(a, b col.AggregateResult) col.AggregateResult {
	// If either result is empty, return the other one
	if a.Count == 0 {
		return b
	}
	if b.Count == 0 {
		return a
	}

	// Merge the results
	merged := col.AggregateResult{
		Count: a.Count + b.Count,
		Min:   minInt64(a.Min, b.Min),
		Max:   maxInt64(a.Max, b.Max),
		Sum:   a.Sum + b.Sum,
	}

	// Calculate the average
	if merged.Count > 0 {
		merged.Avg = float64(merged.Sum) / float64(merged.Count)
	}

	return merged
}

// Helper function to find the minimum of two int64 values
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Helper function to find the maximum of two int64 values
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
