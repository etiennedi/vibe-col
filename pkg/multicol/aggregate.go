// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"fmt"

	"vibe-lsm/pkg/col"

	"github.com/weaviate/sroar"
)

// AggregateSource defines the common interface needed by MultiReader
type AggregateSource interface {
	AggregateWithOptions(opts col.AggregateOptions) col.AggregateResult
	GetGlobalIDBitmap() (*sroar.Bitmap, error)
	GetDeletedIDBitmap() (*sroar.Bitmap, error)
	Close() error
}

// MultiReader represents a collection of data sources
// ordered from oldest (index 0) to newest (last index).
type MultiReader struct {
	readers []AggregateSource
}

// NewMultiReader creates a new MultiReader from a slice of data sources.
// The sources should be ordered from oldest (index 0) to newest (last index).
func NewMultiReader(sources []AggregateSource) *MultiReader {
	return &MultiReader{
		readers: sources,
	}
}

// For backward compatibility
func NewColReaderMultiReader(readers []*col.Reader) *MultiReader {
	sources := make([]AggregateSource, len(readers))
	for i, r := range readers {
		sources[i] = r // col.Reader directly implements AggregateSource
	}
	return NewMultiReader(sources)
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

// AggregateWithOptions aggregates data across all readers, handling updates correctly.
// It processes readers from newest to oldest, using global ID bitmaps as deny lists
// to exclude updated values from older files.
func (mr *MultiReader) AggregateWithOptions(opts col.AggregateOptions) (col.AggregateResult, error) {
	if len(mr.readers) == 0 {
		return col.AggregateResult{}, nil
	}

	// Initialize the result with zero values
	result := col.AggregateResult{}

	// Initialize a deny bitmap from the options, or create a new one if nil
	denyBitmap := opts.DenyFilter
	if denyBitmap == nil {
		denyBitmap = sroar.NewBitmap()
	} else {
		denyBitmap = denyBitmap.Clone() // Clone to avoid modifying the original passed in opts
	}

	// Process readers from newest (end of slice) to oldest (start of slice)
	for i := len(mr.readers) - 1; i >= 0; i-- {
		reader := mr.readers[i]

		// Create aggregation options for this reader, passing the current denyBitmap
		// and other relevant options directly from the input opts.
		readerOpts := col.AggregateOptions{
			SkipPreCalculated: opts.SkipPreCalculated,
			Filter:            opts.Filter,        // Pass through original filter
			DenyFilter:        denyBitmap.Clone(), // Pass a clone of the *accumulated* deny list
			IDRangeStart:      opts.IDRangeStart,  // Pass through range filters
			IDRangeEnd:        opts.IDRangeEnd,
			Parallel:          opts.Parallel, // Pass through parallel setting
		}

		// Aggregate this reader with the current deny filter and other options
		// Note: The reader itself is responsible for handling parallelism internally.
		readerResult := reader.AggregateWithOptions(readerOpts)

		// Get the global ID bitmap for *this* reader (IDs present in this reader)
		globalIDs, err := reader.GetGlobalIDBitmap()
		if err != nil {
			return col.AggregateResult{}, fmt.Errorf("failed to get global ID bitmap from reader %d: %w", i, err)
		}

		// Get the deleted ID bitmap for *this* reader (tombstones in this reader)
		deletedIDs, err := reader.GetDeletedIDBitmap()
		if err != nil {
			return col.AggregateResult{}, fmt.Errorf("failed to get deleted ID bitmap from reader %d: %w", i, err)
		}

		// Add all existing and deleted IDs from the *current* reader to the deny bitmap.
		// This ensures that older readers (processed next) will ignore any IDs
		// that were present (updated or deleted) in this newer reader.
		denyBitmap.Or(globalIDs)
		denyBitmap.Or(deletedIDs)

		// Merge the results
		result = mergeAggregateResults(result, readerResult)
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
