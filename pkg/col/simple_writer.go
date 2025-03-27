package col

import (
	"fmt"
	"sort"
)

// SimpleWriter provides a higher-level abstraction over the column file writer
// that handles blocks as an implementation detail.
type SimpleWriter struct {
	writer          *Writer
	filename        string
	pendingIDs      []uint64
	pendingValues   []int64
	targetBlockSize int
	closed          bool
	totalItems      uint64 // Track total number of items written
}

// NewSimpleWriter creates a new SimpleWriter for the given filename
func NewSimpleWriter(filename string, options ...WriterOption) (*SimpleWriter, error) {
	// Default target block size
	targetBlockSize := 128 * 1024 // 128KB default block size

	// Add our target block size to the writer options
	options = append(options, WithBlockSize(uint32(targetBlockSize)))

	// Create the underlying writer
	writer, err := NewWriter(filename, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer: %w", err)
	}

	return &SimpleWriter{
		writer:          writer,
		filename:        filename,
		pendingIDs:      make([]uint64, 0),
		pendingValues:   make([]int64, 0),
		targetBlockSize: targetBlockSize,
		closed:          false,
		totalItems:      0,
	}, nil
}

// SetTargetBlockSize sets the target block size for the writer
func (sw *SimpleWriter) SetTargetBlockSize(size int) error {
	if sw.closed {
		return fmt.Errorf("writer is already closed")
	}

	sw.targetBlockSize = size

	// Also update the underlying writer's block size target
	sw.writer.blockSizeTarget = uint32(size)

	return nil
}

// Write adds ID-value pairs to the file
// If the IDs are not sorted, they will be sorted automatically
func (sw *SimpleWriter) Write(ids []uint64, values []int64) error {
	if sw.closed {
		return fmt.Errorf("writer is already closed")
	}

	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return nil // Nothing to write
	}

	// Create a copy of the input data to avoid modifying the caller's slices
	newIDs := make([]uint64, len(ids))
	newValues := make([]int64, len(values))
	copy(newIDs, ids)
	copy(newValues, values)

	// Sort the data by ID if necessary
	if !isSorted(newIDs) {
		sortByID(newIDs, newValues)
	}

	// Add to pending data
	sw.pendingIDs = append(sw.pendingIDs, newIDs...)
	sw.pendingValues = append(sw.pendingValues, newValues...)

	// Check if we have enough data to write a block
	return sw.flushIfNeeded(false)
}

// Close finalizes the file and closes it
func (sw *SimpleWriter) Close() error {
	if sw.closed {
		return nil // Already closed
	}

	// Flush any remaining data
	if err := sw.flushIfNeeded(true); err != nil {
		return fmt.Errorf("failed to flush remaining data: %w", err)
	}

	// Finalize and close the file
	if err := sw.writer.FinalizeAndClose(); err != nil {
		return fmt.Errorf("failed to finalize and close file: %w", err)
	}

	sw.closed = true
	return nil
}

// IsClosed returns whether the writer has been closed
func (sw *SimpleWriter) IsClosed() bool {
	return sw.closed
}

// TotalItems returns the total number of items written so far
func (sw *SimpleWriter) TotalItems() uint64 {
	return sw.totalItems
}

// flushIfNeeded writes pending data to a block if there's enough data or if forced
func (sw *SimpleWriter) flushIfNeeded(force bool) error {
	return sw.flushIfNeededWithDepth(force, 0)
}

// flushIfNeededWithDepth is the internal implementation with recursion depth tracking
func (sw *SimpleWriter) flushIfNeededWithDepth(force bool, depth int) error {
	// Prevent infinite recursion
	maxRecursionDepth := 10
	if depth > maxRecursionDepth {
		return fmt.Errorf("maximum recursion depth reached (%d), possible cycle in flush logic", maxRecursionDepth)
	}

	// If there's no data to write, we're done
	if len(sw.pendingIDs) == 0 {
		return nil
	}

	// Determine if we should write a block
	shouldWrite := force

	// If not forced, check if we have enough data to write a block
	if !force {
		// Calculate bytes per entry based on the encoding type
		bytesPerEntry := 16 // Default for raw encoding (8 bytes for ID, 8 bytes for value)

		// For VarInt encoding, we need to estimate more conservatively
		if sw.writer.encodingType == EncodingVarInt ||
			sw.writer.encodingType == EncodingVarIntID ||
			sw.writer.encodingType == EncodingVarIntValue ||
			sw.writer.encodingType == EncodingVarIntBoth {
			// VarInt varies in size but for typical data is more efficient - use 10 bytes
			// This more conservative estimate helps ensure we don't underestimate
			bytesPerEntry = 10
		}

		// Calculate the target number of entries per block
		// Use 98% of the target block size to maximize block utilization
		entriesPerBlock := (sw.targetBlockSize * 98 / 100) / bytesPerEntry

		// Ensure we have a reasonable minimum (at least 1000 entries per block)
		if entriesPerBlock < 1000 {
			entriesPerBlock = 1000
		}

		shouldWrite = len(sw.pendingIDs) >= entriesPerBlock
	}

	if shouldWrite {
		// Always estimate the block size first if we have enough entries
		if len(sw.pendingIDs) > 500 && !force {
			// Try to estimate the block size for the current pending data
			estimatedSize, err := sw.writer.EstimateBlockSize(sw.pendingIDs, sw.pendingValues)
			if err == nil {
				// Target at least 90% of block size for optimal space utilization
				targetEfficiency := 0.90
				if float64(estimatedSize) < float64(sw.targetBlockSize)*targetEfficiency {
					// We're below our efficiency target, don't flush yet unless forced
					return nil
				}
			}
		}

		// Try to write all pending items at once
		err := sw.writer.WriteBlock(sw.pendingIDs, sw.pendingValues)

		// If the write was successful or it's an error other than BlockFullError, handle it
		if err == nil {
			// All items were written successfully
			sw.totalItems += uint64(len(sw.pendingIDs))
			sw.pendingIDs = nil
			sw.pendingValues = nil
			return nil
		} else if _, ok := err.(*BlockFullError); !ok {
			// Some error other than BlockFullError occurred
			return fmt.Errorf("failed to write block: %w", err)
		}

		// If we get here, we got a BlockFullError, which means the block would be too large
		// We'll use a binary search approach to find the optimal batch size

		// Initialize search boundaries
		low := 0
		high := len(sw.pendingIDs)
		batchSize := high / 2
		bestBatchSize := 0
		bestEfficiency := 0.0

		// Binary search with a maximum of 8 iterations to find optimal batch size
		for i := 0; i < 8 && high > low+100; i++ {
			testSize := (low + high) / 2
			if testSize <= 0 {
				break
			}

			// Skip if batch size is too small
			if testSize < 1000 {
				high = testSize
				continue
			}

			// Estimate size for this batch
			estimatedSize, err := sw.writer.EstimateBlockSize(
				sw.pendingIDs[:testSize],
				sw.pendingValues[:testSize])

			if err != nil {
				high = testSize
				continue
			}

			efficiency := float64(estimatedSize) / float64(sw.targetBlockSize)

			// If efficiency is good (80-98%), this is a candidate
			if efficiency >= 0.80 && efficiency <= 0.98 {
				if efficiency > bestEfficiency {
					bestBatchSize = testSize
					bestEfficiency = efficiency
				}
			}

			// Adjust search range
			if float64(estimatedSize) > float64(sw.targetBlockSize)*0.98 {
				high = testSize
			} else {
				low = testSize
			}
		}

		// If we found a good batch size, use it
		if bestBatchSize > 0 {
			batchSize = bestBatchSize
		} else if float64(batchSize) < float64(len(sw.pendingIDs))*0.5 {
			// Ensure we're writing at least half of the pending items
			batchSize = len(sw.pendingIDs) / 2
		}

		// Ensure batch size is not larger than pending items
		if batchSize > len(sw.pendingIDs) {
			batchSize = len(sw.pendingIDs)
		}

		// Ensure a minimum batch size
		if batchSize < 1000 && len(sw.pendingIDs) > 1000 {
			batchSize = 1000
		}

		// Try to write the optimized batch
		err = sw.writer.WriteBlock(sw.pendingIDs[:batchSize], sw.pendingValues[:batchSize])

		if err == nil {
			// Batch was written successfully
			sw.totalItems += uint64(batchSize)
			sw.pendingIDs = sw.pendingIDs[batchSize:]
			sw.pendingValues = sw.pendingValues[batchSize:]

			// If we have more items, recursively try to flush again
			if len(sw.pendingIDs) > 0 {
				return sw.flushIfNeededWithDepth(force, depth+1)
			}
			return nil
		} else if blockFullErr, ok := err.(*BlockFullError); ok {
			// Block was full; if some items were written, update state
			if blockFullErr.ItemsWritten > 0 {
				sw.totalItems += uint64(blockFullErr.ItemsWritten)
				sw.pendingIDs = sw.pendingIDs[blockFullErr.ItemsWritten:]
				sw.pendingValues = sw.pendingValues[blockFullErr.ItemsWritten:]

				// Recursively try again with remaining items
				return sw.flushIfNeededWithDepth(force, depth+1)
			} else {
				// No items were written, try with an absolute minimum batch size
				if len(sw.pendingIDs) > 1 {
					// Try with just one item
					err = sw.writer.WriteBlock(sw.pendingIDs[:1], sw.pendingValues[:1])
					if err == nil {
						sw.totalItems += 1
						sw.pendingIDs = sw.pendingIDs[1:]
						sw.pendingValues = sw.pendingValues[1:]

						// Continue with remaining items
						return sw.flushIfNeededWithDepth(force, depth+1)
					}

					// If we can't even write a single item, something is wrong
					return fmt.Errorf("failed to write even a single item: %w", err)
				}
			}
		} else {
			// Some other error occurred
			return fmt.Errorf("failed to write block: %w", err)
		}
	}

	return nil
}

// isSorted checks if the IDs are sorted in ascending order
func isSorted(ids []uint64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			return false
		}
	}
	return true
}

// sortByID sorts the values by their corresponding IDs
func sortByID(ids []uint64, values []int64) {
	// Create a slice of index-value pairs
	pairs := make([]struct {
		ID    uint64
		Value int64
		Index int
	}, len(ids))

	for i := range ids {
		pairs[i] = struct {
			ID    uint64
			Value int64
			Index int
		}{
			ID:    ids[i],
			Value: values[i],
			Index: i,
		}
	}

	// Sort the pairs by ID
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].ID < pairs[j].ID
	})

	// Update the original slices
	for i := range pairs {
		ids[i] = pairs[i].ID
		values[i] = pairs[i].Value
	}
}
