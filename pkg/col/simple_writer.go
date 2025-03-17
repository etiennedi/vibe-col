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
	// If there's no data to write, we're done
	if len(sw.pendingIDs) == 0 {
		return nil
	}

	// Determine if we should write a block
	shouldWrite := force

	// If not forced, check if we have enough data to write a block
	if !force {
		// The target block size is in bytes (e.g., 128KB), but we're comparing to entry counts
		// A typical entry is about 16 bytes (8 for ID, 8 for value), so we divide the target size
		// by 16 to get a reasonable entry count threshold
		const bytesPerEntry = 16

		// Calculate the target number of entries per block
		// Use a higher percentage (95%) of the target block size to ensure we get closer to the target
		entriesPerBlock := (sw.targetBlockSize * 95 / 100) / bytesPerEntry

		// Ensure we have a reasonable minimum (at least 1000 entries per block)
		if entriesPerBlock < 1000 {
			entriesPerBlock = 1000
		}

		shouldWrite = len(sw.pendingIDs) >= entriesPerBlock
	}

	if shouldWrite {
		// If we have enough data, try to estimate the block size first
		if len(sw.pendingIDs) > 1000 && !force {
			// Try to estimate the block size for the current pending data
			estimatedSize, err := sw.writer.EstimateBlockSize(sw.pendingIDs, sw.pendingValues)
			if err == nil {
				// If the estimated size is significantly below the target, collect more data
				if float64(estimatedSize) < float64(sw.targetBlockSize)*0.85 {
					// We're still below 85% of the target, don't flush yet unless forced
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
		// Let's try writing in smaller batches to find the optimal size

		// Start with a reasonable batch size (3/4 of the pending items)
		batchSize := len(sw.pendingIDs) * 3 / 4
		if batchSize < 1000 {
			batchSize = 1000 // Minimum batch size
		}

		// Ensure batch size is not larger than the pending items
		if batchSize > len(sw.pendingIDs) {
			batchSize = len(sw.pendingIDs)
		}

		// Keep track of the minimum batch size we've tried
		minBatchSize := batchSize

		for len(sw.pendingIDs) > 0 {
			// Adjust batch size if we have fewer items than the batch size
			if batchSize > len(sw.pendingIDs) {
				batchSize = len(sw.pendingIDs)
			}

			// Try to write a batch
			err := sw.writer.WriteBlock(sw.pendingIDs[:batchSize], sw.pendingValues[:batchSize])

			if err == nil {
				// Batch was written successfully
				sw.totalItems += uint64(batchSize)
				sw.pendingIDs = sw.pendingIDs[batchSize:]
				sw.pendingValues = sw.pendingValues[batchSize:]

				// If we've written all items, we're done
				if len(sw.pendingIDs) == 0 {
					break
				}

				// Try a larger batch size for the next iteration if this one succeeded
				batchSize = int(float64(batchSize) * 1.5)
				if batchSize > len(sw.pendingIDs) {
					batchSize = len(sw.pendingIDs)
				}
			} else if blockFullErr, ok := err.(*BlockFullError); ok {
				// Block was full, reduce batch size
				if blockFullErr.ItemsWritten > 0 {
					// Some items were written, update our state
					sw.totalItems += uint64(blockFullErr.ItemsWritten)
					sw.pendingIDs = sw.pendingIDs[blockFullErr.ItemsWritten:]
					sw.pendingValues = sw.pendingValues[blockFullErr.ItemsWritten:]
				} else {
					// No items were written, reduce batch size and try again
					batchSize = batchSize / 2

					// If we've tried a very small batch size and it still doesn't work,
					// there might be an issue with the data or the writer
					if batchSize < 100 {
						// Try with an absolute minimum batch size of 1
						if minBatchSize <= 1 {
							// If we've already tried with batch size 1 and it still doesn't work,
							// there's a serious issue
							return fmt.Errorf("failed to write even a single item")
						}

						// Try with a single item
						batchSize = 1
						minBatchSize = 1
					}
				}
			} else {
				// Some other error occurred
				return fmt.Errorf("failed to write block: %w", err)
			}
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
