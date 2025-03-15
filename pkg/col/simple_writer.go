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

// flushIfNeeded writes a block if there's enough data or if force is true
func (sw *SimpleWriter) flushIfNeeded(force bool) error {
	// If we don't have any data, there's nothing to flush
	if len(sw.pendingIDs) == 0 {
		return nil
	}

	// Determine if we should write a block
	shouldWrite := force

	// If not forced, check if we have enough data to write a block
	if !force {
		// Try to write a block when we have a reasonable amount of data
		// This ensures we create multiple blocks for large datasets
		shouldWrite = len(sw.pendingIDs) >= 1000 // Try to write after accumulating 1000 items
	}

	if shouldWrite {
		// Try to write all pending items
		err := sw.writer.WriteBlock(sw.pendingIDs, sw.pendingValues)

		// Check if the block was full
		if blockFullErr, ok := err.(*BlockFullError); ok {
			// Block was full, update total items count with what was written
			itemsWritten := blockFullErr.ItemsWritten
			sw.totalItems += uint64(itemsWritten)

			// Keep the remaining data for the next block
			sw.pendingIDs = sw.pendingIDs[itemsWritten:]
			sw.pendingValues = sw.pendingValues[itemsWritten:]

			// Try to write the remaining data in a new block
			return sw.flushIfNeeded(force)
		} else if err != nil {
			// Some other error occurred
			return fmt.Errorf("failed to write block: %w", err)
		}

		// All items were written successfully
		sw.totalItems += uint64(len(sw.pendingIDs))
		sw.pendingIDs = nil
		sw.pendingValues = nil
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
