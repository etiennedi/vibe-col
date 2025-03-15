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
		targetBlockSize: 128 * 1024, // 128KB default block size
		closed:          false,
		totalItems:      0,
	}, nil
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

// estimateBlockSize estimates the size of a block with the given number of items
// This takes into account the block header, layout section, and data
func (sw *SimpleWriter) estimateBlockSize(itemCount int) int {
	// Block header (64 bytes) + block layout (16 bytes)
	baseSize := blockHeaderSize + blockLayoutSize

	// Calculate data size based on encoding type
	dataSize := 0

	// Check if we're using varint encoding
	useVarIntForIDs := sw.writer.encodingType == EncodingVarInt ||
		sw.writer.encodingType == EncodingVarIntID ||
		sw.writer.encodingType == EncodingVarIntBoth
	useVarIntForValues := sw.writer.encodingType == EncodingVarInt ||
		sw.writer.encodingType == EncodingVarIntValue ||
		sw.writer.encodingType == EncodingVarIntBoth

	if useVarIntForIDs && useVarIntForValues {
		// For varint encoding of both IDs and values, we estimate an average of 3 bytes per value
		// This is a rough estimate - actual size depends on the magnitude of values
		dataSize = itemCount * 6 // 3 bytes per ID + 3 bytes per value (average)
	} else if useVarIntForIDs {
		// Varint for IDs only, fixed-size for values
		dataSize = itemCount * (3 + 8) // 3 bytes per ID + 8 bytes per value
	} else if useVarIntForValues {
		// Fixed-size for IDs, varint for values
		dataSize = itemCount * (8 + 3) // 8 bytes per ID + 3 bytes per value
	} else {
		// Fixed-size encoding for both IDs and values
		dataSize = itemCount * 16 // 8 bytes per ID + 8 bytes per value
	}

	return baseSize + dataSize
}

// flushIfNeeded writes a block if there's enough data or if force is true
func (sw *SimpleWriter) flushIfNeeded(force bool) error {
	// If we don't have any data, there's nothing to flush
	if len(sw.pendingIDs) == 0 {
		return nil
	}

	// If we have enough data to fill a block or we're forced to flush
	estimatedSize := sw.estimateBlockSize(len(sw.pendingIDs))

	if force || estimatedSize >= sw.targetBlockSize {
		// Determine how many items to write
		itemsToWrite := len(sw.pendingIDs)

		// If not forced and we have more data than the target block size,
		// calculate how many items would fit in a block
		if !force && estimatedSize > sw.targetBlockSize {
			// Calculate how many items would fit in the target block size
			// Account for the fixed overhead of the block
			dataSpace := sw.targetBlockSize - (blockHeaderSize + blockLayoutSize)

			// Calculate items to write based on encoding type
			useVarIntForIDs := sw.writer.encodingType == EncodingVarInt ||
				sw.writer.encodingType == EncodingVarIntID ||
				sw.writer.encodingType == EncodingVarIntBoth
			useVarIntForValues := sw.writer.encodingType == EncodingVarInt ||
				sw.writer.encodingType == EncodingVarIntValue ||
				sw.writer.encodingType == EncodingVarIntBoth

			bytesPerItem := 16 // Default for fixed-size encoding

			if useVarIntForIDs && useVarIntForValues {
				bytesPerItem = 6 // Estimated for varint encoding of both
			} else if useVarIntForIDs || useVarIntForValues {
				bytesPerItem = 11 // Estimated for varint encoding of one field
			}

			itemsToWrite = dataSpace / bytesPerItem

			// Ensure we write at least one item
			if itemsToWrite <= 0 {
				itemsToWrite = 1
			}

			// Don't exceed the number of items we have
			if itemsToWrite > len(sw.pendingIDs) {
				itemsToWrite = len(sw.pendingIDs)
			}
		}

		// Write the block
		if err := sw.writer.WriteBlock(
			sw.pendingIDs[:itemsToWrite],
			sw.pendingValues[:itemsToWrite]); err != nil {
			return fmt.Errorf("failed to write block: %w", err)
		}

		// Update total items count
		sw.totalItems += uint64(itemsToWrite)

		// Keep the remaining data for the next block
		sw.pendingIDs = sw.pendingIDs[itemsToWrite:]
		sw.pendingValues = sw.pendingValues[itemsToWrite:]

		// If we still have enough data for another block, flush again
		if len(sw.pendingIDs) > 0 && sw.estimateBlockSize(len(sw.pendingIDs)) >= sw.targetBlockSize {
			return sw.flushIfNeeded(false)
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
