package col

import (
	"fmt"
)

// readEntireBlock reads an entire block from the file in a single syscall
// and returns the parsed IDs and values
func (r *Reader) readEntireBlock(blockIndex int) ([]uint64, []int64, error) {
	// Validate block index
	if blockIndex < 0 || blockIndex >= len(r.blockIndex) {
		return nil, nil, fmt.Errorf("invalid block index: %d", blockIndex)
	}

	// Get block information from the index
	blockOffset := int64(r.blockIndex[blockIndex].BlockOffset)
	blockSize := int64(r.blockIndex[blockIndex].BlockSize)
	count := int(r.blockIndex[blockIndex].Count)

	// Read the entire block in a single syscall
	blockData, err := r.readBytesAt(blockOffset, int(blockSize))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read block: %w", err)
	}

	// Extract the block header (first 64 bytes)
	headerBytes := blockData[:blockHeaderSize]
	var blockHeader BlockHeader
	if err := blockHeader.Deserialize(headerBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize block header: %w", err)
	}

	// Extract the block layout (next 16 bytes)
	layoutBytes := blockData[blockHeaderSize : blockHeaderSize+blockLayoutSize]
	var layout BlockLayout
	if err := layout.Deserialize(layoutBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize block layout: %w", err)
	}

	// If the section sizes are zero, try the alternate layout offset
	if layout.IDSectionSize == 0 || layout.ValueSectionSize == 0 {
		// Check if there's enough data to read from the alternate offset
		layoutOffset := 128 - blockOffset
		if layoutOffset >= 0 && layoutOffset+blockLayoutSize <= int64(len(blockData)) {
			// Read layout from alternate offset
			altLayoutBytes := blockData[layoutOffset : layoutOffset+blockLayoutSize]
			if err := layout.Deserialize(altLayoutBytes); err != nil {
				return nil, nil, fmt.Errorf("failed to deserialize alternate block layout: %w", err)
			}
		}
	}

	// Validate section sizes
	if layout.IDSectionSize == 0 {
		return nil, nil, fmt.Errorf("id section size in header is 0")
	}
	if layout.ValueSectionSize == 0 {
		return nil, nil, fmt.Errorf("value section size in header is 0")
	}

	// Extract ID and value sections from the buffer
	// The layout section is 16 bytes, followed by the data sections
	idStart := blockHeaderSize + blockLayoutSize + int(layout.IDSectionOffset)
	idEnd := idStart + int(layout.IDSectionSize)

	valueStart := blockHeaderSize + blockLayoutSize + int(layout.ValueSectionOffset)
	valueEnd := valueStart + int(layout.ValueSectionSize)

	// Validate buffer boundaries
	if idEnd > len(blockData) || valueEnd > len(blockData) {
		return nil, nil, fmt.Errorf("section boundaries exceed block data size")
	}

	// Extract the sections
	idBytes := blockData[idStart:idEnd]
	valueBytes := blockData[valueStart:valueEnd]

	// Decode IDs and values
	ids, values, err := decodeBlockData(idBytes, valueBytes, count, r.header.EncodingType)
	if err != nil {
		return nil, nil, err
	}

	return ids, values, nil
}

// readBlock reads a block from the file
// This is now a wrapper around readEntireBlock for backward compatibility
func (r *Reader) readBlock(blockIndex int) ([]uint64, []int64, error) {
	return r.readEntireBlock(blockIndex)
}
