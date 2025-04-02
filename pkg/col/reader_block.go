package col

import (
	"encoding/binary"
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

	// Skip the block header (64 bytes) and parse the layout section (next 16 bytes)
	idSectionOffset := binary.LittleEndian.Uint32(blockData[blockHeaderSize : blockHeaderSize+4])
	idSectionSize := binary.LittleEndian.Uint32(blockData[blockHeaderSize+4 : blockHeaderSize+8])
	valueSectionOffset := binary.LittleEndian.Uint32(blockData[blockHeaderSize+8 : blockHeaderSize+12])
	valueSectionSize := binary.LittleEndian.Uint32(blockData[blockHeaderSize+12 : blockHeaderSize+16])

	// If the section sizes are zero, try the alternate layout offset
	if idSectionSize == 0 || valueSectionSize == 0 {
		// Check if there's enough data to read from the alternate offset
		layoutOffset := 128 - blockOffset
		if layoutOffset >= 0 && layoutOffset+16 <= int64(len(blockData)) {
			// Read layout from alternate offset
			idSectionOffset = binary.LittleEndian.Uint32(blockData[layoutOffset : layoutOffset+4])
			idSectionSize = binary.LittleEndian.Uint32(blockData[layoutOffset+4 : layoutOffset+8])
			valueSectionOffset = binary.LittleEndian.Uint32(blockData[layoutOffset+8 : layoutOffset+12])
			valueSectionSize = binary.LittleEndian.Uint32(blockData[layoutOffset+12 : layoutOffset+16])
		}
	}

	// Validate section sizes
	if idSectionSize == 0 {
		return nil, nil, fmt.Errorf("ID section size in header is 0")
	}
	if valueSectionSize == 0 {
		return nil, nil, fmt.Errorf("Value section size in header is 0")
	}

	// Extract ID and value sections from the buffer
	// The layout section is 16 bytes, followed by the data sections
	idStart := blockHeaderSize + 16 + int(idSectionOffset)
	idEnd := idStart + int(idSectionSize)

	valueStart := blockHeaderSize + 16 + int(valueSectionOffset)
	valueEnd := valueStart + int(valueSectionSize)

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
