package col

import (
	"encoding/binary"
	"fmt"
)

// readBlock reads a block from the file
func (r *Reader) readBlock(blockIndex int) ([]uint64, []int64, error) {
	// Validate block index
	if blockIndex < 0 || blockIndex >= len(r.blockIndex) {
		return nil, nil, fmt.Errorf("invalid block index: %d", blockIndex)
	}

	// Get block information from the index
	blockOffset := int64(r.blockIndex[blockIndex].BlockOffset)
	blockSize := int64(r.blockIndex[blockIndex].BlockSize)
	count := int(r.blockIndex[blockIndex].Count)

	// Read the entire block data in one call (excluding the block header)
	// We need to read the layout section (16 bytes) and the data sections
	dataOffset := blockOffset + blockHeaderSize
	dataSize := int(blockSize) - blockHeaderSize

	// Read all data after the header in one call
	blockData, err := r.readBytesAt(dataOffset, dataSize)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read block data: %w", err)
	}

	// Parse the layout section (first 16 bytes)
	idSectionOffset := binary.LittleEndian.Uint32(blockData[0:4])
	idSectionSize := binary.LittleEndian.Uint32(blockData[4:8])
	valueSectionOffset := binary.LittleEndian.Uint32(blockData[8:12])
	valueSectionSize := binary.LittleEndian.Uint32(blockData[12:16])

	// Validate header values
	if idSectionSize == 0 {
		return nil, nil, fmt.Errorf("ID section size in header is 0")
	}
	if valueSectionSize == 0 {
		return nil, nil, fmt.Errorf("Value section size in header is 0")
	}

	// Extract ID and value sections from the buffer
	// The layout section is 16 bytes, followed by the data sections
	idStart := 16 + int(idSectionOffset)
	idEnd := idStart + int(idSectionSize)

	valueStart := 16 + int(valueSectionOffset)
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
