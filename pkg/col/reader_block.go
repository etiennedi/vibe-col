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

	// Skip the block header and read the block layout (16 bytes)
	layoutOffset := blockOffset + blockHeaderSize
	layoutBuf, err := r.readBytesAt(layoutOffset, 16)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read block layout: %w", err)
	}

	// Parse the layout
	idSectionOffset := binary.LittleEndian.Uint32(layoutBuf[0:4])
	idSectionSize := binary.LittleEndian.Uint32(layoutBuf[4:8])
	valueSectionOffset := binary.LittleEndian.Uint32(layoutBuf[8:12])
	valueSectionSize := binary.LittleEndian.Uint32(layoutBuf[12:16])

	// Validate header values
	if idSectionSize == 0 {
		return nil, nil, fmt.Errorf("ID section size in header is 0")
	}
	if valueSectionSize == 0 {
		return nil, nil, fmt.Errorf("Value section size in header is 0")
	}

	// Calculate absolute offsets for ID and value sections
	dataSectionStart := layoutOffset + 16
	absoluteIdOffset := dataSectionStart + int64(idSectionOffset)
	absoluteValueOffset := dataSectionStart + int64(valueSectionOffset)

	// Read ID section
	idBytes, err := r.readBytesAt(absoluteIdOffset, int(idSectionSize))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read ID section: %w", err)
	}

	// Read value section
	valueBytes, err := r.readBytesAt(absoluteValueOffset, int(valueSectionSize))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read value section: %w", err)
	}

	// Verify we're not reading beyond the block size
	totalBytesRead := blockHeaderSize + 16 + int64(idSectionSize) + int64(valueSectionSize)
	if totalBytesRead > blockSize {
		return nil, nil, fmt.Errorf("read beyond block end: read %d bytes, block size is %d",
			totalBytesRead, blockSize)
	}

	// Decode IDs and values
	ids, values, err := decodeBlockData(idBytes, valueBytes, count, r.header.EncodingType)
	if err != nil {
		return nil, nil, err
	}

	return ids, values, nil
}
