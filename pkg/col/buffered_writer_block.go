package col

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteBlock writes a block of ID-value pairs with alternative implementation
// that follows the exact format used in the TestCreateBasicColumnFile test
func (bw *BufferedWriter) WriteBlock(blockData *BlockData) error {
	if bw.closed {
		return fmt.Errorf("writer is already closed")
	}

	// everything from here on is almost an exact copy of
	// Writer.writeBlockInternal. This can be unified TODO

	// Write block header (64 bytes)
	blockStart, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get block start position: %w", err)
	}

	// Convert int64 values to uint64 for storage

	// Write block header
	headerBuf := make([]byte, blockHeaderSize)
	n := bw.writeBlockHeader(headerBuf, blockData.MinID, blockData.MaxID,
		blockData.MinValue, blockData.MaxValue, blockData.Sum, blockData.Count)

	if n != len(headerBuf) {
		return fmt.Errorf("block header size mismatch: expected=%d, actual=%d",
			len(headerBuf), n)
	}
	headerWritten := int64(0)
	if n, err := bw.file.Write(headerBuf); err != nil {
		return fmt.Errorf("failed to write block header: %w", err)
	} else {
		headerWritten += int64(n)
	}

	blockData.IDSectionSize = uint32(len(blockData.SerializedIDSection))
	blockData.ValueSectionSize = uint32(len(blockData.SerializedValueSection))

	// Validate section sizes
	if blockData.IDSectionSize == 0 {
		return fmt.Errorf("ID section size is 0, which is invalid. count=%d",
			blockData.Count)
	}

	if blockData.ValueSectionSize == 0 {
		return fmt.Errorf("value section size is 0, which is invalid. count=%d",
			blockData.Count)
	}

	// Per spec section 4.2:
	// - ID section comes first in the data section
	// - Value section follows the ID section
	// The offsets are relative to the end of the block header (after the 16-byte layout section)
	idSectionOffset := uint32(0)
	valueSectionOffset := blockData.IDSectionSize

	// Create a layout buffer and fill it
	layoutBuf := make([]byte, 16)
	binary.LittleEndian.PutUint32(layoutBuf[0:4], idSectionOffset)
	binary.LittleEndian.PutUint32(layoutBuf[4:8], blockData.IDSectionSize)
	binary.LittleEndian.PutUint32(layoutBuf[8:12], valueSectionOffset)
	binary.LittleEndian.PutUint32(layoutBuf[12:16], blockData.ValueSectionSize)

	// Write the layout buffer to file
	bytesWritten, err := bw.file.Write(layoutBuf)
	if err != nil {
		return fmt.Errorf("failed to write block layout: %w", err)
	}
	if bytesWritten != 16 {
		return fmt.Errorf("failed to write block layout: wrote %d bytes, expected 16", bytesWritten)
	}

	// Start of data section - this position is important for checksum calculation
	// when that feature is implemented
	dataSectionStart, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get data section position: %w", err)
	}
	_ = dataSectionStart // Unused for now

	// Write ID section directly from pre-serialized data
	actualIdSectionSize, err := bw.file.Write(blockData.SerializedIDSection)
	if err != nil {
		return fmt.Errorf("failed to write ID section: %w", err)
	}

	// Verify ID section size
	if uint32(actualIdSectionSize) != blockData.IDSectionSize {
		return fmt.Errorf("ID section size mismatch: expected=%d, actual=%d",
			blockData.IDSectionSize, actualIdSectionSize)
	}

	// Write Value section directly from pre-serialized data
	actualValueSectionSize, err := bw.file.Write(blockData.SerializedValueSection)
	if err != nil {
		return fmt.Errorf("failed to write value section: %w", err)
	}

	// Verify value section size
	if uint32(actualValueSectionSize) != blockData.ValueSectionSize {
		return fmt.Errorf("value section size mismatch: expected=%d, actual=%d",
			blockData.ValueSectionSize, actualValueSectionSize)
	}

	// Get end position to calculate block size
	blockEnd, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get block end position: %w", err)
	}

	// Calculate actual block size
	blockSize := uint64(blockEnd - blockStart)

	// Add padding if needed to align to page boundary
	padding := calculatePadding(blockEnd, PageSize)
	if padding > 0 {
		// Create padding buffer filled with zeros
		paddingBuf := make([]byte, padding)

		// Write padding bytes
		_, err := bw.file.Write(paddingBuf)
		if err != nil {
			return fmt.Errorf("failed to write padding bytes: %w", err)
		}

		// Update block end position and size after padding
		blockEnd += padding
		blockSize += uint64(padding)
	}

	// Verify block size calculation (only for the actual data, excluding padding)
	expectedBlockSize := blockHeaderSize + blockLayoutSize + uint64(blockData.IDSectionSize) + uint64(blockData.ValueSectionSize)
	blockSizeDifference := (blockSize - uint64(padding)) - expectedBlockSize
	if blockSizeDifference != 0 {
		return fmt.Errorf("block size mismatch: expected=%d, actual=%d, diff=%d",
			expectedBlockSize, blockSize-uint64(padding), blockSizeDifference)
	}

	// Store block statistics for footer
	bw.blockIndex = append(bw.blockIndex, FooterEntry{
		MinID:       blockData.MinID,
		MaxID:       blockData.MaxID,
		MinValue:    int64ToUint64(blockData.MinValue),
		MaxValue:    int64ToUint64(blockData.MaxValue),
		Sum:         int64ToUint64(blockData.Sum),
		Count:       blockData.Count,
		BlockOffset: uint64(blockStart),
		BlockSize:   uint32(blockSize),
	})

	// Update block count
	bw.blockCount++

	// Sync to disk to ensure data consistency
	if err := bw.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// writeBlockHeader writes a block header to the provided buffer
func (bw *BufferedWriter) writeBlockHeader(buffer []byte, minID, maxID uint64, minValue, maxValue, sum int64, count uint32) int {
	// The block header size should be 96 bytes total:
	// - minID (8 bytes)
	// - maxID (8 bytes)
	// - minValue (8 bytes)
	// - maxValue (8 bytes)
	// - sum (8 bytes)
	// - count (4 bytes)
	// - encodingType (4 bytes)
	// - compressionType (4 bytes)
	// - uncompressedSize (4 bytes)
	// - compressedSize (4 bytes)
	// - checksum (8 bytes)
	// - reserved (28 bytes - 96-all of the above)

	offset := 0

	// Use the exact same binary representation as the standard Writer
	binary.LittleEndian.PutUint64(buffer[offset:], minID)
	offset += 8

	binary.LittleEndian.PutUint64(buffer[offset:], maxID)
	offset += 8

	binary.LittleEndian.PutUint64(buffer[offset:], int64ToUint64(minValue))
	offset += 8

	binary.LittleEndian.PutUint64(buffer[offset:], int64ToUint64(maxValue))
	offset += 8

	binary.LittleEndian.PutUint64(buffer[offset:], int64ToUint64(sum))
	offset += 8

	binary.LittleEndian.PutUint32(buffer[offset:], count)
	offset += 4

	binary.LittleEndian.PutUint32(buffer[offset:], bw.encodingType)
	offset += 4

	binary.LittleEndian.PutUint32(buffer[offset:], uint32(CompressionNone))
	offset += 4

	// Add zeros for the rest of the header fields we don't currently use
	// uncompressedSize (4 bytes)
	binary.LittleEndian.PutUint32(buffer[offset:], 0)
	offset += 4

	// compressedSize (4 bytes)
	binary.LittleEndian.PutUint32(buffer[offset:], 0)
	offset += 4

	// checksum (8 bytes)
	binary.LittleEndian.PutUint64(buffer[offset:], 0)
	offset += 8

	reserved := blockHeaderSize - offset
	// do nothing with reserved space, since we have an empty buffer
	offset += reserved

	if offset != blockHeaderSize {
		panic(fmt.Errorf("block header size mismatch: expected=%d, actual=%d",
			blockHeaderSize, offset))
	}

	return offset
}
