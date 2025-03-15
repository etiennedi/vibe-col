package col

import (
	"encoding/binary"
	"fmt"
	"io"
)

// BlockFullError is returned when a block would exceed the target size
type BlockFullError struct {
	ItemsWritten int // Number of items successfully written
}

func (e *BlockFullError) Error() string {
	return fmt.Sprintf("block full after writing %d items", e.ItemsWritten)
}

// WriteBlock writes a block of ID-value pairs
// If the block would exceed the target size, it writes as many items as possible
// and returns a BlockFullError with information about how many items were written
func (w *Writer) WriteBlock(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return fmt.Errorf("cannot write empty block")
	}

	// First, check if the entire block would exceed the target size
	estimatedSize, err := w.EstimateBlockSize(ids, values)
	if err != nil {
		return fmt.Errorf("failed to estimate block size: %w", err)
	}

	// If the block would exceed the target size and we have more than one item,
	// try to find the maximum number of items that would fit
	if estimatedSize > uint64(w.blockSizeTarget) && len(ids) > 1 {
		// Start with a single item and incrementally add more until we reach the target size
		var optimal int = 1

		// Try each size from 1 to len(ids)-1
		for i := 1; i < len(ids); i++ {
			size, err := w.EstimateBlockSize(ids[:i], values[:i])
			if err != nil {
				break
			}

			if size <= uint64(w.blockSizeTarget) {
				optimal = i
			} else {
				// We've exceeded the target size, stop here
				break
			}
		}

		// Write the partial block
		if err := w.writeBlockInternal(ids[:optimal], values[:optimal]); err != nil {
			return err
		}

		// Return a BlockFullError with the number of items written
		return &BlockFullError{ItemsWritten: optimal}
	}

	// If we get here, either the block fits or we couldn't find a partial solution
	return w.writeBlockInternal(ids, values)
}

// writeBlockInternal is the actual implementation of WriteBlock
// It writes the block without checking the target size
func (w *Writer) writeBlockInternal(ids []uint64, values []int64) error {
	// Add all IDs to the global ID bitmap
	for _, id := range ids {
		w.globalIDs.Set(id)
	}

	// Determine if we need to use variable-length encoding
	useVarIntForIDs := w.encodingType == EncodingVarInt ||
		w.encodingType == EncodingVarIntID ||
		w.encodingType == EncodingVarIntBoth
	useVarIntForValues := w.encodingType == EncodingVarInt ||
		w.encodingType == EncodingVarIntValue ||
		w.encodingType == EncodingVarIntBoth

	// Encode IDs and values
	encodedIDs, encodedIdBytes, idSectionSize, err := w.encodeIDs(ids)
	if err != nil {
		return err
	}

	encodedValues, encodedValueBytes, valueSectionSize, err := w.encodeValues(values)
	if err != nil {
		return err
	}

	// Calculate statistics for the block using ORIGINAL values, not encoded values
	// This ensures that aggregations are correct regardless of encoding
	minID, maxID := calculateMinMaxUint64(ids)
	minValue, maxValue := calculateMinMaxInt64(values)
	sum := calculateSumInt64(values)
	count := uint32(len(ids))

	// Write block header (64 bytes)
	blockStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get block start position: %w", err)
	}

	// Store this position so we can refer to it later in the footer
	w.blockPositions = append(w.blockPositions, uint64(blockStart))

	// Convert int64 values to uint64 for storage
	minValueU64 := int64ToUint64(minValue)
	maxValueU64 := int64ToUint64(maxValue)
	sumU64 := int64ToUint64(sum)

	headerWritten := int64(0)
	// Write block header
	if n, err := w.writeBlockHeader(minID, maxID, minValueU64, maxValueU64, sumU64, count); err != nil {
		return err
	} else {
		headerWritten = n
	}

	// Total data size (ID section + value section) helps with debugging
	// but isn't needed for the file format

	uncompressedSize := int32(0)       // Not implemented yet
	compressedSize := uncompressedSize // Same as uncompressed for now

	if err := binary.Write(w.file, binary.LittleEndian, uncompressedSize); err != nil {
		return fmt.Errorf("failed to write uncompressed size: %w", err)
	}
	headerWritten += 4
	if err := binary.Write(w.file, binary.LittleEndian, compressedSize); err != nil {
		return fmt.Errorf("failed to write compressed size: %w", err)
	}
	headerWritten += 4

	// Write checksum placeholder (will be updated later when checksums are implemented)
	if _, err := w.file.Seek(0, io.SeekCurrent); err != nil {
		return fmt.Errorf("failed to get current position: %w", err)
	}

	if err := binary.Write(w.file, binary.LittleEndian, uint64(0)); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	headerWritten += 8

	reserved := blockHeaderSize - headerWritten
	if _, err := w.file.Seek(reserved, io.SeekCurrent); err != nil {
		return fmt.Errorf("failed to skip reserved bytes: %w", err)
	}
	headerWritten += reserved

	if headerWritten != blockHeaderSize {
		return fmt.Errorf("block header size mismatch: expected=%d, actual=%d",
			blockHeaderSize, headerWritten)
	}

	// Write the block layout section (16 bytes)
	// The section layout according to spec:
	// 1. ID section offset (from start of data section)
	// 2. ID section size in bytes
	// 3. Value section offset (from start of data section)
	// 4. Value section size in bytes

	// Validate section sizes
	if idSectionSize == 0 {
		return fmt.Errorf("ID section size is 0, which is invalid. useVarIntForIDs=%v, count=%d",
			useVarIntForIDs, count)
	}

	if valueSectionSize == 0 {
		return fmt.Errorf("Value section size is 0, which is invalid. useVarIntForValues=%v, count=%d",
			useVarIntForValues, count)
	}

	// Per spec section 4.2:
	// - ID section comes first in the data section
	// - Value section follows the ID section
	// The offsets are relative to the end of the block header (after the 16-byte layout section)
	idSectionOffset := uint32(0)
	valueSectionOffset := idSectionSize

	// Create a layout buffer and fill it
	layoutBuf := make([]byte, 16)
	binary.LittleEndian.PutUint32(layoutBuf[0:4], idSectionOffset)
	binary.LittleEndian.PutUint32(layoutBuf[4:8], idSectionSize)
	binary.LittleEndian.PutUint32(layoutBuf[8:12], valueSectionOffset)
	binary.LittleEndian.PutUint32(layoutBuf[12:16], valueSectionSize)

	// Write the layout buffer to file
	bytesWritten, err := w.file.Write(layoutBuf)
	if err != nil {
		return fmt.Errorf("failed to write block layout: %w", err)
	}
	if bytesWritten != 16 {
		return fmt.Errorf("failed to write block layout: wrote %d bytes, expected 16", bytesWritten)
	}

	// Start of data section - this position is important for checksum calculation
	// when that feature is implemented
	dataSectionStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get data section position: %w", err)
	}
	_ = dataSectionStart // Unused for now

	// Write ID array based on encoding type
	var actualIdSectionSize int64 = 0

	if useVarIntForIDs {
		// Use variable-length encoding for IDs (using precomputed values)
		for i := range encodedIDs {
			// Write the precomputed varint bytes for this ID
			written, err := w.file.Write(encodedIdBytes[i])
			if err != nil {
				return fmt.Errorf("failed to write varint ID: %w", err)
			}
			actualIdSectionSize += int64(written)
		}
	} else {
		// Write fixed-length IDs
		for _, id := range encodedIDs {
			if err := binary.Write(w.file, binary.LittleEndian, id); err != nil {
				return fmt.Errorf("failed to write ID: %w", err)
			}
			actualIdSectionSize += 8
		}
	}

	// Verify ID section size
	if uint32(actualIdSectionSize) != idSectionSize {
		return fmt.Errorf("ID section size mismatch: expected=%d, actual=%d",
			idSectionSize, actualIdSectionSize)
	}

	// Write Value array based on encoding type
	var actualValueSectionSize int64 = 0

	if useVarIntForValues {
		// Use variable-length encoding for values (using precomputed values)
		for i := range encodedValues {
			// Write the precomputed varint bytes for this value
			written, err := w.file.Write(encodedValueBytes[i])
			if err != nil {
				return fmt.Errorf("failed to write varint value: %w", err)
			}
			actualValueSectionSize += int64(written)
		}
	} else {
		// Write fixed-length values
		for _, val := range encodedValues {
			if err := binary.Write(w.file, binary.LittleEndian, val); err != nil {
				return fmt.Errorf("failed to write value: %w", err)
			}
			actualValueSectionSize += 8
		}
	}

	// Verify value section size
	if uint32(actualValueSectionSize) != valueSectionSize {
		return fmt.Errorf("value section size mismatch: expected=%d, actual=%d",
			valueSectionSize, actualValueSectionSize)
	}

	// Get end position to calculate block size
	blockEnd, err := w.file.Seek(0, io.SeekCurrent)
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
		_, err := w.file.Write(paddingBuf)
		if err != nil {
			return fmt.Errorf("failed to write padding bytes: %w", err)
		}

		// Update block end position and size after padding
		blockEnd += padding
		blockSize += uint64(padding)
	}

	// Verify block size calculation (only for the actual data, excluding padding)
	expectedBlockSize := blockHeaderSize + blockLayoutSize + uint64(idSectionSize) + uint64(valueSectionSize)
	blockSizeDifference := (blockSize - uint64(padding)) - expectedBlockSize
	if blockSizeDifference != 0 {
		return fmt.Errorf("block size mismatch: expected=%d, actual=%d, diff=%d",
			expectedBlockSize, blockSize-uint64(padding), blockSizeDifference)
	}

	w.blockSizes = append(w.blockSizes, uint32(blockSize))

	// Store block statistics for footer
	w.blockStats = append(w.blockStats, BlockStats{
		MinID:    minID,
		MaxID:    maxID,
		MinValue: minValue,
		MaxValue: maxValue,
		Sum:      sum,
		Count:    count,
	})

	// Increment block count
	w.blockCount++

	// Sync to disk to ensure data consistency
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// EstimateBlockSize calculates the exact size a block would be without writing it
// This is useful for determining if a block would fit within a target size
func (w *Writer) EstimateBlockSize(ids []uint64, values []int64) (uint64, error) {
	if len(ids) != len(values) {
		return 0, fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return 0, fmt.Errorf("cannot estimate empty block")
	}

	// Encode IDs and values to get exact sizes
	_, _, idSectionSize, err := w.encodeIDs(ids)
	if err != nil {
		return 0, err
	}

	_, _, valueSectionSize, err := w.encodeValues(values)
	if err != nil {
		return 0, err
	}

	// Calculate total block size
	// Block header + block layout + ID section + value section
	totalSize := uint64(blockHeaderSize + blockLayoutSize + idSectionSize + valueSectionSize)

	// Add padding size if needed for page alignment
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("failed to get current position: %w", err)
	}

	// Calculate where the block would end
	blockEnd := currentPos + int64(totalSize)

	// Add padding if needed
	padding := calculatePadding(blockEnd, PageSize)
	if padding > 0 {
		totalSize += uint64(padding)
	}

	return totalSize, nil
}
