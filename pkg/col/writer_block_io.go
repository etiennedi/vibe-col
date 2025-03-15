package col

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteBlock writes a block of ID-value pairs
func (w *Writer) WriteBlock(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return fmt.Errorf("cannot write empty block")
	}

	// Add all IDs to the global ID set
	for _, id := range ids {
		w.globalIDs[id] = struct{}{}
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

	// Verify block size calculation
	expectedBlockSize := blockHeaderSize + blockLayoutSize + uint64(idSectionSize) + uint64(valueSectionSize)
	blockSizeDifference := blockSize - expectedBlockSize
	if blockSizeDifference != 0 {
		return fmt.Errorf("block size mismatch: expected=%d, actual=%d, diff=%d",
			expectedBlockSize, blockSize, blockSizeDifference)
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
