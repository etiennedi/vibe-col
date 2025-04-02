package col

import (
	"fmt"
	"io"
)

// writeBlockHeader writes the block header to the file
func (w *Writer) writeBlockHeader(minID, maxID uint64, minValueU64, maxValueU64, sumU64 uint64, count uint32) (int64, error) {
	// Record start position to verify header size
	headerStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("failed to get block header start position: %w", err)
	}

	// Create a block header struct
	header := &BlockHeader{
		MinID:            minID,
		MaxID:            maxID,
		MinValue:         minValueU64,
		MaxValue:         maxValueU64,
		Sum:              sumU64,
		Count:            count,
		EncodingType:     w.encodingType,
		CompressionType:  CompressionNone,
		UncompressedSize: 0, // Will be filled in later
		CompressedSize:   0, // Will be filled in later
		Checksum:         0, // Will be filled in later
	}

	// Serialize the header
	headerBytes := header.Serialize()

	// Write the header
	_, err = w.file.Write(headerBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write block header: %w", err)
	}

	// Verify header size
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("failed to get current position: %w", err)
	}

	writtenSoFar := currentPos - headerStart
	expectedSoFar := int64(len(headerBytes))

	if writtenSoFar != expectedSoFar {
		return writtenSoFar, fmt.Errorf("block header size mismatch: expected=%d, actual=%d",
			expectedSoFar, writtenSoFar)
	}

	return writtenSoFar, nil
}

// writeBlockFooter writes the block footer to the file
func (w *Writer) writeBlockFooter(blockOffset, blockSize uint64, minID, maxID uint64, minValue, maxValue, sum int64, count uint32) error {
	// Record start position to verify footer entry size
	footerEntryStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get footer entry start position: %w", err)
	}

	// Create a footer entry struct
	entry := NewFooterEntry(
		blockOffset,
		uint32(blockSize),
		minID, maxID,
		minValue, maxValue, sum,
		count,
	)

	// Serialize the footer entry
	entryBytes := entry.Serialize()

	// Write the footer entry
	_, err = w.file.Write(entryBytes)
	if err != nil {
		return fmt.Errorf("failed to write footer entry: %w", err)
	}

	// Verify footer entry size
	footerEntryEnd, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get footer entry end position: %w", err)
	}

	actualFooterEntrySize := footerEntryEnd - footerEntryStart
	expectedFooterEntrySize := int64(len(entryBytes))

	if actualFooterEntrySize != expectedFooterEntrySize {
		return fmt.Errorf("footer entry size mismatch: expected=%d, actual=%d",
			expectedFooterEntrySize, actualFooterEntrySize)
	}

	return nil
}

// encodeIDs encodes the IDs based on the encoding type
func (w *Writer) encodeIDs(ids []uint64) ([]uint64, [][]byte, uint32, error) {
	return encodeData(w.encodingType, ids, deltaEncode, encodeVarInt)
}

// encodeValues encodes the values based on the encoding type
func (w *Writer) encodeValues(values []int64) ([]int64, [][]byte, uint32, error) {
	return encodeData(w.encodingType, values, deltaEncodeInt64, encodeSignedVarInt)
}
