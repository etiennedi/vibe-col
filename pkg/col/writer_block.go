package col

import (
	"encoding/binary"
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

	// Write block header fields
	if err := binary.Write(w.file, binary.LittleEndian, minID); err != nil {
		return 0, fmt.Errorf("failed to write min ID: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, maxID); err != nil {
		return 0, fmt.Errorf("failed to write max ID: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, minValueU64); err != nil {
		return 0, fmt.Errorf("failed to write min value: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, maxValueU64); err != nil {
		return 0, fmt.Errorf("failed to write max value: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, sumU64); err != nil {
		return 0, fmt.Errorf("failed to write sum: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, count); err != nil {
		return 0, fmt.Errorf("failed to write count: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, w.encodingType); err != nil {
		return 0, fmt.Errorf("failed to write encoding type: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, uint32(CompressionNone)); err != nil {
		return 0, fmt.Errorf("failed to write compression type: %w", err)
	}

	// This is not the complete block header yet - the next part of the code will write:
	// - uncompressedSize (4 bytes)
	// - compressedSize (4 bytes)
	// - checksum (8 bytes)
	// - reserved bytes (8 bytes)
	// These are not included in this function, but they are part of the 64-byte block header

	// The block header up to this point should be 48 bytes:
	// - minID (8 bytes)
	// - maxID (8 bytes)
	// - minValue (8 bytes)
	// - maxValue (8 bytes)
	// - sum (8 bytes)
	// - count (4 bytes)
	// - encodingType (4 bytes)
	// - compressionType (4 bytes)
	// = 52 bytes

	// Verify we've written the expected number of bytes so far
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("failed to get current position: %w", err)
	}

	writtenSoFar := currentPos - headerStart
	expectedSoFar := int64(8 + 8 + 8 + 8 + 8 + 4 + 4 + 4) // 52 bytes

	if writtenSoFar != expectedSoFar {
		return writtenSoFar, fmt.Errorf("block header partial size mismatch: expected=%d, actual=%d",
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

	entry := NewFooterEntry(
		blockOffset,
		uint32(blockSize),
		minID, maxID,
		minValue, maxValue, sum,
		count,
	)

	if err := binary.Write(w.file, binary.LittleEndian, entry.BlockOffset); err != nil {
		return fmt.Errorf("failed to write block offset: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, entry.BlockSize); err != nil {
		return fmt.Errorf("failed to write block size: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, entry.MinID); err != nil {
		return fmt.Errorf("failed to write min ID: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, entry.MaxID); err != nil {
		return fmt.Errorf("failed to write max ID: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, entry.MinValue); err != nil {
		return fmt.Errorf("failed to write min value: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, entry.MaxValue); err != nil {
		return fmt.Errorf("failed to write max value: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, entry.Sum); err != nil {
		return fmt.Errorf("failed to write sum: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, entry.Count); err != nil {
		return fmt.Errorf("failed to write count: %w", err)
	}

	// Verify footer entry size
	footerEntryEnd, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get footer entry end position: %w", err)
	}

	actualFooterEntrySize := footerEntryEnd - footerEntryStart
	// A footer entry consists of:
	// - BlockOffset (8 bytes)
	// - BlockSize (4 bytes)
	// - MinID (8 bytes)
	// - MaxID (8 bytes)
	// - MinValue (8 bytes)
	// - MaxValue (8 bytes)
	// - Sum (8 bytes)
	// - Count (4 bytes)
	// Total: 56 bytes
	expectedFooterEntrySize := int64(8 + 4 + 8 + 8 + 8 + 8 + 8 + 4)

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
