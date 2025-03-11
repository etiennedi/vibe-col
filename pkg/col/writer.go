package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	// File format constants
	headerSize       = 64       // Size of file header in bytes
	blockHeaderSize  = 96       // Size of block header in bytes
	blockLayoutSize  = 16       // Size of block layout section in bytes
	uint64Size       = 8        // Size of uint64 in bytes
	uint32Size       = 4        // Size of uint32 in bytes
	defaultBlockSize = 4 * 1024 // Default target block size (4KB)
)

// BlockStats holds statistics for a block
type BlockStats struct {
	MinID    uint64
	MaxID    uint64
	MinValue int64
	MaxValue int64
	Sum      int64
	Count    uint32
}

// calculateMinMaxUint64 calculates the minimum and maximum values in a uint64 slice
func calculateMinMaxUint64(values []uint64) (min, max uint64) {
	if len(values) == 0 {
		return 0, 0
	}

	min = values[0]
	max = values[0]

	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max
}

// calculateMinMaxInt64 calculates the minimum and maximum values in an int64 slice
func calculateMinMaxInt64(values []int64) (min, max int64) {
	if len(values) == 0 {
		return 0, 0
	}

	min = values[0]
	max = values[0]

	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max
}

// calculateSumInt64 calculates the sum of values in an int64 slice
func calculateSumInt64(values []int64) int64 {
	var sum int64
	for _, v := range values {
		sum += v
	}
	return sum
}

// WriterOption is a function that configures a Writer
type WriterOption func(*Writer)

// WithEncoding sets the encoding type for the writer
func WithEncoding(encodingType uint32) WriterOption {
	return func(w *Writer) {
		w.encodingType = encodingType
	}
}

// WithBlockSize sets the target block size for the writer
func WithBlockSize(blockSize uint32) WriterOption {
	return func(w *Writer) {
		w.blockSizeTarget = blockSize
	}
}

// Helper function to get minimum of two ints - internal to writer.go
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Writer writes a column file
type Writer struct {
	file            *os.File
	blockCount      uint64
	encodingType    uint32
	blockSizeTarget uint32
	blockPositions  []uint64     // Position of each block in the file
	blockSizes      []uint32     // Size of each block in bytes
	blockStats      []BlockStats // Statistics for each block
}

// NewWriter creates a new column file writer
func NewWriter(filename string, options ...WriterOption) (*Writer, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	writer := &Writer{
		file:            file,
		blockCount:      0,
		encodingType:    EncodingRaw, // Default
		blockSizeTarget: defaultBlockSize,
		blockPositions:  make([]uint64, 0),
		blockSizes:      make([]uint32, 0),
		blockStats:      make([]BlockStats, 0),
	}

	// Apply options
	for _, option := range options {
		option(writer)
	}

	// Write the file header
	if err := writer.writeHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return writer, nil
}

// writeHeader writes the file header to the file
func (w *Writer) writeHeader() error {
	// Record start position to verify header size
	headerStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get header start position: %w", err)
	}

	// Create the header with default values
	header := NewFileHeader(0, w.blockSizeTarget, w.encodingType)

	// Create a buffer for the header fields
	headerFields := []interface{}{
		header.Magic,
		header.Version,
		header.ColumnType,
		header.BlockCount,
		header.BlockSizeTarget,
		header.CompressionType,
		header.EncodingType,
		header.CreationTime,
	}

	// Write all header fields
	for i, field := range headerFields {
		if err := binary.Write(w.file, binary.LittleEndian, field); err != nil {
			return fmt.Errorf("failed to write header field %d: %w", i, err)
		}
	}

	// Calculate reserved space - sum of the sizes of the header fields we've written
	headerFieldsSize := uint64Size + uint32Size + uint32Size + uint64Size +
		uint32Size + uint32Size + uint32Size + uint64Size
	reservedSize := headerSize - headerFieldsSize

	// Write reserved space to fill up to 64 bytes
	reserved := make([]byte, reservedSize)
	if _, err := w.file.Write(reserved); err != nil {
		return fmt.Errorf("failed to write reserved space: %w", err)
	}

	// Verify header size
	headerEnd, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get header end position: %w", err)
	}

	// Calculate actual header size
	actualHeaderSize := headerEnd - headerStart

	// Validate header size
	if actualHeaderSize != int64(headerSize) {
		return fmt.Errorf("header size mismatch: expected=%d, actual=%d", headerSize, actualHeaderSize)
	}

	return nil
}

// encodeData is a helper function to encode data based on the encoding type
func encodeData[T any](encodingType uint32, data []T, deltaEncodeFunc func([]T) []T, encodeVarIntFunc func(T) []byte) ([]T, [][]byte, uint32, error) {
	var encodedData []T
	var encodedDataBytes [][]byte
	var sectionSize uint32

	// First apply delta encoding if needed
	switch encodingType {
	case EncodingRaw, EncodingVarInt, EncodingVarIntID:
		// These encoding types don't use delta encoding
		encodedData = make([]T, len(data))
		copy(encodedData, data)
	case EncodingDeltaID, EncodingDeltaValue, EncodingDeltaBoth, EncodingVarIntValue, EncodingVarIntBoth:
		// These encoding types use delta encoding
		encodedData = deltaEncodeFunc(data)
	default:
		return nil, nil, 0, fmt.Errorf("unsupported encoding type: %d", encodingType)
	}

	// Then apply varint encoding if needed
	switch encodingType {
	case EncodingRaw, EncodingDeltaID, EncodingDeltaValue, EncodingDeltaBoth:
		// Fixed-width encoding
		sectionSize = uint32(len(encodedData) * 8)
	case EncodingVarInt, EncodingVarIntID, EncodingVarIntBoth, EncodingVarIntValue:
		// Variable-width encoding
		encodedDataBytes = make([][]byte, len(encodedData))
		sectionSize = 0
		for i, d := range encodedData {
			encodedDataBytes[i] = encodeVarIntFunc(d)
			dataSize := uint32(len(encodedDataBytes[i]))
			if dataSize == 0 {
				return nil, nil, 0, fmt.Errorf("encoded size of data at index %d is 0", i)
			}
			sectionSize += dataSize
		}
		if sectionSize == 0 && len(encodedData) > 0 {
			return nil, nil, 0, fmt.Errorf("calculated section size is 0 with %d items", len(encodedData))
		}
	}

	return encodedData, encodedDataBytes, sectionSize, nil
}

// encodeIDs encodes the IDs based on the encoding type
func (w *Writer) encodeIDs(ids []uint64) ([]uint64, [][]byte, uint32, error) {
	return encodeData(w.encodingType, ids, deltaEncode, encodeVarInt)
}

// encodeValues encodes the values based on the encoding type
func (w *Writer) encodeValues(values []int64) ([]int64, [][]byte, uint32, error) {
	return encodeData(w.encodingType, values, deltaEncodeInt64, encodeSignedVarInt)
}

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

// WriteBlock writes a block of ID-value pairs
func (w *Writer) WriteBlock(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return fmt.Errorf("cannot write empty block")
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

	// // Start of value section
	// valueSectionStart, err := w.file.Seek(0, io.SeekCurrent)
	// if err != nil {
	// 	return fmt.Errorf("failed to get value section position: %w", err)
	// }

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

// FinalizeAndClose finalizes the file by writing the footer and closes the file
func (w *Writer) FinalizeAndClose() error {
	if err := w.Finalize(); err != nil {
		return err
	}
	return w.file.Close()
}

// Finalize finalizes the file by writing the footer
func (w *Writer) Finalize() error {
	// Update file header with final block count
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start: %w", err)
	}

	// Create updated header
	header := NewFileHeader(w.blockCount, w.blockSizeTarget, w.encodingType)

	// Write header fields
	headerFields := []interface{}{
		header.Magic,
		header.Version,
		header.ColumnType,
		header.BlockCount,
	}

	// Write the fields we need to update
	for i, field := range headerFields {
		if err := binary.Write(w.file, binary.LittleEndian, field); err != nil {
			return fmt.Errorf("failed to write header field %d: %w", i, err)
		}
	}
	// Skip the rest of the header - unchanged fields

	// Seek to the end to write the footer
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end: %w", err)
	}

	// Get current position - start of footer
	footerStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// Write block index count
	if err := binary.Write(w.file, binary.LittleEndian, uint32(w.blockCount)); err != nil {
		return fmt.Errorf("failed to write block index count: %w", err)
	}

	// Only write block info if we have any blocks
	if w.blockCount > 0 {
		// Check that we have block positions for all blocks
		if len(w.blockPositions) != int(w.blockCount) {
			return fmt.Errorf("block position tracking error: expected %d positions, got %d",
				w.blockCount, len(w.blockPositions))
		}

		// Process each block
		for blockIdx := uint64(0); blockIdx < w.blockCount; blockIdx++ {
			blockOffset := w.blockPositions[blockIdx]
			blockSize := w.blockSizes[blockIdx]
			stats := w.blockStats[blockIdx]

			// Write block footer using the stats collected during WriteBlock
			if err := w.writeBlockFooter(
				blockOffset,
				uint64(blockSize),
				stats.MinID,
				stats.MaxID,
				stats.MinValue,
				stats.MaxValue,
				stats.Sum,
				stats.Count); err != nil {
				return err
			}
		}
	}

	// Get current position - end of footer content
	footerEnd, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// Calculate footer size
	footerSize := footerEnd - footerStart
	footerMetaStart := footerEnd

	// Write footer metadata
	if err := binary.Write(w.file, binary.LittleEndian, uint64(footerSize)); err != nil {
		return fmt.Errorf("failed to write footer size: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, uint64(0)); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, MagicNumber); err != nil {
		return fmt.Errorf("failed to write magic number: %w", err)
	}

	// Verify footer metadata size
	footerMetaEnd, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get footer metadata end position: %w", err)
	}

	// The footer metadata consists of:
	// - Footer size (8 bytes)
	// - Checksum (8 bytes)
	// - Magic number (8 bytes)
	// Total: 24 bytes
	footerMetaSize := footerMetaEnd - footerMetaStart
	if footerMetaSize != 24 {
		return fmt.Errorf("footer metadata size mismatch: expected=24, actual=%d", footerMetaSize)
	}

	// Verify total footer size
	totalFooterSize := footerMetaEnd - footerStart
	if totalFooterSize != footerSize+24 {
		return fmt.Errorf("total footer size mismatch: expected=%d, actual=%d",
			footerSize+24, totalFooterSize)
	}

	// Final sync to ensure everything is written to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file during finalization: %w", err)
	}

	return nil
}

// Close closes the file without finalizing it
func (w *Writer) Close() error {
	return w.file.Close()
}
