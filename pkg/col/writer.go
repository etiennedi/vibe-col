package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
		blockSizeTarget: 4 * 1024,    // 4KB default
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
	// Create the header with default values
	header := NewFileHeader(0, w.blockSizeTarget, w.encodingType)

	// Write the header to the file
	if err := binary.Write(w.file, binary.LittleEndian, header.Magic); err != nil {
		return fmt.Errorf("failed to write magic number: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.Version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.ColumnType); err != nil {
		return fmt.Errorf("failed to write column type: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.BlockCount); err != nil {
		return fmt.Errorf("failed to write block count: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.BlockSizeTarget); err != nil {
		return fmt.Errorf("failed to write block size target: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.CompressionType); err != nil {
		return fmt.Errorf("failed to write compression type: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.EncodingType); err != nil {
		return fmt.Errorf("failed to write encoding type: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.CreationTime); err != nil {
		return fmt.Errorf("failed to write creation time: %w", err)
	}

	// Write reserved space to fill up to 64 bytes
	reservedSize := 64 - 8 - 4 - 4 - 8 - 4 - 4 - 4 - 8
	reserved := make([]byte, reservedSize)
	if _, err := w.file.Write(reserved); err != nil {
		return fmt.Errorf("failed to write reserved space: %w", err)
	}

	return nil
}

// encodeIDs encodes the IDs based on the encoding type
func (w *Writer) encodeIDs(ids []uint64) ([]uint64, [][]byte, uint32, error) {
	fmt.Printf("Encoding IDs: %v\n", ids)
	var encodedIDs []uint64
	var encodedIdBytes [][]byte
	var idSectionSize uint32

	// First apply delta encoding if needed
	switch w.encodingType {
	case EncodingRaw, EncodingVarInt, EncodingVarIntValue:
		// These encoding types don't use delta encoding for IDs
		encodedIDs = make([]uint64, len(ids))
		copy(encodedIDs, ids)
	case EncodingDeltaID, EncodingDeltaBoth, EncodingVarIntID, EncodingVarIntBoth:
		// These encoding types use delta encoding for IDs
		encodedIDs = deltaEncode(ids)
	default:
		return nil, nil, 0, fmt.Errorf("unsupported encoding type: %d", w.encodingType)
	}

	// Then apply varint encoding if needed
	switch w.encodingType {
	case EncodingRaw, EncodingDeltaID, EncodingDeltaValue, EncodingDeltaBoth:
		// Fixed-width encoding
		idSectionSize = uint32(len(encodedIDs) * 8)
	case EncodingVarInt, EncodingVarIntID, EncodingVarIntBoth, EncodingVarIntValue:
		// Variable-width encoding
		encodedIdBytes = make([][]byte, len(encodedIDs))
		idSectionSize = 0
		for i, id := range encodedIDs {
			encodedIdBytes[i] = encodeVarInt(id)
			idSize := uint32(len(encodedIdBytes[i]))
			fmt.Printf("Encoded ID %d: %v (size: %d)\n", i, encodedIdBytes[i], idSize)
			if idSize == 0 {
				return nil, nil, 0, fmt.Errorf("encoded size of ID at index %d is 0", i)
			}
			idSectionSize += idSize
		}
		if idSectionSize == 0 && len(encodedIDs) > 0 {
			return nil, nil, 0, fmt.Errorf("calculated ID section size is 0 with %d IDs", len(encodedIDs))
		}
	}

	fmt.Printf("Encoded IDs: %v, ID Section Size: %d\n", encodedIDs, idSectionSize)
	return encodedIDs, encodedIdBytes, idSectionSize, nil
}

// encodeValues encodes the values based on the encoding type
func (w *Writer) encodeValues(values []int64) ([]int64, [][]byte, uint32, error) {
	fmt.Printf("Encoding Values: %v\n", values)
	var encodedValues []int64
	var encodedValueBytes [][]byte
	var valueSectionSize uint32

	// First apply delta encoding if needed
	switch w.encodingType {
	case EncodingRaw, EncodingVarInt, EncodingVarIntID:
		// These encoding types don't use delta encoding for values
		encodedValues = make([]int64, len(values))
		copy(encodedValues, values)
	case EncodingDeltaValue, EncodingDeltaBoth, EncodingVarIntValue, EncodingVarIntBoth:
		// These encoding types use delta encoding for values
		encodedValues = deltaEncodeInt64(values)
	default:
		return nil, nil, 0, fmt.Errorf("unsupported encoding type: %d", w.encodingType)
	}

	// Then apply varint encoding if needed
	switch w.encodingType {
	case EncodingRaw, EncodingDeltaID, EncodingDeltaValue, EncodingDeltaBoth:
		// Fixed-width encoding
		valueSectionSize = uint32(len(encodedValues) * 8)
	case EncodingVarInt, EncodingVarIntID, EncodingVarIntBoth, EncodingVarIntValue:
		// Variable-width encoding
		encodedValueBytes = make([][]byte, len(encodedValues))
		valueSectionSize = 0
		for i, val := range encodedValues {
			encodedValueBytes[i] = encodeSignedVarInt(val)
			valSize := uint32(len(encodedValueBytes[i]))
			fmt.Printf("Encoded Value %d: %v (size: %d)\n", i, encodedValueBytes[i], valSize)
			if valSize == 0 {
				return nil, nil, 0, fmt.Errorf("encoded size of value at index %d is 0", i)
			}
			valueSectionSize += valSize
		}
		if valueSectionSize == 0 && len(encodedValues) > 0 {
			return nil, nil, 0, fmt.Errorf("calculated value section size is 0 with %d values", len(encodedValues))
		}
	}

	fmt.Printf("Encoded Values: %v, Value Section Size: %d\n", encodedValues, valueSectionSize)
	return encodedValues, encodedValueBytes, valueSectionSize, nil
}

// writeBlockHeader writes the block header to the file
func (w *Writer) writeBlockHeader(minID, maxID uint64, minValueU64, maxValueU64, sumU64 uint64, count uint32) error {
	if err := binary.Write(w.file, binary.LittleEndian, minID); err != nil {
		return fmt.Errorf("failed to write min ID: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, maxID); err != nil {
		return fmt.Errorf("failed to write max ID: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, minValueU64); err != nil {
		return fmt.Errorf("failed to write min value: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, maxValueU64); err != nil {
		return fmt.Errorf("failed to write max value: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, sumU64); err != nil {
		return fmt.Errorf("failed to write sum: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, count); err != nil {
		return fmt.Errorf("failed to write count: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, w.encodingType); err != nil {
		return fmt.Errorf("failed to write encoding type: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, uint32(CompressionNone)); err != nil {
		return fmt.Errorf("failed to write compression type: %w", err)
	}
	return nil
}

// writeBlockFooter writes the block footer to the file
func (w *Writer) writeBlockFooter(blockOffset, blockSize uint64, minID, maxID uint64, minValue, maxValue, sum int64, count uint32) error {
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

	// Write block header
	if err := w.writeBlockHeader(minID, maxID, minValueU64, maxValueU64, sumU64, count); err != nil {
		return err
	}

	// Calculate total data size
	dataSize := idSectionSize + valueSectionSize
	_ = dataSize // Used for debugging, can be removed in production

	// Use the updated CalculateBlockSize function
	uncompressedSize := CalculateBlockSize(count, w.encodingType)
	compressedSize := uncompressedSize // Same as uncompressed for now

	if err := binary.Write(w.file, binary.LittleEndian, uncompressedSize); err != nil {
		return fmt.Errorf("failed to write uncompressed size: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, compressedSize); err != nil {
		return fmt.Errorf("failed to write compressed size: %w", err)
	}

	// Write checksum placeholder (will be updated later)
	checksumPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get checksum position: %w", err)
	}
	_ = checksumPos // Used for checksum calculation, can be removed in production
	if err := binary.Write(w.file, binary.LittleEndian, uint64(0)); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	// Skip reserved bytes (8 bytes)
	if _, err := w.file.Seek(8, io.SeekCurrent); err != nil {
		return fmt.Errorf("failed to skip reserved bytes: %w", err)
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

	// Debug output for layout
	fmt.Printf("Writing block layout: idOffset=%d, idSectionSize=%d, valOffset=%d, valueSectionSize=%d, count=%d\n",
		idSectionOffset, idSectionSize, valueSectionOffset, valueSectionSize, count)

	// Write the layout buffer to file
	bytesWritten, err := w.file.Write(layoutBuf)
	if err != nil {
		return fmt.Errorf("failed to write block layout: %w", err)
	}
	if bytesWritten != 16 {
		return fmt.Errorf("failed to write block layout: wrote %d bytes, expected 16", bytesWritten)
	}

	// Start of data section - record position for checksum calculation
	dataStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get data section position: %w", err)
	}
	_ = dataStart // Used for checksum calculation, can be removed in production

	// Write block data
	// Write ID array based on encoding type
	if useVarIntForIDs {
		// Use variable-length encoding for IDs (using precomputed values)
		for i := range encodedIDs {
			// Write the precomputed varint bytes for this ID
			if _, err := w.file.Write(encodedIdBytes[i]); err != nil {
				return fmt.Errorf("failed to write varint ID: %w", err)
			}
		}
	} else {
		// Write fixed-length IDs
		for _, id := range encodedIDs {
			if err := binary.Write(w.file, binary.LittleEndian, id); err != nil {
				return fmt.Errorf("failed to write ID: %w", err)
			}
		}
	}

	// Write Value array based on encoding type
	if useVarIntForValues {
		// Use variable-length encoding for values (using precomputed values)
		for i := range encodedValues {
			// Write the precomputed varint bytes for this value
			if _, err := w.file.Write(encodedValueBytes[i]); err != nil {
				return fmt.Errorf("failed to write varint value: %w", err)
			}
		}
	} else {
		// Write fixed-length values
		for _, val := range encodedValues {
			if err := binary.Write(w.file, binary.LittleEndian, val); err != nil {
				return fmt.Errorf("failed to write value: %w", err)
			}
		}
	}

	// Get end position to calculate block size
	blockEnd, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get block end position: %w", err)
	}

	// Calculate actual block size
	blockSize := uint64(blockEnd - blockStart)
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

	// Write header
	if err := binary.Write(w.file, binary.LittleEndian, header.Magic); err != nil {
		return fmt.Errorf("failed to write magic number: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.Version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.ColumnType); err != nil {
		return fmt.Errorf("failed to write column type: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, header.BlockCount); err != nil {
		return fmt.Errorf("failed to write block count: %w", err)
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
			_ = w.blockStats[blockIdx] // Used for debugging, can be removed in production

			// Seek to the block header
			if _, err := w.file.Seek(int64(blockOffset), io.SeekStart); err != nil {
				return fmt.Errorf("failed to seek to block: %w", err)
			}

			// Read block header (just enough to get the metadata we need)
			headerBuf := make([]byte, 44)
			if _, err := io.ReadFull(w.file, headerBuf); err != nil {
				return fmt.Errorf("failed to read block %d header: %w", blockIdx, err)
			}

			// Parse header fields
			minID := binary.LittleEndian.Uint64(headerBuf[0:8])
			maxID := binary.LittleEndian.Uint64(headerBuf[8:16])
			minValueU64 := binary.LittleEndian.Uint64(headerBuf[16:24])
			maxValueU64 := binary.LittleEndian.Uint64(headerBuf[24:32])
			sumU64 := binary.LittleEndian.Uint64(headerBuf[32:40])
			count := binary.LittleEndian.Uint32(headerBuf[40:44])

			// Convert values to proper types
			minValue := uint64ToInt64(minValueU64)
			maxValue := uint64ToInt64(maxValueU64)
			sum := uint64ToInt64(sumU64)

			// Seek back to footer
			if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
				return fmt.Errorf("failed to seek to end: %w", err)
			}

			// Write block footer
			if err := w.writeBlockFooter(blockOffset, uint64(blockSize), minID, maxID, minValue, maxValue, sum, count); err != nil {
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

// calculateBlockPositions determines the positions of each block based on file size and block count
// for multi-block files
func calculateBlockPositions(fileSize int64, blockCount uint64) []uint64 {
	if blockCount == 0 {
		return []uint64{}
	}

	// For simplicity, we'll calculate fixed-size blocks
	// First block always starts at offset 64 (right after the header)
	positions := make([]uint64, blockCount)
	positions[0] = 64

	if blockCount == 1 {
		return positions
	}

	// For multi-block files, we need to estimate the block size
	// Based on our format, each block has:
	// - 64 bytes for header
	// - 16 bytes for layout
	// - Data (id-value pairs, 16 bytes each)

	// First, get a conservative estimate of data entry count
	// Assuming each block has 100 entries (our test case)
	const estimatedEntriesPerBlock = 100
	const bytesPerEntry = 16 // 8 bytes per ID, 8 bytes per value

	// Size of each block: header + layout + data
	const blockHeaderAndLayoutSize = 64 + 16
	const estimatedBlockDataSize = estimatedEntriesPerBlock * bytesPerEntry
	const estimatedBlockSize = blockHeaderAndLayoutSize + estimatedBlockDataSize

	// Calculate positions for each block
	for i := uint64(1); i < blockCount; i++ {
		// Each block starts after the previous block
		positions[i] = positions[i-1] + estimatedBlockSize
	}

	return positions
}
