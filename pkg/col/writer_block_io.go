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

// BlockData encapsulates all the data needed to write a block to disk
type BlockData struct {
	// Block metadata
	MinID    uint64
	MaxID    uint64
	MinValue int64
	MaxValue int64
	Sum      int64
	Count    uint32

	// Section sizes
	IDSectionSize    uint32
	ValueSectionSize uint32

	// Serialized data ready to be written directly
	SerializedIDSection    []byte
	SerializedValueSection []byte

	// Total expected size of the block (excluding padding)
	ExpectedSize uint64
}

// PrepareBlockData encodes data and calculates metadata for writing a block
func (w *Writer) PrepareBlockData(ids []uint64, values []int64) (*BlockData, error) {
	if len(ids) != len(values) {
		return nil, fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return nil, fmt.Errorf("cannot prepare empty block")
	}

	// Calculate statistics from original values
	minID, maxID := calculateMinMaxUint64(ids)
	minValue, maxValue := calculateMinMaxInt64(values)
	sum := calculateSumInt64(values)
	count := uint32(len(ids))

	// Serialize ID section based on encoding type
	serializedIDSection, err := w.serializeIDSection(ids)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize ID section: %w", err)
	}
	idSectionSize := uint32(len(serializedIDSection))

	// Serialize value section based on encoding type
	serializedValueSection, err := w.serializeValueSection(values)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize value section: %w", err)
	}
	valueSectionSize := uint32(len(serializedValueSection))

	// Calculate expected block size
	expectedSize := uint64(blockHeaderSize + blockLayoutSize + idSectionSize + valueSectionSize)

	// Create and return BlockData
	return &BlockData{
		MinID:                  minID,
		MaxID:                  maxID,
		MinValue:               minValue,
		MaxValue:               maxValue,
		Sum:                    sum,
		Count:                  count,
		IDSectionSize:          idSectionSize,
		ValueSectionSize:       valueSectionSize,
		SerializedIDSection:    serializedIDSection,
		SerializedValueSection: serializedValueSection,
		ExpectedSize:           expectedSize,
	}, nil
}

// serializeIDSection takes IDs and returns a byte slice ready to be written to disk
func (w *Writer) serializeIDSection(ids []uint64) ([]byte, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("cannot serialize empty ID section")
	}

	// Apply delta encoding if needed
	var encodedIds []uint64

	if w.encodingType == EncodingDeltaID || w.encodingType == EncodingDeltaBoth ||
		w.encodingType == EncodingVarIntID || w.encodingType == EncodingVarIntBoth {
		// Apply delta encoding
		encodedIds = deltaEncode(ids)
	} else {
		// Use original IDs
		encodedIds = make([]uint64, len(ids))
		copy(encodedIds, ids)
	}

	// Then choose appropriate serialization format
	if w.encodingType == EncodingVarInt || w.encodingType == EncodingVarIntID ||
		w.encodingType == EncodingVarIntBoth {
		// Use variable-length encoding
		return w.serializeVarIntIDs(encodedIds)
	} else {
		// Use fixed-length encoding
		return w.serializeFixedLengthIDs(encodedIds)
	}
}

// serializeValueSection takes values and returns a byte slice ready to be written to disk
func (w *Writer) serializeValueSection(values []int64) ([]byte, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("cannot serialize empty value section")
	}

	// Apply delta encoding if needed
	var encodedValues []int64

	if w.encodingType == EncodingDeltaValue || w.encodingType == EncodingDeltaBoth ||
		w.encodingType == EncodingVarIntValue || w.encodingType == EncodingVarIntBoth {
		// Apply delta encoding
		encodedValues = deltaEncodeInt64(values)
	} else {
		// Use original values
		encodedValues = make([]int64, len(values))
		copy(encodedValues, values)
	}

	// Then choose appropriate serialization format
	if w.encodingType == EncodingVarInt || w.encodingType == EncodingVarIntValue ||
		w.encodingType == EncodingVarIntBoth {
		// Use variable-length encoding
		return w.serializeVarIntValues(encodedValues)
	} else {
		// Use fixed-length encoding
		return w.serializeFixedLengthValues(encodedValues)
	}
}

// serializeVarIntIDs serializes IDs using variable-length encoding
func (w *Writer) serializeVarIntIDs(ids []uint64) ([]byte, error) {
	_, encodedIDBytes, _, err := w.encodeIDs(ids)
	if err != nil {
		return nil, err
	}

	// Pre-calculate total buffer size
	totalSize := 0
	for i := range encodedIDBytes {
		totalSize += len(encodedIDBytes[i])
	}

	// Create a single buffer for all IDs
	buffer := make([]byte, totalSize)
	offset := 0

	// Copy all encoded bytes into the buffer
	for i := range encodedIDBytes {
		copy(buffer[offset:], encodedIDBytes[i])
		offset += len(encodedIDBytes[i])
	}

	return buffer, nil
}

// serializeFixedLengthIDs serializes IDs using fixed-length encoding
func (w *Writer) serializeFixedLengthIDs(ids []uint64) ([]byte, error) {
	// Pre-calculate buffer size: 8 bytes per ID
	buffer := make([]byte, len(ids)*8)

	// Write each ID into the buffer
	for i, id := range ids {
		binary.LittleEndian.PutUint64(buffer[i*8:], id)
	}

	return buffer, nil
}

// serializeVarIntValues serializes values using variable-length encoding
func (w *Writer) serializeVarIntValues(values []int64) ([]byte, error) {
	_, encodedValueBytes, _, err := w.encodeValues(values)
	if err != nil {
		return nil, err
	}

	// Pre-calculate total buffer size
	totalSize := 0
	for i := range encodedValueBytes {
		totalSize += len(encodedValueBytes[i])
	}

	// Create a single buffer for all values
	buffer := make([]byte, totalSize)
	offset := 0

	// Copy all encoded bytes into the buffer
	for i := range encodedValueBytes {
		copy(buffer[offset:], encodedValueBytes[i])
		offset += len(encodedValueBytes[i])
	}

	return buffer, nil
}

// serializeFixedLengthValues serializes values using fixed-length encoding
func (w *Writer) serializeFixedLengthValues(values []int64) ([]byte, error) {
	// Pre-calculate buffer size: 8 bytes per value
	buffer := make([]byte, len(values)*8)

	// Write each value into the buffer
	for i, value := range values {
		binary.LittleEndian.PutUint64(buffer[i*8:], int64ToUint64(value))
	}

	return buffer, nil
}

// Modified WriteBlock function to use the new approach
func (w *Writer) WriteBlock(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return fmt.Errorf("cannot write empty block")
	}

	// Get the current position to track how much we've written
	startPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current position: %w", err)
	}

	// Prepare the block data with all encoding done
	blockData, err := w.PrepareBlockData(ids, values)
	if err != nil {
		return err
	}

	// Check if this block would exceed the target size
	if blockData.ExpectedSize > uint64(w.blockSizeTarget) {
		return &BlockFullError{ItemsWritten: 0}
	}

	// Add all IDs to the global ID bitmap
	for _, id := range ids {
		w.globalIDs.Set(id)
	}

	// Write the block to disk
	err = w.writeBlockInternal(blockData)

	// If we successfully wrote the block, we're done
	if err == nil {
		return nil
	}

	// If we got an error that's not a BlockFullError, return it
	if _, ok := err.(*BlockFullError); !ok {
		return err
	}

	// If we got a BlockFullError, get the current position to see how much we wrote
	endPos, seekErr := w.file.Seek(0, io.SeekCurrent)
	if seekErr != nil {
		// If we can't get the position, return the original error
		return err
	}

	// Calculate how much we wrote
	bytesWritten := endPos - startPos

	// If we wrote less than the target size, return the original error
	if bytesWritten <= int64(w.blockSizeTarget) {
		return err
	}

	// If we wrote more than the target size, we need to truncate back to the start
	// and return a BlockFullError with 0 items written
	if _, seekErr := w.file.Seek(startPos, io.SeekStart); seekErr != nil {
		return fmt.Errorf("failed to seek back to start position: %w", seekErr)
	}

	return &BlockFullError{ItemsWritten: 0}
}

// Refactored writeBlockInternal to be completely agnostic of encoding
func (w *Writer) writeBlockInternal(blockData *BlockData) error {
	// Write block header (64 bytes)
	blockStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get block start position: %w", err)
	}

	// Store this position so we can refer to it later in the footer
	w.blockPositions = append(w.blockPositions, uint64(blockStart))

	// Convert int64 values to uint64 for storage
	minValueU64 := int64ToUint64(blockData.MinValue)
	maxValueU64 := int64ToUint64(blockData.MaxValue)
	sumU64 := int64ToUint64(blockData.Sum)

	headerWritten := int64(0)
	// Write block header
	if n, err := w.writeBlockHeader(blockData.MinID, blockData.MaxID, minValueU64, maxValueU64, sumU64, blockData.Count); err != nil {
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
	if blockData.IDSectionSize == 0 {
		return fmt.Errorf("ID section size is 0, which is invalid. count=%d",
			blockData.Count)
	}

	if blockData.ValueSectionSize == 0 {
		return fmt.Errorf("Value section size is 0, which is invalid. count=%d",
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

	// Write ID section directly from pre-serialized data
	actualIdSectionSize, err := w.file.Write(blockData.SerializedIDSection)
	if err != nil {
		return fmt.Errorf("failed to write ID section: %w", err)
	}

	// Verify ID section size
	if uint32(actualIdSectionSize) != blockData.IDSectionSize {
		return fmt.Errorf("ID section size mismatch: expected=%d, actual=%d",
			blockData.IDSectionSize, actualIdSectionSize)
	}

	// Write Value section directly from pre-serialized data
	actualValueSectionSize, err := w.file.Write(blockData.SerializedValueSection)
	if err != nil {
		return fmt.Errorf("failed to write value section: %w", err)
	}

	// Verify value section size
	if uint32(actualValueSectionSize) != blockData.ValueSectionSize {
		return fmt.Errorf("value section size mismatch: expected=%d, actual=%d",
			blockData.ValueSectionSize, actualValueSectionSize)
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
	expectedBlockSize := blockHeaderSize + blockLayoutSize + uint64(blockData.IDSectionSize) + uint64(blockData.ValueSectionSize)
	blockSizeDifference := (blockSize - uint64(padding)) - expectedBlockSize
	if blockSizeDifference != 0 {
		return fmt.Errorf("block size mismatch: expected=%d, actual=%d, diff=%d",
			expectedBlockSize, blockSize-uint64(padding), blockSizeDifference)
	}

	w.blockSizes = append(w.blockSizes, uint32(blockSize))

	// Store block statistics for footer
	w.blockStats = append(w.blockStats, BlockStats{
		MinID:    blockData.MinID,
		MaxID:    blockData.MaxID,
		MinValue: blockData.MinValue,
		MaxValue: blockData.MaxValue,
		Sum:      blockData.Sum,
		Count:    blockData.Count,
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
	blockData, err := w.PrepareBlockData(ids, values)
	if err != nil {
		return 0, err
	}

	// Calculate where the block would end
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("failed to get current position: %w", err)
	}

	blockEnd := currentPos + int64(blockData.ExpectedSize)

	// Add padding if needed
	padding := calculatePadding(blockEnd, PageSize)
	totalSize := blockData.ExpectedSize
	if padding > 0 {
		totalSize += uint64(padding)
	}

	return totalSize, nil
}
