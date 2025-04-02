package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/weaviate/sroar"
)

// BufferedWriterOption defines a function type for configuring a BufferedWriter
type BufferedWriterOption func(*BufferedWriter)

// WithBufferedEncoding sets the encoding type for the BufferedWriter
func WithBufferedEncoding(encodingType uint32) BufferedWriterOption {
	return func(bw *BufferedWriter) {
		bw.encodingType = encodingType
	}
}

// WithBufferedBlockSize sets the block size target for the BufferedWriter
func WithBufferedBlockSize(blockSize uint32) BufferedWriterOption {
	return func(bw *BufferedWriter) {
		bw.blockSizeTarget = blockSize
	}
}

// BufferedWriter implements a writer for column files that buffers data in memory
// using BlockData structures and writes directly to disk.
type BufferedWriter struct {
	file            *os.File
	filename        string
	blockCount      uint64
	encodingType    uint32
	blockSizeTarget uint32
	// blockPositions  []uint64        // Position of each block in the file
	// blockSizes      []uint32        // Size of each block in bytes
	// blockStats      []BlockStats    // Statistics for each block
	blockIndex  []FooterEntry // Detailed index of blocks
	globalIDs   *sroar.Bitmap
	globalMinID uint64 // Global minimum ID
	globalMaxID uint64 // Global maximum ID
	closed      bool

	// BlockData to buffer data before writing to disk
	pendingData *BlockData
	lastID      uint64 // for delta encoding calculation
	lastValue   int64  // for delta encoding calculation
}

// NewBufferedWriter creates a new BufferedWriter for the given filename
func NewBufferedWriter(filename string, options ...BufferedWriterOption) (*BufferedWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	writer := &BufferedWriter{
		file:            file,
		filename:        filename,
		blockCount:      0,
		encodingType:    EncodingRaw, // Default
		blockSizeTarget: defaultBlockSize,
		// blockPositions:  make([]uint64, 0),
		// blockSizes:      make([]uint32, 0),
		// blockStats:      make([]BlockStats, 0),
		blockIndex:  make([]FooterEntry, 0),
		globalIDs:   sroar.NewBitmap(),
		closed:      false,
		pendingData: nil,
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

// Add adds a single ID-value pair
func (bw *BufferedWriter) Add(id uint64, value int64) error {
	if bw.closed {
		return fmt.Errorf("writer is already closed")
	}

	// Create a new BlockData for this single item if needed
	if bw.pendingData == nil {
		bw.pendingData = &BlockData{
			MinID:                  id,
			MaxID:                  id,
			MinValue:               value,
			MaxValue:               value,
			Sum:                    value,
			Count:                  1,
			SerializedIDSection:    make([]byte, 0, 4096),
			SerializedValueSection: make([]byte, 0, 4096),
		}

		// for now pretend encoding doesn't exist TODO
		bw.pendingData.SerializedIDSection = bw.pendingData.SerializedIDSection[:8] // always safe, we create a much larger buffer

		// First ID is not delta encoded
		binary.LittleEndian.PutUint64(bw.pendingData.SerializedIDSection, id)
		bw.pendingData.SerializedValueSection = bw.pendingData.SerializedValueSection[:8] // always safe, we create a much larger buffer

		// first value is not delta encoded
		binary.LittleEndian.PutUint64(bw.pendingData.SerializedValueSection, int64ToUint64(value))

	} else {
		// for now pretend encoding doesn't exist TODO
		if len(bw.pendingData.SerializedIDSection)+8 > cap(bw.pendingData.SerializedIDSection) {
			// we need to grow the slice
			newSlice := make([]byte, len(bw.pendingData.SerializedIDSection), cap(bw.pendingData.SerializedIDSection)*2)
			copy(newSlice, bw.pendingData.SerializedIDSection)
			bw.pendingData.SerializedIDSection = newSlice
		}

		// we know we have enough space
		bw.pendingData.SerializedIDSection = bw.pendingData.SerializedIDSection[:len(bw.pendingData.SerializedIDSection)+8]

		// in case of delta encoding we need to override the id with the delta id
		idToWrite := id
		if bw.encodingType == EncodingDeltaID || bw.encodingType == EncodingDeltaBoth || bw.encodingType == EncodingVarIntID || bw.encodingType == EncodingVarIntBoth {
			idToWrite = id - bw.lastID
		}
		binary.LittleEndian.PutUint64(bw.pendingData.SerializedIDSection[len(bw.pendingData.SerializedIDSection)-8:], idToWrite)

		if len(bw.pendingData.SerializedValueSection)+8 > cap(bw.pendingData.SerializedValueSection) {
			// we need to grow the slice
			newSlice := make([]byte, len(bw.pendingData.SerializedValueSection), cap(bw.pendingData.SerializedValueSection)*2)
			copy(newSlice, bw.pendingData.SerializedValueSection)
			bw.pendingData.SerializedValueSection = newSlice
		}

		// we know we have enough space
		bw.pendingData.SerializedValueSection = bw.pendingData.SerializedValueSection[:len(bw.pendingData.SerializedValueSection)+8]

		// in case of delta encoding we need to override the value with the delta value
		valueToWrite := value
		if bw.encodingType == EncodingDeltaValue || bw.encodingType == EncodingDeltaBoth || bw.encodingType == EncodingVarIntValue || bw.encodingType == EncodingVarIntBoth {
			valueToWrite = value - bw.lastValue
		}
		binary.LittleEndian.PutUint64(bw.pendingData.SerializedValueSection[len(bw.pendingData.SerializedValueSection)-8:], int64ToUint64(valueToWrite))

		// Update statistics
		if id < bw.pendingData.MinID {
			bw.pendingData.MinID = id
		}
		if id > bw.pendingData.MaxID {
			bw.pendingData.MaxID = id
		}
		if value < bw.pendingData.MinValue {
			bw.pendingData.MinValue = value
		}
		if value > bw.pendingData.MaxValue {
			bw.pendingData.MaxValue = value
		}
		bw.pendingData.Sum += value
		bw.pendingData.Count++
	}

	bw.globalIDs.Set(id)
	bw.lastID = id
	bw.lastValue = value

	// // If we've accumulated enough data, flush it
	// if bw.pendingData != nil && len(bw.pendingData.IDs) >= 1000 {
	// 	if err := bw.Flush(); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

// // AddBatch adds multiple ID-value pairs
// func (bw *BufferedWriter) AddBatch(ids []uint64, values []int64) error {
// 	if bw.closed {
// 		return fmt.Errorf("writer is already closed")
// 	}

// 	if len(ids) != len(values) {
// 		return fmt.Errorf("ids and values must have the same length")
// 	}

// 	if len(ids) == 0 {
// 		return nil
// 	}

// 	// Create new BlockData from this batch
// 	minID, maxID := calculateMinMaxUint64(ids)
// 	minValue, maxValue := calculateMinMaxInt64(values)
// 	sum := calculateSumInt64(values)
// 	count := uint32(len(ids))

// 	newData := &BlockData{
// 		IDs:      make([]uint64, len(ids)),
// 		Values:   make([]int64, len(values)),
// 		MinID:    minID,
// 		MaxID:    maxID,
// 		MinValue: minValue,
// 		MaxValue: maxValue,
// 		Sum:      sum,
// 		Count:    count,
// 	}

// 	// Copy the data
// 	copy(newData.IDs, ids)
// 	copy(newData.Values, values)

// 	// If we don't have any pending data, use this batch directly
// 	if bw.pendingData == nil {
// 		bw.pendingData = newData
// 	} else {
// 		// Merge with existing pending data
// 		bw.pendingData = bw.mergeBlockData(bw.pendingData, newData)
// 	}

// 	// If we've accumulated enough data, flush it
// 	if len(bw.pendingData.IDs) >= 1000 {
// 		if err := bw.Flush(); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// // mergeBlockData merges two BlockData structures into one
// func (bw *BufferedWriter) mergeBlockData(a, b *BlockData) *BlockData {
// 	// Create a new BlockData with combined capacity
// 	result := &BlockData{
// 		IDs:    make([]uint64, 0, len(a.IDs)+len(b.IDs)),
// 		Values: make([]int64, 0, len(a.Values)+len(b.Values)),
// 	}

// 	// Append all IDs and values
// 	result.IDs = append(result.IDs, a.IDs...)
// 	result.IDs = append(result.IDs, b.IDs...)
// 	result.Values = append(result.Values, a.Values...)
// 	result.Values = append(result.Values, b.Values...)

// 	// Recalculate statistics
// 	result.MinID = minUint64(a.MinID, b.MinID)
// 	result.MaxID = maxUint64(a.MaxID, b.MaxID)
// 	result.MinValue = minInt64(a.MinValue, b.MinValue)
// 	result.MaxValue = maxInt64(a.MaxValue, b.MaxValue)
// 	result.Sum = a.Sum + b.Sum
// 	result.Count = a.Count + b.Count

// 	return result
// }

// Flush finalizes and writes any pending data to disk
func (bw *BufferedWriter) Flush() error {
	if bw.closed {
		return fmt.Errorf("writer is already closed")
	}

	// If we have pending data, write it as a block
	if bw.pendingData != nil && bw.pendingData.Count > 0 {
		if err := bw.WriteBlock(bw.pendingData); err != nil {
			return fmt.Errorf("failed to write block: %w", err)
		}
		// Clear pending data
		bw.pendingData = nil
	}

	return nil
}

// Close finalizes and closes the file
func (bw *BufferedWriter) Close() error {
	if bw.closed {
		return nil // Already closed
	}

	// Flush any pending data
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	// Finalize the file
	if err := bw.finalize(); err != nil {
		return fmt.Errorf("failed to finalize: %w", err)
	}

	// Close the file
	if err := bw.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	bw.closed = true
	return nil
}

// TotalBlocks returns the number of blocks written so far
func (bw *BufferedWriter) TotalBlocks() uint64 {
	return bw.blockCount
}

// writeHeader writes the file header to the file
func (bw *BufferedWriter) writeHeader() error {
	// Record start position to verify header size
	headerStart, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get header start position: %w", err)
	}

	// Create the header with default values
	header := NewFileHeader(0, bw.blockSizeTarget, bw.encodingType)

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
		header.BitmapOffset,
		header.BitmapSize,
	}

	// Write all header fields
	for i, field := range headerFields {
		if err := binary.Write(bw.file, binary.LittleEndian, field); err != nil {
			return fmt.Errorf("failed to write header field %d: %w", i, err)
		}
	}

	// Calculate reserved space - sum of the sizes of the header fields we've written
	headerFieldsSize := uint64Size + uint32Size + uint32Size + uint64Size +
		uint32Size + uint32Size + uint32Size + uint64Size + uint64Size + uint64Size
	reservedSize := headerSize - headerFieldsSize

	// Write reserved space to fill up to 64 bytes
	reserved := make([]byte, reservedSize)
	if _, err := bw.file.Write(reserved); err != nil {
		return fmt.Errorf("failed to write reserved space: %w", err)
	}

	// Verify header size
	headerEnd, err := bw.file.Seek(0, io.SeekCurrent)
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

// prepareBlock has been replaced by direct calculation in WriteBlock

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

// writeBlockLayout writes a block layout to the provided buffer
func (bw *BufferedWriter) writeBlockLayout(buffer []byte, idSectionSize, valueSectionSize uint32) int {
	offset := 0

	// ID section offset - always 0 relative to the section
	binary.LittleEndian.PutUint32(buffer[offset:], 0)
	offset += 4

	// ID section size
	binary.LittleEndian.PutUint32(buffer[offset:], idSectionSize)
	offset += 4

	// Value section offset - always the size of the ID section
	binary.LittleEndian.PutUint32(buffer[offset:], idSectionSize)
	offset += 4

	// Value section size
	binary.LittleEndian.PutUint32(buffer[offset:], valueSectionSize)
	offset += 4

	return offset
}

// finalize writes the footer and updates the header
func (bw *BufferedWriter) finalize() error {
	// Write the global ID bitmap
	bitmapOffset, bitmapSize, err := bw.writeGlobalIDBitmap()
	if err != nil {
		return fmt.Errorf("failed to write global ID bitmap: %w", err)
	}

	// Get current position - this is where the footer will start
	footerStart, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// Add padding if needed to align to page boundary
	padding := calculatePadding(footerStart, PageSize)
	if padding > 0 {
		// Create padding buffer filled with zeros
		paddingBuf := make([]byte, padding)
		if _, err := bw.file.Write(paddingBuf); err != nil {
			return fmt.Errorf("failed to write padding bytes: %w", err)
		}
		// Update footer start position after padding
		footerStart += padding
	}

	// Write block index count
	blockCount := uint32(len(bw.blockIndex))
	if blockCount == 0 {
		// Create a dummy entry for empty files
		blockCount = 1
	}

	if err := binary.Write(bw.file, binary.LittleEndian, blockCount); err != nil {
		return fmt.Errorf("failed to write block index count: %w", err)
	}

	// Write block index entries
	if len(bw.blockIndex) > 0 {
		// Write actual block entries
		for _, entry := range bw.blockIndex {
			// Verify that the entry is the correct size before writing
			entryStart, _ := bw.file.Seek(0, io.SeekCurrent)

			if err := binary.Write(bw.file, binary.LittleEndian, entry); err != nil {
				return fmt.Errorf("failed to write footer entry: %w", err)
			}

			entryEnd, _ := bw.file.Seek(0, io.SeekCurrent)
			entrySize := entryEnd - entryStart

			// Each footer entry should be 56 bytes (struct FooterEntry)
			if entrySize != 56 {
				return fmt.Errorf("footer entry size mismatch: expected=56, actual=%d", entrySize)
			}
		}
	} else {
		return fmt.Errorf("no block index entries to write")
	}

	// Get current position - end of footer content
	footerEnd, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// Calculate footer size
	footerSize := footerEnd - footerStart

	// Write footer metadata
	if err := binary.Write(bw.file, binary.LittleEndian, uint64(footerSize)); err != nil {
		return fmt.Errorf("failed to write footer size: %w", err)
	}
	if err := binary.Write(bw.file, binary.LittleEndian, uint64(0)); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	if err := binary.Write(bw.file, binary.LittleEndian, MagicNumber); err != nil {
		return fmt.Errorf("failed to write magic number: %w", err)
	}

	// Go back and update the header with final information
	if _, err := bw.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start: %w", err)
	}

	// Create updated header
	header := NewFileHeader(uint64(blockCount), bw.blockSizeTarget, bw.encodingType)
	header.BitmapOffset = bitmapOffset
	header.BitmapSize = bitmapSize
	header.CreationTime = uint64(time.Now().Unix())

	// Write header fields
	headerFields := []interface{}{
		header.Magic,
		header.Version,
		header.ColumnType,
		header.BlockCount,
		header.BlockSizeTarget,
		header.CompressionType,
		header.EncodingType,
		header.CreationTime,
		header.BitmapOffset,
		header.BitmapSize,
	}

	// Write the fields we need to update
	for i, field := range headerFields {
		if err := binary.Write(bw.file, binary.LittleEndian, field); err != nil {
			return fmt.Errorf("failed to write header field %d: %w", i, err)
		}
	}

	// Write the footer offset - this is not in the header struct
	// but we need to write it at the end of the header fields
	if _, err := bw.file.Seek(uint64Size+uint32Size+uint32Size+uint64Size+
		uint32Size+uint32Size+uint32Size+uint64Size+uint64Size+uint64Size, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to footer offset position: %w", err)
	}

	if err := binary.Write(bw.file, binary.LittleEndian, uint64(footerStart)); err != nil {
		return fmt.Errorf("failed to write footer offset: %w", err)
	}

	// Final sync to ensure everything is written to disk
	if err := bw.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file during finalization: %w", err)
	}

	return nil
}

// writeGlobalIDBitmap writes the global ID bitmap and returns its offset and size
func (bw *BufferedWriter) writeGlobalIDBitmap() (uint64, uint64, error) {
	// Get current position for bitmap
	bitmapOffset, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get bitmap offset: %w", err)
	}

	// Serialize bitmap
	bitmapData := bw.globalIDs.ToBuffer()
	bitmapSize := uint32(len(bitmapData))

	// Write the bitmap size
	if err := binary.Write(bw.file, binary.LittleEndian, bitmapSize); err != nil {
		return 0, 0, fmt.Errorf("failed to write bitmap size: %w", err)
	}

	// Write the bitmap data
	if _, err := bw.file.Write(bitmapData); err != nil {
		return 0, 0, fmt.Errorf("failed to write bitmap data: %w", err)
	}

	return uint64(bitmapOffset), uint64(4 + bitmapSize), nil
}

// serializeIDSection takes IDs and returns a byte slice ready to be written to disk
func (bw *BufferedWriter) serializeIDSection(ids []uint64) ([]byte, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("cannot serialize empty ID section")
	}

	// Apply delta encoding if needed
	var encodedIds []uint64
	if bw.encodingType == EncodingDeltaID || bw.encodingType == EncodingDeltaBoth {
		// Apply delta encoding
		encodedIds = deltaEncode(ids)
	} else {
		// Use original IDs
		encodedIds = make([]uint64, len(ids))
		copy(encodedIds, ids)
	}

	// Then choose appropriate serialization format
	if bw.encodingType == EncodingVarInt || bw.encodingType == EncodingVarIntID ||
		bw.encodingType == EncodingVarIntBoth {
		// Use variable-length encoding
		return bw.serializeVarIntIDs(encodedIds)
	} else {
		// Use fixed-length encoding
		return bw.serializeFixedLengthIDs(encodedIds)
	}
}

// serializeValueSection takes values and returns a byte slice ready to be written to disk
func (bw *BufferedWriter) serializeValueSection(values []int64) ([]byte, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("cannot serialize empty value section")
	}

	// Apply delta encoding if needed
	var encodedValues []int64
	if bw.encodingType == EncodingDeltaValue || bw.encodingType == EncodingDeltaBoth {
		// Apply delta encoding
		encodedValues = deltaEncodeInt64(values)
	} else {
		// Use original values
		encodedValues = make([]int64, len(values))
		copy(encodedValues, values)
	}

	// Then choose appropriate serialization format
	if bw.encodingType == EncodingVarInt || bw.encodingType == EncodingVarIntValue ||
		bw.encodingType == EncodingVarIntBoth {
		// Use variable-length encoding
		return bw.serializeVarIntValues(encodedValues)
	} else {
		// Use fixed-length encoding
		return bw.serializeFixedLengthValues(encodedValues)
	}
}

// serializeVarIntIDs serializes IDs using variable-length encoding
func (bw *BufferedWriter) serializeVarIntIDs(ids []uint64) ([]byte, error) {
	var encodedBytes [][]byte
	for _, id := range ids {
		encoded := encodeVarInt(id)
		encodedBytes = append(encodedBytes, encoded)
	}

	// Pre-calculate total buffer size
	totalSize := 0
	for i := range encodedBytes {
		totalSize += len(encodedBytes[i])
	}

	// Create a single buffer for all IDs
	buffer := make([]byte, totalSize)
	offset := 0

	// Copy all encoded bytes into the buffer
	for i := range encodedBytes {
		copy(buffer[offset:], encodedBytes[i])
		offset += len(encodedBytes[i])
	}

	return buffer, nil
}

// serializeFixedLengthIDs serializes IDs using fixed-length encoding
func (bw *BufferedWriter) serializeFixedLengthIDs(ids []uint64) ([]byte, error) {
	// Pre-calculate buffer size: 8 bytes per ID
	buffer := make([]byte, len(ids)*8)

	// Write each ID into the buffer
	for i, id := range ids {
		binary.LittleEndian.PutUint64(buffer[i*8:], id)
	}

	return buffer, nil
}

// serializeVarIntValues serializes values using variable-length encoding
func (bw *BufferedWriter) serializeVarIntValues(values []int64) ([]byte, error) {
	var encodedBytes [][]byte
	for _, value := range values {
		encoded := encodeSignedVarInt(value)
		encodedBytes = append(encodedBytes, encoded)
	}

	// Pre-calculate total buffer size
	totalSize := 0
	for i := range encodedBytes {
		totalSize += len(encodedBytes[i])
	}

	// Create a single buffer for all values
	buffer := make([]byte, totalSize)
	offset := 0

	// Copy all encoded bytes into the buffer
	for i := range encodedBytes {
		copy(buffer[offset:], encodedBytes[i])
		offset += len(encodedBytes[i])
	}

	return buffer, nil
}

// serializeFixedLengthValues serializes values using fixed-length encoding
func (bw *BufferedWriter) serializeFixedLengthValues(values []int64) ([]byte, error) {
	// Pre-calculate buffer size: 8 bytes per value
	buffer := make([]byte, len(values)*8)

	// Write each value into the buffer
	for i, value := range values {
		binary.LittleEndian.PutUint64(buffer[i*8:], int64ToUint64(value))
	}

	return buffer, nil
}

// encodeIDs encodes the IDs based on the encoding type
func (bw *BufferedWriter) encodeIDs(ids []uint64) ([]uint64, [][]byte, uint32, error) {
	return encodeData(bw.encodingType, ids, deltaEncode, encodeVarInt)
}

// encodeValues encodes the values based on the encoding type
func (bw *BufferedWriter) encodeValues(values []int64) ([]int64, [][]byte, uint32, error) {
	return encodeData(bw.encodingType, values, deltaEncodeInt64, encodeSignedVarInt)
}

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

	// Sync to disk to ensure data consistency
	if err := bw.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}
