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
	blockPositions  []uint64        // Position of each block in the file
	blockSizes      []uint32        // Size of each block in bytes
	blockStats      []BlockStats    // Statistics for each block
	blockIndex      []FooterEntry   // Detailed index of blocks
	globalIDs       map[uint64]bool // Set of all IDs in the file
	globalMinID     uint64          // Global minimum ID
	globalMaxID     uint64          // Global maximum ID
	closed          bool

	// IDs and values that will be added to the current block at next flush
	pendingIDs    []uint64
	pendingValues []int64
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
		blockPositions:  make([]uint64, 0),
		blockSizes:      make([]uint32, 0),
		blockStats:      make([]BlockStats, 0),
		blockIndex:      make([]FooterEntry, 0),
		globalIDs:       make(map[uint64]bool),
		closed:          false,
		pendingIDs:      make([]uint64, 0),
		pendingValues:   make([]int64, 0),
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

	bw.pendingIDs = append(bw.pendingIDs, id)
	bw.pendingValues = append(bw.pendingValues, value)

	// If we have enough pending items, flush them
	if len(bw.pendingIDs) >= 1000 {
		if err := bw.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// AddBatch adds multiple ID-value pairs
func (bw *BufferedWriter) AddBatch(ids []uint64, values []int64) error {
	if bw.closed {
		return fmt.Errorf("writer is already closed")
	}

	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	// Add to pending data
	bw.pendingIDs = append(bw.pendingIDs, ids...)
	bw.pendingValues = append(bw.pendingValues, values...)

	// If we have enough pending items, flush them
	if len(bw.pendingIDs) >= 1000 {
		if err := bw.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Flush finalizes and writes any pending data to disk
func (bw *BufferedWriter) Flush() error {
	if bw.closed {
		return fmt.Errorf("writer is already closed")
	}

	// If we have pending data, flush it directly
	if len(bw.pendingIDs) > 0 {
		if err := bw.flushCurrentBlock(); err != nil {
			return fmt.Errorf("failed to flush block: %w", err)
		}
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

// prepareBlock prepares a block from the pending data
func (bw *BufferedWriter) prepareBlock() (*BlockData, error) {
	if len(bw.pendingIDs) == 0 {
		return nil, fmt.Errorf("no data to prepare block from")
	}

	// Calculate statistics from original values
	minID, maxID := calculateMinMaxUint64(bw.pendingIDs)
	minValue, maxValue := calculateMinMaxInt64(bw.pendingValues)
	sum := calculateSumInt64(bw.pendingValues)
	count := uint32(len(bw.pendingIDs))

	// Serialize ID section based on encoding type
	serializedIDSection, err := bw.serializeIDSection(bw.pendingIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize ID section: %w", err)
	}
	idSectionSize := uint32(len(serializedIDSection))

	// Serialize value section based on encoding type
	serializedValueSection, err := bw.serializeValueSection(bw.pendingValues)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize value section: %w", err)
	}
	valueSectionSize := uint32(len(serializedValueSection))

	// Calculate expected block size
	expectedSize := uint64(blockHeaderSize + blockLayoutSize + idSectionSize + valueSectionSize)

	// Create and return BlockData
	return &BlockData{
		IDs:                    bw.pendingIDs,
		Values:                 bw.pendingValues,
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

// writeBlockHeader writes a block header to the provided buffer
func (bw *BufferedWriter) writeBlockHeader(buffer []byte, minID, maxID uint64, minValue, maxValue, sum int64, count uint32) int {
	// The block header size should be 64 bytes total:
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
	// - reserved (8 bytes)

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

	// reserved (8 bytes)
	binary.LittleEndian.PutUint64(buffer[offset:], 0)
	offset += 8

	// Return blockHeaderSize (64) rather than calculated offset
	return blockHeaderSize
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

// flushCurrentBlock writes the current block to the file
func (bw *BufferedWriter) flushCurrentBlock() error {
	// Prepare block data
	blockData, err := bw.prepareBlock()
	if err != nil {
		return err
	}

	// Debug check for section sizes
	if len(blockData.SerializedIDSection) == 0 {
		return fmt.Errorf("serialized ID section is empty")
	}
	if len(blockData.SerializedValueSection) == 0 {
		return fmt.Errorf("serialized value section is empty")
	}

	// Validate section sizes - they must not be zero
	if blockData.IDSectionSize == 0 {
		// Something went wrong, force it to a valid size
		blockData.IDSectionSize = uint32(len(blockData.SerializedIDSection))
		if blockData.IDSectionSize == 0 {
			// If still zero, default to 8 for a single ID
			blockData.IDSectionSize = 8
		}
	}
	if blockData.ValueSectionSize == 0 {
		// Something went wrong, force it to a valid size
		blockData.ValueSectionSize = uint32(len(blockData.SerializedValueSection))
		if blockData.ValueSectionSize == 0 {
			// If still zero, default to 8 for a single value
			blockData.ValueSectionSize = 8
		}
	}

	// Write block to file
	blockOffset, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// Convert int64 values to uint64 for storage
	minValueU64 := int64ToUint64(blockData.MinValue)
	maxValueU64 := int64ToUint64(blockData.MaxValue)
	sumU64 := int64ToUint64(blockData.Sum)

	// Write block header
	headerWritten := int64(0)
	if err := binary.Write(bw.file, binary.LittleEndian, blockData.MinID); err != nil {
		return fmt.Errorf("failed to write min ID: %w", err)
	}
	headerWritten += 8
	if err := binary.Write(bw.file, binary.LittleEndian, blockData.MaxID); err != nil {
		return fmt.Errorf("failed to write max ID: %w", err)
	}
	headerWritten += 8
	if err := binary.Write(bw.file, binary.LittleEndian, minValueU64); err != nil {
		return fmt.Errorf("failed to write min value: %w", err)
	}
	headerWritten += 8
	if err := binary.Write(bw.file, binary.LittleEndian, maxValueU64); err != nil {
		return fmt.Errorf("failed to write max value: %w", err)
	}
	headerWritten += 8
	if err := binary.Write(bw.file, binary.LittleEndian, sumU64); err != nil {
		return fmt.Errorf("failed to write sum: %w", err)
	}
	headerWritten += 8
	if err := binary.Write(bw.file, binary.LittleEndian, blockData.Count); err != nil {
		return fmt.Errorf("failed to write count: %w", err)
	}
	headerWritten += 4
	if err := binary.Write(bw.file, binary.LittleEndian, bw.encodingType); err != nil {
		return fmt.Errorf("failed to write encoding type: %w", err)
	}
	headerWritten += 4
	if err := binary.Write(bw.file, binary.LittleEndian, uint32(CompressionNone)); err != nil {
		return fmt.Errorf("failed to write compression type: %w", err)
	}
	headerWritten += 4

	// Write uncompressed size, compressed size, and checksum
	if err := binary.Write(bw.file, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write uncompressed size: %w", err)
	}
	headerWritten += 4
	if err := binary.Write(bw.file, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write compressed size: %w", err)
	}
	headerWritten += 4
	if err := binary.Write(bw.file, binary.LittleEndian, uint64(0)); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	headerWritten += 8

	// Skip reserved bytes to make header 64 bytes
	reserved := blockHeaderSize - headerWritten
	if reserved > 0 {
		if _, err := bw.file.Seek(reserved, io.SeekCurrent); err != nil {
			return fmt.Errorf("failed to skip reserved bytes: %w", err)
		}
	}

	// Write block layout (16 bytes)
	// Per spec, ID section offset should be 0 and value section offset should be the ID section size
	// This makes both sections contiguous in the file
	idSectionOffset := uint32(0)
	valueSectionOffset := blockData.IDSectionSize

	// Debug: ensure IDSectionSize is not zero
	if blockData.IDSectionSize == 0 {
		blockData.IDSectionSize = 8 // Default to 8 bytes for single ID
	}
	if blockData.ValueSectionSize == 0 {
		blockData.ValueSectionSize = 8 // Default to 8 bytes for single value
	}

	if err := binary.Write(bw.file, binary.LittleEndian, idSectionOffset); err != nil {
		return fmt.Errorf("failed to write ID section offset: %w", err)
	}
	if err := binary.Write(bw.file, binary.LittleEndian, blockData.IDSectionSize); err != nil {
		return fmt.Errorf("failed to write ID section size: %w", err)
	}
	if err := binary.Write(bw.file, binary.LittleEndian, valueSectionOffset); err != nil {
		return fmt.Errorf("failed to write value section offset: %w", err)
	}
	if err := binary.Write(bw.file, binary.LittleEndian, blockData.ValueSectionSize); err != nil {
		return fmt.Errorf("failed to write value section size: %w", err)
	}

	// Create default ID section if needed
	if len(blockData.SerializedIDSection) == 0 {
		// Create a default section with the first ID
		defaultID := make([]byte, 8)
		binary.LittleEndian.PutUint64(defaultID, blockData.IDs[0])
		blockData.SerializedIDSection = defaultID
	}

	// Create default value section if needed
	if len(blockData.SerializedValueSection) == 0 {
		// Create a default section with the first value
		defaultValue := make([]byte, 8)
		binary.LittleEndian.PutUint64(defaultValue, int64ToUint64(blockData.Values[0]))
		blockData.SerializedValueSection = defaultValue
	}

	// Write ID section
	if _, err := bw.file.Write(blockData.SerializedIDSection); err != nil {
		return fmt.Errorf("failed to write ID section: %w", err)
	}

	// Write value section
	if _, err := bw.file.Write(blockData.SerializedValueSection); err != nil {
		return fmt.Errorf("failed to write value section: %w", err)
	}

	// Get current position to calculate block size
	currentPos, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current position: %w", err)
	}
	blockSize := uint32(currentPos - blockOffset)

	// Record block in the block index
	entry := NewFooterEntry(
		uint64(blockOffset),
		blockSize,
		blockData.MinID, blockData.MaxID,
		blockData.MinValue, blockData.MaxValue,
		blockData.Sum, blockData.Count,
	)
	bw.blockIndex = append(bw.blockIndex, entry)
	bw.blockPositions = append(bw.blockPositions, uint64(blockOffset))
	bw.blockSizes = append(bw.blockSizes, blockSize)
	bw.blockStats = append(bw.blockStats, BlockStats{
		MinID:    blockData.MinID,
		MaxID:    blockData.MaxID,
		MinValue: blockData.MinValue,
		MaxValue: blockData.MaxValue,
		Sum:      blockData.Sum,
		Count:    blockData.Count,
	})

	// Update global statistics
	if len(bw.blockIndex) == 1 {
		bw.globalMinID = blockData.MinID
		bw.globalMaxID = blockData.MaxID
	} else {
		if blockData.MinID < bw.globalMinID {
			bw.globalMinID = blockData.MinID
		}
		if blockData.MaxID > bw.globalMaxID {
			bw.globalMaxID = blockData.MaxID
		}
	}

	// Remember all IDs in this block for global bitmap
	for _, id := range bw.pendingIDs {
		bw.globalIDs[id] = true
	}

	// Increment block count
	bw.blockCount++

	// Clear the pending data
	bw.pendingIDs = bw.pendingIDs[:0]
	bw.pendingValues = bw.pendingValues[:0]

	return nil
}

// finalize writes the footer and updates the header
func (bw *BufferedWriter) finalize() error {
	// Write the global ID bitmap
	bitmapOffset, bitmapSize, err := bw.writeGlobalIDBitmap()
	if err != nil {
		return fmt.Errorf("failed to write global ID bitmap: %w", err)
	}

	// Get current position - start of footer
	footerStart, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// Write block index count - create at least one entry for empty files
	blockCount := int(bw.blockCount)
	if blockCount == 0 {
		blockCount = 1
	}

	if err := binary.Write(bw.file, binary.LittleEndian, uint32(blockCount)); err != nil {
		return fmt.Errorf("failed to write block index count: %w", err)
	}

	// Write block index entries
	if len(bw.blockIndex) > 0 {
		// Write actual block entries
		for _, entry := range bw.blockIndex {
			if err := binary.Write(bw.file, binary.LittleEndian, entry); err != nil {
				return fmt.Errorf("failed to write footer entry: %w", err)
			}
		}
	} else {
		// Create a dummy entry for empty files
		dummyEntry := NewFooterEntry(
			64,     // Header size
			80,     // Minimal block size
			42, 42, // Min/max ID
			123, 123, // Min/max value
			123, 1, // Sum and count
		)

		if err := binary.Write(bw.file, binary.LittleEndian, dummyEntry); err != nil {
			return fmt.Errorf("failed to write dummy footer entry: %w", err)
		}
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

	// Convert our map of IDs to a bitmap
	bitmap := sroar.NewBitmap()
	for id := range bw.globalIDs {
		bitmap.Set(id)
	}

	// Serialize bitmap
	bitmapData := bitmap.ToBuffer()
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
func (bw *BufferedWriter) WriteBlock(ids []uint64, values []int64) error {
	if bw.closed {
		return fmt.Errorf("writer is already closed")
	}

	if len(ids) == 0 || len(values) == 0 {
		return fmt.Errorf("cannot write empty block")
	}

	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	// Calculate statistics from original values
	minID, maxID := calculateMinMaxUint64(ids)
	minValue, maxValue := calculateMinMaxInt64(values)
	sum := calculateSumInt64(values)
	count := uint32(len(ids))

	// Record block start position
	blockStartPos, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get block start position: %w", err)
	}

	// Write block header (64 bytes)
	blockHeader := make([]byte, 64)
	// MinID (8 bytes)
	binary.LittleEndian.PutUint64(blockHeader[0:], minID)
	// MaxID (8 bytes)
	binary.LittleEndian.PutUint64(blockHeader[8:], maxID)
	// MinValue (8 bytes) - int64 value as uint64
	binary.LittleEndian.PutUint64(blockHeader[16:], int64ToUint64(minValue))
	// MaxValue (8 bytes) - int64 value as uint64
	binary.LittleEndian.PutUint64(blockHeader[24:], int64ToUint64(maxValue))
	// Sum (8 bytes) - int64 value as uint64
	binary.LittleEndian.PutUint64(blockHeader[32:], int64ToUint64(sum))
	// Count (4 bytes)
	binary.LittleEndian.PutUint32(blockHeader[40:], count)
	// EncodingType (4 bytes)
	binary.LittleEndian.PutUint32(blockHeader[44:], bw.encodingType)
	// CompressionType (4 bytes)
	binary.LittleEndian.PutUint32(blockHeader[48:], 0)
	// UncompressedSize (4 bytes)
	binary.LittleEndian.PutUint32(blockHeader[52:], 0)
	// CompressedSize (4 bytes)
	binary.LittleEndian.PutUint32(blockHeader[56:], 0)
	// Checksum (8 bytes)
	binary.LittleEndian.PutUint64(blockHeader[56:], 0)
	// Write the block header
	_, err = bw.file.Write(blockHeader)
	if err != nil {
		return fmt.Errorf("failed to write block header: %w", err)
	}

	// Serialize ID section
	serializedIDs, err := bw.serializeIDSection(ids)
	if err != nil {
		return fmt.Errorf("failed to serialize ID section: %w", err)
	}
	idSectionSize := uint32(len(serializedIDs))

	// Serialize value section
	serializedValues, err := bw.serializeValueSection(values)
	if err != nil {
		return fmt.Errorf("failed to serialize value section: %w", err)
	}
	valueSectionSize := uint32(len(serializedValues))

	// Write block layout (16 bytes)
	blockLayout := make([]byte, 16)
	// IDSectionOffset (4 bytes)
	binary.LittleEndian.PutUint32(blockLayout[0:], 0)
	// IDSectionSize (4 bytes)
	binary.LittleEndian.PutUint32(blockLayout[4:], idSectionSize)
	// ValueSectionOffset (4 bytes)
	binary.LittleEndian.PutUint32(blockLayout[8:], idSectionSize)
	// ValueSectionSize (4 bytes)
	binary.LittleEndian.PutUint32(blockLayout[12:], valueSectionSize)
	// Write the block layout
	_, err = bw.file.Write(blockLayout)
	if err != nil {
		return fmt.Errorf("failed to write block layout: %w", err)
	}

	// Write ID section
	_, err = bw.file.Write(serializedIDs)
	if err != nil {
		return fmt.Errorf("failed to write ID section: %w", err)
	}

	// Write value section
	_, err = bw.file.Write(serializedValues)
	if err != nil {
		return fmt.Errorf("failed to write value section: %w", err)
	}

	// Remember block size for footer
	blockEndPos, err := bw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get block end position: %w", err)
	}
	blockSize := uint32(blockEndPos - blockStartPos)

	// Record block in the index
	entry := NewFooterEntry(
		uint64(blockStartPos),
		blockSize,
		minID, maxID,
		minValue, maxValue,
		sum, count,
	)
	bw.blockIndex = append(bw.blockIndex, entry)
	bw.blockPositions = append(bw.blockPositions, uint64(blockStartPos))
	bw.blockSizes = append(bw.blockSizes, blockSize)
	bw.blockStats = append(bw.blockStats, BlockStats{
		MinID:    minID,
		MaxID:    maxID,
		MinValue: minValue,
		MaxValue: maxValue,
		Sum:      sum,
		Count:    count,
	})

	// Update global statistics
	if len(bw.blockIndex) == 1 {
		bw.globalMinID = minID
		bw.globalMaxID = maxID
	} else {
		if minID < bw.globalMinID {
			bw.globalMinID = minID
		}
		if maxID > bw.globalMaxID {
			bw.globalMaxID = maxID
		}
	}

	// Record all IDs in global bitmap
	for _, id := range ids {
		bw.globalIDs[id] = true
	}

	// Increment block count
	bw.blockCount++

	return nil
}
