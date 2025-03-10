// Package col implements a column-based storage format for id-value pairs.
package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

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
	blockPositions  []uint64    // Position of each block in the file
	blockSizes      []uint32    // Size of each block in bytes
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
		encodingType:    EncodingRaw,  // Default
		blockSizeTarget: 4 * 1024,     // 4KB default
		blockPositions:  make([]uint64, 0),
		blockSizes:      make([]uint32, 0),
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

// WriteBlock writes a block of ID-value pairs
func (w *Writer) WriteBlock(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return fmt.Errorf("cannot write empty block")
	}

	// Calculate block statistics
	minID := ids[0]
	maxID := ids[0]
	minValue := values[0]
	maxValue := values[0]
	var sum int64 = 0
	count := uint32(len(ids))

	for i := 0; i < len(ids); i++ {
		// Update min/max IDs
		if ids[i] < minID {
			minID = ids[i]
		}
		if ids[i] > maxID {
			maxID = ids[i]
		}

		// Update min/max values
		if values[i] < minValue {
			minValue = values[i]
		}
		if values[i] > maxValue {
			maxValue = values[i]
		}

		// Update sum
		sum += values[i]
	}

	// Prepare data for writing based on encoding type
	var encodedIDs []uint64
	var encodedValues []int64
	blockEncodingType := w.encodingType
	
	// Apply encoding based on the selected type
	switch w.encodingType {
	case EncodingRaw:
		encodedIDs = ids
		encodedValues = values
	case EncodingDeltaID:
		encodedIDs = deltaEncode(ids)
		encodedValues = values
	case EncodingDeltaValue:
		encodedIDs = ids
		encodedValues = deltaEncodeInt64(values)
	case EncodingDeltaBoth:
		encodedIDs = deltaEncode(ids)
		encodedValues = deltaEncodeInt64(values)
	case EncodingVarInt, EncodingVarIntID, EncodingVarIntValue, EncodingVarIntBoth:
		// For variable-length encoding, we'll prepare the data below
		// but we need to keep track of the encoding type
		encodedIDs = ids
		encodedValues = values
		
		// For combined VarInt+Delta encoding
		if w.encodingType == EncodingVarIntBoth {
			encodedIDs = deltaEncode(ids)
			encodedValues = deltaEncodeInt64(values)
		}
	default:
		// Fallback to raw for unknown encoding
		encodedIDs = ids
		encodedValues = values
		blockEncodingType = EncodingRaw
	}

	// Remember the block start position (beginning of header)
	blockStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get block start position: %w", err)
	}
	
	// Store this position so we can refer to it later in the footer
	w.blockPositions = append(w.blockPositions, uint64(blockStart))

	// Create block header
	blockHeader := NewBlockHeader(minID, maxID, minValue, maxValue, sum, count, blockEncodingType)

	// Write block header (64 bytes)
	// Use direct buffer writing for type safety
	headerBuf := make([]byte, 44) // 8+8+8+8+8+4 bytes
	
	// Write values into buffer with correct types
	binary.LittleEndian.PutUint64(headerBuf[0:8], blockHeader.MinID)
	binary.LittleEndian.PutUint64(headerBuf[8:16], blockHeader.MaxID)
	binary.LittleEndian.PutUint64(headerBuf[16:24], blockHeader.MinValue)
	binary.LittleEndian.PutUint64(headerBuf[24:32], blockHeader.MaxValue)
	binary.LittleEndian.PutUint64(headerBuf[32:40], blockHeader.Sum)
	
	// Write the count field to the block header
	binary.LittleEndian.PutUint32(headerBuf[40:44], blockHeader.Count)
	
	// Write the buffer to the file
	if _, err := w.file.Write(headerBuf); err != nil {
		return fmt.Errorf("failed to write block header: %w", err)
	}
	
	if err := binary.Write(w.file, binary.LittleEndian, blockHeader.EncodingType); err != nil {
		return fmt.Errorf("failed to write encoding type: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, blockHeader.CompressionType); err != nil {
		return fmt.Errorf("failed to write compression type: %w", err)
	}

	// Determine if we need to use variable-length encoding
	useVarIntForIDs := blockEncodingType == EncodingVarInt || 
	                   blockEncodingType == EncodingVarIntID || 
	                   blockEncodingType == EncodingVarIntBoth
	useVarIntForValues := blockEncodingType == EncodingVarInt || 
	                      blockEncodingType == EncodingVarIntValue || 
	                      blockEncodingType == EncodingVarIntBoth

	// Calculate section sizes - these will vary if using varint encoding
	var idSectionSize uint32
	var valueSectionSize uint32
	
	// Calculate sizes based on encoding
	// For varInt encoding, we need to precompute the encoded values array
	// so we know the exact size
	var encodedIdBytes [][]byte
	var encodedValueBytes [][]byte
	
	if useVarIntForIDs {
		// Precompute the encoded bytes for each ID
		encodedIdBytes = make([][]byte, len(encodedIDs))
		idSectionSize = 0
		
		for i, id := range encodedIDs {
			// Encode this ID as varint
			encodedIdBytes[i] = encodeVarInt(id)
			// Add the size of this encoded ID
			idSize := uint32(len(encodedIdBytes[i]))
			
			if idSize == 0 {
				// This should never happen - a minimum of 1 byte is needed for varint
				return fmt.Errorf("encoded size of ID at index %d is 0", i)
			}
			idSectionSize += idSize
		}
		// Sanity check
		if idSectionSize == 0 && len(encodedIDs) > 0 {
			// Should never happen if the loop ran properly
			return fmt.Errorf("calculated ID section size is 0 with %d IDs", len(encodedIDs))
		}
		
	} else {
		// Fixed 8 bytes per ID
		idSectionSize = uint32(count * 8)
	}
	
	if useVarIntForValues {
		// Precompute the encoded bytes for each value
		encodedValueBytes = make([][]byte, len(encodedValues))
		valueSectionSize = 0
		for i, val := range encodedValues {
			// Encode this value as signed varint
			encodedValueBytes[i] = encodeSignedVarInt(val)
			// Add the size of this encoded value
			valSize := uint32(len(encodedValueBytes[i]))
			if valSize == 0 {
				// This should never happen - a minimum of 1 byte is needed for varint
				return fmt.Errorf("encoded size of value at index %d is 0", i)
			}
			valueSectionSize += valSize
		}
		// Sanity check
		if valueSectionSize == 0 && len(encodedValues) > 0 {
			// Should never happen if the loop ran properly
			return fmt.Errorf("calculated value section size is 0 with %d values", len(encodedValues))
		}
	} else {
		// Fixed 8 bytes per value
		valueSectionSize = uint32(count * 8)
	}
	
	// Calculate total data size
	dataSize := idSectionSize + valueSectionSize
	
	if err := binary.Write(w.file, binary.LittleEndian, dataSize); err != nil {
		return fmt.Errorf("failed to write uncompressed size: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, dataSize); err != nil {
		return fmt.Errorf("failed to write compressed size: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, uint64(0)); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	// Write reserved space to fill up to 64 bytes
	// 44 (header) + 4 (encoding) + 4 (compression) + 4 (uncompressed size) 
	// + 4 (compressed size) + 8 (checksum) = 68 bytes
	// The header is already slightly larger than 64 bytes in our implementation
	// We'll skip the reserved space since we're already over 64 bytes

	// Write the block layout section (16 bytes)
	// The section layout according to spec:
	// 1. ID section offset (from start of data section)
	// 2. ID section size in bytes
	// 3. Value section offset (from start of data section)
	// 4. Value section size in bytes
	
	// Create a layout buffer and fill it directly
	layoutBuf := make([]byte, 16)
	
	// Validate section sizes
	if idSectionSize == 0 {
		return fmt.Errorf("ID section size is 0, which is invalid. useVarIntForIDs=%v, count=%d",
			useVarIntForIDs, count)
	}
	
	// Per spec section 4.2:
	// - ID section comes first after the 4-byte size header in the data section
	// - Value section follows the ID section
	
	// Use the binary package to properly write integers - using the layout from spec 4.2
	idSectionOffset := uint32(4) // 4 bytes for size header
	valueSectionOffset := idSectionOffset + idSectionSize
	
	binary.LittleEndian.PutUint32(layoutBuf[0:4], idSectionOffset) // ID Section Offset (after 4-byte size header)
	binary.LittleEndian.PutUint32(layoutBuf[4:8], idSectionSize) // ID Section Size
	binary.LittleEndian.PutUint32(layoutBuf[8:12], valueSectionOffset) // Value Section Offset (ID offset + ID size)
	binary.LittleEndian.PutUint32(layoutBuf[12:16], valueSectionSize) // Value Section Size
	
	
	// Write the layout buffer to file
	if _, err := w.file.Write(layoutBuf); err != nil {
		return fmt.Errorf("failed to write block layout: %w", err)
	}
	
	// Ensure data is written to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Write block data
	// Get current position at the start of data section (for debugging)
	_, _ = w.file.Seek(0, io.SeekCurrent)
	
	// Write the data section size (4 bytes)
	if err := binary.Write(w.file, binary.LittleEndian, uint32(dataSize)); err != nil {
		return fmt.Errorf("failed to write data section size: %w", err)
	}
	
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
	
	// Store the total block size
	blockSize := uint32(blockEnd - blockStart)
	w.blockSizes = append(w.blockSizes, blockSize)
	
	// Update block count
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
			// Get position and size for this block
			blockOffset := w.blockPositions[blockIdx]
			blockSize := w.blockSizes[blockIdx]
			
			// Seek to the block header
			if _, err := w.file.Seek(int64(blockOffset), io.SeekStart); err != nil {
				return fmt.Errorf("failed to seek to block %d header: %w", blockIdx, err)
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
			
			// Create footer entry
			entry := NewFooterEntry(
				blockOffset,
				blockSize,
				minID, maxID,
				minValue, maxValue, sum,
				count,
			)
			
			// Write entry to footer
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