package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

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
