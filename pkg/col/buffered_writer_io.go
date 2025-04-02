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

	// Allocate a single buffer for the entire header (64 bytes)
	headerBuf := make([]byte, headerSize)
	offset := 0

	// Write all fields directly into the buffer
	binary.LittleEndian.PutUint64(headerBuf[offset:], header.Magic)
	offset += 8

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.Version)
	offset += 4

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.ColumnType)
	offset += 4

	binary.LittleEndian.PutUint64(headerBuf[offset:], header.BlockCount)
	offset += 8

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.BlockSizeTarget)
	offset += 4

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.CompressionType)
	offset += 4

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.EncodingType)
	offset += 4

	binary.LittleEndian.PutUint64(headerBuf[offset:], header.CreationTime)
	offset += 8

	binary.LittleEndian.PutUint64(headerBuf[offset:], header.BitmapOffset)
	offset += 8

	binary.LittleEndian.PutUint64(headerBuf[offset:], header.BitmapSize)
	offset += 8

	// The rest of the buffer is already zeroed by make(), which serves as the reserved space

	// Write the entire buffer at once
	if _, err := bw.file.Write(headerBuf); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
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

	// Allocate buffer for the block count (4 bytes)
	blockCountBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockCountBuf, blockCount)
	if _, err := bw.file.Write(blockCountBuf); err != nil {
		return fmt.Errorf("failed to write block index count: %w", err)
	}

	// Write block index entries
	if len(bw.blockIndex) > 0 {
		// Each footer entry should be 56 bytes (struct FooterEntry)
		// Allocate a buffer for all entries at once
		entrySize := 56
		entriesBuf := make([]byte, entrySize*len(bw.blockIndex))

		for i, entry := range bw.blockIndex {
			offset := i * entrySize

			// Write entry fields to buffer
			binary.LittleEndian.PutUint64(entriesBuf[offset:], entry.BlockOffset)
			offset += 8

			binary.LittleEndian.PutUint32(entriesBuf[offset:], entry.BlockSize)
			offset += 4

			binary.LittleEndian.PutUint64(entriesBuf[offset:], entry.MinID)
			offset += 8

			binary.LittleEndian.PutUint64(entriesBuf[offset:], entry.MaxID)
			offset += 8

			binary.LittleEndian.PutUint64(entriesBuf[offset:], entry.MinValue)
			offset += 8

			binary.LittleEndian.PutUint64(entriesBuf[offset:], entry.MaxValue)
			offset += 8

			binary.LittleEndian.PutUint64(entriesBuf[offset:], entry.Sum)
			offset += 8

			binary.LittleEndian.PutUint32(entriesBuf[offset:], entry.Count)
			offset += 4
		}

		// Write all entries at once
		if _, err := bw.file.Write(entriesBuf); err != nil {
			return fmt.Errorf("failed to write footer entries: %w", err)
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

	// Allocate buffer for footer metadata (24 bytes: footerSize + checksum + magic)
	footerMetaBuf := make([]byte, 24)
	binary.LittleEndian.PutUint64(footerMetaBuf[0:], uint64(footerSize))
	binary.LittleEndian.PutUint64(footerMetaBuf[8:], uint64(0)) // checksum placeholder
	binary.LittleEndian.PutUint64(footerMetaBuf[16:], MagicNumber)

	// Write footer metadata
	if _, err := bw.file.Write(footerMetaBuf); err != nil {
		return fmt.Errorf("failed to write footer metadata: %w", err)
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

	// Calculate total size needed for header plus footer offset
	// headerSize (64 bytes) plus 8 bytes for the footer offset
	totalHeaderSize := headerSize + 8

	// Allocate a buffer for the header plus the footer offset
	headerBuf := make([]byte, totalHeaderSize)
	offset := 0

	// Write all fields directly into the buffer
	binary.LittleEndian.PutUint64(headerBuf[offset:], header.Magic)
	offset += 8

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.Version)
	offset += 4

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.ColumnType)
	offset += 4

	binary.LittleEndian.PutUint64(headerBuf[offset:], header.BlockCount)
	offset += 8

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.BlockSizeTarget)
	offset += 4

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.CompressionType)
	offset += 4

	binary.LittleEndian.PutUint32(headerBuf[offset:], header.EncodingType)
	offset += 4

	binary.LittleEndian.PutUint64(headerBuf[offset:], header.CreationTime)
	offset += 8

	binary.LittleEndian.PutUint64(headerBuf[offset:], header.BitmapOffset)
	offset += 8

	binary.LittleEndian.PutUint64(headerBuf[offset:], header.BitmapSize)
	offset += 8

	// Add footer offset after the header
	binary.LittleEndian.PutUint64(headerBuf[offset:], uint64(footerStart))

	// Write the header buffer
	if _, err := bw.file.Write(headerBuf); err != nil {
		return fmt.Errorf("failed to write updated header: %w", err)
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

	// Create a buffer for bitmap size (4 bytes)
	bitmapSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(bitmapSizeBuf, bitmapSize)

	// Write bitmap size
	if _, err := bw.file.Write(bitmapSizeBuf); err != nil {
		return 0, 0, fmt.Errorf("failed to write bitmap size: %w", err)
	}

	// Write the bitmap data
	if _, err := bw.file.Write(bitmapData); err != nil {
		return 0, 0, fmt.Errorf("failed to write bitmap data: %w", err)
	}

	return uint64(bitmapOffset), uint64(4 + bitmapSize), nil
}
