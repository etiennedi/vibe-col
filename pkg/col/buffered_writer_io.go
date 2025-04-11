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
	// Use tracked position - should be 0 at this point
	headerStart := bw.currentPosition

	// Create the header with default values
	header := NewFileHeader(0, bw.blockSizeTarget, bw.encodingType)

	// Set the level from the writer
	header.Level = bw.level

	// Serialize the header
	headerBuf := header.Serialize()

	// Write the entire buffer at once and track position
	if n, err := bw.writeAndTrack(headerBuf); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	} else if n != len(headerBuf) {
		return fmt.Errorf("failed to write full header: wrote %d bytes, expected %d", n, len(headerBuf))
	}

	// Calculate actual header size using tracked position
	headerEnd := bw.currentPosition
	actualHeaderSize := headerEnd - headerStart

	// Validate header size
	if actualHeaderSize != int64(headerSize) {
		return fmt.Errorf("header size mismatch: expected=%d, actual=%d", headerSize, actualHeaderSize)
	}

	return nil
}

// writeDeletedIDBitmap writes the deleted ID bitmap and returns its offset and size
func (bw *BufferedWriter) writeDeletedIDBitmap() (uint64, uint64, error) {
	// Get current position from tracker
	bitmapOffset := bw.currentPosition

	// Serialize bitmap
	bitmapData := bw.deletedIDs.ToBuffer()
	bitmapSize := uint32(len(bitmapData))

	// Create a buffer for bitmap size (4 bytes)
	bitmapSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(bitmapSizeBuf, bitmapSize)

	// Write bitmap size
	if _, err := bw.writeAndTrack(bitmapSizeBuf); err != nil {
		return 0, 0, fmt.Errorf("failed to write deleted bitmap size: %w", err)
	}

	// Write the bitmap data
	if _, err := bw.writeAndTrack(bitmapData); err != nil {
		return 0, 0, fmt.Errorf("failed to write deleted bitmap data: %w", err)
	}

	return uint64(bitmapOffset), uint64(4 + bitmapSize), nil
}

// finalize writes the footer and updates the header
func (bw *BufferedWriter) finalize() error {
	// Write the global ID bitmap
	bitmapOffset, bitmapSize, err := bw.writeGlobalIDBitmap()
	if err != nil {
		return fmt.Errorf("failed to write global ID bitmap: %w", err)
	}

	// Write the deleted ID bitmap
	deletedBitmapOffset, deletedBitmapSize, err := bw.writeDeletedIDBitmap()
	if err != nil {
		return fmt.Errorf("failed to write deleted ID bitmap: %w", err)
	}

	// Get current position from tracker - this is where the footer will start
	footerStart := bw.currentPosition

	// Add padding if needed to align to page boundary
	padding := calculatePadding(footerStart, PageSize)
	if padding > 0 {
		// Create padding buffer filled with zeros
		paddingBuf := make([]byte, padding)
		if _, err := bw.writeAndTrack(paddingBuf); err != nil {
			return fmt.Errorf("failed to write padding bytes: %w", err)
		}
		// Update footer start position after padding
		footerStart = bw.currentPosition
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
	if _, err := bw.writeAndTrack(blockCountBuf); err != nil {
		return fmt.Errorf("failed to write block index count: %w", err)
	}

	// Write block index entries
	// If there are no blocks, write a dummy entry
	if blockCount == 1 && len(bw.blockIndex) == 0 {
		// Create a dummy entry with zeros
		dummyEntry := FooterEntry{
			BlockOffset: 0,
			BlockSize:   0,
			MinID:       0,
			MaxID:       0,
			MinValue:    0,
			MaxValue:    0,
			Sum:         0,
			Count:       0,
		}

		// Serialize and write the dummy entry
		entryBuf := dummyEntry.Serialize()
		if _, err := bw.writeAndTrack(entryBuf); err != nil {
			return fmt.Errorf("failed to write dummy block index entry: %w", err)
		}
	} else {
		// Write real block index entries
		for _, entry := range bw.blockIndex {
			entryBuf := entry.Serialize()
			if _, err := bw.writeAndTrack(entryBuf); err != nil {
				return fmt.Errorf("failed to write block index entry: %w", err)
			}
		}
	}

	// Get current position after writing block index
	currentPos := bw.currentPosition

	// Calculate footer size
	footerSize := currentPos - footerStart

	// Write footer metadata
	footerMeta := FooterMetadata{
		FooterSize: uint64(footerSize),
		Checksum:   uint64(0), // Checksum placeholder
		Magic:      MagicNumber,
	}

	// Serialize and write footer metadata
	footerMetaBuf := footerMeta.Serialize()
	if _, err := bw.writeAndTrack(footerMetaBuf); err != nil {
		return fmt.Errorf("failed to write footer metadata: %w", err)
	}

	// Jump back to start of file to update header
	if _, err := bw.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start: %w", err)
	}

	// Update our position tracker
	bw.currentPosition = 0

	// Create updated header
	header := NewFileHeader(uint64(blockCount), bw.blockSizeTarget, bw.encodingType)
	header.BitmapOffset = bitmapOffset
	header.BitmapSize = bitmapSize
	header.DeletedBitmapOffset = deletedBitmapOffset
	header.DeletedBitmapSize = deletedBitmapSize
	header.CreationTime = uint64(time.Now().Unix())
	header.Level = bw.level // Preserve the level field from the writer

	// Serialize the header with footer offset
	headerBuf := header.SerializeWithFooterOffset(uint64(footerStart))

	// Write the header buffer and track position
	if _, err := bw.writeAndTrack(headerBuf); err != nil {
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
	// Get current position from tracker
	bitmapOffset := bw.currentPosition

	// Serialize bitmap
	bitmapData := bw.globalIDs.ToBuffer()
	bitmapSize := uint32(len(bitmapData))

	// Create a buffer for bitmap size (4 bytes)
	bitmapSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(bitmapSizeBuf, bitmapSize)

	// Write bitmap size
	if _, err := bw.writeAndTrack(bitmapSizeBuf); err != nil {
		return 0, 0, fmt.Errorf("failed to write bitmap size: %w", err)
	}

	// Write the bitmap data
	if _, err := bw.writeAndTrack(bitmapData); err != nil {
		return 0, 0, fmt.Errorf("failed to write bitmap data: %w", err)
	}

	return uint64(bitmapOffset), uint64(4 + bitmapSize), nil
}
