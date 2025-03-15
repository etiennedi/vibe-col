package col

import (
	"encoding/binary"
	"fmt"
	"io"
)

// writeGlobalIDBitmap writes the global ID bitmap to the file
func (w *Writer) writeGlobalIDBitmap() (uint64, uint64, error) {
	// Get the current position - this is where the bitmap will start
	bitmapOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get bitmap offset: %w", err)
	}

	// Get the buffer from the bitmap
	// The sroar bitmap is already a serialized representation
	buf := w.globalIDs.ToBuffer()

	// Write the size of the bitmap
	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(buf))); err != nil {
		return 0, 0, fmt.Errorf("failed to write bitmap size: %w", err)
	}

	// Write the bitmap data
	if _, err := w.file.Write(buf); err != nil {
		return 0, 0, fmt.Errorf("failed to write bitmap data: %w", err)
	}

	// Get the current position - this is where the bitmap ends
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current position: %w", err)
	}

	// Calculate the size of the bitmap (including the size field)
	bitmapSize := currentPos - bitmapOffset

	return uint64(bitmapOffset), uint64(bitmapSize), nil
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
	// Write the global ID bitmap
	bitmapOffset, bitmapSize, err := w.writeGlobalIDBitmap()
	if err != nil {
		return fmt.Errorf("failed to write global ID bitmap: %w", err)
	}

	// Update file header with final block count and bitmap information
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start: %w", err)
	}

	// Create updated header
	header := NewFileHeader(w.blockCount, w.blockSizeTarget, w.encodingType)
	header.BitmapOffset = bitmapOffset
	header.BitmapSize = bitmapSize

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
