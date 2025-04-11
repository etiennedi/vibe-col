package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
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

// writeDeletedIDBitmap writes the deleted ID bitmap to the file
func (w *Writer) writeDeletedIDBitmap() (uint64, uint64, error) {
	// Get the current position - this is where the bitmap will start
	bitmapOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get deleted bitmap offset: %w", err)
	}

	// Get the buffer from the bitmap
	// The sroar bitmap is already a serialized representation
	buf := w.deletedIDs.ToBuffer()

	// Write the size of the bitmap
	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(buf))); err != nil {
		return 0, 0, fmt.Errorf("failed to write deleted bitmap size: %w", err)
	}

	// Write the bitmap data
	if _, err := w.file.Write(buf); err != nil {
		return 0, 0, fmt.Errorf("failed to write deleted bitmap data: %w", err)
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

	// Write the deleted ID bitmap
	deletedBitmapOffset, deletedBitmapSize, err := w.writeDeletedIDBitmap()
	if err != nil {
		return fmt.Errorf("failed to write deleted ID bitmap: %w", err)
	}

	// Seek to the end to write the footer
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end: %w", err)
	}

	// Get current position - before padding
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current position: %w", err)
	}

	// Add padding to align to page boundary if necessary
	padding := calculatePadding(currentPos, PageSize)
	if padding > 0 {
		// Create padding buffer filled with zeros
		paddingBuf := make([]byte, padding)

		// Write padding bytes
		if _, err := w.file.Write(paddingBuf); err != nil {
			return fmt.Errorf("failed to write footer padding bytes: %w", err)
		}
	}

	// Get current position - start of footer (now page-aligned)
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

	// Create footer metadata and serialize it
	footerMeta := FooterMetadata{
		FooterSize: uint64(footerSize),
		Checksum:   uint64(0), // Checksum placeholder
		Magic:      MagicNumber,
	}

	// Serialize and write footer metadata
	footerMetaBuf := footerMeta.Serialize()
	if _, err := w.file.Write(footerMetaBuf); err != nil {
		return fmt.Errorf("failed to write footer metadata: %w", err)
	}

	// Update the header with final block count and bitmap information
	return w.updateHeader(bitmapOffset, bitmapSize, deletedBitmapOffset, deletedBitmapSize, footerStart)
}

// updateHeader updates the file header with the final information
func (w *Writer) updateHeader(bitmapOffset, bitmapSize, deletedBitmapOffset, deletedBitmapSize uint64, footerStart int64) error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start: %w", err)
	}

	// Create updated header
	header := NewFileHeader(w.blockCount, w.blockSizeTarget, w.encodingType)
	header.BitmapOffset = bitmapOffset
	header.BitmapSize = bitmapSize
	header.DeletedBitmapOffset = deletedBitmapOffset
	header.DeletedBitmapSize = deletedBitmapSize
	header.CreationTime = uint64(time.Now().Unix())
	header.Level = w.level // Preserve the level field from the writer

	// Serialize the header with footer offset
	headerBuf := header.SerializeWithFooterOffset(uint64(footerStart))

	// Write the header
	if _, err := w.file.Write(headerBuf); err != nil {
		return fmt.Errorf("failed to write updated header: %w", err)
	}

	return nil
}

// Close closes the file without finalizing it
func (w *Writer) Close() error {
	return w.file.Close()
}
