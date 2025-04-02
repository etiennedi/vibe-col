package col

import (
	"fmt"
	"io"
)

// writeHeader writes the file header to the file
func (w *Writer) writeHeader() error {
	// Record start position to verify header size
	headerStart, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get header start position: %w", err)
	}

	// Create the header with default values
	header := NewFileHeader(0, w.blockSizeTarget, w.encodingType)

	// Serialize the header
	headerBuf := header.Serialize()

	// Write the header buffer
	if _, err := w.file.Write(headerBuf); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Verify header size
	headerEnd, err := w.file.Seek(0, io.SeekCurrent)
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
