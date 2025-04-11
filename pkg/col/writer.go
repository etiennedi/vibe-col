package col

import (
	"fmt"
	"os"

	"github.com/weaviate/sroar"
)

// Writer writes a column file
type Writer struct {
	file            *os.File
	blockCount      uint64
	encodingType    uint32
	blockSizeTarget uint32
	blockPositions  []uint64      // Position of each block in the file
	blockSizes      []uint32      // Size of each block in bytes
	blockStats      []BlockStats  // Statistics for each block
	globalIDs       *sroar.Bitmap // Bitmap of all IDs in the file
	deletedIDs      *sroar.Bitmap // Bitmap of deleted IDs
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
		encodingType:    EncodingRaw, // Default
		blockSizeTarget: defaultBlockSize,
		blockPositions:  make([]uint64, 0),
		blockSizes:      make([]uint32, 0),
		blockStats:      make([]BlockStats, 0),
		globalIDs:       sroar.NewBitmap(),
		deletedIDs:      sroar.NewBitmap(),
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

// AddDeletedID adds a deleted ID to the writer
func (w *Writer) AddDeletedID(id uint64) {
	w.deletedIDs.Set(id)
}

// AddDeletedIDBitmap adds all IDs from the provided bitmap to the deleted IDs bitmap
func (w *Writer) AddDeletedIDBitmap(bitmap *sroar.Bitmap) {
	// Merge the provided bitmap with our deleted IDs bitmap
	w.deletedIDs = w.deletedIDs.Or(bitmap)
}

// BatchAddDeletedIDs adds multiple deleted IDs to the writer
func (w *Writer) BatchAddDeletedIDs(ids []uint64) {
	for _, id := range ids {
		w.deletedIDs.Set(id)
	}
}
