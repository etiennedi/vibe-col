package col

import (
	"fmt"
	"os"

	"github.com/weaviate/sroar"
)

// Helper function to get minimum of two ints - internal to writer.go
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
