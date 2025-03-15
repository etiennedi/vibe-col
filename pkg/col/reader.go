package col

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/weaviate/sroar"
)

// Reader reads a column file
type Reader struct {
	file           *os.File
	fileSize       int64
	header         FileHeader
	footerMeta     FooterMetadata
	blockIndex     []FooterEntry
	globalIDs      *sroar.Bitmap
	cacheGlobalIDs bool // Whether to cache the global ID bitmap
}

// NewReader creates a new column file reader
func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file size immediately as we'll need it for various offset calculations
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	reader := &Reader{
		file:           file,
		fileSize:       fileSize,
		cacheGlobalIDs: false, // Caching is off by default
	}

	// Read the file header
	if err := reader.readHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Read the footer
	if err := reader.readFooter(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	return reader, nil
}

// GetPairs returns the ID-value pairs from a block
func (r *Reader) GetPairs(blockIdx uint64) ([]uint64, []int64, error) {
	return r.readBlock(int(blockIdx))
}

// Version returns the file format version
func (r *Reader) Version() uint32 {
	return r.header.Version
}

// EncodingType returns the file encoding type
func (r *Reader) EncodingType() uint32 {
	return r.header.EncodingType
}

// IsDeltaEncoded returns whether the file is delta encoded
func (r *Reader) IsDeltaEncoded() bool {
	return r.header.EncodingType == EncodingDeltaID ||
		r.header.EncodingType == EncodingDeltaValue ||
		r.header.EncodingType == EncodingDeltaBoth
}

// IsVarIntEncoded returns whether the file uses variable-length encoding
func (r *Reader) IsVarIntEncoded() bool {
	return r.header.EncodingType == EncodingVarInt ||
		r.header.EncodingType == EncodingVarIntID ||
		r.header.EncodingType == EncodingVarIntValue ||
		r.header.EncodingType == EncodingVarIntBoth
}

// BlockCount returns the number of blocks in the file
func (r *Reader) BlockCount() uint64 {
	return r.header.BlockCount
}

// Close closes the file
func (r *Reader) Close() error {
	return r.file.Close()
}

// DebugInfo returns debug information about the file
func (r *Reader) DebugInfo() string {
	info := fmt.Sprintf("File header: Magic=0x%X, Version=%d, BlockCount=%d\n",
		r.header.Magic, r.header.Version, r.header.BlockCount)

	info += fmt.Sprintf("    Encoding: Type=%d, Compression=%d\n",
		r.header.EncodingType, r.header.CompressionType)

	info += fmt.Sprintf("    Footer: Size=%d, Magic=0x%X\n",
		r.footerMeta.FooterSize, r.footerMeta.Magic)

	info += fmt.Sprintf("    Block index entries: %d\n", len(r.blockIndex))

	for i, entry := range r.blockIndex {
		info += fmt.Sprintf("      Block %d: Offset=%d, Size=%d, Count=%d\n",
			i, entry.BlockOffset, entry.BlockSize, entry.Count)

		// Convert stored uint64 values back to int64
		minValue := uint64ToInt64(entry.MinValue)
		maxValue := uint64ToInt64(entry.MaxValue)

		info += fmt.Sprintf("        ID range: %d-%d\n", entry.MinID, entry.MaxID)
		info += fmt.Sprintf("        Value range: %d-%d, Sum=%d\n",
			minValue, maxValue, uint64ToInt64(entry.Sum))
	}

	return info
}

// EnableGlobalIDBitmapCaching enables caching of the global ID bitmap
func (r *Reader) EnableGlobalIDBitmapCaching() {
	r.cacheGlobalIDs = true
}

// DisableGlobalIDBitmapCaching disables caching of the global ID bitmap
func (r *Reader) DisableGlobalIDBitmapCaching() {
	r.cacheGlobalIDs = false
	r.globalIDs = nil // Clear any cached bitmap
}

// GetGlobalIDBitmap returns the global ID bitmap from the file
// If the file doesn't have a global ID bitmap, it returns an empty bitmap
// The bitmap is cached only if caching is enabled
func (r *Reader) GetGlobalIDBitmap() (*sroar.Bitmap, error) {
	// If we've already loaded the bitmap and caching is enabled, return it
	if r.globalIDs != nil && r.cacheGlobalIDs {
		return r.globalIDs, nil
	}

	// If the file doesn't have a bitmap, return an empty one
	if r.header.BitmapOffset == 0 || r.header.BitmapSize == 0 {
		bitmap := sroar.NewBitmap()
		// Only cache if enabled
		if r.cacheGlobalIDs {
			r.globalIDs = bitmap
		}
		return bitmap, nil
	}

	// Read the bitmap size (first 4 bytes)
	sizeBuf, err := r.readBytesAt(int64(r.header.BitmapOffset), 4)
	if err != nil {
		return nil, fmt.Errorf("failed to read bitmap size: %w", err)
	}
	bitmapSize := binary.LittleEndian.Uint32(sizeBuf)

	// Read the bitmap data
	bitmapBuf, err := r.readBytesAt(int64(r.header.BitmapOffset)+4, int(bitmapSize))
	if err != nil {
		return nil, fmt.Errorf("failed to read bitmap data: %w", err)
	}

	// Create a bitmap from the buffer
	bitmap := sroar.FromBuffer(bitmapBuf)

	// Only cache if enabled
	if r.cacheGlobalIDs {
		r.globalIDs = bitmap
	}

	return bitmap, nil
}
