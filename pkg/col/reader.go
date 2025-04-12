package col

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/weaviate/sroar"
)

// Reader represents a COL file reader
type Reader struct {
	filePath        string // Store the file path for logging/debugging
	file            *os.File
	fileSize        int64
	header          FileHeader // Store header info
	footerMeta      FooterMetadata
	blockIndex      []FooterEntry // Store footer info
	globalIDs       *sroar.Bitmap // Bitmap of all IDs in the file
	deletedIDs      *sroar.Bitmap // Bitmap of deleted IDs (tombstones)
	cacheGlobalIDs  bool          // Whether to cache the global ID bitmap
	cacheDeletedIDs bool          // Whether to cache the deleted IDs bitmap
	mutex           sync.RWMutex  // Mutex for thread safety during concurrent reads

	// Aggregation source implementation
	isAggregateSource bool // Marker to satisfy interface
}

// NewReader creates a new column file reader
func NewReader(filePath string) (*Reader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	// Get file size immediately as we'll need it for various offset calculations
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info for %s: %w", filePath, err)
	}
	fileSize := fileInfo.Size()

	reader := &Reader{
		file:              file,
		filePath:          filePath, // Set the file path
		fileSize:          fileSize,
		cacheGlobalIDs:    false, // Caching is off by default
		cacheDeletedIDs:   false, // Caching is off by default
		isAggregateSource: true,
	}

	// Read the file header
	if err := reader.readHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header from %s: %w", filePath, err)
	}

	// Read the footer
	if err := reader.readFooter(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read footer from %s: %w", filePath, err)
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

	// Create an empty bitmap as fallback
	emptyBitmap := sroar.NewBitmap()

	// Read the bitmap size (first 4 bytes)
	sizeBuf, err := r.readBytesAt(int64(r.header.BitmapOffset), 4)
	if err != nil {
		// If we can't read the bitmap size, return an empty bitmap
		if r.cacheGlobalIDs {
			r.globalIDs = emptyBitmap
		}
		return emptyBitmap, nil
	}
	bitmapSize := binary.LittleEndian.Uint32(sizeBuf)

	// If bitmap size is 0, return an empty bitmap
	if bitmapSize == 0 {
		if r.cacheGlobalIDs {
			r.globalIDs = emptyBitmap
		}
		return emptyBitmap, nil
	}

	// Read the bitmap data
	bitmapBuf, err := r.readBytesAt(int64(r.header.BitmapOffset)+4, int(bitmapSize))
	if err != nil {
		// If we can't read the bitmap data, return an empty bitmap
		if r.cacheGlobalIDs {
			r.globalIDs = emptyBitmap
		}
		return emptyBitmap, nil
	}

	// If bitmap buffer is empty, return an empty bitmap
	if len(bitmapBuf) == 0 {
		if r.cacheGlobalIDs {
			r.globalIDs = emptyBitmap
		}
		return emptyBitmap, nil
	}

	var bitmap *sroar.Bitmap
	// Use a defer-recover to catch any panics from sroar.FromBuffer
	func() {
		defer func() {
			if r := recover(); r != nil {
				// If there's a panic, set bitmap to empty
				bitmap = emptyBitmap
			}
		}()
		// Try to create a bitmap from the buffer
		bitmap = sroar.FromBuffer(bitmapBuf)
	}()

	// If bitmap is nil (shouldn't happen, but just to be safe), use empty bitmap
	if bitmap == nil {
		bitmap = emptyBitmap
	}

	// Only cache if enabled
	if r.cacheGlobalIDs {
		r.globalIDs = bitmap
	}

	return bitmap, nil
}

// EnableDeletedIDBitmapCaching enables caching of the deleted ID bitmap
func (r *Reader) EnableDeletedIDBitmapCaching() {
	r.cacheDeletedIDs = true
}

// DisableDeletedIDBitmapCaching disables caching of the deleted ID bitmap
func (r *Reader) DisableDeletedIDBitmapCaching() {
	r.cacheDeletedIDs = false
	r.deletedIDs = nil // Clear any cached bitmap
}

// GetDeletedIDBitmap returns the deleted ID bitmap from the file
// If the file doesn't have a deleted ID bitmap, it returns an empty bitmap
// The bitmap is cached only if caching is enabled
func (r *Reader) GetDeletedIDBitmap() (*sroar.Bitmap, error) {
	// If we've already loaded the bitmap and caching is enabled, return it
	if r.deletedIDs != nil && r.cacheDeletedIDs {
		return r.deletedIDs, nil
	}

	// If the file doesn't have a bitmap, return an empty one
	if r.header.DeletedBitmapOffset == 0 || r.header.DeletedBitmapSize == 0 {
		bitmap := sroar.NewBitmap()
		// Only cache if enabled
		if r.cacheDeletedIDs {
			r.deletedIDs = bitmap
		}
		return bitmap, nil
	}

	// Create an empty bitmap as fallback
	emptyBitmap := sroar.NewBitmap()

	// Read the bitmap size (first 4 bytes)
	sizeBuf, err := r.readBytesAt(int64(r.header.DeletedBitmapOffset), 4)
	if err != nil {
		// If we can't read the bitmap size, return an empty bitmap
		if r.cacheDeletedIDs {
			r.deletedIDs = emptyBitmap
		}
		return emptyBitmap, nil
	}
	bitmapSize := binary.LittleEndian.Uint32(sizeBuf)

	// If bitmap size is 0, return an empty bitmap
	if bitmapSize == 0 {
		if r.cacheDeletedIDs {
			r.deletedIDs = emptyBitmap
		}
		return emptyBitmap, nil
	}

	// Read the bitmap data
	bitmapBuf, err := r.readBytesAt(int64(r.header.DeletedBitmapOffset)+4, int(bitmapSize))
	if err != nil {
		// If we can't read the bitmap data, return an empty bitmap
		if r.cacheDeletedIDs {
			r.deletedIDs = emptyBitmap
		}
		return emptyBitmap, nil
	}

	// If bitmap buffer is empty, return an empty bitmap
	if len(bitmapBuf) == 0 {
		if r.cacheDeletedIDs {
			r.deletedIDs = emptyBitmap
		}
		return emptyBitmap, nil
	}

	var bitmap *sroar.Bitmap
	// Use a defer-recover to catch any panics from sroar.FromBuffer
	func() {
		defer func() {
			if r := recover(); r != nil {
				// If there's a panic, set bitmap to empty
				bitmap = emptyBitmap
			}
		}()
		// Try to create a bitmap from the buffer
		bitmap = sroar.FromBuffer(bitmapBuf)
	}()

	// If bitmap is nil (shouldn't happen, but just to be safe), use empty bitmap
	if bitmap == nil {
		bitmap = emptyBitmap
	}

	// Only cache if enabled
	if r.cacheDeletedIDs {
		r.deletedIDs = bitmap
	}

	return bitmap, nil
}

// Level returns the compaction level of the file
func (r *Reader) Level() uint16 {
	return r.header.Level
}

// FilePath returns the path of the file being read
func (r *Reader) FilePath() string {
	return r.filePath
}
