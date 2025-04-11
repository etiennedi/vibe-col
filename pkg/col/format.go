// Package col implements a column-based storage format for id-value pairs.
package col

import (
	"encoding/binary"
	"fmt"
	"time"
)

const (
	// Magic number for the file format
	MagicNumber uint64 = 0x5642455F434F4C00 // "VIBE_COL" in ASCII

	// Version of the file format
	Version uint32 = 1

	// Data types
	DataTypeInt64 uint32 = 0

	// Encoding types
	EncodingRaw         uint32 = 0
	EncodingDeltaID     uint32 = 1 // Delta encoding for IDs
	EncodingDeltaValue  uint32 = 2 // Delta encoding for values
	EncodingDeltaBoth   uint32 = 3 // Delta encoding for both IDs and values
	EncodingVarInt      uint32 = 4 // Variable-length integer encoding
	EncodingVarIntID    uint32 = 5 // Variable-length encoding for IDs
	EncodingVarIntValue uint32 = 6 // Variable-length encoding for values
	EncodingVarIntBoth  uint32 = 7 // Variable-length encoding for both IDs and values

	// Compression types
	CompressionNone uint32 = 0
)

// FileHeader represents the header of a column file
type FileHeader struct {
	Magic               uint64
	Version             uint32
	ColumnType          uint32
	BlockCount          uint64
	BlockSizeTarget     uint32
	CompressionType     uint32
	EncodingType        uint32
	CreationTime        uint64
	BitmapOffset        uint64 // Offset to the global ID bitmap
	BitmapSize          uint64 // Size of the global ID bitmap in bytes
	DeletedBitmapOffset uint64 // Offset to the deleted IDs bitmap
	DeletedBitmapSize   uint64 // Size of the deleted IDs bitmap in bytes
	FooterOffset        uint64 // Offset to the start of the footer
	// Reserved space - fills up to 96 bytes
}

// Serialize serializes the FileHeader into a byte slice
func (h *FileHeader) Serialize() []byte {
	// Create a buffer for the entire header (96 bytes)
	buf := make([]byte, headerSize)
	offset := 0

	// Write all fields directly into the buffer
	binary.LittleEndian.PutUint64(buf[offset:], h.Magic)
	offset += 8

	binary.LittleEndian.PutUint32(buf[offset:], h.Version)
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:], h.ColumnType)
	offset += 4

	binary.LittleEndian.PutUint64(buf[offset:], h.BlockCount)
	offset += 8

	binary.LittleEndian.PutUint32(buf[offset:], h.BlockSizeTarget)
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:], h.CompressionType)
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:], h.EncodingType)
	offset += 4

	binary.LittleEndian.PutUint64(buf[offset:], h.CreationTime)
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:], h.BitmapOffset)
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:], h.BitmapSize)
	offset += 8

	// Write new fields for deleted IDs bitmap
	binary.LittleEndian.PutUint64(buf[offset:], h.DeletedBitmapOffset)
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:], h.DeletedBitmapSize)
	offset += 8

	// The rest of the buffer is already zeroed by make(), which serves as the reserved space

	return buf
}

// SerializeWithFooterOffset serializes the FileHeader and includes the footer offset after the header
func (h *FileHeader) SerializeWithFooterOffset(footerOffset uint64) []byte {
	// Serialize the header first
	headerBuf := h.Serialize()

	// Create a buffer that can hold the header plus the footer offset (8 bytes)
	buf := make([]byte, headerSize+8)

	// Copy the header into the buffer
	copy(buf[:headerSize], headerBuf)

	// Write the footer offset after the header
	binary.LittleEndian.PutUint64(buf[headerSize:], footerOffset)

	return buf
}

// Deserialize deserializes a byte slice into the FileHeader
func (h *FileHeader) Deserialize(buf []byte) error {
	if len(buf) < headerSize {
		return fmt.Errorf("buffer too small for FileHeader: expected at least %d bytes, got %d", headerSize, len(buf))
	}

	offset := 0

	// Read magic number
	h.Magic = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Read version
	h.Version = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	// Read column type
	h.ColumnType = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	// Read block count
	h.BlockCount = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Read block size target
	h.BlockSizeTarget = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	// Read compression type
	h.CompressionType = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	// Read encoding type
	h.EncodingType = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	// Read creation time
	h.CreationTime = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Read bitmap offset
	h.BitmapOffset = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Read bitmap size
	h.BitmapSize = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Read deleted bitmap offset
	h.DeletedBitmapOffset = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Read deleted bitmap size
	h.DeletedBitmapSize = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Try to read footer offset if available
	if len(buf) >= headerSize+8 {
		h.FooterOffset = binary.LittleEndian.Uint64(buf[headerSize:])
	}

	// Validation
	if h.Magic != MagicNumber {
		return fmt.Errorf("invalid magic number: 0x%X", h.Magic)
	}
	if h.Version != Version {
		return fmt.Errorf("unsupported version: %d", h.Version)
	}

	return nil
}

// BlockHeader represents the header of a block
type BlockHeader struct {
	MinID            uint64
	MaxID            uint64
	MinValue         uint64 // Stored as uint64, but represents int64
	MaxValue         uint64 // Stored as uint64, but represents int64
	Sum              uint64 // Stored as uint64, but represents int64
	Count            uint32
	EncodingType     uint32
	CompressionType  uint32
	UncompressedSize uint32
	CompressedSize   uint32
	Checksum         uint64
	// Reserved space - fills up to 64 bytes
}

// Serialize serializes the BlockHeader into a byte slice
func (bh *BlockHeader) Serialize() []byte {
	buf := make([]byte, blockHeaderSize)
	offset := 0

	// Write all fields directly into the buffer
	binary.LittleEndian.PutUint64(buf[offset:], bh.MinID)
	offset += uint64Size

	binary.LittleEndian.PutUint64(buf[offset:], bh.MaxID)
	offset += uint64Size

	binary.LittleEndian.PutUint64(buf[offset:], bh.MinValue)
	offset += uint64Size

	binary.LittleEndian.PutUint64(buf[offset:], bh.MaxValue)
	offset += uint64Size

	binary.LittleEndian.PutUint64(buf[offset:], bh.Sum)
	offset += uint64Size

	binary.LittleEndian.PutUint32(buf[offset:], bh.Count)
	offset += uint32Size

	binary.LittleEndian.PutUint32(buf[offset:], bh.EncodingType)
	offset += uint32Size

	binary.LittleEndian.PutUint32(buf[offset:], uint32(bh.CompressionType))
	offset += uint32Size

	binary.LittleEndian.PutUint32(buf[offset:], bh.UncompressedSize)
	offset += uint32Size

	binary.LittleEndian.PutUint32(buf[offset:], bh.CompressedSize)
	offset += uint32Size

	binary.LittleEndian.PutUint64(buf[offset:], bh.Checksum)
	offset += uint64Size

	// The rest of the buffer (32 bytes) is already zeroed by make(), which serves as the reserved space

	return buf
}

// Deserialize deserializes a byte slice into the BlockHeader
func (bh *BlockHeader) Deserialize(buf []byte) error {
	if len(buf) < blockHeaderSize {
		return fmt.Errorf("buffer too small for BlockHeader: expected %d bytes, got %d", blockHeaderSize, len(buf))
	}

	offset := 0

	// Read min ID
	bh.MinID = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read max ID
	bh.MaxID = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read min value
	bh.MinValue = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read max value
	bh.MaxValue = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read sum
	bh.Sum = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read count
	bh.Count = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read encoding type
	bh.EncodingType = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read compression type
	bh.CompressionType = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read uncompressed size
	bh.UncompressedSize = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read compressed size
	bh.CompressedSize = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read checksum
	bh.Checksum = binary.LittleEndian.Uint64(buf[offset:])

	return nil
}

// BlockLayout represents the layout of a block
type BlockLayout struct {
	IDSectionOffset    uint32
	IDSectionSize      uint32
	ValueSectionOffset uint32
	ValueSectionSize   uint32
}

// Serialize serializes the BlockLayout into a byte slice
func (bl *BlockLayout) Serialize() []byte {
	buf := make([]byte, 16)
	offset := 0

	// Write all fields directly into the buffer
	binary.LittleEndian.PutUint32(buf[offset:], bl.IDSectionOffset)
	offset += uint32Size

	binary.LittleEndian.PutUint32(buf[offset:], bl.IDSectionSize)
	offset += uint32Size

	binary.LittleEndian.PutUint32(buf[offset:], bl.ValueSectionOffset)
	offset += uint32Size

	binary.LittleEndian.PutUint32(buf[offset:], bl.ValueSectionSize)

	return buf
}

// Deserialize deserializes a byte slice into the BlockLayout
func (bl *BlockLayout) Deserialize(buf []byte) error {
	if len(buf) < 16 {
		return fmt.Errorf("buffer too small for BlockLayout: expected 16 bytes, got %d", len(buf))
	}

	offset := 0

	// Read ID section offset
	bl.IDSectionOffset = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read ID section size
	bl.IDSectionSize = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read value section offset
	bl.ValueSectionOffset = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read value section size
	bl.ValueSectionSize = binary.LittleEndian.Uint32(buf[offset:])

	return nil
}

// FooterEntry represents an entry in the footer
type FooterEntry struct {
	BlockOffset uint64
	BlockSize   uint32
	MinID       uint64
	MaxID       uint64
	MinValue    uint64 // Stored as uint64, but represents int64
	MaxValue    uint64 // Stored as uint64, but represents int64
	Sum         uint64 // Stored as uint64, but represents int64
	Count       uint32
}

// Serialize serializes the FooterEntry into a byte slice
func (fe *FooterEntry) Serialize() []byte {
	// Each footer entry is 56 bytes (8+4+8+8+8+8+8+4)
	buf := make([]byte, 56)
	offset := 0

	// Write all fields directly into the buffer
	binary.LittleEndian.PutUint64(buf[offset:], fe.BlockOffset)
	offset += uint64Size

	binary.LittleEndian.PutUint32(buf[offset:], fe.BlockSize)
	offset += uint32Size

	binary.LittleEndian.PutUint64(buf[offset:], fe.MinID)
	offset += uint64Size

	binary.LittleEndian.PutUint64(buf[offset:], fe.MaxID)
	offset += uint64Size

	binary.LittleEndian.PutUint64(buf[offset:], fe.MinValue)
	offset += uint64Size

	binary.LittleEndian.PutUint64(buf[offset:], fe.MaxValue)
	offset += uint64Size

	binary.LittleEndian.PutUint64(buf[offset:], fe.Sum)
	offset += uint64Size

	binary.LittleEndian.PutUint32(buf[offset:], fe.Count)

	return buf
}

// Deserialize deserializes a byte slice into the FooterEntry
func (fe *FooterEntry) Deserialize(buf []byte) error {
	if len(buf) < 56 {
		return fmt.Errorf("buffer too small for FooterEntry: expected 56 bytes, got %d", len(buf))
	}

	offset := 0

	// Read block offset
	fe.BlockOffset = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read block size
	fe.BlockSize = binary.LittleEndian.Uint32(buf[offset:])
	offset += uint32Size

	// Read min ID
	fe.MinID = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read max ID
	fe.MaxID = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read min value
	fe.MinValue = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read max value
	fe.MaxValue = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read sum
	fe.Sum = binary.LittleEndian.Uint64(buf[offset:])
	offset += uint64Size

	// Read count
	fe.Count = binary.LittleEndian.Uint32(buf[offset:])

	return nil
}

// FooterMetadata represents the metadata at the end of the footer
type FooterMetadata struct {
	FooterSize uint64
	Checksum   uint64
	Magic      uint64
}

// Serialize serializes the FooterMetadata into a byte slice
func (fm *FooterMetadata) Serialize() []byte {
	// Create a buffer for footer metadata (24 bytes: footerSize + checksum + magic)
	buf := make([]byte, 24)

	binary.LittleEndian.PutUint64(buf[0:], fm.FooterSize)
	binary.LittleEndian.PutUint64(buf[8:], fm.Checksum)
	binary.LittleEndian.PutUint64(buf[16:], fm.Magic)

	return buf
}

// Deserialize deserializes a byte slice into the FooterMetadata
func (fm *FooterMetadata) Deserialize(buf []byte) error {
	if len(buf) < 24 {
		return fmt.Errorf("buffer too small for FooterMetadata: expected 24 bytes, got %d", len(buf))
	}

	fm.FooterSize = binary.LittleEndian.Uint64(buf[0:])
	fm.Checksum = binary.LittleEndian.Uint64(buf[8:])
	fm.Magic = binary.LittleEndian.Uint64(buf[16:])

	// Validation
	if fm.Magic != MagicNumber {
		return fmt.Errorf("invalid footer magic number: 0x%X", fm.Magic)
	}

	return nil
}

// AggregateResult represents the result of an aggregation
type AggregateResult struct {
	Count int
	Min   int64
	Max   int64
	Sum   int64
	Avg   float64
}

// NewFileHeader creates a new file header with default values
func NewFileHeader(blockCount uint64, blockSizeTarget uint32, encodingType uint32) FileHeader {
	return FileHeader{
		Magic:               MagicNumber,
		Version:             Version,
		ColumnType:          DataTypeInt64,
		BlockCount:          blockCount,
		BlockSizeTarget:     blockSizeTarget,
		CompressionType:     CompressionNone,
		EncodingType:        encodingType,
		CreationTime:        uint64(time.Now().Unix()),
		BitmapOffset:        0, // Will be updated when writing the bitmap
		BitmapSize:          0, // Will be updated when writing the bitmap
		DeletedBitmapOffset: 0, // Will be updated when writing the deleted IDs bitmap
		DeletedBitmapSize:   0, // Will be updated when writing the deleted IDs bitmap
		FooterOffset:        0, // Will be updated when writing the footer
	}
}

// NewBlockHeader creates a new block header with specified values
func NewBlockHeader(
	minID, maxID uint64,
	minValue, maxValue, sum int64,
	count uint32,
	encodingType uint32,
) BlockHeader {
	// Convert int64 values to uint64 for storage
	minValueU64 := int64ToUint64(minValue)
	maxValueU64 := int64ToUint64(maxValue)
	sumU64 := int64ToUint64(sum)

	return BlockHeader{
		MinID:            minID,
		MaxID:            maxID,
		MinValue:         minValueU64,
		MaxValue:         maxValueU64,
		Sum:              sumU64,
		Count:            count,
		EncodingType:     encodingType,
		CompressionType:  CompressionNone,
		UncompressedSize: 0, // Not implemented yet
		CompressedSize:   0, // Not implemented yet
		Checksum:         0, // Not implemented yet
	}
}

// NewFooterEntry creates a new footer entry
func NewFooterEntry(
	blockOffset uint64,
	blockSize uint32,
	minID, maxID uint64,
	minValue, maxValue, sum int64,
	count uint32,
) FooterEntry {
	// Convert int64 values to uint64 for storage
	minValueU64 := int64ToUint64(minValue)
	maxValueU64 := int64ToUint64(maxValue)
	sumU64 := int64ToUint64(sum)

	return FooterEntry{
		BlockOffset: blockOffset,
		BlockSize:   blockSize,
		MinID:       minID,
		MaxID:       maxID,
		MinValue:    minValueU64,
		MaxValue:    maxValueU64,
		Sum:         sumU64,
		Count:       count,
	}
}
