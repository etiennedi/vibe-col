// Package col implements a column-based storage format for id-value pairs.
package col

import (
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
	Magic           uint64
	Version         uint32
	ColumnType      uint32
	BlockCount      uint64
	BlockSizeTarget uint32
	CompressionType uint32
	EncodingType    uint32
	CreationTime    uint64
	// Reserved space - fills up to 64 bytes
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

// BlockLayout represents the layout of a block
type BlockLayout struct {
	IDSectionOffset    uint32
	IDSectionSize      uint32
	ValueSectionOffset uint32
	ValueSectionSize   uint32
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

// FooterMetadata represents the metadata at the end of the footer
type FooterMetadata struct {
	FooterSize uint64
	Checksum   uint64
	Magic      uint64
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
		Magic:           MagicNumber,
		Version:         Version,
		ColumnType:      DataTypeInt64,
		BlockCount:      blockCount,
		BlockSizeTarget: blockSizeTarget,
		CompressionType: CompressionNone,
		EncodingType:    encodingType,
		CreationTime:    uint64(time.Now().Unix()),
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
