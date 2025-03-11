package col

import (
	"encoding/binary"
	"fmt"
)

// readHeader reads the file header from the file
func (r *Reader) readHeader() error {
	// Read the entire header in one call (64 bytes)
	headerBuf, err := r.readBytesAt(0, headerSize)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Extract fields from the buffer
	offset := 0

	// Read magic number
	r.header.Magic = readBufferedUint64(headerBuf, offset)
	offset += 8

	// Read version
	r.header.Version = readBufferedUint32(headerBuf, offset)
	offset += 4

	// Read column type
	r.header.ColumnType = readBufferedUint32(headerBuf, offset)
	offset += 4

	// Read block count
	r.header.BlockCount = readBufferedUint64(headerBuf, offset)
	offset += 8

	// Read block size target
	r.header.BlockSizeTarget = readBufferedUint32(headerBuf, offset)
	offset += 4

	// Read compression type
	r.header.CompressionType = readBufferedUint32(headerBuf, offset)
	offset += 4

	// Read encoding type
	r.header.EncodingType = readBufferedUint32(headerBuf, offset)
	offset += 4

	// Read creation time
	r.header.CreationTime = readBufferedUint64(headerBuf, offset)

	// Validate header
	if r.header.Magic != MagicNumber {
		return fmt.Errorf("invalid magic number: 0x%X", r.header.Magic)
	}
	if r.header.Version != Version {
		return fmt.Errorf("unsupported version: %d", r.header.Version)
	}

	return nil
}

// readFooter reads the footer from the file
func (r *Reader) readFooter() error {
	// The last 24 bytes of the file are the footer metadata
	if r.fileSize < 24 {
		return fmt.Errorf("file too small for footer: %d bytes", r.fileSize)
	}

	// Read footer metadata from the end of the file in one call
	footerMetaOffset := r.fileSize - 24
	footerMetaBuf, err := r.readBytesAt(footerMetaOffset, 24)
	if err != nil {
		return fmt.Errorf("failed to read footer metadata: %w", err)
	}

	// Extract fields from the buffer
	r.footerMeta.FooterSize = readBufferedUint64(footerMetaBuf, 0)
	r.footerMeta.Checksum = readBufferedUint64(footerMetaBuf, 8)
	r.footerMeta.Magic = readBufferedUint64(footerMetaBuf, 16)

	// Validate footer metadata
	if r.footerMeta.Magic != MagicNumber {
		return fmt.Errorf("invalid footer magic number: 0x%X", r.footerMeta.Magic)
	}

	// Read the rest of the footer
	footerStart := footerMetaOffset - int64(r.footerMeta.FooterSize)
	if footerStart < 64 { // Footer cannot start before the header
		return fmt.Errorf("invalid footer size: %d", r.footerMeta.FooterSize)
	}

	// Read block index count (first 4 bytes of footer)
	blockIndexCountBuf, err := r.readBytesAt(footerStart, 4)
	if err != nil {
		return fmt.Errorf("failed to read block index count: %w", err)
	}
	blockIndexCount := binary.LittleEndian.Uint32(blockIndexCountBuf)

	// Check if block count matches with header
	if uint64(blockIndexCount) != r.header.BlockCount {
		// Use the higher value to ensure we don't miss data
		if uint64(blockIndexCount) > r.header.BlockCount {
			r.header.BlockCount = uint64(blockIndexCount)
		}
	}

	// Calculate the size of the block index
	// Each entry is 56 bytes (8+4+8+8+8+8+8+4)
	blockIndexSize := int(blockIndexCount) * 56

	// Read the entire block index in one call
	blockIndexBuf, err := r.readBytesAt(footerStart+4, blockIndexSize)
	if err != nil {
		return fmt.Errorf("failed to read block index: %w", err)
	}

	// Parse the block index entries
	r.blockIndex = make([]FooterEntry, blockIndexCount)
	for i := uint32(0); i < blockIndexCount; i++ {
		entryOffset := i * 56

		r.blockIndex[i] = FooterEntry{
			BlockOffset: readBufferedUint64(blockIndexBuf, int(entryOffset)),
			BlockSize:   readBufferedUint32(blockIndexBuf, int(entryOffset+8)),
			MinID:       readBufferedUint64(blockIndexBuf, int(entryOffset+12)),
			MaxID:       readBufferedUint64(blockIndexBuf, int(entryOffset+20)),
			MinValue:    readBufferedUint64(blockIndexBuf, int(entryOffset+28)),
			MaxValue:    readBufferedUint64(blockIndexBuf, int(entryOffset+36)),
			Sum:         readBufferedUint64(blockIndexBuf, int(entryOffset+44)),
			Count:       readBufferedUint32(blockIndexBuf, int(entryOffset+52)),
		}
	}

	return nil
}
