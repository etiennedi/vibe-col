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

	// Try to read an additional 8 bytes for the footer offset if available
	var extendedHeaderBuf []byte
	if r.fileSize >= int64(headerSize+8) {
		extendedHeaderBuf, err = r.readBytesAt(0, headerSize+8)
		if err == nil {
			// Use the extended buffer if available
			headerBuf = extendedHeaderBuf
		}
	}

	// Deserialize the header
	if err := r.header.Deserialize(headerBuf); err != nil {
		return fmt.Errorf("failed to deserialize header: %w", err)
	}

	return nil
}

// readFooter reads the footer from the file
func (r *Reader) readFooter() error {
	// Use footer offset from header if available, otherwise calculate from file size
	var footerStart int64
	if r.header.FooterOffset > 0 {
		footerStart = int64(r.header.FooterOffset)
	} else {
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

		// Deserialize footer metadata
		if err := r.footerMeta.Deserialize(footerMetaBuf); err != nil {
			return fmt.Errorf("failed to deserialize footer metadata: %w", err)
		}

		// Calculate the footer start position
		footerStart = footerMetaOffset - int64(r.footerMeta.FooterSize)
		if footerStart < 64 { // Footer cannot start before the header
			return fmt.Errorf("invalid footer size: %d", r.footerMeta.FooterSize)
		}
	}

	// Read footer metadata from the end of the file if not already read
	if r.header.FooterOffset > 0 {
		// We need to calculate where the footer metadata starts
		// Get file size if not already done
		if r.fileSize == 0 {
			fileInfo, err := r.file.Stat()
			if err != nil {
				return fmt.Errorf("failed to get file info: %w", err)
			}
			r.fileSize = fileInfo.Size()
		}

		footerMetaOffset := r.fileSize - 24
		footerMetaBuf, err := r.readBytesAt(footerMetaOffset, 24)
		if err != nil {
			return fmt.Errorf("failed to read footer metadata: %w", err)
		}

		// Deserialize footer metadata
		if err := r.footerMeta.Deserialize(footerMetaBuf); err != nil {
			return fmt.Errorf("failed to deserialize footer metadata: %w", err)
		}
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
			BlockOffset: readBufferedUint64(&blockIndexBuf, int(entryOffset)),
			BlockSize:   readBufferedUint32(&blockIndexBuf, int(entryOffset+8)),
			MinID:       readBufferedUint64(&blockIndexBuf, int(entryOffset+12)),
			MaxID:       readBufferedUint64(&blockIndexBuf, int(entryOffset+20)),
			MinValue:    readBufferedUint64(&blockIndexBuf, int(entryOffset+28)),
			MaxValue:    readBufferedUint64(&blockIndexBuf, int(entryOffset+36)),
			Sum:         readBufferedUint64(&blockIndexBuf, int(entryOffset+44)),
			Count:       readBufferedUint32(&blockIndexBuf, int(entryOffset+52)),
		}
	}

	return nil
}
