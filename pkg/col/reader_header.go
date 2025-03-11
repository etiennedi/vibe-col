package col

import (
	"encoding/binary"
	"fmt"
)

// readHeader reads the file header from the file
func (r *Reader) readHeader() error {
	// Read header fields using ReadAt
	var err error
	var offset int64 = 0

	// Read magic number
	r.header.Magic, err = r.readUint64At(offset)
	if err != nil {
		return fmt.Errorf("failed to read magic number: %w", err)
	}
	offset += 8

	// Read version
	r.header.Version, err = r.readUint32At(offset)
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	offset += 4

	// Read column type
	r.header.ColumnType, err = r.readUint32At(offset)
	if err != nil {
		return fmt.Errorf("failed to read column type: %w", err)
	}
	offset += 4

	// Read block count
	r.header.BlockCount, err = r.readUint64At(offset)
	if err != nil {
		return fmt.Errorf("failed to read block count: %w", err)
	}
	offset += 8

	// Read block size target
	r.header.BlockSizeTarget, err = r.readUint32At(offset)
	if err != nil {
		return fmt.Errorf("failed to read block size target: %w", err)
	}
	offset += 4

	// Read compression type
	r.header.CompressionType, err = r.readUint32At(offset)
	if err != nil {
		return fmt.Errorf("failed to read compression type: %w", err)
	}
	offset += 4

	// Read encoding type
	r.header.EncodingType, err = r.readUint32At(offset)
	if err != nil {
		return fmt.Errorf("failed to read encoding type: %w", err)
	}
	offset += 4

	// Read creation time
	r.header.CreationTime, err = r.readUint64At(offset)
	if err != nil {
		return fmt.Errorf("failed to read creation time: %w", err)
	}

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

	// Read footer metadata from the end of the file
	footerMetaOffset := r.fileSize - 24

	// Read footer size
	var err error
	r.footerMeta.FooterSize, err = r.readUint64At(footerMetaOffset)
	if err != nil {
		return fmt.Errorf("failed to read footer size: %w", err)
	}

	// Read checksum
	r.footerMeta.Checksum, err = r.readUint64At(footerMetaOffset + 8)
	if err != nil {
		return fmt.Errorf("failed to read checksum: %w", err)
	}

	// Read footer magic
	r.footerMeta.Magic, err = r.readUint64At(footerMetaOffset + 16)
	if err != nil {
		return fmt.Errorf("failed to read footer magic: %w", err)
	}

	// Validate footer metadata
	if r.footerMeta.Magic != MagicNumber {
		return fmt.Errorf("invalid footer magic number: 0x%X", r.footerMeta.Magic)
	}

	// Read the rest of the footer
	footerStart := footerMetaOffset - int64(r.footerMeta.FooterSize)
	if footerStart < 64 { // Footer cannot start before the header
		return fmt.Errorf("invalid footer size: %d", r.footerMeta.FooterSize)
	}

	// Read block index count
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

	// Read block index
	r.blockIndex = make([]FooterEntry, blockIndexCount)
	offset := footerStart + 4 // Start after the block index count

	for i := uint32(0); i < blockIndexCount; i++ {
		// Read each field of the footer entry
		blockOffset, err := r.readUint64At(offset)
		if err != nil {
			return fmt.Errorf("failed to read block offset: %w", err)
		}
		offset += 8

		blockSize, err := r.readUint32At(offset)
		if err != nil {
			return fmt.Errorf("failed to read block size: %w", err)
		}
		offset += 4

		minID, err := r.readUint64At(offset)
		if err != nil {
			return fmt.Errorf("failed to read min ID: %w", err)
		}
		offset += 8

		maxID, err := r.readUint64At(offset)
		if err != nil {
			return fmt.Errorf("failed to read max ID: %w", err)
		}
		offset += 8

		minValue, err := r.readUint64At(offset)
		if err != nil {
			return fmt.Errorf("failed to read min value: %w", err)
		}
		offset += 8

		maxValue, err := r.readUint64At(offset)
		if err != nil {
			return fmt.Errorf("failed to read max value: %w", err)
		}
		offset += 8

		sum, err := r.readUint64At(offset)
		if err != nil {
			return fmt.Errorf("failed to read sum: %w", err)
		}
		offset += 8

		count, err := r.readUint32At(offset)
		if err != nil {
			return fmt.Errorf("failed to read count: %w", err)
		}
		offset += 4

		r.blockIndex[i] = FooterEntry{
			BlockOffset: blockOffset,
			BlockSize:   blockSize,
			MinID:       minID,
			MaxID:       maxID,
			MinValue:    minValue,
			MaxValue:    maxValue,
			Sum:         sum,
			Count:       count,
		}
	}

	return nil
}
