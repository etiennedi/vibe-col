package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Reader reads a column file
type Reader struct {
	file       *os.File
	header     FileHeader
	footerMeta FooterMetadata
	blockIndex []FooterEntry
}

// NewReader creates a new column file reader
func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	reader := &Reader{
		file: file,
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

// readHeader reads the file header from the file
func (r *Reader) readHeader() error {
	// Seek to the beginning of the file
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start: %w", err)
	}

	// Read header fields
	if err := binary.Read(r.file, binary.LittleEndian, &r.header.Magic); err != nil {
		return fmt.Errorf("failed to read magic number: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.header.Version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.header.ColumnType); err != nil {
		return fmt.Errorf("failed to read column type: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.header.BlockCount); err != nil {
		return fmt.Errorf("failed to read block count: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.header.BlockSizeTarget); err != nil {
		return fmt.Errorf("failed to read block size target: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.header.CompressionType); err != nil {
		return fmt.Errorf("failed to read compression type: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.header.EncodingType); err != nil {
		return fmt.Errorf("failed to read encoding type: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.header.CreationTime); err != nil {
		return fmt.Errorf("failed to read creation time: %w", err)
	}

	// Skip reserved space
	reservedSize := 64 - 8 - 4 - 4 - 8 - 4 - 4 - 4 - 8
	if _, err := r.file.Seek(int64(reservedSize), io.SeekCurrent); err != nil {
		return fmt.Errorf("failed to skip reserved space: %w", err)
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
	// Get file size
	fileInfo, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// The last 24 bytes of the file are the footer metadata
	if fileSize < 24 {
		return fmt.Errorf("file too small for footer: %d bytes", fileSize)
	}

	// Read footer metadata
	if _, err := r.file.Seek(fileSize-24, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to footer metadata: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.footerMeta.FooterSize); err != nil {
		return fmt.Errorf("failed to read footer size: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.footerMeta.Checksum); err != nil {
		return fmt.Errorf("failed to read checksum: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.footerMeta.Magic); err != nil {
		return fmt.Errorf("failed to read footer magic: %w", err)
	}

	// Validate footer metadata
	if r.footerMeta.Magic != MagicNumber {
		return fmt.Errorf("invalid footer magic number: 0x%X", r.footerMeta.Magic)
	}

	// Read the rest of the footer
	footerStart := fileSize - 24 - int64(r.footerMeta.FooterSize)
	if footerStart < 64 { // Footer cannot start before the header
		return fmt.Errorf("invalid footer size: %d", r.footerMeta.FooterSize)
	}

	// Read block index count
	if _, err := r.file.Seek(footerStart, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to footer: %w", err)
	}
	var blockIndexCount uint32
	if err := binary.Read(r.file, binary.LittleEndian, &blockIndexCount); err != nil {
		return fmt.Errorf("failed to read block index count: %w", err)
	}

	// Check if block count matches with header
	if uint64(blockIndexCount) != r.header.BlockCount {
		// This is a warning, but we'll use the header value
		// fmt.Printf("Warning: block count mismatch: header=%d, footer=%d\n", r.header.BlockCount, blockIndexCount)
		
		// Use the higher value to ensure we don't miss data
		if uint64(blockIndexCount) > r.header.BlockCount {
			r.header.BlockCount = uint64(blockIndexCount)
		}
	}

	// Read block index
	r.blockIndex = make([]FooterEntry, blockIndexCount)
	for i := uint32(0); i < blockIndexCount; i++ {
		var entry FooterEntry
		if err := binary.Read(r.file, binary.LittleEndian, &entry.BlockOffset); err != nil {
			return fmt.Errorf("failed to read block offset: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &entry.BlockSize); err != nil {
			return fmt.Errorf("failed to read block size: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &entry.MinID); err != nil {
			return fmt.Errorf("failed to read min ID: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &entry.MaxID); err != nil {
			return fmt.Errorf("failed to read max ID: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &entry.MinValue); err != nil {
			return fmt.Errorf("failed to read min value: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &entry.MaxValue); err != nil {
			return fmt.Errorf("failed to read max value: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &entry.Sum); err != nil {
			return fmt.Errorf("failed to read sum: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &entry.Count); err != nil {
			return fmt.Errorf("failed to read count: %w", err)
		}
		r.blockIndex[i] = entry
	}

	return nil
}

// readBlock reads a block from the file
func (r *Reader) readBlock(blockIdx uint64) ([]uint64, []int64, BlockHeader, uint32, error) {
	if blockIdx >= r.header.BlockCount {
		return nil, nil, BlockHeader{}, 0, fmt.Errorf("block index out of range: %d", blockIdx)
	}

	// Get block information from footer index
	var blockOffset uint64
	var blockSize uint32
	var count uint32

	// If we have block index information, use it
	if blockIdx < uint64(len(r.blockIndex)) {
		blockOffset = r.blockIndex[blockIdx].BlockOffset
		blockSize = r.blockIndex[blockIdx].BlockSize
		count = r.blockIndex[blockIdx].Count
	} else {
		// Otherwise, calculate based on the file layout
		if blockIdx == 0 {
			blockOffset = 64 // First block starts right after the header
		} else {
			// The calculation would be more complex for multiple blocks
			return nil, nil, BlockHeader{}, 0, fmt.Errorf("cannot calculate offset for block: %d", blockIdx)
		}
		
		// Conservative size estimation - we don't know the size from the index
		blockSize = 64 + 16 + (10 * 8 * 2) // header + layout + estimated data (10 pairs)
	}

	// Seek to the block
	if _, err := r.file.Seek(int64(blockOffset), io.SeekStart); err != nil {
		return nil, nil, BlockHeader{}, 0, fmt.Errorf("failed to seek to block: %w", err)
	}

	// Read block header
	var header BlockHeader
	headerBuf := make([]byte, 64)
	if _, err := io.ReadFull(r.file, headerBuf); err != nil {
		return nil, nil, BlockHeader{}, 0, fmt.Errorf("failed to read block header: %w", err)
	}

	// Parse header values
	header.MinID = binary.LittleEndian.Uint64(headerBuf[0:8])
	header.MaxID = binary.LittleEndian.Uint64(headerBuf[8:16])
	header.MinValue = binary.LittleEndian.Uint64(headerBuf[16:24])
	header.MaxValue = binary.LittleEndian.Uint64(headerBuf[24:32])
	header.Sum = binary.LittleEndian.Uint64(headerBuf[32:40])
	header.Count = binary.LittleEndian.Uint32(headerBuf[40:44])
	header.EncodingType = binary.LittleEndian.Uint32(headerBuf[44:48])
	header.CompressionType = binary.LittleEndian.Uint32(headerBuf[48:52])
	header.UncompressedSize = binary.LittleEndian.Uint32(headerBuf[52:56])
	if len(headerBuf) >= 64 {
		header.CompressedSize = binary.LittleEndian.Uint32(headerBuf[56:60])
		// The checksum might start at offset 60, but ensure we don't exceed buffer size
		if len(headerBuf) >= 68 {
			header.Checksum = binary.LittleEndian.Uint64(headerBuf[60:68])
		}
	}

	// Try to infer the correct count if there's a mismatch
	blockHeaderCount := header.Count
	blockFooterCount := count

	// Use block header count if valid, otherwise use footer count
	var blockCount uint32
	if blockHeaderCount > 0 && blockHeaderCount < 1000000 { // Sanity check
		blockCount = blockHeaderCount
	} else if blockFooterCount > 0 && blockFooterCount < 1000000 {
		blockCount = blockFooterCount
	} else if len(r.blockIndex) > 0 && blockIdx < uint64(len(r.blockIndex)) {
		// Use footer info if possible
		blockCount = r.blockIndex[blockIdx].Count
	} else {
		// Estimate from the block size
		// Block size = header (64) + layout (16) + data (count * 16)
		// So count = (blockSize - 64 - 16) / 16
		estimatedCount := (blockSize - 64 - 16) / 16
		blockCount = estimatedCount
	}

	// Read block layout
	var layout BlockLayout
	if err := binary.Read(r.file, binary.LittleEndian, &layout.IDSectionOffset); err != nil {
		return nil, nil, header, 0, fmt.Errorf("failed to read ID section offset: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &layout.IDSectionSize); err != nil {
		return nil, nil, header, 0, fmt.Errorf("failed to read ID section size: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &layout.ValueSectionOffset); err != nil {
		return nil, nil, header, 0, fmt.Errorf("failed to read value section offset: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &layout.ValueSectionSize); err != nil {
		return nil, nil, header, 0, fmt.Errorf("failed to read value section size: %w", err)
	}

	// Calculate count from layout
	layoutCount := layout.IDSectionSize / 8 // 8 bytes per ID
	
	// Verify count is consistent
	if layoutCount > 0 && (blockCount != layoutCount) {
		// This is a warning, but we'll use the layout value since it's likely more accurate
		// fmt.Printf("Warning: count mismatch: header=%d, layout=%d\n", blockCount, layoutCount)
		blockCount = layoutCount
	}

	// Compute section positions
	idSectionStart := blockOffset + 64 + 16 + uint64(layout.IDSectionOffset)
	valueSectionStart := blockOffset + 64 + 16 + uint64(layout.ValueSectionOffset)

	// Read ID data
	if _, err := r.file.Seek(int64(idSectionStart), io.SeekStart); err != nil {
		return nil, nil, header, 0, fmt.Errorf("failed to seek to ID section: %w", err)
	}
	
	ids := make([]uint64, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		if err := binary.Read(r.file, binary.LittleEndian, &ids[i]); err != nil {
			return nil, nil, header, 0, fmt.Errorf("failed to read ID at index %d: %w", i, err)
		}
	}

	// Read value data
	if _, err := r.file.Seek(int64(valueSectionStart), io.SeekStart); err != nil {
		return nil, nil, header, 0, fmt.Errorf("failed to seek to value section: %w", err)
	}
	
	values := make([]int64, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		if err := binary.Read(r.file, binary.LittleEndian, &values[i]); err != nil {
			return nil, nil, header, 0, fmt.Errorf("failed to read value at index %d: %w", i, err)
		}
	}

	// Apply decoding if needed
	encodingType := header.EncodingType
	switch encodingType {
	case EncodingRaw:
		// No decoding needed
	case EncodingDeltaID:
		ids = deltaDecode(ids)
	case EncodingDeltaValue:
		values = deltaDecodeInt64(values)
	case EncodingDeltaBoth:
		ids = deltaDecode(ids)
		values = deltaDecodeInt64(values)
	default:
		// Unknown encoding - return raw data
		encodingType = EncodingRaw
	}

	return ids, values, header, encodingType, nil
}

// GetPairs returns the ID-value pairs from a block
func (r *Reader) GetPairs(blockIdx uint64) ([]uint64, []int64, error) {
	ids, values, _, _, err := r.readBlock(blockIdx)
	return ids, values, err
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

// BlockCount returns the number of blocks in the file
func (r *Reader) BlockCount() uint64 {
	return r.header.BlockCount
}

// Aggregate aggregates all blocks and returns the result
func (r *Reader) Aggregate() AggregateResult {
	// If we have a footer with block statistics, use it for efficient aggregation
	if len(r.blockIndex) > 0 {
		var count int
		var min int64 = 9223372036854775807 // Max int64
		var max int64 = -9223372036854775808 // Min int64
		var sum int64 = 0

		for _, entry := range r.blockIndex {
			// Convert stored uint64 values back to int64
			minValue := uint64ToInt64(entry.MinValue)
			maxValue := uint64ToInt64(entry.MaxValue)
			blockSum := uint64ToInt64(entry.Sum)

			// Update aggregates
			count += int(entry.Count)
			if minValue < min {
				min = minValue
			}
			if maxValue > max {
				max = maxValue
			}
			sum += blockSum
		}

		// Calculate average
		var avg float64 = 0
		if count > 0 {
			avg = float64(sum) / float64(count)
		}

		return AggregateResult{
			Count: count,
			Min:   min,
			Max:   max,
			Sum:   sum,
			Avg:   avg,
		}
	}

	// Fallback: read and aggregate all blocks
	var count int
	var min int64 = 9223372036854775807 // Max int64
	var max int64 = -9223372036854775808 // Min int64
	var sum int64 = 0

	for i := uint64(0); i < r.header.BlockCount; i++ {
		_, values, err := r.GetPairs(i)
		if err != nil {
			continue
		}

		for _, value := range values {
			count++
			if value < min {
				min = value
			}
			if value > max {
				max = value
			}
			sum += value
		}
	}

	// Calculate average
	var avg float64 = 0
	if count > 0 {
		avg = float64(sum) / float64(count)
	}

	return AggregateResult{
		Count: count,
		Min:   min,
		Max:   max,
		Sum:   sum,
		Avg:   avg,
	}
}

// Close closes the file
func (r *Reader) Close() error {
	return r.file.Close()
}

// DebugInfo returns debug information about the reader
func (r *Reader) DebugInfo() string {
	info := fmt.Sprintf("File header: Magic=0x%X, Version=%d, BlockCount=%d\n", 
		r.header.Magic, r.header.Version, r.header.BlockCount)
	
	info += fmt.Sprintf("Encoding: Type=%d, Compression=%d\n",
		r.header.EncodingType, r.header.CompressionType)
	
	info += fmt.Sprintf("Footer: Size=%d, Magic=0x%X\n",
		r.footerMeta.FooterSize, r.footerMeta.Magic)
	
	if len(r.blockIndex) > 0 {
		info += fmt.Sprintf("Block index entries: %d\n", len(r.blockIndex))
		for i, entry := range r.blockIndex {
			info += fmt.Sprintf("  Block %d: Offset=%d, Size=%d, Count=%d\n",
				i, entry.BlockOffset, entry.BlockSize, entry.Count)
			info += fmt.Sprintf("    ID range: %d-%d\n", entry.MinID, entry.MaxID)
			info += fmt.Sprintf("    Value range: %d-%d, Sum=%d\n", 
				uint64ToInt64(entry.MinValue), uint64ToInt64(entry.MaxValue), uint64ToInt64(entry.Sum))
		}
	}
	
	return info
}