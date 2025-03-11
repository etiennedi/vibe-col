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
func (r *Reader) readBlock(blockIndex int) ([]uint64, []int64, error) {
	// Validate block index
	if blockIndex < 0 || blockIndex >= len(r.blockIndex) {
		return nil, nil, fmt.Errorf("invalid block index: %d", blockIndex)
	}

	// Get block information from the index
	blockOffset := int64(r.blockIndex[blockIndex].BlockOffset)
	count := int(r.blockIndex[blockIndex].Count)

	// Seek to the block start
	if _, err := r.file.Seek(blockOffset, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("failed to seek to block start: %w", err)
	}

	// Skip the block header (64 bytes)
	if _, err := r.file.Seek(64, io.SeekCurrent); err != nil {
		return nil, nil, fmt.Errorf("failed to skip block header: %w", err)
	}

	// Read the block layout (16 bytes)
	var layout [4]uint32
	if err := binary.Read(r.file, binary.LittleEndian, &layout); err != nil {
		return nil, nil, fmt.Errorf("failed to read block layout: %w", err)
	}

	// Get the layout values - we only need sizes, not offsets
	idSectionSize := layout[1]
	valueSectionSize := layout[3]

	// Handle the case where section sizes are not specified in the layout
	// This happens with some encoding types that use data section headers
	if idSectionSize == 0 || valueSectionSize == 0 {
		// Read the data section header
		// Format for VarInt encoding:
		// - ID section size (4 bytes)
		// - Value section size (4 bytes)
		// - Count (4 bytes)
		var dataSectionHeader [3]uint32
		if err := binary.Read(r.file, binary.LittleEndian, &dataSectionHeader); err != nil {
			return nil, nil, fmt.Errorf("failed to read data section header: %w", err)
		}

		// Uncomment for debugging if needed
		// fmt.Printf("Data section header: idSectionSize=%d, valueSectionSize=%d, count=%d\n",
		//	dataSectionHeader[0], dataSectionHeader[1], dataSectionHeader[2])

		// Update the section sizes
		if idSectionSize == 0 {
			idSectionSize = dataSectionHeader[0]
		}
		if valueSectionSize == 0 {
			valueSectionSize = dataSectionHeader[1]
		}

		// If the count in the header is greater than the section size, it might indicate
		// that the value section size is larger than reported (for variable-length encoding)
		if dataSectionHeader[2] > dataSectionHeader[1] && r.IsVarIntEncoded() {
			// Get the file size and calculate available bytes for value section
			fileInfo, err := r.file.Stat()
			if err == nil {
				// Calculate the position after reading the ID section
				currentPos, err := r.file.Seek(0, io.SeekCurrent)
				if err == nil {
					idSectionEnd := currentPos + int64(idSectionSize)
					// The available bytes are from the end of the ID section to the footer
					// We need to leave at least 24 bytes for the footer metadata
					availableBytes := fileInfo.Size() - idSectionEnd - 24

					// If the footer has been read, we can be more precise
					if len(r.blockIndex) > 0 && blockIndex < len(r.blockIndex) {
						blockEnd := int64(r.blockIndex[blockIndex].BlockOffset) + int64(r.blockIndex[blockIndex].BlockSize)
						availableBytes = blockEnd - idSectionEnd
					}

					// If there are more available bytes than the reported value section size,
					// adjust the value section size accordingly, but be cautious
					if availableBytes > int64(valueSectionSize) {
						// Get the block size from the block header
						blockSize := r.blockIndex[blockIndex].BlockSize
						blockOffset := r.blockIndex[blockIndex].BlockOffset
						// Calculate the expected value section size
						expectedValueSize := blockSize - uint32(currentPos-int64(blockOffset)) - idSectionSize
						if expectedValueSize > valueSectionSize && expectedValueSize < 10000000 { // Sanity check
							fmt.Printf("Data section sizes: reported=%d, using=%d\n",
								valueSectionSize, expectedValueSize)
							valueSectionSize = expectedValueSize
						}
					}
				}
			}
		}
	}

	// Ensure we have valid section sizes
	if idSectionSize == 0 {
		return nil, nil, fmt.Errorf("ID section size is 0")
	}
	if valueSectionSize == 0 {
		return nil, nil, fmt.Errorf("value section size is 0")
	}

	// Read ID section
	idBytes := make([]byte, idSectionSize)
	if _, err := io.ReadFull(r.file, idBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to read ID section: %w", err)
	}

	// Read value section
	valueBytes := make([]byte, valueSectionSize)
	if _, err := io.ReadFull(r.file, valueBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to read value section: %w", err)
	}

	// Decode IDs based on encoding type
	var ids []uint64
	var err error

	isVarInt := r.IsVarIntEncoded()

	if isVarInt {
		// For variable-length encoding, use the decodeUVarInts function
		ids, err = decodeUVarInts(idBytes, count)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode varint IDs: %w", err)
		}
	} else {
		// For fixed-length encoding, read 8 bytes per ID
		ids = make([]uint64, count)
		for i := 0; i < count; i++ {
			if i*8+8 > len(idBytes) {
				return nil, nil, fmt.Errorf("ID section too small: %d bytes for %d IDs", len(idBytes), count)
			}
			ids[i] = binary.LittleEndian.Uint64(idBytes[i*8 : i*8+8])
		}
	}

	// Decode values based on encoding type
	values := make([]int64, count)

	if isVarInt {
		// Variable-length encoding for values
		// We need to decode signed varints
		offset := 0
		for i := 0; i < count && offset < len(valueBytes); i++ {
			if offset >= len(valueBytes) {
				return nil, nil, fmt.Errorf("value section too small")
			}

			var bytesRead int
			values[i], bytesRead = decodeSignedVarInt(valueBytes[offset:])
			if bytesRead <= 0 {
				return nil, nil, fmt.Errorf("failed to decode signed varint at index %d", i)
			}
			offset += bytesRead

			// Debug output for troubleshooting large values
			if i >= count-10 && count > 50 {
				// Print the last few values for debugging
				fmt.Printf("Decoded value at index %d: %d, bytesRead: %d\n", i, values[i], bytesRead)
			}
		}
	} else {
		// Fixed-length encoding for values
		for i := 0; i < count; i++ {
			if i*8+8 > len(valueBytes) {
				return nil, nil, fmt.Errorf("value section too small: %d bytes for %d values", len(valueBytes), count)
			}
			values[i] = int64(binary.LittleEndian.Uint64(valueBytes[i*8 : i*8+8]))
		}
	}

	// Apply delta decoding if needed
	encodingType := r.header.EncodingType

	if encodingType == EncodingDeltaBoth || encodingType == EncodingVarIntBoth {
		// Delta decode both IDs and values
		for i := 1; i < count; i++ {
			ids[i] += ids[i-1]
			values[i] += values[i-1]
		}
	} else if encodingType == EncodingDeltaID || encodingType == EncodingVarIntID {
		// Delta decode only IDs
		for i := 1; i < count; i++ {
			ids[i] += ids[i-1]
		}
	} else if encodingType == EncodingDeltaValue || encodingType == EncodingVarIntValue {
		// Delta decode only values
		for i := 1; i < count; i++ {
			values[i] += values[i-1]
		}
	}

	return ids, values, nil
}

// Helper function to decode exactly 'count' UVarInts from buf
func decodeUVarInts(buf []byte, count int) ([]uint64, error) {
	vals := make([]uint64, 0, count)
	offset := 0

	// Uncomment for debugging if needed
	// fmt.Printf("Decoding %d UVarInts from buffer of size %d bytes\n", count, len(buf))
	// 
	// // Print the first few bytes of the buffer
	// if len(buf) > 0 {
	// 	fmt.Printf("First 20 bytes of buffer: ")
	// 	for i := 0; i < 20 && i < len(buf); i++ {
	// 		fmt.Printf("%02x ", buf[i])
	// 	}
	// 	fmt.Println()
	// }

	// Try to decode up to 'count' varints, but stop if we run out of data
	for i := 0; i < count && offset < len(buf); i++ {
		// Make sure we have at least one byte to read
		if offset >= len(buf) {
			fmt.Printf("Ran out of data at index %d after decoding %d values\n", i, len(vals))
			break
		}

		// Try to decode a varint
		v, n := binary.Uvarint(buf[offset:])
		if n <= 0 {
			// If we can't decode any more varints but we've already decoded some,
			// return what we have instead of failing
			if i > 0 {
				return vals, nil
			}
			return nil, fmt.Errorf("failed to decode uvarint at index %d, bytes remaining: %d", i, len(buf)-offset)
		}

		vals = append(vals, v)
		offset += n
	}

	// If we couldn't decode enough varints, return an error
	if len(vals) < count {
		// Uncomment for debugging if needed
		// fmt.Printf("Warning: Only decoded %d varints, expected %d\n", len(vals), count)
		return nil, fmt.Errorf("incomplete data: only decoded %d of %d expected varints", len(vals), count)
	}

	return vals, nil
}

// Zigzag decoding converts an unsigned varint into a signed int64
func zigzagDecode(n uint64) int64 {
	return int64((n >> 1) ^ uint64((int64(n&1) * -1)))
}

// GetPairs returns the ID-value pairs from a block
func (r *Reader) GetPairs(blockIdx uint64) ([]uint64, []int64, error) {
	ids, values, err := r.readBlock(int(blockIdx))
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

// AggregateOptions contains options for the aggregation process
type AggregateOptions struct {
	// SkipPreCalculated forces the aggregation to read all values from blocks
	// instead of using pre-calculated values from the footer
	SkipPreCalculated bool
}

// DefaultAggregateOptions returns the default options for aggregation
func DefaultAggregateOptions() AggregateOptions {
	return AggregateOptions{
		SkipPreCalculated: false,
	}
}

// Aggregate aggregates all blocks and returns the result using default options
func (r *Reader) Aggregate() AggregateResult {
	return r.AggregateWithOptions(DefaultAggregateOptions())
}

// AggregateWithOptions aggregates all blocks with the specified options and returns the result
func (r *Reader) AggregateWithOptions(opts AggregateOptions) AggregateResult {
	// If we have a footer with block statistics and we're not skipping pre-calculated values, use it for efficient aggregation
	if len(r.blockIndex) > 0 && !opts.SkipPreCalculated {
		var count int
		var min int64 = 9223372036854775807  // Max int64
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
	var min int64 = 9223372036854775807  // Max int64
	var max int64 = -9223372036854775808 // Min int64
	var sum int64 = 0

	for i := uint64(0); i < r.header.BlockCount; i++ {
		_, values, err := r.GetPairs(i)
		if err != nil {
			// Skip blocks with errors
			continue
		}

		count += len(values)
		for _, v := range values {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
			sum += v
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
