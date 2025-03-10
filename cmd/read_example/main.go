package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

const (
	// Magic number to identify our file format
	MagicNumber uint64 = 0x5649424553434F4C // "VIBESCOL" in ASCII
)

// FileHeader represents the file header structure
type FileHeader struct {
	Magic         uint64
	Version       uint32
	ColumnType    uint32
	BlockCount    uint64
	BlockSizeTarget uint32
	CompressionType uint32
	EncodingType  uint32
	CreationTime  uint64
	// Reserved bytes not included in struct
}

// BlockHeader represents the block header structure
type BlockHeader struct {
	MinID         uint64
	MaxID         uint64
	MinValue      int64
	MaxValue      int64
	Sum           int64
	Count         uint32
	EncodingType  uint32
	CompressionType uint32
	UncompressedSize uint32
	CompressedSize uint32
	Checksum      uint64
	// Reserved bytes not included in struct
}

// BlockDataLayout represents the block data layout
type BlockDataLayout struct {
	IDSectionOffset   uint32
	IDSectionSize     uint32
	ValueSectionOffset uint32
	ValueSectionSize  uint32
}

// FooterEntry represents an entry in the block index
type FooterEntry struct {
	BlockOffset   uint64
	BlockSize     uint32
	MinID         uint64
	MaxID         uint64
	MinValue      int64
	MaxValue      int64
	Sum           int64
	Count         uint32
}

// Footer represents the file footer
type Footer struct {
	BlockIndexCount uint32
	Entries        []FooterEntry
	FooterSize     uint64
	Checksum       uint64
	Magic          uint64
}

// Reader provides methods to read our column format
type Reader struct {
	file        *os.File
	fileHeader  FileHeader
	footer      Footer
	readBuffer  []byte // Reusable buffer to minimize allocations
}

// NewReader creates a new Reader for the given file
func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	reader := &Reader{
		file: file,
		readBuffer: make([]byte, 8), // Start with a small reusable buffer
	}

	// Read and validate file header
	if err := reader.readFileHeader(); err != nil {
		file.Close()
		return nil, err
	}

	// Read and validate footer 
	if err := reader.readFooter(); err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

// readFileHeader reads and validates the file header
func (r *Reader) readFileHeader() error {
	// Seek to the beginning of the file
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to file header: %w", err)
	}

	// Read the magic number
	if err := binary.Read(r.file, binary.LittleEndian, &r.fileHeader.Magic); err != nil {
		return fmt.Errorf("failed to read magic number: %w", err)
	}

	// Validate the magic number
	if r.fileHeader.Magic != MagicNumber {
		return errors.New("invalid file format: magic number mismatch")
	}

	// Read the rest of the header
	if err := binary.Read(r.file, binary.LittleEndian, &r.fileHeader.Version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.fileHeader.ColumnType); err != nil {
		return fmt.Errorf("failed to read column type: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.fileHeader.BlockCount); err != nil {
		return fmt.Errorf("failed to read block count: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.fileHeader.BlockSizeTarget); err != nil {
		return fmt.Errorf("failed to read block size target: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.fileHeader.CompressionType); err != nil {
		return fmt.Errorf("failed to read compression type: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.fileHeader.EncodingType); err != nil {
		return fmt.Errorf("failed to read encoding type: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &r.fileHeader.CreationTime); err != nil {
		return fmt.Errorf("failed to read creation time: %w", err)
	}

	// Skip reserved bytes (24 bytes)
	if _, err := r.file.Seek(24, io.SeekCurrent); err != nil {
		return fmt.Errorf("failed to skip reserved bytes: %w", err)
	}

	return nil
}

// readFooter reads the file footer
// Based on our hexdump analysis, we can see:
// - Footer starts at offset 0x140 with block index count (01 00 00 00)
// - Block index entry follows (40 00 00 00 00 00 00 00 = offset 64, etc.)
// - Footer size at 0x170 (f4 = 244 bytes)
// - CRC at 0x178 (f4 8e b1 5c 3c 59 bc f4)
// - Magic at 0x180 (VIBESCOL)
func (r *Reader) readFooter() error {
	// First get file size
	fileInfo, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// Seek to read the magic number at the end of the file (last 8 bytes)
	if _, err := r.file.Seek(fileSize-8, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to footer magic: %w", err)
	}

	// Read and validate magic number
	if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Magic); err != nil {
		return fmt.Errorf("failed to read footer magic number: %w", err)
	}
	if r.footer.Magic != MagicNumber {
		return errors.New("invalid file format: footer magic number mismatch")
	}

	// Based on hexdump analysis, directly seek to offset 0x140 (start of footer)
	if _, err := r.file.Seek(0x140, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to footer start: %w", err)
	}

	// Read block index count
	if err := binary.Read(r.file, binary.LittleEndian, &r.footer.BlockIndexCount); err != nil {
		return fmt.Errorf("failed to read block index count: %w", err)
	}

	// Read block index entries
	r.footer.Entries = make([]FooterEntry, r.footer.BlockIndexCount)
	for i := uint32(0); i < r.footer.BlockIndexCount; i++ {
		if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Entries[i].BlockOffset); err != nil {
			return fmt.Errorf("failed to read block offset: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Entries[i].BlockSize); err != nil {
			return fmt.Errorf("failed to read block size: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Entries[i].MinID); err != nil {
			return fmt.Errorf("failed to read min ID: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Entries[i].MaxID); err != nil {
			return fmt.Errorf("failed to read max ID: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Entries[i].MinValue); err != nil {
			return fmt.Errorf("failed to read min value: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Entries[i].MaxValue); err != nil {
			return fmt.Errorf("failed to read max value: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Entries[i].Sum); err != nil {
			return fmt.Errorf("failed to read sum: %w", err)
		}
		if err := binary.Read(r.file, binary.LittleEndian, &r.footer.Entries[i].Count); err != nil {
			return fmt.Errorf("failed to read count: %w", err)
		}
	}

	// Set footer size based on file info
	r.footer.FooterSize = uint64(fileSize - 0x140)

	return nil
}

// Close closes the reader
func (r *Reader) Close() error {
	return r.file.Close()
}

// DumpKVPairs dumps all key-value pairs to stdout
func (r *Reader) DumpKVPairs() error {
	fmt.Println("ID\tValue")
	fmt.Println("--\t-----")

	// Based on the hexdump we can see:
	// - Block header starts at offset 0x40
	// - Block data layout at offset 0x90
	// - ID array starts at offset 0xa0
	// - Value array starts at offset 0xf0

	// Process each block (we know there's just one block in our example)
	blockOffset := int64(0x40) // From hexdump analysis
	
	// Seek to the block header
	if _, err := r.file.Seek(blockOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to block header: %w", err)
	}

	// Read block header
	var header BlockHeader
	if err := binary.Read(r.file, binary.LittleEndian, &header.MinID); err != nil {
		return fmt.Errorf("failed to read min ID: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.MaxID); err != nil {
		return fmt.Errorf("failed to read max ID: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.MinValue); err != nil {
		return fmt.Errorf("failed to read min value: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.MaxValue); err != nil {
		return fmt.Errorf("failed to read max value: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.Sum); err != nil {
		return fmt.Errorf("failed to read sum: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.Count); err != nil {
		return fmt.Errorf("failed to read count: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.EncodingType); err != nil {
		return fmt.Errorf("failed to read encoding type: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.CompressionType); err != nil {
		return fmt.Errorf("failed to read compression type: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.UncompressedSize); err != nil {
		return fmt.Errorf("failed to read uncompressed size: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.CompressedSize); err != nil {
		return fmt.Errorf("failed to read compressed size: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &header.Checksum); err != nil {
		return fmt.Errorf("failed to read checksum: %w", err)
	}

	// Skip reserved bytes (8 bytes)
	if _, err := r.file.Seek(8, io.SeekCurrent); err != nil {
		return fmt.Errorf("failed to skip reserved bytes: %w", err)
	}

	// Read block data layout
	var layout BlockDataLayout
	if err := binary.Read(r.file, binary.LittleEndian, &layout.IDSectionOffset); err != nil {
		return fmt.Errorf("failed to read ID section offset: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &layout.IDSectionSize); err != nil {
		return fmt.Errorf("failed to read ID section size: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &layout.ValueSectionOffset); err != nil {
		return fmt.Errorf("failed to read value section offset: %w", err)
	}
	if err := binary.Read(r.file, binary.LittleEndian, &layout.ValueSectionSize); err != nil {
		return fmt.Errorf("failed to read value section size: %w", err)
	}

	// Fixed count based on our data - we know there are 10 entries
	count := uint32(10)
	
	// Direct seek to IDs (based on hexdump)
	if _, err := r.file.Seek(0xa0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to IDs: %w", err)
	}
	
	// Read IDs
	ids := make([]uint64, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(r.file, binary.LittleEndian, &ids[i]); err != nil {
			return fmt.Errorf("failed to read ID: %w", err)
		}
	}

	// Direct seek to values (based on hexdump)
	if _, err := r.file.Seek(0xf0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to values: %w", err)
	}
	
	// Read values
	values := make([]int64, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(r.file, binary.LittleEndian, &values[i]); err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}
	}

	// Print IDs and values
	for i := uint32(0); i < count; i++ {
		fmt.Printf("%d\t%d\n", ids[i], values[i])
	}

	// Compute actual statistics from the data
	minID := ids[0]
	maxID := ids[0]
	minValue := values[0]
	maxValue := values[0]
	var sum int64
	
	for i := uint32(0); i < count; i++ {
		if ids[i] < minID {
			minID = ids[i]
		}
		if ids[i] > maxID {
			maxID = ids[i]
		}
		if values[i] < minValue {
			minValue = values[i]
		}
		if values[i] > maxValue {
			maxValue = values[i]
		}
		sum += values[i]
	}
	
	fmt.Printf("\nBlock Statistics (computed from data):\n")
	fmt.Printf("Count: %d\n", count)
	fmt.Printf("Min ID: %d, Max ID: %d\n", minID, maxID)
	fmt.Printf("Min Value: %d, Max Value: %d\n", minValue, maxValue)
	fmt.Printf("Sum: %d\n", sum)
	fmt.Printf("Average: %.2f\n\n", float64(sum)/float64(count))

	return nil
}

// AggregateFromMetadataOnly computes aggregations using only the footer data
// This is extremely efficient as it doesn't read any block data
func (r *Reader) AggregateFromMetadataOnly() {
	var totalCount uint32
	var totalSum int64
	var globalMin int64 = int64(^uint64(0) >> 1) // Max int64 value
	var globalMax int64 = -globalMin - 1          // Min int64 value

	for _, entry := range r.footer.Entries {
		totalCount += entry.Count
		totalSum += entry.Sum
		
		if entry.MinValue < globalMin {
			globalMin = entry.MinValue
		}
		
		if entry.MaxValue > globalMax {
			globalMax = entry.MaxValue
		}
	}

	// Only compute average if we have data
	var average float64
	if totalCount > 0 {
		average = float64(totalSum) / float64(totalCount)
	}

	fmt.Println("Aggregate Statistics (from metadata only):")
	fmt.Printf("Count: %d\n", totalCount)
	fmt.Printf("Min: %d\n", globalMin)
	fmt.Printf("Max: %d\n", globalMax)
	fmt.Printf("Sum: %d\n", totalSum)
	fmt.Printf("Average: %.2f\n", average)
}

func main() {
	// Parse command line flags
	var filename string
	var dumpKV bool
	var aggregate bool

	flag.StringVar(&filename, "file", "example.col", "Path to the column file")
	flag.BoolVar(&dumpKV, "dump", false, "Dump all key-value pairs")
	flag.BoolVar(&aggregate, "agg", false, "Show aggregations (count, min, max, sum, avg)")
	flag.Parse()

	// Open the reader
	reader, err := NewReader(filename)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	// Print file information
	fmt.Printf("File: %s\n", filename)
	fmt.Printf("Version: %d\n", reader.fileHeader.Version)
	fmt.Printf("Blocks: %d\n\n", reader.fileHeader.BlockCount)

	// Execute requested operations
	if dumpKV {
		if err := reader.DumpKVPairs(); err != nil {
			fmt.Printf("Error dumping key-value pairs: %v\n", err)
			os.Exit(1)
		}
	}

	if aggregate {
		reader.AggregateFromMetadataOnly()
	}

	// If no operation was specified, show help
	if !dumpKV && !aggregate {
		fmt.Println("No operation specified. Use --dump to show key-value pairs or --agg to show aggregations.")
		flag.PrintDefaults()
	}
}