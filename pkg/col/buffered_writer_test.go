package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareWriterImplementations(t *testing.T) {
	tests := []struct {
		name         string
		ids          []uint64
		values       []int64
		encodingType uint32
	}{
		{
			name:         "SinglePair",
			ids:          []uint64{42},
			values:       []int64{123},
			encodingType: EncodingRaw,
		},
		{
			name:         "MultiplePairs",
			ids:          generateSequentialIDs(0, 10),
			values:       generateSequentialValues(0, 10),
			encodingType: EncodingRaw,
		},
		{
			name:         "DeltaEncoding",
			ids:          generateSequentialIDs(0, 10),
			values:       generateSequentialValues(0, 10),
			encodingType: EncodingDeltaBoth,
		},
		{
			name:         "VarIntEncoding",
			ids:          generateSequentialIDs(0, 10),
			values:       generateSequentialValues(0, 10),
			encodingType: EncodingVarIntBoth,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareWriters(t, tt.ids, tt.values, tt.encodingType)
			if !result {
				analyzeFileDifferences(t, "", "") // The file paths are not used in this function anymore
				t.Fail()
			}
		})
	}
}

// readBlockLayout reads the layout section of a block at the given offset
func readBlockLayout(file string, blockOffset int64) (BlockLayout, error) {
	var layout BlockLayout

	// Open the file
	f, err := os.Open(file)
	if err != nil {
		return layout, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()
	_, err = f.Seek(blockOffset, io.SeekStart)
	if err != nil {
		return layout, fmt.Errorf("failed to seek to layout section: %w", err)
	}

	// Read the layout section (16 bytes)
	layoutBytes := make([]byte, 16)
	_, err = f.Read(layoutBytes)
	if err != nil {
		return layout, fmt.Errorf("failed to read layout section: %w", err)
	}

	// Decode the layout section
	layout.IDSectionOffset = binary.LittleEndian.Uint32(layoutBytes[0:4])
	layout.IDSectionSize = binary.LittleEndian.Uint32(layoutBytes[4:8])
	layout.ValueSectionOffset = binary.LittleEndian.Uint32(layoutBytes[8:12])
	layout.ValueSectionSize = binary.LittleEndian.Uint32(layoutBytes[12:16])

	return layout, nil
}

func readHeader(t *testing.T, buf []byte) FileHeader {
	header := FileHeader{}

	// Extract fields from the buffer
	header.Magic = binary.LittleEndian.Uint64(buf[0:8])
	header.Version = binary.LittleEndian.Uint32(buf[8:12])
	header.ColumnType = binary.LittleEndian.Uint32(buf[12:16])
	header.BlockCount = binary.LittleEndian.Uint64(buf[16:24])
	header.BlockSizeTarget = binary.LittleEndian.Uint32(buf[24:28])
	header.CompressionType = binary.LittleEndian.Uint32(buf[28:32])
	header.EncodingType = binary.LittleEndian.Uint32(buf[32:36])
	header.CreationTime = binary.LittleEndian.Uint64(buf[36:44])
	header.BitmapOffset = binary.LittleEndian.Uint64(buf[44:52])
	header.BitmapSize = binary.LittleEndian.Uint64(buf[52:60])

	return header
}

// readBlockHeader reads the header of a block at the given offset
func readBlockHeader(file string, blockOffset int64) (BlockHeader, error) {
	var header BlockHeader

	// Open the file
	f, err := os.Open(file)
	if err != nil {
		return header, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Seek to the header
	_, err = f.Seek(blockOffset, io.SeekStart)
	if err != nil {
		return header, fmt.Errorf("failed to seek to block header: %w", err)
	}

	// Read the header (64 bytes)
	headerBytes := make([]byte, blockHeaderSize)
	n, err := f.Read(headerBytes)
	if err != nil {
		return header, fmt.Errorf("failed to read block header: %w", err)
	}

	// Check if we read enough bytes
	if n < blockHeaderSize {
		return header, fmt.Errorf("incomplete block header: read %d bytes, expected %d", n, blockHeaderSize)
	}

	// Extract fields from the buffer
	header.MinID = binary.LittleEndian.Uint64(headerBytes[0:8])
	header.MaxID = binary.LittleEndian.Uint64(headerBytes[8:16])
	header.MinValue = binary.LittleEndian.Uint64(headerBytes[16:24])
	header.MaxValue = binary.LittleEndian.Uint64(headerBytes[24:32])
	header.Sum = binary.LittleEndian.Uint64(headerBytes[32:40])
	header.Count = binary.LittleEndian.Uint32(headerBytes[40:44])
	header.EncodingType = binary.LittleEndian.Uint32(headerBytes[44:48])
	header.CompressionType = binary.LittleEndian.Uint32(headerBytes[48:52])
	header.UncompressedSize = binary.LittleEndian.Uint32(headerBytes[52:56])
	header.CompressedSize = binary.LittleEndian.Uint32(headerBytes[56:60])
	header.Checksum = binary.LittleEndian.Uint64(headerBytes[56:64])

	return header, nil
}

// compareWriters creates equivalent files with both writer implementations and compares them
func compareWriters(t *testing.T, ids []uint64, values []int64, encodingType uint32) bool {
	// Create a temporary directory for our test files
	tempDir := t.TempDir()

	// Create files with standard Writer
	standardFile := filepath.Join(tempDir, "standard.col")
	standardWriter, err := NewWriter(standardFile, WithEncoding(encodingType))
	require.NoError(t, err)

	err = standardWriter.WriteBlock(ids, values)
	require.NoError(t, err)

	err = standardWriter.FinalizeAndClose()
	require.NoError(t, err)

	// Create files with BufferedWriter
	bufferedFile := filepath.Join(tempDir, "buffered.col")
	bufferedWriter, err := NewBufferedWriter(bufferedFile, WithBufferedBlockSize(128*1024), WithBufferedEncoding(encodingType)) // 128KB blocks and same encoding type
	require.NoError(t, err)

	// Use WriteBlock directly instead of adding items one by one
	for i, id := range ids {
		err = bufferedWriter.Add(id, values[i])
		require.NoError(t, err)
	}

	err = bufferedWriter.Close()
	require.NoError(t, err)

	// Read both files as raw bytes
	standardBytes, err := os.ReadFile(standardFile)
	require.NoError(t, err)

	bufferedBytes, err := os.ReadFile(bufferedFile)
	require.NoError(t, err)

	// Compare file sizes
	t.Logf("Standard file size: %d bytes", len(standardBytes))
	t.Logf("Buffered file size: %d bytes", len(bufferedBytes))

	require.Equal(t, len(standardBytes), len(bufferedBytes), "File sizes should match")

	// Compare headers
	standardHeader := readHeader(t, standardBytes)
	bufferedHeader := readHeader(t, bufferedBytes)
	t.Logf("Standard header: %+v", standardHeader)
	t.Logf("Buffered header: %+v", bufferedHeader)

	// The timestamp is allowed to differ, all other values have to be identical:
	assert.Equal(t, standardHeader.Magic, bufferedHeader.Magic, "Magic should match")
	assert.Equal(t, standardHeader.Version, bufferedHeader.Version, "Version should match")
	assert.Equal(t, standardHeader.ColumnType, bufferedHeader.ColumnType, "ColumnType should match")
	assert.Equal(t, standardHeader.BlockCount, bufferedHeader.BlockCount, "BlockCount should match")
	assert.Equal(t, standardHeader.BlockSizeTarget, bufferedHeader.BlockSizeTarget, "BlockSizeTarget should match")
	assert.Equal(t, standardHeader.CompressionType, bufferedHeader.CompressionType, "CompressionType should match")
	assert.Equal(t, standardHeader.EncodingType, bufferedHeader.EncodingType, "EncodingType should match")
	assert.Equal(t, standardHeader.BitmapOffset, bufferedHeader.BitmapOffset, "BitmapOffset should match")
	assert.Equal(t, standardHeader.BitmapSize, bufferedHeader.BitmapSize, "BitmapSize should match")

	// Compare with Reader
	standardReader, standardErr := NewReader(standardFile)
	bufferedReader, bufferedErr := NewReader(bufferedFile)

	defer func() {
		if standardReader != nil {
			standardReader.Close()
		}
		if bufferedReader != nil {
			bufferedReader.Close()
		}
	}()

	if standardErr != nil || bufferedErr != nil {
		t.Logf("Standard reader error: %v", standardErr)
		t.Logf("Buffered reader error: %v", bufferedErr)
		return false
	}

	t.Logf("Both readers succeeded")

	// Compare block count
	standardBlockCount := standardReader.BlockCount()
	bufferedBlockCount := bufferedReader.BlockCount()
	t.Logf("Standard block count: %d", standardBlockCount)
	t.Logf("Buffered block count: %d", bufferedBlockCount)
	if standardBlockCount != bufferedBlockCount {
		return false
	}

	// Compare first block header
	standardBlockHeader, err := readBlockHeader(standardFile, 64)
	require.NoError(t, err)
	t.Logf("Standard block header: %+v", standardBlockHeader)

	standardBlockLayout, err := readBlockLayout(standardFile, 64+blockHeaderSize)
	require.NoError(t, err)
	t.Logf("Standard block layout: IDOffset=%d, IDSize=%d, ValueOffset=%d, ValueSize=%d",
		standardBlockLayout.IDSectionOffset, standardBlockLayout.IDSectionSize,
		standardBlockLayout.ValueSectionOffset, standardBlockLayout.ValueSectionSize)

	var desiredIDs []uint64
	var desiredValues []int64
	// Read standard blocks
	for i := uint64(0); i < standardBlockCount; i++ {
		standardIDs, standardValues, err := standardReader.GetPairs(i)
		if err != nil {
			t.Logf("Error reading standard block: %v", err)
			return false
		}
		desiredIDs = standardIDs
		desiredValues = standardValues
		t.Logf("Standard block %d: %d pairs", i, len(standardIDs))
		if len(standardIDs) > 0 {
			t.Logf("Standard block first ID: %d, first value: %d", standardIDs[0], standardValues[0])
		}
	}

	// Compare with buffered block
	bufferedBlockHeader, err := readBlockHeader(bufferedFile, 64)
	require.NoError(t, err)
	t.Logf("Buffered block header: %+v", bufferedBlockHeader)

	bufferedBlockLayout, err := readBlockLayout(bufferedFile, 64+blockHeaderSize)
	require.NoError(t, err)
	t.Logf("Buffered block layout: IDOffset=%d, IDSize=%d, ValueOffset=%d, ValueSize=%d",
		bufferedBlockLayout.IDSectionOffset, bufferedBlockLayout.IDSectionSize,
		bufferedBlockLayout.ValueSectionOffset, bufferedBlockLayout.ValueSectionSize)

	// Read buffered blocks
	for i := uint64(0); i < bufferedBlockCount; i++ {
		bufferedIDs, bufferedValues, err := bufferedReader.GetPairs(i)
		if err != nil {
			t.Logf("Error reading buffered block: %v", err)
			return false
		}
		t.Logf("Buffered block %d: %d pairs", i, len(bufferedIDs))
		if len(bufferedIDs) > 0 {
			t.Logf("Buffered block first ID: %d, first value: %d", bufferedIDs[0], bufferedValues[0])
		}

		assert.Equal(t, desiredIDs, bufferedIDs, "IDs should match")
		assert.Equal(t, desiredValues, bufferedValues, "Values should match")
	}

	return true
}

// analyzeFileDifferences attempts to analyze the differences between the standard and buffered files
func analyzeFileDifferences(t *testing.T, standardFile, bufferedFile string) {
	// If empty file paths are provided, skip file-specific analysis
	if standardFile == "" || bufferedFile == "" {
		return
	}

	// Open both files
	standardReader, stdErr := NewReader(standardFile)
	if stdErr != nil {
		t.Logf("Error opening standard file for analysis: %v", stdErr)
		return
	}
	defer standardReader.Close()

	bufferedReader, bufErr := NewReader(bufferedFile)
	if bufErr != nil {
		t.Logf("Error opening buffered file for analysis: %v", bufErr)
		return
	}
	defer bufferedReader.Close()

	// Analyze file headers
	standardHeader, standardFooterMeta, err := readAndDecodeHeader(standardReader.file)
	if err != nil {
		t.Logf("Error reading standard header: %v", err)
		return
	}

	bufferedHeader, bufferedFooterMeta, err := readAndDecodeHeader(bufferedReader.file)
	if err != nil {
		t.Logf("Error reading buffered header: %v", err)
		return
	}

	t.Logf("Standard file header: %+v", standardHeader)
	t.Logf("Buffered file header: %+v", bufferedHeader)
	t.Logf("Standard footer meta: %+v", standardFooterMeta)
	t.Logf("Buffered footer meta: %+v", bufferedFooterMeta)
}

// readAndDecodeHeader reads the file header and footer metadata
func readAndDecodeHeader(file *os.File) (FileHeader, FooterMetadata, error) {
	var header FileHeader
	var footerMeta FooterMetadata

	// Seek to start of file
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return header, footerMeta, err
	}

	// Read header
	headerBytes := make([]byte, 64)
	_, err = file.Read(headerBytes)
	if err != nil {
		return header, footerMeta, err
	}

	// Decode header (simplified, just extract some fields for comparison)
	// You would normally use binary.Read here, but we're just getting the info
	// we need for comparison

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return header, footerMeta, err
	}

	// Try to read footer metadata (last 24 bytes)
	_, err = file.Seek(fileInfo.Size()-24, io.SeekStart)
	if err != nil {
		return header, footerMeta, err
	}

	footerBytes := make([]byte, 24)
	_, err = file.Read(footerBytes)
	if err != nil {
		return header, footerMeta, err
	}

	// For now, return partially filled structures
	// In a real implementation, you would fully decode these
	return header, footerMeta, nil
}

func TestBufferedWriterBasics(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test-bufferedwriter-*.col")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Create a new buffered writer
	t.Logf("Creating buffered writer for file: %s", tempFile.Name())
	writer, err := NewBufferedWriter(tempFile.Name(), WithBufferedBlockSize(128*1024)) // 128KB blocks
	require.NoError(t, err)

	// Write a block directly
	t.Logf("Writing block with ID=42, Value=123")
	err = writer.Add(42, 123)
	require.NoError(t, err)

	// Close the writer
	t.Logf("Closing writer")
	err = writer.Close()
	require.NoError(t, err)

	// Read the file and validate
	t.Logf("Reading file to validate contents")
	reader, err := NewReader(tempFile.Name())
	if err != nil {
		t.Fatalf("Error creating reader: %v", err)
	}

	// Debug information about the file
	t.Logf("File size: %d bytes", reader.fileSize)
	t.Logf("File header: magic=%x, version=%d, blockCount=%d, encoding=%d",
		reader.header.Magic, reader.header.Version, reader.header.BlockCount, reader.header.EncodingType)
	t.Logf("Block index entries: %d", len(reader.blockIndex))
	for i, entry := range reader.blockIndex {
		t.Logf("Block %d: offset=%d, size=%d", i, entry.BlockOffset, entry.BlockSize)
	}

	found := false
	for i := 0; i < int(reader.header.BlockCount); i++ {
		t.Logf("Attempting to read block %d", i)
		ids, values, err := reader.readBlock(i)
		if err != nil {
			t.Logf("Error reading block %d: %v", i, err)
			continue
		}

		t.Logf("Block %d: Read %d IDs and %d values", i, len(ids), len(values))

		// Check if the ID and value pair is in the block
		for j := 0; j < len(ids); j++ {
			t.Logf("  Entry %d: ID=%d, Value=%d", j, ids[j], values[j])
			if ids[j] == 42 && values[j] == 123 {
				found = true
			}
		}
	}

	require.True(t, found, "Expected to find ID=42, Value=123 in one of the blocks")
}

// generateSequentialIDs generates sequential IDs from start to start+count-1
func generateSequentialIDs(start, count int) []uint64 {
	ids := make([]uint64, count)
	for i := 0; i < count; i++ {
		ids[i] = uint64(start + i)
	}
	return ids
}

// generateSequentialValues generates sequential values from start*10 to (start+count-1)*10
func generateSequentialValues(start, count int) []int64 {
	values := make([]int64, count)
	for i := 0; i < count; i++ {
		values[i] = int64((start + i) * 30)
	}
	return values
}

// TestLargeBufferedWrite tests adding a large number of entries with the buffered writer
// and analyzes the resulting block statistics, validating that blocks are close to the target size
func TestLargeBufferedWrite(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	const numEntries = 1000000 // Increased from 100k to 1M

	// Test with only 128KB block size for profiling
	blockSizes := []int{128 * 1024}

	for _, targetBlockSize := range blockSizes {
		t.Run(fmt.Sprintf("BlockSize_%dKB_BatchAdd", targetBlockSize/1024), func(t *testing.T) {
			// Create a temporary file for testing
			f, err := os.CreateTemp("", "test-large-bufferedwriter-batch-*.col")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(f.Name())
			defer f.Close()

			// Create a buffered writer
			bufferedWriter, err := NewBufferedWriter(f.Name(), WithBufferedBlockSize(uint32(targetBlockSize)))
			if err != nil {
				t.Fatalf("Failed to create buffered writer: %v", err)
			}

			// Generate test data
			t.Logf("Adding %d entries to buffered writer with target block size %d bytes", numEntries, targetBlockSize)
			var ids []uint64
			var values []int64

			for i := 0; i < numEntries; i++ {
				ids = append(ids, uint64(i+1))
				values = append(values, int64(i*10))
			}

			// Profiling: we'll time just the writing portion
			writeStart := time.Now()

			// Add entries in batches to demonstrate BatchAdd performance
			const batchSize = 1000
			for i := 0; i < numEntries; i += batchSize {
				end := i + batchSize
				if end > numEntries {
					end = numEntries
				}

				// Use BatchAdd method for better performance
				err = bufferedWriter.BatchAdd(ids[i:end], values[i:end])
				if err != nil {
					t.Fatalf("Failed to batch add entries %d-%d: %v", i, end, err)
				}
			}

			// Close the writer to finalize the file
			err = bufferedWriter.Close()
			if err != nil {
				t.Fatalf("Failed to close writer: %v", err)
			}

			// Calculate write time
			writeTime := time.Since(writeStart)
			entriesPerSecond := float64(numEntries) / writeTime.Seconds()
			bytesPerSecond := float64(numEntries*16) / writeTime.Seconds() // 8 bytes for ID, 8 bytes for value

			t.Logf("Performance metrics (BatchAdd):")
			t.Logf("  Total write time: %.3f seconds", writeTime.Seconds())
			t.Logf("  Entries per second: %.2f", entriesPerSecond)
			t.Logf("  Bytes per second: %.2f (%.2f MB/s)", bytesPerSecond, bytesPerSecond/1024/1024)

			analyzeFile(t, f.Name())
		})
	}
}

// analyzeFile is a helper function to analyze a column file
func analyzeFile(t *testing.T, filename string) {
	// Reopen the file for reading
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to reopen file: %v", err)
	}
	defer f.Close()

	// Create a reader
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}

	// Get file size
	fileInfo, err := f.Stat()
	if err != nil {
		t.Fatalf("Failed to get file stats: %v", err)
	}
	t.Logf("File size: %d bytes", fileInfo.Size())

	// Analyze the blocks
	blockCount := len(reader.blockIndex)
	t.Logf("Total blocks: %d", blockCount)

	// Calculate statistics
	totalEntries := 0
	minBlockSize := int64(math.MaxInt64)
	maxBlockSize := int64(0)
	totalBlockSize := int64(0)

	minEntriesPerBlock := math.MaxInt64
	maxEntriesPerBlock := 0

	// Block size distribution buckets (in KB)
	sizeDistribution := make(map[int]int)
	// Keep track of blocks outside the target range
	blocksOutsideTargetRange := 0

	// Define acceptable range: blocks should be within 25% of target size
	// except for the last block which can be smaller
	targetBlockSize := int64(reader.header.BlockSizeTarget)
	acceptableMinSize := int64(float64(targetBlockSize) * 0.75)
	acceptableMaxSize := int64(float64(targetBlockSize) * 1.25)

	for i := 0; i < blockCount; i++ {
		blockOffset := reader.blockIndex[i].BlockOffset
		blockSize := int64(reader.blockIndex[i].BlockSize)
		entriesCount := int(reader.blockIndex[i].Count)

		totalEntries += entriesCount
		totalBlockSize += blockSize

		if blockSize < minBlockSize {
			minBlockSize = blockSize
		}
		if blockSize > maxBlockSize {
			maxBlockSize = blockSize
		}

		if entriesCount < minEntriesPerBlock {
			minEntriesPerBlock = entriesCount
		}
		if entriesCount > maxEntriesPerBlock {
			maxEntriesPerBlock = entriesCount
		}

		// Record in distribution buckets (round to nearest KB)
		sizeInKB := int(blockSize / 1024)
		sizeDistribution[sizeInKB]++

		// Print detailed info for first few and last few blocks
		if i < 3 || i >= blockCount-3 {
			t.Logf("Block %d: Offset=%d, Size=%d bytes, Entries=%d",
				i, blockOffset, blockSize, entriesCount)
		} else if i == 3 && blockCount > 6 {
			t.Logf("... skipping %d blocks ...", blockCount-6)
		}

		// Check if block size is within acceptable range
		// The last block is allowed to be smaller
		if i < blockCount-1 && (blockSize < acceptableMinSize || blockSize > acceptableMaxSize) {
			blocksOutsideTargetRange++
			t.Logf("Block %d size %d bytes is outside acceptable range (%d-%d bytes)",
				i, blockSize, acceptableMinSize, acceptableMaxSize)
		}
	}

	// Calculate and display statistics
	var avgBlockSize float64
	var avgEntriesPerBlock float64

	if blockCount > 0 {
		avgBlockSize = float64(totalBlockSize) / float64(blockCount)
		avgEntriesPerBlock = float64(totalEntries) / float64(blockCount)
	}

	t.Logf("Block size statistics:")
	if blockCount > 0 {
		t.Logf("  Min: %d bytes", minBlockSize)
		t.Logf("  Max: %d bytes", maxBlockSize)
		t.Logf("  Avg: %.2f bytes", avgBlockSize)
		t.Logf("  Target: %d bytes", targetBlockSize)
	} else {
		t.Logf("  No blocks found")
	}
	t.Logf("  Total: %d bytes", totalBlockSize)

	t.Logf("Entries per block statistics:")
	if blockCount > 0 {
		t.Logf("  Min: %d entries", minEntriesPerBlock)
		t.Logf("  Max: %d entries", maxEntriesPerBlock)
		t.Logf("  Avg: %.2f entries", avgEntriesPerBlock)
	} else {
		t.Logf("  No blocks found")
	}
	t.Logf("  Total: %d entries", totalEntries)

	// Sort and display size distribution
	var sizes []int
	for size := range sizeDistribution {
		sizes = append(sizes, size)
	}
	sort.Ints(sizes)

	t.Logf("Block size distribution (KB):")
	for _, size := range sizes {
		count := sizeDistribution[size]
		percentage := float64(count) / float64(blockCount) * 100
		t.Logf("  %d KB: %d blocks (%.1f%%)", size, count, percentage)
	}

	// Verify correct number of entries
	if totalEntries != 1000000 { // hardcoded from the test's numEntries const
		t.Errorf("Expected %d total entries, got %d", 1000000, totalEntries)
	}

	// Calculate acceptable percentage of blocks outside the target range
	// For simplicity, we'll allow 5% of blocks to be outside range, excluding the last block
	maxAllowedOutsideRange := int(float64(blockCount-1) * 0.05)
	if blocksOutsideTargetRange > maxAllowedOutsideRange {
		t.Errorf("Too many blocks (%d) outside acceptable size range (%d-%d bytes). Maximum allowed: %d",
			blocksOutsideTargetRange, acceptableMinSize, acceptableMaxSize, maxAllowedOutsideRange)
	}
}

func TestBufferedWriter(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-buffered-writer-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file
	filePath := filepath.Join(tempDir, "buffered_test.col")

	// Create a BufferedWriter
	writer, err := NewBufferedWriter(filePath, WithBufferedEncoding(EncodingRaw))
	require.NoError(t, err)

	// Write a large dataset to ensure multiple blocks are created
	// We'll create 20,000 ID-value pairs, which should be around 320KB
	// This should result in at least 2-3 blocks with our 128KB target
	const numPairs = 20000
	ids := make([]uint64, numPairs)
	values := make([]int64, numPairs)

	// Fill with data (intentionally not sorted)
	for i := 0; i < numPairs; i++ {
		// Use a pattern that's not sorted to test sorting
		ids[i] = uint64((i * 7) % numPairs)
		values[i] = int64(i * 100)
	}

	// Sort the data by ID (BufferedWriter requires sorted input)
	sortByID(ids, values)

	// Write the data
	err = writer.BatchAdd(ids, values)
	require.NoError(t, err)

	// Close the writer (this should finalize the file)
	err = writer.Close()
	require.NoError(t, err)

	// Open the file for reading to verify the blocks
	reader, err := NewReader(filePath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify block count - should be at least 2 with our data size
	blockCount := reader.BlockCount()
	assert.GreaterOrEqual(t, blockCount, uint64(2), "Expected at least 2 blocks")
	t.Logf("Created %d blocks", blockCount)

	// Verify each block's size
	var totalItems uint32
	for i := uint64(0); i < blockCount; i++ {
		// Get the block stats
		blockStats := reader.blockIndex[i]

		// Add to total count
		totalItems += blockStats.Count

		// Log block info
		t.Logf("Block %d: count=%d, size=%d", i, blockStats.Count, blockStats.BlockSize)

		// Verify block alignment (except first block)
		if i > 0 {
			blockOffset := reader.blockIndex[i].BlockOffset
			assert.Equal(t, uint64(0), blockOffset%uint64(PageSize),
				"Block %d offset %d is not page-aligned", i, blockOffset)
		}
	}

	// Verify we have all our items
	assert.Equal(t, uint32(numPairs), totalItems, "Total items should match input count")

	// Read all the data back and verify it's sorted
	var allIDs []uint64
	var allValues []int64

	for i := uint64(0); i < blockCount; i++ {
		ids, values, err := reader.GetPairs(i)
		require.NoError(t, err)

		allIDs = append(allIDs, ids...)
		allValues = append(allValues, values...)
	}

	// Verify we got all the data back
	assert.Equal(t, numPairs, len(allIDs), "Should have read all IDs")
	assert.Equal(t, numPairs, len(allValues), "Should have read all values")

	// Verify the data is sorted by ID
	assert.True(t, isSorted(allIDs), "IDs should be sorted")
}

// Test using Add method instead of BatchAdd
func TestBufferedWriterAdd(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-buffered-writer-add-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file
	filePath := filepath.Join(tempDir, "buffered_add_test.col")

	// Create a BufferedWriter with a smaller target block size for testing
	writer, err := NewBufferedWriter(filePath,
		WithBufferedEncoding(EncodingRaw),
		WithBufferedBlockSize(16*1024)) // 16KB blocks
	require.NoError(t, err)

	// Write multiple items using Add
	const numItems = 10000
	expectedIDs := make([]uint64, 0, numItems)
	expectedValues := make([]int64, 0, numItems)

	for i := 0; i < numItems; i++ {
		id := uint64(i)
		value := int64(i * 10)

		err = writer.Add(id, value)
		require.NoError(t, err)

		expectedIDs = append(expectedIDs, id)
		expectedValues = append(expectedValues, value)
	}

	// Close the writer
	err = writer.Close()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(filePath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify total count
	var totalItems uint32
	blockCount := reader.BlockCount()
	for i := uint64(0); i < blockCount; i++ {
		totalItems += reader.blockIndex[i].Count
	}

	assert.Equal(t, uint32(numItems), totalItems, "Total items should match input count")

	// Read all data
	var allIDs []uint64
	var allValues []int64
	for i := uint64(0); i < blockCount; i++ {
		ids, values, err := reader.GetPairs(i)
		require.NoError(t, err)
		allIDs = append(allIDs, ids...)
		allValues = append(allValues, values...)
	}

	// Verify we got all the data back
	assert.Equal(t, numItems, len(allIDs), "Should have read all IDs")
	assert.Equal(t, numItems, len(allValues), "Should have read all values")

	// Verify the data is correct (should be sorted by ID)
	for i := 0; i < numItems; i++ {
		assert.Equal(t, uint64(i), allIDs[i], "ID at index %d should match", i)
		assert.Equal(t, int64(i*10), allValues[i], "Value at index %d should match", i)
	}
}

// Test with varint encoding to verify block size estimation
func TestBufferedWriterVarIntEncoding(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-buffered-writer-varint-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file
	filePath := filepath.Join(tempDir, "varint_test.col")

	// Create a BufferedWriter with varint encoding
	writer, err := NewBufferedWriter(filePath,
		WithBufferedEncoding(EncodingVarIntBoth),
		WithBufferedBlockSize(32*1024)) // 32KB
	require.NoError(t, err)

	// Test with sequential IDs with small values
	// This batch is large enough to create multiple blocks
	const numPairs = 50000
	ids := make([]uint64, numPairs)
	values := make([]int64, numPairs)

	// Sequential IDs with small values (1-100)
	for i := 0; i < numPairs; i++ {
		ids[i] = uint64(i)         // Sequential IDs
		values[i] = int64(i % 100) // Small values
	}

	// Write the data - this should create multiple blocks
	err = writer.BatchAdd(ids, values)
	require.NoError(t, err)

	// Test with sparse IDs with mixed values
	sparseIDs := make([]uint64, numPairs)
	sparseValues := make([]int64, numPairs)

	// Use a fixed seed for reproducibility
	r := rand.New(rand.NewSource(42))

	// Sparse IDs with mixed values
	for i := 0; i < numPairs; i++ {
		sparseIDs[i] = uint64(100000 + i*10) // Sparse IDs (100000, 100010, 100020, ...)

		// Mix of small, medium, and large values, some negative
		switch i % 4 {
		case 0:
			sparseValues[i] = int64(r.Intn(100)) // Small positive
		case 1:
			sparseValues[i] = int64(r.Intn(10000)) // Medium positive
		case 2:
			sparseValues[i] = int64(r.Intn(1000000)) // Large positive
		case 3:
			sparseValues[i] = -int64(r.Intn(100000)) // Negative
		}
	}

	// Write the data - this should create multiple blocks
	err = writer.BatchAdd(sparseIDs, sparseValues)
	require.NoError(t, err)

	// Close the writer
	err = writer.Close()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(filePath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify encoding type
	assert.Equal(t, EncodingVarIntBoth, reader.EncodingType(), "Encoding type should be VarIntBoth")

	// Verify block count
	blockCount := reader.BlockCount()
	assert.GreaterOrEqual(t, blockCount, uint64(6), "Expected at least 6 blocks")
	t.Logf("Created %d blocks with varint encoding", blockCount)

	// Verify each block's size and count
	var totalItems uint32
	var totalSize uint64
	for i := uint64(0); i < blockCount; i++ {
		// Get the block stats
		blockStats := reader.blockIndex[i]

		// Add to totals
		totalItems += blockStats.Count
		totalSize += uint64(blockStats.BlockSize)

		// Log block info
		t.Logf("Block %d: count=%d, size=%d", i, blockStats.Count, blockStats.BlockSize)
	}

	// Verify total items
	assert.Equal(t, uint32(numPairs*2), totalItems, "Expected correct total items")
}
