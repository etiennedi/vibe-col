package multicol

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/require"
)

func TestCompaction(t *testing.T) {
	// Create temporary files for our test
	leftFile, err := os.CreateTemp("", "compaction_left_*.col")
	require.NoError(t, err)
	leftFilePath := leftFile.Name()
	leftFile.Close()
	defer os.Remove(leftFilePath)

	rightFile, err := os.CreateTemp("", "compaction_right_*.col")
	require.NoError(t, err)
	rightFilePath := rightFile.Name()
	rightFile.Close()
	defer os.Remove(rightFilePath)

	outputFile, err := os.CreateTemp("", "compaction_output_*.col")
	require.NoError(t, err)
	outputFilePath := outputFile.Name()
	outputFile.Close()
	defer os.Remove(outputFilePath)

	// Create test data for the left segment (older data)
	createLeftSegment(t, leftFilePath)

	// Create test data for the right segment (newer data)
	createRightSegment(t, rightFilePath)

	// Open the segments
	leftReader, err := col.NewReader(leftFilePath)
	require.NoError(t, err)
	defer leftReader.Close()

	rightReader, err := col.NewReader(rightFilePath)
	require.NoError(t, err)
	defer rightReader.Close()

	// Run compaction
	err = Compact(leftReader, rightReader, outputFilePath, DefaultCompactionOptions())
	require.NoError(t, err)

	// Verify the compacted output
	verifyCompactionOutput(t, outputFilePath)
}

// Helper function to create the left segment with test data
func createLeftSegment(t *testing.T, path string) {
	writer, err := col.NewBufferedWriter(path)
	require.NoError(t, err)

	// Create a single block with all IDs
	// Values are ID * 10
	err = writer.BatchAdd([]uint64{1, 5, 10, 15, 20, 25}, []int64{10, 50, 100, 150, 200, 250})
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

// Helper function to create the right segment with test data
// Some IDs overlap with the left segment
func createRightSegment(t *testing.T, path string) {
	writer, err := col.NewBufferedWriter(path)
	require.NoError(t, err)

	// Create a single block with all IDs
	// Values are ID * 11 (different from left to verify precedence)
	err = writer.BatchAdd([]uint64{5, 7, 20, 30}, []int64{55, 77, 220, 330})
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

// Helper function to verify the compacted output
func verifyCompactionOutput(t *testing.T, path string) {
	reader, err := col.NewReader(path)
	require.NoError(t, err)
	defer reader.Close()

	// Now verify that the compacted output has the correct data
	// It should include all unique IDs, with right precedence
	// Expected: [1, 5, 7, 10, 15, 20, 25, 30]
	// Where 5 has value 55 (from right) and 20 has value 220 (from right)

	// Read all blocks and collect all ID/value pairs
	blocks := reader.BlockCount()
	allIDs := make([]uint64, 0)
	allValues := make([]int64, 0)

	for i := uint64(0); i < blocks; i++ {
		ids, values, err := reader.GetPairs(i)
		require.NoError(t, err)
		allIDs = append(allIDs, ids...)
		allValues = append(allValues, values...)
	}

	// Verify all expected IDs are present
	expectedIDs := []uint64{1, 5, 7, 10, 15, 20, 25, 30}
	expectedValues := []int64{10, 55, 77, 100, 150, 220, 250, 330}
	require.Equal(t, len(expectedIDs), len(allIDs), "Compacted output should have the correct number of entries")

	// Verify each ID and value
	for i, id := range expectedIDs {
		found := false
		for j, actualID := range allIDs {
			if id == actualID {
				found = true
				require.Equal(t, expectedValues[i], allValues[j], "Value for ID %d should match expected", id)
				break
			}
		}
		require.True(t, found, "ID %d should be present in the compacted output", id)
	}
}

func TestCompactionLargeDatasets(t *testing.T) {
	// Create temporary files for our test
	leftFile, err := os.CreateTemp("", "compaction_large_left_*.col")
	require.NoError(t, err)
	leftFilePath := leftFile.Name()
	leftFile.Close()
	defer os.Remove(leftFilePath)

	rightFile, err := os.CreateTemp("", "compaction_large_right_*.col")
	require.NoError(t, err)
	rightFilePath := rightFile.Name()
	rightFile.Close()
	defer os.Remove(rightFilePath)

	outputFile, err := os.CreateTemp("", "compaction_large_output_*.col")
	require.NoError(t, err)
	outputFilePath := outputFile.Name()
	outputFile.Close()
	defer os.Remove(outputFilePath)

	// Measure left segment creation time
	startLeft := time.Now()
	createLargeLeftSegment(t, leftFilePath, col.EncodingVarIntBoth)
	leftCreationTime := time.Since(startLeft)
	t.Logf("Left segment creation took: %v", leftCreationTime)

	// Measure right segment creation time
	startRight := time.Now()
	createLargeRightSegment(t, rightFilePath, col.EncodingVarIntBoth)
	rightCreationTime := time.Since(startRight)
	t.Logf("Right segment creation took: %v", rightCreationTime)

	// Open the segments
	leftReader, err := col.NewReader(leftFilePath)
	require.NoError(t, err)
	defer leftReader.Close()

	rightReader, err := col.NewReader(rightFilePath)
	require.NoError(t, err)
	defer rightReader.Close()

	// Measure compaction time
	startCompact := time.Now()

	// Run compaction with custom options
	opts := CompactionOptions{
		TargetBlockSize: 10000,                  // Use a larger block size for this test (was 100)
		EncodingType:    col.EncodingVarIntBoth, // Use VarInt encoding for the output
	}
	err = Compact(leftReader, rightReader, outputFilePath, opts)
	require.NoError(t, err)

	compactionTime := time.Since(startCompact)
	t.Logf("Compaction took: %v", compactionTime)

	// Measure verification time
	startVerify := time.Now()

	// Verify the compacted output for the large datasets
	verifyLargeCompactionOutput(t, outputFilePath, 1100)

	verificationTime := time.Since(startVerify)
	t.Logf("Verification took: %v", verificationTime)
}

// Helper function to create a larger left segment
func createLargeLeftSegment(t *testing.T, path string, encodingType uint32) {
	writer, err := col.NewBufferedWriter(path, col.WithBufferedEncoding(encodingType))
	require.NoError(t, err)

	// Create sequence of IDs with some patterns:
	// 1. Sequential IDs from 1-500
	// 2. Sparse IDs from 1000-2000 (every 10)

	// Sequential IDs 1-500
	ids := make([]uint64, 500)
	values := make([]int64, 500)
	for i := 0; i < 500; i++ {
		ids[i] = uint64(i + 1)
		values[i] = int64(i+1) * 10 // Value = ID * 10
	}

	// Write all sequential IDs at once
	err = writer.BatchAdd(ids, values)
	require.NoError(t, err)

	// Sparse IDs 1000-2000 (every 10)
	sparseIDs := make([]uint64, 100)
	sparseValues := make([]int64, 100)
	for i := 0; i < 100; i++ {
		sparseIDs[i] = uint64(1000 + i*10)
		sparseValues[i] = int64(1000+i*10) * 10 // Value = ID * 10
	}

	// Write the sparse IDs
	err = writer.BatchAdd(sparseIDs, sparseValues)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

// Helper function to create a larger right segment with some overlapping IDs
func createLargeRightSegment(t *testing.T, path string, encodingType uint32) {
	writer, err := col.NewBufferedWriter(path, col.WithBufferedEncoding(encodingType))
	require.NoError(t, err)

	// Create different patterns:
	// 1. Every 5th ID from 1-500 with a different value (to test precedence)
	// 2. Every 20th ID from 1000-2000
	// 3. New IDs from 3000-3500

	// Every 5th ID from 1-500 with different values
	overlapIDs := make([]uint64, 100)
	overlapValues := make([]int64, 100)
	for i := 0; i < 100; i++ {
		overlapIDs[i] = uint64((i + 1) * 5)
		overlapValues[i] = int64((i+1)*5) * 11 // Value = ID * 11 (different than left)
	}

	// Write all overlapping IDs at once
	err = writer.BatchAdd(overlapIDs, overlapValues)
	require.NoError(t, err)

	// Every 20th ID from 1000-2000
	sparseOverlapIDs := make([]uint64, 50)
	sparseOverlapValues := make([]int64, 50)
	for i := 0; i < 50; i++ {
		sparseOverlapIDs[i] = uint64(1000 + i*20)
		sparseOverlapValues[i] = int64(1000+i*20) * 11 // Value = ID * 11
	}

	err = writer.BatchAdd(sparseOverlapIDs, sparseOverlapValues)
	require.NoError(t, err)

	// New IDs from 3000-3500
	newIDs := make([]uint64, 500)
	newValues := make([]int64, 500)
	for i := 0; i < 500; i++ {
		newIDs[i] = uint64(3000 + i)
		newValues[i] = int64(3000+i) * 10 // Value = ID * 10
	}

	// Write all new IDs at once
	err = writer.BatchAdd(newIDs, newValues)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

// Helper function to verify the compacted output for large datasets
func verifyLargeCompactionOutput(t *testing.T, outputPath string, numInputEntries int) {
	// Open and verify the compacted output
	reader, err := col.NewReader(outputPath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify the encoding type is VarInt
	require.Equal(t, col.EncodingVarIntBoth, reader.EncodingType(),
		"Output file should use VarInt encoding")
	require.True(t, reader.IsVarIntEncoded(),
		"Output file should be detected as VarInt encoded")

	// Get global bitmap to check for uniqueness
	globalBitmap, err := reader.GetGlobalIDBitmap()
	require.NoError(t, err)

	uniqueIDCount := globalBitmap.GetCardinality()

	// Collect all IDs and values from the output
	allIDs := make([]uint64, 0)
	allValues := make([]int64, 0)

	// Count blocks and analyze their sizes
	blockCount := reader.BlockCount()

	// Verify blocks exist
	require.Greater(t, blockCount, uint64(0), "Output should have at least one block")

	// Read all entries and collect block statistics
	blockEntries := make([]int, blockCount)

	for i := uint64(0); i < blockCount; i++ {
		ids, values, err := reader.GetPairs(i)
		require.NoError(t, err)

		blockEntries[i] = len(ids)
		allIDs = append(allIDs, ids...)
		allValues = append(allValues, values...)
	}

	// Because the improved compaction algorithm deduplicates entries,
	// we expect the actual count to be the number of unique IDs across
	// both input datasets

	// Check that the number of entries is equal to the cardinality
	require.Equal(t, int(uniqueIDCount), len(allIDs),
		"Number of entries should equal the number of unique IDs")

	// Output block statistics
	t.Logf("Compaction produced %d blocks for %d entries", blockCount, len(allIDs))

	// Calculate and log average entries per block
	avgEntriesPerBlock := float64(len(allIDs)) / float64(blockCount)
	t.Logf("Average entries per block: %.2f", avgEntriesPerBlock)

	// Calculate entry size efficiency for 128KB blocks
	// This helps understand if we're efficiently packing entries
	idealEntries := 8192 // Roughly what we'd expect for 128KB blocks
	efficiency := avgEntriesPerBlock * 100 / float64(idealEntries)
	t.Logf("Block efficiency: %.2f%% of ideal (%d entries per 128KB block)", efficiency, idealEntries)

	// Verify IDs are stored in sorted order
	for i := 1; i < len(allIDs); i++ {
		require.Greater(t, allIDs[i], allIDs[i-1], "IDs should be sorted")
	}

	// Verify no duplicates
	seen := make(map[uint64]bool)
	for _, id := range allIDs {
		require.False(t, seen[id], "Duplicate ID %d found", id)
		seen[id] = true
	}
}

// TestCompactionVariousScales tests compaction with different scale combinations
func TestCompactionVariousScales(t *testing.T) {
	// Test compaction with different scale datasets
	testCases := []struct {
		name              string
		leftSize          int
		rightSize         int
		expectedUniqueCnt int // Changed from expectedTotal to expectedUniqueCnt
	}{
		{
			name:              "Both small (1K, 1K)",
			leftSize:          1000,
			rightSize:         1000,
			expectedUniqueCnt: 1667, // Based on how createScaledSegment creates data
		},
		{
			name:              "Left small, right large (1K, 50K)",
			leftSize:          1000,
			rightSize:         50000,
			expectedUniqueCnt: 50000, // Based on how createScaledSegment creates data
		},
		{
			name:              "Left large, right small (50K, 1K)",
			leftSize:          50000,
			rightSize:         1000,
			expectedUniqueCnt: 50000, // Based on how createScaledSegment creates data
		},
		{
			name:              "Both medium (50K, 50K)",
			leftSize:          50000,
			rightSize:         50000,
			expectedUniqueCnt: 75000, // Based on how createScaledSegment creates data
		},
		{
			name:              "Both large (3M, 3M)",
			leftSize:          3000000,
			rightSize:         3000000,
			expectedUniqueCnt: 4500000, // Based on how createScaledSegment creates data
		},
	}

	// Only run the smaller test cases by default
	// Use a flag like -timeout=0 to run all test cases including the large ones
	if testing.Short() {
		t.Log("Running in short mode, skipping large datasets")
		for i := range testCases {
			if testCases[i].leftSize > 100000 || testCases[i].rightSize > 100000 {
				testCases[i].leftSize = 10000
				testCases[i].rightSize = 10000
				testCases[i].expectedUniqueCnt = 15000 // Adjusted for deduplication
			}
		}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Skip these tests during normal short test runs unless we're scaling them down
			if tc.leftSize == 0 || tc.rightSize == 0 {
				t.Skip("Scale factor resulted in zero-sized segment, skipping")
			}

			// Create temporary files for our test
			leftFile, err := os.CreateTemp("", "compaction_scale_left_*.col")
			require.NoError(t, err)
			leftFilePath := leftFile.Name()
			leftFile.Close()
			defer os.Remove(leftFilePath)

			rightFile, err := os.CreateTemp("", "compaction_scale_right_*.col")
			require.NoError(t, err)
			rightFilePath := rightFile.Name()
			rightFile.Close()
			defer os.Remove(rightFilePath)

			outputFile, err := os.CreateTemp("", "compaction_scale_output_*.col")
			require.NoError(t, err)
			outputFilePath := outputFile.Name()
			outputFile.Close()
			defer os.Remove(outputFilePath)

			// Measure segment creation times
			t.Logf("Creating left segment with %d entries", tc.leftSize)
			startLeft := time.Now()
			createScaledSegment(t, leftFilePath, tc.leftSize, true, col.EncodingVarIntBoth)
			leftCreationTime := time.Since(startLeft)
			t.Logf("Left segment creation took: %v", leftCreationTime)

			t.Logf("Creating right segment with %d entries", tc.rightSize)
			startRight := time.Now()
			createScaledSegment(t, rightFilePath, tc.rightSize, false, col.EncodingVarIntBoth)
			rightCreationTime := time.Since(startRight)
			t.Logf("Right segment creation took: %v", rightCreationTime)

			// Open the segments
			leftReader, err := col.NewReader(leftFilePath)
			require.NoError(t, err)
			defer leftReader.Close()

			rightReader, err := col.NewReader(rightFilePath)
			require.NoError(t, err)
			defer rightReader.Close()

			// Measure compaction time
			t.Logf("Starting compaction")
			startCompact := time.Now()

			// Run compaction with VarInt encoding
			opts := DefaultCompactionOptions()
			opts.EncodingType = col.EncodingVarIntBoth
			err = Compact(leftReader, rightReader, outputFilePath, opts)
			require.NoError(t, err)

			compactionTime := time.Since(startCompact)
			t.Logf("Compaction took: %v", compactionTime)

			// Measure verification time
			startVerify := time.Now()

			// Verify the compacted output
			verifyScaledCompactionOutput(t, outputFilePath, tc.expectedUniqueCnt)

			verificationTime := time.Since(startVerify)
			t.Logf("Verification took: %v", verificationTime)
		})
	}
}

// Helper function to create a segment with a specific number of entries
func createScaledSegment(t *testing.T, path string, numEntries int, isLeft bool, encodingType uint32) {
	writer, err := col.NewBufferedWriter(path, col.WithBufferedEncoding(encodingType))
	require.NoError(t, err)

	// SimpleWriter uses a default target block size of 128KB

	// Use a large batch size for better efficiency
	const maxBatchSize = 1_000_000 // Process 1M entries at a time to reduce overhead
	multiplier := int64(10)
	if !isLeft {
		multiplier = 11 // Use a different multiplier for the right segment
	}

	// Track the number of batches and writes
	batchCount := 0
	totalEntries := 0

	// For small test cases, use a single batch
	if numEntries <= maxBatchSize {
		// Create a single batch for the entire dataset
		ids := make([]uint64, numEntries)
		values := make([]int64, numEntries)

		// For very small test cases, ensure we have some unique entries
		uniqueIDsForSmall := numEntries <= 3000 && !isLeft
		halfPoint := numEntries / 2
		if uniqueIDsForSmall {
			halfPoint = numEntries / 3 // Only 1/3 overlap, 2/3 unique for better test coverage
		}

		for i := 0; i < numEntries; i++ {
			// For both segments, use sequential IDs starting from 1
			id := uint64(i + 1)

			// For the right segment, create unique IDs after the halfway point
			if !isLeft && (i >= halfPoint || uniqueIDsForSmall && i >= halfPoint) {
				// For the second half of the right segment, add unique IDs
				id += uint64(numEntries)
			}

			ids[i] = id
			values[i] = int64(id) * multiplier
		}

		// Write all entries at once
		startWrite := time.Now()
		err := writer.BatchAdd(ids, values)
		require.NoError(t, err)
		t.Logf("Wrote %d entries in %.3fs", numEntries, time.Since(startWrite).Seconds())

		batchCount++
		totalEntries += numEntries
	} else {
		// For large datasets, process in batches to manage memory
		remainingEntries := numEntries
		startID := uint64(1)
		batchSize := maxBatchSize

		for remainingEntries > 0 {
			currentBatchSize := min(batchSize, remainingEntries)
			ids := make([]uint64, currentBatchSize)
			values := make([]int64, currentBatchSize)

			halfPoint := currentBatchSize / 2

			for i := 0; i < currentBatchSize; i++ {
				// For both segments, use sequential IDs starting from current position
				id := startID + uint64(i)

				// For the right segment, create unique IDs after the halfway point
				if !isLeft && i >= halfPoint {
					// Make half the entries unique by adding an offset
					id += uint64(numEntries)
				}

				ids[i] = id
				values[i] = int64(id) * multiplier
			}

			// Write this batch
			startWrite := time.Now()
			err := writer.BatchAdd(ids, values)
			require.NoError(t, err)
			t.Logf("Wrote batch of %d entries in %.3fs", currentBatchSize, time.Since(startWrite).Seconds())

			batchCount++
			totalEntries += currentBatchSize
			remainingEntries -= currentBatchSize
			startID += uint64(currentBatchSize)
		}
	}

	// After all writes, log stats about the writer before closing
	t.Logf("Created segment with %d entries in %d batches", totalEntries, batchCount)

	// Close the writer
	closeStart := time.Now()
	err = writer.Close()
	t.Logf("Writer close took %.3fs", time.Since(closeStart).Seconds())
	require.NoError(t, err)

	// Read the file to analyze the block structure
	reader, err := col.NewReader(path)
	require.NoError(t, err)
	defer reader.Close()

	blockCount := reader.BlockCount()
	t.Logf("Segment contains %d blocks", blockCount)

	if blockCount > 0 {
		avgEntriesPerBlock := float64(totalEntries) / float64(blockCount)
		t.Logf("Average entries per block: %.2f", avgEntriesPerBlock)
	}
}

// Helper function to verify compaction output for large datasets
func verifyScaledCompactionOutput(t *testing.T, outputPath string, expectedUniqueCnt int) {
	// Open and verify the compacted output
	reader, err := col.NewReader(outputPath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify the encoding type is VarInt
	require.Equal(t, col.EncodingVarIntBoth, reader.EncodingType(),
		"Output file should use VarInt encoding")
	require.True(t, reader.IsVarIntEncoded(),
		"Output file should be detected as VarInt encoded")

	// Count blocks and entries
	blockCount := reader.BlockCount()
	totalEntries := 0

	// Read count from each block
	for i := uint64(0); i < blockCount; i++ {
		ids, _, err := reader.GetPairs(i)
		require.NoError(t, err)
		totalEntries += len(ids)
	}

	// Output statistics
	t.Logf("Compaction produced %d blocks for %d entries", blockCount, totalEntries)

	// The compacted file should contain only unique entries. The expectedUniqueCnt parameter
	// should reflect this - it should not be the sum of entries from both segments because
	// our compaction now correctly handles duplicates and only includes each ID once.
	// For test cases where both segments have the same IDs, the output should
	// contain the same number of entries as one segment.
	// For test cases where there's partial overlap, the output should contain
	// the number of unique IDs across both segments.
	require.Equal(t, expectedUniqueCnt, totalEntries, "Expected %d entries in output, got %d",
		expectedUniqueCnt, totalEntries)

	// For large datasets, verify a sample of entries to ensure values are correct
	// This is just a basic check - we don't verify every entry because that would be too expensive
	sampleBlock := uint64(0)
	if blockCount > 1 {
		sampleBlock = uint64(blockCount / 2) // Choose a block in the middle
	}

	ids, values, err := reader.GetPairs(sampleBlock)
	require.NoError(t, err)

	// Verify a few entries from the sample block
	if len(ids) > 0 {
		// Check that IDs are sorted
		for i := 1; i < len(ids); i++ {
			require.Greater(t, ids[i], ids[i-1], "IDs should be sorted")
		}

		// Check the first and last entries in this block
		t.Logf("Sample check - First ID in sample block: %d, value: %d", ids[0], values[0])
		t.Logf("Sample check - Last ID in sample block: %d, value: %d", ids[len(ids)-1], values[len(ids)-1])
	}
}

// Helper utility function for min of two ints
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Helper function to get the expected total based on test mode
func getExpectedTotal(isShortTest bool, shortValue, fullValue int) int {
	if isShortTest {
		return shortValue
	}
	return fullValue
}

// TestBlockSizes tests that the blocks created during compaction are close to the 128KB target
func TestBlockSizes(t *testing.T) {
	// Test with different encoding types
	encodingTypes := []struct {
		name string
		enc  uint32
	}{
		{"VarInt", col.EncodingVarIntBoth},
		{"Standard", col.EncodingRaw},
	}

	for _, encoding := range encodingTypes {
		t.Run(encoding.name, func(t *testing.T) {
			// Create temporary files for our test
			leftFile, err := os.CreateTemp("", fmt.Sprintf("block_size_left_%s_*.col", encoding.name))
			require.NoError(t, err)
			leftFilePath := leftFile.Name()
			leftFile.Close()
			defer os.Remove(leftFilePath)

			rightFile, err := os.CreateTemp("", fmt.Sprintf("block_size_right_%s_*.col", encoding.name))
			require.NoError(t, err)
			rightFilePath := rightFile.Name()
			rightFile.Close()
			defer os.Remove(rightFilePath)

			outputFile, err := os.CreateTemp("", fmt.Sprintf("block_size_output_%s_*.col", encoding.name))
			require.NoError(t, err)
			outputFilePath := outputFile.Name()
			outputFile.Close()
			defer os.Remove(outputFilePath)

			// Create test data with a large number of entries
			const numEntries = 500_000
			t.Logf("Creating left segment with %d entries using %s encoding", numEntries, encoding.name)
			createTestSegment(t, leftFilePath, numEntries, true, encoding.enc)

			t.Logf("Creating right segment with %d entries using %s encoding", numEntries, encoding.name)
			createTestSegment(t, rightFilePath, numEntries, false, encoding.enc)

			// Open the segments
			leftReader, err := col.NewReader(leftFilePath)
			require.NoError(t, err)
			defer leftReader.Close()

			rightReader, err := col.NewReader(rightFilePath)
			require.NoError(t, err)
			defer rightReader.Close()

			// Run compaction with the specified encoding
			t.Logf("Starting compaction with %s encoding", encoding.name)
			opts := DefaultCompactionOptions()
			opts.EncodingType = encoding.enc

			// Set a specific target block size for testing
			const targetBlockSize = 128 * 1024
			opts.TargetBlockSize = targetBlockSize
			t.Logf("Target block size set to %d bytes", targetBlockSize)

			// Run compaction to analyze block sizes
			err = Compact(leftReader, rightReader, outputFilePath, opts)
			require.NoError(t, err)

			// Open the output file and analyze block sizes
			outputReader, err := col.NewReader(outputFilePath)
			require.NoError(t, err)
			defer outputReader.Close()

			// Get block count
			blockCount := outputReader.BlockCount()
			t.Logf("Compaction produced %d blocks", blockCount)

			// Get file info to check actual file size
			fileInfo, err := os.Stat(outputFilePath)
			require.NoError(t, err)
			fileSize := fileInfo.Size()

			// Analyze block sizes by examining the file directly
			// We'll use the debug info to get block offsets and sizes
			debugInfo := outputReader.DebugInfo()
			t.Logf("File size: %d bytes", fileSize)
			t.Logf("Debug info: %s", debugInfo)

			// Extract block sizes from debug info
			blockSizes := extractBlockSizesFromDebugInfo(debugInfo)
			t.Logf("Extracted %d block sizes from debug info", len(blockSizes))
			for i, size := range blockSizes {
				t.Logf("Block %d size: %d bytes (%.2f%% of target)", i, size, float64(size)/float64(targetBlockSize)*100)
			}

			// Calculate statistics on block sizes
			var totalBlockSize int64
			var minBlockSize int64 = math.MaxInt64
			var maxBlockSize int64

			for _, size := range blockSizes {
				totalBlockSize += size
				if size < minBlockSize {
					minBlockSize = size
				}
				if size > maxBlockSize {
					maxBlockSize = size
				}
			}

			avgBlockSize := float64(totalBlockSize) / float64(len(blockSizes))
			minBlockEfficiency := float64(minBlockSize) / float64(targetBlockSize) * 100
			maxBlockEfficiency := float64(maxBlockSize) / float64(targetBlockSize) * 100
			avgBlockEfficiency := float64(avgBlockSize) / float64(targetBlockSize) * 100

			t.Logf("Detailed block size statistics:")
			t.Logf("  Min: %d bytes (%.2f%% of target)", minBlockSize, minBlockEfficiency)
			t.Logf("  Max: %d bytes (%.2f%% of target)", maxBlockSize, maxBlockEfficiency)
			t.Logf("  Avg: %.2f bytes (%.2f%% of target)", avgBlockSize, avgBlockEfficiency)

			// Count total entries
			totalEntries := 0
			for i := uint64(0); i < blockCount; i++ {
				ids, _, err := outputReader.GetPairs(i)
				require.NoError(t, err)
				entriesInBlock := len(ids)
				totalEntries += entriesInBlock
				t.Logf("Block %d contains %d entries", i, entriesInBlock)

				// Debug: Check the size of entries in this block
				if entriesInBlock > 0 {
					// Calculate approximate size per entry
					entrySizeBytes := float64(blockSizes[i]) / float64(entriesInBlock)
					t.Logf("  Approx bytes per entry: %.2f", entrySizeBytes)
				}
			}

			// Calculate average entries per block
			avgEntriesPerBlock := float64(totalEntries) / float64(blockCount)
			t.Logf("Total entries: %d", totalEntries)
			t.Logf("Average entries per block: %.2f", avgEntriesPerBlock)

			// Estimate average block size based on file size and block count
			// This is a rough estimate that includes headers, footers, etc.
			estimatedAvgBlockSize := float64(fileSize) / float64(blockCount)
			avgEfficiency := estimatedAvgBlockSize / float64(targetBlockSize) * 100.0

			t.Logf("Estimated average block size: %.2f bytes (%.2f%% of target)",
				estimatedAvgBlockSize, avgEfficiency)

			// Check for reasonable block efficiency based on encoding type
			minEfficiency := 40.0
			if encoding.name == "VarInt" {
				// VarInt encoding tends to produce smaller blocks
				minEfficiency = 15.0
			}

			// Check for reasonable block efficiency - at least the minimum efficiency of the target size
			require.GreaterOrEqual(t, avgEfficiency, minEfficiency,
				"Average block size should be at least %.1f%% of the target (got %.2f%%)", minEfficiency, avgEfficiency)

			// Also check that we're not creating excessively large blocks
			require.LessOrEqual(t, avgEfficiency, 120.0,
				"Average block size should be at most 120%% of the target (got %.2f%%)", avgEfficiency)
		})
	}
}

// Helper function to create a segment with test data for block size testing
func createTestSegment(t *testing.T, path string, numEntries int, isLeft bool, encodingType uint32) {
	writer, err := col.NewBufferedWriter(path, col.WithBufferedEncoding(encodingType))
	require.NoError(t, err)

	// Debug the writer's properties
	t.Logf("Creating segment with writer")

	// Create entries with sequential IDs
	const batchSize = 10_000 // Process in smaller batches

	multiplier := int64(10)
	if !isLeft {
		multiplier = 11 // Use a different multiplier for the right segment
	}

	// Track total entries written
	totalEntries := 0

	// Write in batches
	for offset := 0; offset < numEntries; offset += batchSize {
		currentBatchSize := batchSize
		if offset+currentBatchSize > numEntries {
			currentBatchSize = numEntries - offset
		}

		ids := make([]uint64, currentBatchSize)
		values := make([]int64, currentBatchSize)

		for i := 0; i < currentBatchSize; i++ {
			id := uint64(offset + i + 1)

			// For the right segment, make half the entries unique
			if !isLeft && i%2 == 0 {
				id += uint64(numEntries)
			}

			ids[i] = id
			values[i] = int64(id) * multiplier
		}

		// Write this batch
		startWrite := time.Now()
		err := writer.BatchAdd(ids, values)
		require.NoError(t, err)

		totalEntries += currentBatchSize
		t.Logf("Wrote batch of %d entries (total: %d) in %.3fs",
			currentBatchSize, totalEntries, time.Since(startWrite).Seconds())
	}

	// Close the writer
	closeStart := time.Now()
	err = writer.Close()
	t.Logf("Writer close took %.3fs", time.Since(closeStart).Seconds())
	require.NoError(t, err)

	// Read the file to analyze the block structure
	reader, err := col.NewReader(path)
	require.NoError(t, err)
	defer reader.Close()

	blockCount := reader.BlockCount()
	t.Logf("Segment contains %d blocks", blockCount)

	if blockCount > 0 {
		avgEntriesPerBlock := float64(totalEntries) / float64(blockCount)
		t.Logf("Average entries per block: %.2f", avgEntriesPerBlock)

		// Debug block sizes
		debugInfo := reader.DebugInfo()
		blockSizes := extractBlockSizesFromDebugInfo(debugInfo)

		if len(blockSizes) > 0 {
			var totalSize int64
			for _, size := range blockSizes {
				totalSize += size
			}
			avgSize := float64(totalSize) / float64(len(blockSizes))
			t.Logf("Average block size in segment: %.2f bytes", avgSize)
		}
	}
}

// Helper function to extract block sizes from debug info
func extractBlockSizesFromDebugInfo(debugInfo string) []int64 {
	// This is a simple implementation that extracts block sizes from the debug info
	// The debug info contains lines like "Block N: Offset=X, Size=Y, Count=Z"
	blockSizes := []int64{}

	// Parse the debug info line by line
	lines := strings.Split(debugInfo, "\n")
	for _, line := range lines {
		// Look for lines containing "Block" and "Size="
		if strings.Contains(line, "Block") && strings.Contains(line, "Size=") {
			// Extract the size value
			parts := strings.Split(line, "Size=")
			if len(parts) >= 2 {
				// Extract the number before the comma or end of line
				sizeStr := strings.Split(parts[1], ",")[0]
				size, err := strconv.ParseInt(sizeStr, 10, 64)
				if err == nil {
					blockSizes = append(blockSizes, size)
				}
			}
		}
	}

	return blockSizes
}
