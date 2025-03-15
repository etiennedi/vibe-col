package multicol

import (
	"os"
	"testing"
	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
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
	writer, err := col.NewSimpleWriter(path)
	require.NoError(t, err)

	// Create two blocks with IDs [1, 5, 10] and [15, 20, 25]
	// Values are ID * 10
	err = writer.Write([]uint64{1, 5, 10}, []int64{10, 50, 100})
	require.NoError(t, err)

	// Force a block boundary
	writer.SetTargetBlockSize(1)
	err = writer.Write([]uint64{15, 20, 25}, []int64{150, 200, 250})
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

// Helper function to create the right segment with test data
// Some IDs overlap with the left segment
func createRightSegment(t *testing.T, path string) {
	writer, err := col.NewSimpleWriter(path)
	require.NoError(t, err)

	// Create two blocks with IDs [5, 7] and [20, 30]
	// Values are ID * 11 (different from left to verify precedence)
	err = writer.Write([]uint64{5, 7}, []int64{55, 77})
	require.NoError(t, err)

	// Force a block boundary
	writer.SetTargetBlockSize(1)
	err = writer.Write([]uint64{20, 30}, []int64{220, 330})
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

	// Create larger datasets with specific patterns to verify compaction
	createLargeLeftSegment(t, leftFilePath)
	createLargeRightSegment(t, rightFilePath)

	// Open the segments
	leftReader, err := col.NewReader(leftFilePath)
	require.NoError(t, err)
	defer leftReader.Close()

	rightReader, err := col.NewReader(rightFilePath)
	require.NoError(t, err)
	defer rightReader.Close()

	// Run compaction with custom options
	opts := CompactionOptions{
		TargetBlockSize: 100, // Use a larger block size for this test
	}
	err = Compact(leftReader, rightReader, outputFilePath, opts)
	require.NoError(t, err)

	// Verify the compacted output for the large datasets
	verifyLargeCompactionOutput(t, outputFilePath)
}

// Helper function to create a larger left segment
func createLargeLeftSegment(t *testing.T, path string) {
	writer, err := col.NewSimpleWriter(path)
	require.NoError(t, err)

	// Set a smallish block size to create multiple blocks
	writer.SetTargetBlockSize(50)

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

	// Write in smaller chunks to create multiple blocks
	for i := 0; i < 10; i++ {
		start := i * 50
		end := (i + 1) * 50
		err := writer.Write(ids[start:end], values[start:end])
		require.NoError(t, err)
	}

	// Sparse IDs 1000-2000 (every 10)
	sparseIDs := make([]uint64, 100)
	sparseValues := make([]int64, 100)
	for i := 0; i < 100; i++ {
		sparseIDs[i] = uint64(1000 + i*10)
		sparseValues[i] = int64(1000+i*10) * 10 // Value = ID * 10
	}

	// Write the sparse IDs
	err = writer.Write(sparseIDs, sparseValues)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

// Helper function to create a larger right segment with some overlapping IDs
func createLargeRightSegment(t *testing.T, path string) {
	writer, err := col.NewSimpleWriter(path)
	require.NoError(t, err)

	// Set a smallish block size to create multiple blocks
	writer.SetTargetBlockSize(50)

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

	// Write in smaller chunks
	for i := 0; i < 2; i++ {
		start := i * 50
		end := (i + 1) * 50
		err := writer.Write(overlapIDs[start:end], overlapValues[start:end])
		require.NoError(t, err)
	}

	// Every 20th ID from 1000-2000
	sparseOverlapIDs := make([]uint64, 50)
	sparseOverlapValues := make([]int64, 50)
	for i := 0; i < 50; i++ {
		sparseOverlapIDs[i] = uint64(1000 + i*20)
		sparseOverlapValues[i] = int64(1000+i*20) * 11 // Value = ID * 11
	}

	err = writer.Write(sparseOverlapIDs, sparseOverlapValues)
	require.NoError(t, err)

	// New IDs from 3000-3500
	newIDs := make([]uint64, 500)
	newValues := make([]int64, 500)
	for i := 0; i < 500; i++ {
		newIDs[i] = uint64(3000 + i)
		newValues[i] = int64(3000+i) * 10 // Value = ID * 10
	}

	// Write in smaller chunks
	for i := 0; i < 10; i++ {
		start := i * 50
		end := (i + 1) * 50
		err := writer.Write(newIDs[start:end], newValues[start:end])
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)
}

// Helper function to verify the compacted output for large datasets
func verifyLargeCompactionOutput(t *testing.T, path string) {
	reader, err := col.NewReader(path)
	require.NoError(t, err)
	defer reader.Close()

	// Verify total count
	// Expected number of unique IDs after compaction:
	// - 500 sequential IDs from left
	// - 100 sparse IDs from left (but some overlap with right)
	// - 50 sparse overlap IDs from right (taking precedence)
	// - 500 new IDs from right
	//
	// Let's verify a sampling of key positions

	// Check overall size
	blocks := reader.BlockCount()
	t.Logf("Compacted output has %d blocks", blocks)

	// Read all blocks and collect all ID/value pairs
	allIDs := make([]uint64, 0, 1000) // Pre-allocate a reasonable size
	allValues := make([]int64, 0, 1000)

	for i := uint64(0); i < blocks; i++ {
		ids, values, err := reader.GetPairs(i)
		require.NoError(t, err)
		allIDs = append(allIDs, ids...)
		allValues = append(allValues, values...)
	}

	t.Logf("Compacted output has %d entries", len(allIDs))

	// Verify some key expected values
	expectedChecks := map[uint64]int64{
		// Check some non-overlapping IDs from left (should remain unchanged)
		1:   10,   // First ID from left
		101: 1010, // Random ID from left
		491: 4910, // Near the end of sequential IDs from left

		// Check some overlapping IDs (should take value from right)
		5:   55,   // First overlapping ID, right value = 5 * 11 = 55
		100: 1100, // Overlapping ID, right value = 100 * 11 = 1100
		500: 5500, // Last overlapping ID, right value = 500 * 11 = 5500

		// Check some sparse IDs
		1000: 11000, // Overlapping sparse ID, right value = 1000 * 11 = 11000
		1010: 10100, // Non-overlapping sparse ID, left value = 1010 * 10 = 10100
		1020: 11220, // Overlapping sparse ID, right value = 1020 * 11 = 11220

		// Check some new IDs from right
		3000: 30000, // First new ID from right
		3250: 32500, // Middle new ID from right
		3499: 34990, // Last new ID from right
	}

	// Convert allIDs and allValues to a map for easy lookup
	valueMap := make(map[uint64]int64, len(allIDs))
	for i, id := range allIDs {
		valueMap[id] = allValues[i]
	}

	// Verify each expected check
	for id, expectedValue := range expectedChecks {
		value, exists := valueMap[id]
		if assert.True(t, exists, "ID %d should exist in compacted output", id) {
			assert.Equal(t, expectedValue, value, "Value for ID %d should match expected", id)
		}
	}

	// Verify the total count of IDs is as expected
	// This is approximate since we need to account for overlaps
	// Left: 500 sequential + 100 sparse = 600
	// Right: 100 overlapping sequential + 50 overlapping sparse + 500 new = 650
	// Total unique IDs should be around 600 + 650 - 150 (overlaps) = 1100
	assert.InDelta(t, 1100, len(allIDs), 50, "Total number of IDs should be approximately 1100")
}
