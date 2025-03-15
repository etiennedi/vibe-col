package col

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleWriter(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-simple-writer-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file
	filePath := filepath.Join(tempDir, "simple_test.col")

	// Create a SimpleWriter
	writer, err := NewSimpleWriter(filePath, WithEncoding(EncodingRaw))
	require.NoError(t, err)

	// Set a smaller target block size for testing
	writer.targetBlockSize = 32 * 1024 // 32KB instead of 128KB

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

	// Write the data
	err = writer.Write(ids, values)
	require.NoError(t, err)

	// Check IsClosed before closing
	assert.False(t, writer.IsClosed(), "Writer should not be closed yet")

	// Close the writer (this should finalize the file)
	err = writer.Close()
	require.NoError(t, err)

	// Verify writer is now closed
	assert.True(t, writer.IsClosed(), "Writer should be closed after Close()")

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
			assert.Equal(t, uint64(0), blockStats.BlockOffset%uint64(PageSize),
				"Block %d offset should be page-aligned", i)
		}
	}

	// Verify we have all our items
	assert.Equal(t, uint32(numPairs), totalItems, "Total items should match input count")
	assert.Equal(t, uint64(numPairs), writer.TotalItems(), "Writer's total items should match input count")

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

	// Verify the data matches what we wrote (after sorting)
	// First, sort our original data for comparison
	originalIDs := make([]uint64, numPairs)
	originalValues := make([]int64, numPairs)
	copy(originalIDs, ids)
	copy(originalValues, values)
	sortByID(originalIDs, originalValues)

	// Now compare
	for i := 0; i < numPairs; i++ {
		assert.Equal(t, originalIDs[i], allIDs[i], "ID at index %d should match", i)
		assert.Equal(t, originalValues[i], allValues[i], "Value at index %d should match", i)
	}
}

// Test with small batches of writes
func TestSimpleWriterMultipleBatches(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-simple-writer-batches-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file
	filePath := filepath.Join(tempDir, "batches_test.col")

	// Create a SimpleWriter with a smaller target block size for testing
	writer, err := NewSimpleWriter(filePath, WithEncoding(EncodingRaw))
	require.NoError(t, err)

	// Set a smaller target block size for testing
	writer.targetBlockSize = 16 * 1024 // 16KB instead of 128KB

	// Write multiple small batches
	const batchSize = 1000
	const numBatches = 10

	for b := 0; b < numBatches; b++ {
		ids := make([]uint64, batchSize)
		values := make([]int64, batchSize)

		// Fill with data for this batch
		for i := 0; i < batchSize; i++ {
			// Use a pattern that ensures uniqueness across batches
			ids[i] = uint64(b*batchSize + i)
			values[i] = int64((b*batchSize + i) * 100)
		}

		// Write the batch
		err = writer.Write(ids, values)
		require.NoError(t, err)
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
	for i := uint64(0); i < reader.BlockCount(); i++ {
		totalItems += reader.blockIndex[i].Count
	}

	assert.Equal(t, uint32(batchSize*numBatches), totalItems,
		"Total items should match input count")
	assert.Equal(t, uint64(batchSize*numBatches), writer.TotalItems(),
		"Writer's total items should match input count")

	// Log block info
	t.Logf("Created %d blocks with %d total items", reader.BlockCount(), totalItems)
}

// Test with varint encoding to verify block size estimation
func TestSimpleWriterVarIntEncoding(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-simple-writer-varint-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file
	filePath := filepath.Join(tempDir, "varint_test.col")

	// Create a SimpleWriter with varint encoding
	writer, err := NewSimpleWriter(filePath, WithEncoding(EncodingVarIntBoth))
	require.NoError(t, err)

	// Set a smaller target block size for testing
	writer.targetBlockSize = 32 * 1024 // 32KB instead of 128KB

	// Write a dataset with small values that will benefit from varint encoding
	// Small values will use fewer bytes in varint encoding
	const numPairs = 20000
	ids := make([]uint64, numPairs)
	values := make([]int64, numPairs)

	// Fill with small values (1-100) that will use 1-2 bytes in varint encoding
	// instead of 8 bytes in fixed encoding
	for i := 0; i < numPairs; i++ {
		ids[i] = uint64(i)         // Sequential IDs
		values[i] = int64(i % 100) // Small values
	}

	// Write the data
	err = writer.Write(ids, values)
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
	assert.GreaterOrEqual(t, blockCount, uint64(2), "Expected at least 2 blocks")
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

		// Verify block alignment (except first block)
		if i > 0 {
			assert.Equal(t, uint64(0), blockStats.BlockOffset%uint64(PageSize),
				"Block %d offset should be page-aligned", i)
		}
	}

	// Verify we have all our items
	assert.Equal(t, uint32(numPairs), totalItems, "Total items should match input count")
	assert.Equal(t, uint64(numPairs), writer.TotalItems(), "Writer's total items should match input count")

	// With varint encoding, the total size should be significantly smaller than with fixed encoding
	// Fixed encoding would be approximately:
	// numPairs * 16 bytes (8 for ID, 8 for value) + overhead
	fixedEncodingEstimate := numPairs*16 + int(blockCount)*(blockHeaderSize+blockLayoutSize)
	t.Logf("Total size with varint: %d bytes, fixed encoding estimate: %d bytes", totalSize, fixedEncodingEstimate)
	assert.Less(t, totalSize, uint64(fixedEncodingEstimate),
		"Varint encoding should result in smaller file size than fixed encoding")

	// Read all the data back and verify it matches what we wrote
	var allIDs []uint64
	var allValues []int64

	for i := uint64(0); i < blockCount; i++ {
		blockIDs, blockValues, err := reader.GetPairs(i)
		require.NoError(t, err)

		allIDs = append(allIDs, blockIDs...)
		allValues = append(allValues, blockValues...)
	}

	// Verify we got all the data back
	assert.Equal(t, numPairs, len(allIDs), "Should have read all IDs")
	assert.Equal(t, numPairs, len(allValues), "Should have read all values")

	// Verify the data matches what we wrote
	for i := 0; i < numPairs; i++ {
		assert.Equal(t, uint64(i), allIDs[i], "ID at index %d should match", i)
		assert.Equal(t, int64(i%100), allValues[i], "Value at index %d should match", i)
	}
}
