package col

import (
	"math/rand"
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
			blockOffset := reader.blockIndex[i].BlockOffset
			assert.Equal(t, uint64(0), blockOffset%uint64(PageSize),
				"Block %d offset %d is not page-aligned", i, blockOffset)
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
	err = writer.SetTargetBlockSize(32 * 1024) // 32KB instead of 128KB
	require.NoError(t, err)

	// Test with different write patterns
	t.Run("Large batch of sequential IDs with small values", func(t *testing.T) {
		// Write a dataset with sequential IDs and small values
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
		err = writer.Write(ids, values)
		require.NoError(t, err)
	})

	t.Run("Large batch of sparse IDs with mixed values", func(t *testing.T) {
		// Write a dataset with sparse IDs and mixed values
		// This batch is large enough to create multiple blocks
		const numPairs = 40000
		ids := make([]uint64, numPairs)
		values := make([]int64, numPairs)

		// Use a fixed seed for reproducibility
		r := rand.New(rand.NewSource(42))

		// Sparse IDs with mixed values
		for i := 0; i < numPairs; i++ {
			ids[i] = uint64(100000 + i*10) // Sparse IDs (100000, 100010, 100020, ...)

			// Mix of small, medium, and large values, some negative
			switch i % 4 {
			case 0:
				values[i] = int64(r.Intn(100)) // Small positive
			case 1:
				values[i] = int64(r.Intn(10000)) // Medium positive
			case 2:
				values[i] = int64(r.Intn(1000000)) // Large positive
			case 3:
				values[i] = -int64(r.Intn(100000)) // Negative
			}
		}

		// Write the data - this should create multiple blocks
		err = writer.Write(ids, values)
		require.NoError(t, err)
	})

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

		// Verify block alignment (except first block)
		if i > 0 {
			blockOffset := reader.blockIndex[i].BlockOffset
			assert.Equal(t, uint64(0), blockOffset%uint64(PageSize),
				"Block %d offset %d is not page-aligned", i, blockOffset)
		}
	}

	// Verify total items
	assert.Equal(t, uint32(90000), totalItems, "Expected 90000 total items")

	// Calculate what the size would be with fixed encoding
	// Fixed encoding: 8 bytes per ID + 8 bytes per value = 16 bytes per item
	fixedEncodingEstimate := uint64(totalItems) * 16

	// Log the size comparison
	t.Logf("Total size with varint: %d bytes, fixed encoding estimate: %d bytes",
		totalSize, fixedEncodingEstimate)

	// Verify that varint encoding is more efficient
	assert.Less(t, totalSize, fixedEncodingEstimate,
		"VarInt encoding should be more efficient than fixed encoding")

	// Verify data integrity by reading back all blocks
	var allIDs []uint64
	var allValues []int64

	for i := uint64(0); i < blockCount; i++ {
		ids, values, err := reader.GetPairs(i)
		require.NoError(t, err)
		allIDs = append(allIDs, ids...)
		allValues = append(allValues, values...)
	}

	// Verify total count
	assert.Equal(t, 90000, len(allIDs), "Expected 90000 total IDs")
	assert.Equal(t, 90000, len(allValues), "Expected 90000 total values")
}
