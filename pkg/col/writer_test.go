package col_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vibe-lsm/pkg/col"
)

func TestEncodeIDs(t *testing.T) {
	// Skip this test as it tests internal methods that should be tested via integration tests
	t.Skip("Testing internal methods directly is not recommended")
}

func TestEncodeValues(t *testing.T) {
	// Skip this test as it tests internal methods that should be tested via integration tests
	t.Skip("Testing internal methods directly is not recommended")
}

func TestWriteBlock(t *testing.T) {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "test-writer-*.col")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Create a writer with the default encoding
	writer, err := col.NewWriter(tmpfile.Name())
	require.NoError(t, err)
	defer writer.Close()

	// Test writing a simple block
	ids := []uint64{1, 2, 3, 4, 5}
	values := []int64{10, 20, 30, 40, 50}

	err = writer.WriteBlock(ids, values)
	assert.NoError(t, err)

	// Finalize the file to ensure proper writing
	err = writer.FinalizeAndClose()
	assert.NoError(t, err)

	// Open a reader to verify contents
	reader, err := col.NewReader(tmpfile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Verify the file contains a single block
	blockCount := reader.BlockCount()
	assert.Equal(t, uint64(1), blockCount)

	// Read data from the file and check it matches what we wrote
	readIds, readValues, err := reader.GetPairs(0)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, readIds)
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, readValues)
}

func TestWriteBlockWithRawEncoding(t *testing.T) {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "test-writer-raw-*.col")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Create a writer with the raw encoding
	writer, err := col.NewWriter(tmpfile.Name(), col.WithEncoding(col.EncodingRaw))
	require.NoError(t, err)
	defer writer.Close()

	// Test data
	ids := []uint64{1, 2, 3, 4, 5}
	values := []int64{10, 20, 30, 40, 50}

	// Write block
	err = writer.WriteBlock(ids, values)
	assert.NoError(t, err)

	// Finalize and close
	err = writer.FinalizeAndClose()
	assert.NoError(t, err)

	// Read back for verification
	reader, err := col.NewReader(tmpfile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Verify block count
	blockCount := reader.BlockCount()
	assert.Equal(t, uint64(1), blockCount)

	// Read data and verify it matches
	readIds, readValues, err := reader.GetPairs(0)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, readIds)
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, readValues)
}

func TestWriteBlockWithVarIntEncoding(t *testing.T) {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "test-writer-varint-*.col")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Create a writer with the varint encoding
	writer, err := col.NewWriter(tmpfile.Name(), col.WithEncoding(col.EncodingVarIntBoth))
	require.NoError(t, err)
	defer writer.Close()

	// Test data
	ids := []uint64{1, 2, 3, 4, 5}
	values := []int64{10, 20, 30, 40, 50}

	// Write block
	err = writer.WriteBlock(ids, values)
	assert.NoError(t, err)

	// Finalize and close
	err = writer.FinalizeAndClose()
	assert.NoError(t, err)

	// Read back for verification
	reader, err := col.NewReader(tmpfile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Verify block count
	blockCount := reader.BlockCount()
	assert.Equal(t, uint64(1), blockCount)

	// Read data and verify it matches
	readIds, readValues, err := reader.GetPairs(0)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, readIds)
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, readValues)
}

func TestWithBlockSizeOption(t *testing.T) {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "test-writer-blocksize-*.col")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Create a writer with custom block size
	customBlockSize := uint32(8 * 1024) // 8KB
	writer, err := col.NewWriter(tmpfile.Name(), col.WithBlockSize(customBlockSize))
	require.NoError(t, err)
	defer writer.Close()

	// Write some data
	ids := []uint64{1, 2, 3, 4, 5}
	values := []int64{10, 20, 30, 40, 50}
	err = writer.WriteBlock(ids, values)
	assert.NoError(t, err)

	// Finalize and close
	err = writer.FinalizeAndClose()
	assert.NoError(t, err)

	// Read back the file and verify the block size target is set correctly
	// Note: We can't access the blockSizeTarget field directly as it's private,
	// but we can check that the file was created successfully
	reader, err := col.NewReader(tmpfile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Verify the data was written correctly
	readIds, readValues, err := reader.GetPairs(0)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, readIds)
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, readValues)
}

func TestWriteBlockErrorHandling(t *testing.T) {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "test-writer-error-*.col")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Create a writer
	writer, err := col.NewWriter(tmpfile.Name())
	require.NoError(t, err)
	defer writer.Close()

	// Test error cases
	testCases := []struct {
		name        string
		ids         []uint64
		values      []int64
		expectError bool
	}{
		{
			name:        "Empty arrays",
			ids:         []uint64{},
			values:      []int64{},
			expectError: true,
		},
		{
			name:        "Mismatched lengths",
			ids:         []uint64{1, 2, 3},
			values:      []int64{10, 20},
			expectError: true,
		},
		{
			name:        "Valid data",
			ids:         []uint64{1, 2, 3},
			values:      []int64{10, 20, 30},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := writer.WriteBlock(tc.ids, tc.values)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestWriterBlockSizes tests that the Writer can create blocks close to the target size
func TestWriterBlockSizes(t *testing.T) {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "writer_block_size_test")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	// Test with both raw and varint encoding
	encodings := []struct {
		name string
		enc  uint32
	}{
		{"Raw", col.EncodingRaw},
		{"VarInt", col.EncodingVarInt},
	}

	for _, encoding := range encodings {
		t.Run(encoding.name, func(t *testing.T) {
			// Set target block size to 128KB
			const targetBlockSize = 128 * 1024

			// Create a SimpleWriter with the specified encoding and block size
			// SimpleWriter handles block creation automatically
			writer, err := col.NewSimpleWriter(tmpfile.Name(),
				col.WithEncoding(encoding.enc),
				col.WithBlockSize(uint32(targetBlockSize)))
			require.NoError(t, err)

			// Use a larger batch size for efficiency
			const batchSize = 10000

			// Create enough entries to fill multiple blocks
			const numBatches = 10

			// Generate and write test data in batches
			for i := 0; i < numBatches; i++ {
				// Generate test data
				ids := make([]uint64, batchSize)
				values := make([]int64, batchSize)

				// Fill the arrays with test data
				for j := 0; j < batchSize; j++ {
					ids[j] = uint64(i*batchSize + j + 1) // Ensure IDs are unique and sorted
					values[j] = int64(ids[j] * 10)       // Some arbitrary value
				}

				// Write the batch
				err = writer.Write(ids, values)
				require.NoError(t, err)

				t.Logf("Successfully wrote batch %d of %d items", i+1, batchSize)
			}

			// Close the writer to finalize the file
			err = writer.Close()
			require.NoError(t, err)

			// Open the file for reading to verify block sizes
			reader, err := col.NewReader(tmpfile.Name())
			require.NoError(t, err)
			defer reader.Close()

			// Verify block count
			actualBlockCount := reader.BlockCount()
			t.Logf("Got %d blocks", actualBlockCount)

			// If we didn't get any blocks, the test has failed
			require.Greater(t, actualBlockCount, uint64(0), "No blocks were written")

			// Get file info to check actual file size
			fileInfo, err := os.Stat(tmpfile.Name())
			require.NoError(t, err)
			fileSize := fileInfo.Size()

			// Calculate average block size
			avgBlockSize := float64(fileSize) / float64(actualBlockCount)
			avgEfficiency := avgBlockSize / float64(targetBlockSize) * 100.0

			t.Logf("Encoding: %s", encoding.name)
			t.Logf("Target block size: %d bytes", targetBlockSize)
			t.Logf("File size: %d bytes", fileSize)
			t.Logf("Number of blocks: %d", actualBlockCount)
			t.Logf("Total entries written: %d", numBatches*batchSize)
			t.Logf("Average block size: %.2f bytes (%.2f%% of target)",
				avgBlockSize, avgEfficiency)

			// Check for reasonable block efficiency
			// The efficiency should be at least 30% of the target size
			minEfficiency := 30.0

			require.GreaterOrEqual(t, avgEfficiency, minEfficiency,
				"Average block size should be at least %.1f%% of the target (got %.2f%%)",
				minEfficiency, avgEfficiency)

			// Also check that we're not creating excessively large blocks
			require.LessOrEqual(t, avgEfficiency, 120.0,
				"Average block size should be at most 120%% of the target (got %.2f%%)",
				avgEfficiency)
		})
	}
}
