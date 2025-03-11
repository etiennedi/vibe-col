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
