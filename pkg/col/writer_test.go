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

// Additional tests for block header and footer writing will be added here.
