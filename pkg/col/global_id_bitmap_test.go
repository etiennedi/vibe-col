package col

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalIDBitmap(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "global_id_bitmap_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create a writer
	writer, err := NewWriter(tmpFile.Name())
	require.NoError(t, err)

	// Write some blocks with different IDs
	ids1 := []uint64{1, 2, 3, 4, 5}
	values1 := []int64{10, 20, 30, 40, 50}
	err = writer.WriteBlock(ids1, values1)
	require.NoError(t, err)

	ids2 := []uint64{6, 7, 8, 9, 10}
	values2 := []int64{60, 70, 80, 90, 100}
	err = writer.WriteBlock(ids2, values2)
	require.NoError(t, err)

	ids3 := []uint64{5, 10, 15, 20, 25}
	values3 := []int64{5, 10, 15, 20, 25}
	err = writer.WriteBlock(ids3, values3)
	require.NoError(t, err)

	// Finalize the file
	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(tmpFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Get the global ID bitmap
	bitmap, err := reader.GetGlobalIDBitmap()
	require.NoError(t, err)

	// Check that the bitmap contains all the IDs we wrote
	expectedIDs := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25}
	for _, id := range expectedIDs {
		assert.True(t, bitmap.Contains(id), "Bitmap should contain ID %d", id)
	}

	// Check that the bitmap doesn't contain IDs we didn't write
	unexpectedIDs := []uint64{11, 12, 13, 14, 16, 17, 18, 19, 21, 22, 23, 24, 26}
	for _, id := range unexpectedIDs {
		assert.False(t, bitmap.Contains(id), "Bitmap should not contain ID %d", id)
	}

	// Check the cardinality of the bitmap
	assert.Equal(t, 13, bitmap.GetCardinality(), "Bitmap should contain 13 IDs")
}

func TestEmptyGlobalIDBitmap(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "empty_global_id_bitmap_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create a writer
	writer, err := NewWriter(tmpFile.Name())
	require.NoError(t, err)

	// Finalize the file without writing any blocks
	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(tmpFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Get the global ID bitmap
	bitmap, err := reader.GetGlobalIDBitmap()
	require.NoError(t, err)

	// Check that the bitmap is empty
	assert.Equal(t, 0, bitmap.GetCardinality(), "Bitmap should be empty")
}

func TestGlobalIDBitmapWithLargeIDs(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "large_id_global_id_bitmap_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create a writer
	writer, err := NewWriter(tmpFile.Name())
	require.NoError(t, err)

	// Write a block with large IDs
	ids := []uint64{1000000, 2000000, 3000000, 4000000, 5000000}
	values := []int64{10, 20, 30, 40, 50}
	err = writer.WriteBlock(ids, values)
	require.NoError(t, err)

	// Finalize the file
	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(tmpFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Get the global ID bitmap
	bitmap, err := reader.GetGlobalIDBitmap()
	require.NoError(t, err)

	// Check that the bitmap contains all the large IDs we wrote
	for _, id := range ids {
		assert.True(t, bitmap.Contains(id), "Bitmap should contain ID %d", id)
	}

	// Check the cardinality of the bitmap
	assert.Equal(t, 5, bitmap.GetCardinality(), "Bitmap should contain 5 IDs")
}
