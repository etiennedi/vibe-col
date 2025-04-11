package col

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeletedIDBitmap(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "deleted_id_bitmap_test_*.col")
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

	// Add some deleted IDs
	deletedIDs := []uint64{6, 7, 8, 9, 10}
	writer.BatchAddDeletedIDs(deletedIDs)

	// Finalize the file
	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(tmpFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Get the global ID bitmap
	globalBitmap, err := reader.GetGlobalIDBitmap()
	require.NoError(t, err)

	// Check that the bitmap contains all the IDs we wrote
	for _, id := range ids1 {
		assert.True(t, globalBitmap.Contains(id), "Global bitmap should contain ID %d", id)
	}

	// Get the deleted ID bitmap
	deletedBitmap, err := reader.GetDeletedIDBitmap()
	require.NoError(t, err)

	// Check that the deleted bitmap contains all the deleted IDs
	for _, id := range deletedIDs {
		assert.True(t, deletedBitmap.Contains(id), "Deleted bitmap should contain ID %d", id)
	}

	// Check that the deleted bitmap doesn't contain non-deleted IDs
	for _, id := range ids1 {
		assert.False(t, deletedBitmap.Contains(id), "Deleted bitmap should not contain ID %d", id)
	}

	// Check the cardinality of both bitmaps
	assert.Equal(t, len(ids1), globalBitmap.GetCardinality(), "Global bitmap should contain %d IDs", len(ids1))
	assert.Equal(t, len(deletedIDs), deletedBitmap.GetCardinality(), "Deleted bitmap should contain %d IDs", len(deletedIDs))
}

func TestDeletedIDBitmapCaching(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "deleted_id_bitmap_caching_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create a writer
	writer, err := NewWriter(tmpFile.Name())
	require.NoError(t, err)

	// Write a block with some IDs
	ids := []uint64{1, 2, 3, 4, 5}
	values := []int64{10, 20, 30, 40, 50}
	err = writer.WriteBlock(ids, values)
	require.NoError(t, err)

	// Add some deleted IDs
	deletedIDs := []uint64{6, 7, 8, 9, 10}
	writer.BatchAddDeletedIDs(deletedIDs)

	// Finalize the file
	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(tmpFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Get the deleted ID bitmap without caching (default)
	bitmap1, err := reader.GetDeletedIDBitmap()
	require.NoError(t, err)

	// Get the bitmap again - should be a different instance
	bitmap2, err := reader.GetDeletedIDBitmap()
	require.NoError(t, err)

	// Both bitmaps should contain the same IDs
	for _, id := range deletedIDs {
		assert.True(t, bitmap1.Contains(id), "Bitmap1 should contain ID %d", id)
		assert.True(t, bitmap2.Contains(id), "Bitmap2 should contain ID %d", id)
	}

	// Enable caching
	reader.EnableDeletedIDBitmapCaching()

	// Get the bitmap with caching enabled
	bitmap3, err := reader.GetDeletedIDBitmap()
	require.NoError(t, err)

	// Get the bitmap again - should be the same instance
	bitmap4, err := reader.GetDeletedIDBitmap()
	require.NoError(t, err)

	// Both bitmaps should contain the same IDs
	for _, id := range deletedIDs {
		assert.True(t, bitmap3.Contains(id), "Bitmap3 should contain ID %d", id)
		assert.True(t, bitmap4.Contains(id), "Bitmap4 should contain ID %d", id)
	}

	// Disable caching
	reader.DisableDeletedIDBitmapCaching()

	// Get the bitmap again - should be a new instance
	bitmap5, err := reader.GetDeletedIDBitmap()
	require.NoError(t, err)

	// All bitmaps should contain the same IDs
	for _, id := range deletedIDs {
		assert.True(t, bitmap5.Contains(id), "Bitmap5 should contain ID %d", id)
	}
}

func TestBufferedWriterDeletedIDBitmap(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "buffered_deleted_id_bitmap_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create a buffered writer
	writer, err := NewBufferedWriter(tmpFile.Name())
	require.NoError(t, err)

	// Add some IDs
	ids := []uint64{1, 2, 3, 4, 5}
	values := []int64{10, 20, 30, 40, 50}
	for i, id := range ids {
		err = writer.Add(id, values[i])
		require.NoError(t, err)
	}

	// Add some deleted IDs
	deletedIDs := []uint64{6, 7, 8, 9, 10}
	writer.BatchAddDeletedIDs(deletedIDs)

	// Finalize the file
	err = writer.Close()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(tmpFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Get the global ID bitmap
	globalBitmap, err := reader.GetGlobalIDBitmap()
	require.NoError(t, err)

	// Check that the bitmap contains all the IDs we wrote
	for _, id := range ids {
		assert.True(t, globalBitmap.Contains(id), "Global bitmap should contain ID %d", id)
	}

	// Get the deleted ID bitmap
	deletedBitmap, err := reader.GetDeletedIDBitmap()
	require.NoError(t, err)

	// Check that the deleted bitmap contains all the deleted IDs
	for _, id := range deletedIDs {
		assert.True(t, deletedBitmap.Contains(id), "Deleted bitmap should contain ID %d", id)
	}

	// Check that the deleted bitmap doesn't contain non-deleted IDs
	for _, id := range ids {
		assert.False(t, deletedBitmap.Contains(id), "Deleted bitmap should not contain ID %d", id)
	}

	// Check the cardinality of both bitmaps
	assert.Equal(t, len(ids), globalBitmap.GetCardinality(), "Global bitmap should contain %d IDs", len(ids))
	assert.Equal(t, len(deletedIDs), deletedBitmap.GetCardinality(), "Deleted bitmap should contain %d IDs", len(deletedIDs))
}
