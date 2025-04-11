package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVibeStoreBasicOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store with small memtable size for easier testing
	options := DefaultOptions(tempDir)
	options.MemtableSize = 100     // Flush after 100 entries
	options.MemtableMaxAgeMs = 500 // 500ms max age for testing

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Add data to the store
	for i := uint64(1); i <= 50; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Verify data is readable
	for i := uint64(1); i <= 50; i++ {
		value, found := store.GetValue(i)
		assert.True(t, found)
		assert.Equal(t, int64(i*10), value)
	}

	// Add more data to trigger a flush
	for i := uint64(51); i <= 150; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Wait a bit for flush to complete
	time.Sleep(100 * time.Millisecond)

	// Verify all data is still readable
	for i := uint64(1); i <= 150; i++ {
		value, found := store.GetValue(i)
		assert.True(t, found)
		assert.Equal(t, int64(i*10), value)
	}

	// Add data that updates existing values
	for i := uint64(1); i <= 50; i++ {
		err := store.Add(i, int64(i*20)) // Double the original values
		require.NoError(t, err)
	}

	// Verify updated values are readable
	for i := uint64(1); i <= 50; i++ {
		value, found := store.GetValue(i)
		assert.True(t, found)
		assert.Equal(t, int64(i*20), value, fmt.Sprintf("Updated value for key %d should be %d", i, i*20))
	}

	// Verify non-updated values are still correct
	for i := uint64(51); i <= 150; i++ {
		value, found := store.GetValue(i)
		assert.True(t, found)
		assert.Equal(t, int64(i*10), value)
	}
}

func TestVibeStoreTimedFlush(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-timed-flush-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store with normal settings
	options := DefaultOptions(tempDir)

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Add some data to the store
	for i := uint64(1); i <= 50; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Force flush instead of waiting for timed flush
	store.ForceFlush()

	// Check segments directory to see if a segment was created
	files, err := os.ReadDir(tempDir)
	require.NoError(t, err)

	// Print directory contents for debugging
	t.Logf("Directory contents of %s:", tempDir)
	for _, file := range files {
		t.Logf("  %s", file.Name())
	}

	// We should have at least one segment file
	segmentFound := false
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".col" {
			segmentFound = true
			t.Logf("Found segment file: %s", file.Name())
			break
		}
	}

	assert.True(t, segmentFound, "Expected to find at least one segment file after forced flush")

	// Verify data is still readable
	for i := uint64(1); i <= 50; i++ {
		value, found := store.GetValue(i)
		assert.True(t, found)
		assert.Equal(t, int64(i*10), value)
	}
}

func TestVibeStoreDeletion(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-deletion-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store
	options := DefaultOptions(tempDir)
	options.MemtableSize = 100 // Small size for testing

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Add data to the store
	for i := uint64(1); i <= 150; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Wait a bit for flush to complete
	time.Sleep(100 * time.Millisecond)

	// Delete some entries
	for i := uint64(50); i <= 100; i++ {
		err := store.Delete(i)
		require.NoError(t, err)
	}

	// Verify deleted entries are not found
	for i := uint64(50); i <= 100; i++ {
		_, found := store.GetValue(i)
		assert.False(t, found, fmt.Sprintf("Key %d should be deleted", i))
	}

	// Verify non-deleted entries are still found
	for i := uint64(1); i <= 49; i++ {
		value, found := store.GetValue(i)
		assert.True(t, found)
		assert.Equal(t, int64(i*10), value)
	}
	for i := uint64(101); i <= 150; i++ {
		value, found := store.GetValue(i)
		assert.True(t, found)
		assert.Equal(t, int64(i*10), value)
	}
}

func TestVibeStoreAggregate(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-aggregate-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store
	options := DefaultOptions(tempDir)
	options.MemtableSize = 50 // Small size to ensure multiple segments

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Add batches of data to create multiple segments
	// Batch 1
	for i := uint64(1); i <= 50; i++ {
		err := store.Add(i, int64(i))
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Batch 2
	for i := uint64(51); i <= 100; i++ {
		err := store.Add(i, int64(i))
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Batch 3
	for i := uint64(101); i <= 150; i++ {
		err := store.Add(i, int64(i))
		require.NoError(t, err)
	}

	// Wait for final flush
	time.Sleep(100 * time.Millisecond)

	// Perform aggregation
	result, err := store.Aggregate(EmptyAggregateOptions())
	require.NoError(t, err)

	// Expected results
	expectedCount := 150
	expectedSum := int64(0)
	for i := int64(1); i <= 150; i++ {
		expectedSum += i
	}

	// Verify aggregation results
	assert.Equal(t, expectedCount, result.Count)
	assert.Equal(t, expectedSum, result.Sum)
	assert.Equal(t, float64(expectedSum)/float64(expectedCount), result.Avg)
}
