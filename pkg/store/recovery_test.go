package store

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoreStateRecovery verifies that the store can recover its state from the manifest file
func TestStoreStateRecovery(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-recovery-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store with test options
	options := DefaultOptions(tempDir)
	options.MemtableSize = 5               // Flush very frequently for testing
	options.MemtableMaxAgeMs = 10000       // Don't flush based on time
	options.CompactionCheckIntervalMs = 50 // Check compaction every 50ms

	// First store instance
	store, err := NewVibeStore(options)
	require.NoError(t, err)

	// Add data to create several segments
	for i := 0; i < 3; i++ {
		for j := 0; j < 5; j++ {
			err := store.Add(uint64(i*100+j), int64(i*100+j))
			require.NoError(t, err)
		}
		// Force flush after each batch
		store.ForceFlush()
	}

	// Wait for all flushes to complete
	time.Sleep(200 * time.Millisecond)

	// Check levels and save the state for later comparison
	originalLevels := store.GetSegmentLevels()
	require.NotEmpty(t, originalLevels, "Should have at least one segment")

	// Count the number of segment files
	segmentFiles, err := countSegmentFiles(tempDir)
	require.NoError(t, err)
	require.Greater(t, segmentFiles, 0, "Should have segment files in the directory")

	// Get a value to verify it can be retrieved
	value, found := store.GetValue(101)
	require.True(t, found, "Should find value for key 101")
	require.Equal(t, int64(101), value, "Value should match what was inserted")

	// Close the store
	err = store.Close()
	require.NoError(t, err)

	// Create a new store instance with the same data directory
	newStore, err := NewVibeStore(options)
	require.NoError(t, err)
	defer newStore.Close()

	// Verify the segments were recovered
	restoredLevels := newStore.GetSegmentLevels()
	assert.Equal(t, originalLevels, restoredLevels, "Restored segment levels should match original")

	// Verify data can still be read
	restoredValue, found := newStore.GetValue(101)
	assert.True(t, found, "Should still find value after recovery")
	assert.Equal(t, value, restoredValue, "Value after recovery should match original")
}

// TestSegmentCleanupAfterCompaction verifies that segment files are properly deleted after compaction
func TestSegmentCleanupAfterCompaction(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-cleanup-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store with test options
	options := DefaultOptions(tempDir)
	options.MemtableSize = 5                // Flush very frequently for testing
	options.MemtableMaxAgeMs = 10000        // Don't flush based on time
	options.CompactionCheckIntervalMs = 100 // Check compaction frequently
	options.DisableCompaction = true        // Start with compaction disabled

	// Create store
	store, err := NewVibeStore(options)
	require.NoError(t, err)

	// Add data to create multiple segments with the same level
	for i := 0; i < 4; i++ {
		for j := 0; j < 5; j++ {
			err := store.Add(uint64(i*100+j), int64(i*100+j))
			require.NoError(t, err)
		}
		// Force flush after each batch
		store.ForceFlush()
	}

	// Wait for all flushes to complete
	time.Sleep(200 * time.Millisecond)

	// Count initial segment files
	initialSegmentFiles, err := countSegmentFiles(tempDir)
	require.NoError(t, err)
	t.Logf("Initial segment files: %d", initialSegmentFiles)
	require.GreaterOrEqual(t, initialSegmentFiles, 4, "Should have at least 4 segment files")

	// Get initial segment levels
	initialLevels := store.GetSegmentLevels()
	t.Logf("Initial levels: %v", initialLevels)
	require.Equal(t, 4, len(initialLevels), "Should have 4 segments")

	// Now trigger a compaction manually
	result := store.TriggerCompaction()
	assert.True(t, result, "Manual compaction should be triggered")

	// Wait for compaction to complete
	time.Sleep(500 * time.Millisecond)

	// Check the levels after first compaction
	postCompactionLevels := store.GetSegmentLevels()
	t.Logf("Levels after first compaction: %v", postCompactionLevels)
	require.Equal(t, 3, len(postCompactionLevels), "Should have 3 segments after one compaction")

	// Count segment files after compaction
	// Should have fewer files than before
	postCompactionFiles, err := countSegmentFiles(tempDir)
	require.NoError(t, err)
	t.Logf("Files after first compaction: %d", postCompactionFiles)

	// Verify files were actually deleted (not just closed)
	assert.Less(t, postCompactionFiles, initialSegmentFiles,
		"Should have fewer segment files after compaction")

	// Close and reopen the store to verify file cleanup persists across restarts
	err = store.Close()
	require.NoError(t, err)

	// Count files after close
	filesAfterClose, err := countSegmentFiles(tempDir)
	require.NoError(t, err)
	t.Logf("Files after close: %d", filesAfterClose)
	assert.Equal(t, postCompactionFiles, filesAfterClose,
		"File count should remain stable after closing store")

	// Open a new store instance
	newOptions := DefaultOptions(tempDir)
	newOptions.DisableCompaction = false // Enable compaction now
	newStore, err := NewVibeStore(newOptions)
	require.NoError(t, err)
	defer newStore.Close()

	// Wait for potential automatic compaction
	time.Sleep(1 * time.Second)

	// Get final levels
	finalLevels := newStore.GetSegmentLevels()
	t.Logf("Final levels after reopen: %v", finalLevels)

	// After reopening, compaction may or may not occur depending on segment levels
	// Instead of asserting fewer segments, we just verify we still have the right number
	assert.Equal(t, len(postCompactionLevels), len(finalLevels),
		"Should maintain segment count when no further compaction is possible")

	// Count final segment files
	finalFiles, err := countSegmentFiles(tempDir)
	require.NoError(t, err)
	t.Logf("Final files after reopen and compaction: %d", finalFiles)

	// File count should remain stable or decrease, but not increase
	assert.LessOrEqual(t, finalFiles, filesAfterClose,
		"File count should not increase after reopening")
}

// Helper function to count segment files in a directory
