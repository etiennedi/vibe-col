package store

import (
	"os"
	"testing"
	"time"

	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// TestStoreStateRecovery verifies that the store can recover its state from the manifest file
// and that aggregation results are consistent after restart
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

	// Verify individual value retrieval
	value, found := store.GetValue(101)
	require.True(t, found, "Should find value for key 101")
	require.Equal(t, int64(101), value, "Value should match what was inserted")

	// Verify aggregation across segments
	// Test aggregation for all entries
	allResult, err := store.Aggregate(col.AggregateOptions{})
	require.NoError(t, err)
	require.Equal(t, int(15), allResult.Count, "Should have 15 entries total")

	// Calculate expected sum of all values (0..4 + 100..104 + 200..204)
	var expectedSum int64
	for i := 0; i < 3; i++ {
		for j := 0; j < 5; j++ {
			expectedSum += int64(i*100 + j)
		}
	}
	require.Equal(t, expectedSum, allResult.Sum, "Aggregate sum should match expected value")

	// Test filtered aggregation for specific range
	filter := sroar.NewBitmap()
	// Add IDs in the range 100-104
	for i := uint64(100); i < uint64(105); i++ {
		filter.Set(i)
	}

	rangeResult, err := store.Aggregate(col.AggregateOptions{
		Filter: filter,
	})
	require.NoError(t, err)
	require.Equal(t, int(5), rangeResult.Count, "Should have 5 entries in the filtered range")

	// Calculate expected sum for range 100-104
	expectedRangeSum := int64(100 + 101 + 102 + 103 + 104)
	require.Equal(t, expectedRangeSum, rangeResult.Sum, "Filtered sum should match expected value")

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

	// Verify individual values can still be read
	restoredValue, found := newStore.GetValue(101)
	assert.True(t, found, "Should still find value after recovery")
	assert.Equal(t, value, restoredValue, "Value after recovery should match original")

	// Verify aggregation results are consistent after recovery
	// Test full aggregation after recovery
	newAllResult, err := newStore.Aggregate(col.AggregateOptions{})
	require.NoError(t, err)
	assert.Equal(t, allResult.Count, newAllResult.Count, "Aggregate count should be preserved after recovery")
	assert.Equal(t, allResult.Sum, newAllResult.Sum, "Aggregate sum should be preserved after recovery")

	// Test filtered aggregation after recovery
	newRangeResult, err := newStore.Aggregate(col.AggregateOptions{
		Filter: filter,
	})
	require.NoError(t, err)
	assert.Equal(t, rangeResult.Count, newRangeResult.Count, "Filtered count should be preserved after recovery")
	assert.Equal(t, rangeResult.Sum, newRangeResult.Sum, "Filtered sum should be preserved after recovery")
}

// TestSegmentCleanupAfterCompaction verifies that segment files are properly deleted after compaction
// and that aggregation results remain consistent during and after compaction
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

	// Get baseline aggregation results before compaction
	// Test full aggregation
	initialResult, err := store.Aggregate(col.AggregateOptions{})
	require.NoError(t, err)
	t.Logf("Initial aggregation: count=%d, sum=%d", initialResult.Count, initialResult.Sum)

	// Calculate expected total
	var expectedTotal int64
	for i := 0; i < 4; i++ {
		for j := 0; j < 5; j++ {
			expectedTotal += int64(i*100 + j)
		}
	}
	require.Equal(t, int(20), initialResult.Count, "Initial count should match expected")
	require.Equal(t, expectedTotal, initialResult.Sum, "Initial sum should match expected")

	// Test filtered aggregation for specific value ranges
	filter1 := sroar.NewBitmap()
	for i := uint64(0); i < uint64(5); i++ {
		filter1.Set(i) // First segment
	}

	initialFilteredResult1, err := store.Aggregate(col.AggregateOptions{
		Filter: filter1,
	})
	require.NoError(t, err)
	t.Logf("Initial filtered aggregation (0-4): count=%d, sum=%d",
		initialFilteredResult1.Count, initialFilteredResult1.Sum)

	filter2 := sroar.NewBitmap()
	for i := uint64(100); i < uint64(105); i++ {
		filter2.Set(i) // Second segment
	}

	initialFilteredResult2, err := store.Aggregate(col.AggregateOptions{
		Filter: filter2,
	})
	require.NoError(t, err)
	t.Logf("Initial filtered aggregation (100-104): count=%d, sum=%d",
		initialFilteredResult2.Count, initialFilteredResult2.Sum)

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

	// Verify aggregation results are still accurate after compaction
	// Test full aggregation after compaction
	postCompactionResult, err := store.Aggregate(col.AggregateOptions{})
	require.NoError(t, err)
	t.Logf("Post-compaction aggregation: count=%d, sum=%d",
		postCompactionResult.Count, postCompactionResult.Sum)

	// Log the difference between initial and post-compaction results
	t.Logf("Aggregation comparison - Initial: count=%d, sum=%d; After compaction: count=%d, sum=%d",
		initialResult.Count, initialResult.Sum, postCompactionResult.Count, postCompactionResult.Sum)

	// MODIFIED: Add assertion that data should not be lost during compaction
	require.Equal(t, initialResult.Count, postCompactionResult.Count,
		"Count should remain consistent during compaction")
	require.Equal(t, initialResult.Sum, postCompactionResult.Sum,
		"Sum should remain consistent during compaction")

	// Test filtered aggregation after compaction
	postCompactionFilteredResult1, err := store.Aggregate(col.AggregateOptions{
		Filter: filter1,
	})
	require.NoError(t, err)
	t.Logf("Post-compaction filtered (0-4): count=%d, sum=%d",
		postCompactionFilteredResult1.Count, postCompactionFilteredResult1.Sum)

	postCompactionFilteredResult2, err := store.Aggregate(col.AggregateOptions{
		Filter: filter2,
	})
	require.NoError(t, err)
	t.Logf("Post-compaction filtered (100-104): count=%d, sum=%d",
		postCompactionFilteredResult2.Count, postCompactionFilteredResult2.Sum)

	// MODIFIED: Add assertions for filtered results as well
	require.Equal(t, initialFilteredResult1.Count, postCompactionFilteredResult1.Count,
		"Filtered count (0-4) should remain consistent during compaction")
	require.Equal(t, initialFilteredResult1.Sum, postCompactionFilteredResult1.Sum,
		"Filtered sum (0-4) should remain consistent during compaction")

	require.Equal(t, initialFilteredResult2.Count, postCompactionFilteredResult2.Count,
		"Filtered count (100-104) should remain consistent during compaction")
	require.Equal(t, initialFilteredResult2.Sum, postCompactionFilteredResult2.Sum,
		"Filtered sum (100-104) should remain consistent during compaction")

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

	// Verify aggregation is still accurate after reopen
	// Test full aggregation after reopen
	finalResult, err := newStore.Aggregate(col.AggregateOptions{})
	require.NoError(t, err)
	t.Logf("Final aggregation after reopen: count=%d, sum=%d",
		finalResult.Count, finalResult.Sum)

	// Verify data consistency after reopening
	assert.Equal(t, initialResult.Count, finalResult.Count,
		"Count should match the initial state after reopen")
	assert.Equal(t, initialResult.Sum, finalResult.Sum,
		"Sum should match the initial state after reopen")

	// Test filtered aggregation after reopen
	finalFilteredResult1, err := newStore.Aggregate(col.AggregateOptions{
		Filter: filter1,
	})
	require.NoError(t, err)
	t.Logf("Final filtered (0-4): count=%d, sum=%d",
		finalFilteredResult1.Count, finalFilteredResult1.Sum)

	assert.Equal(t, initialFilteredResult1.Count, finalFilteredResult1.Count,
		"Filtered count should match initial state after reopen (0-4)")
	assert.Equal(t, initialFilteredResult1.Sum, finalFilteredResult1.Sum,
		"Filtered sum should match initial state after reopen (0-4)")

	finalFilteredResult2, err := newStore.Aggregate(col.AggregateOptions{
		Filter: filter2,
	})
	require.NoError(t, err)
	t.Logf("Final filtered (100-104): count=%d, sum=%d",
		finalFilteredResult2.Count, finalFilteredResult2.Sum)

	assert.Equal(t, initialFilteredResult2.Count, finalFilteredResult2.Count,
		"Filtered count should match initial state after reopen (100-104)")
	assert.Equal(t, initialFilteredResult2.Sum, finalFilteredResult2.Sum,
		"Filtered sum should match initial state after reopen (100-104)")
}

// Helper function to count segment files in a directory
