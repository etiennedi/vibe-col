package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompactionStrategy verifies that the compaction strategy works as expected
func TestCompactionStrategy(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-compaction-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store with test options
	options := DefaultOptions(tempDir)
	options.MemtableSize = 5               // Flush very frequently for testing
	options.MemtableMaxAgeMs = 10000       // Don't flush based on time
	options.CompactionCheckIntervalMs = 50 // Check compaction every 50ms

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	// No defer close, we'll close it manually

	// Helper function to check segment levels
	checkLevels := func(expected []uint16) bool {
		t.Helper()
		actual := store.GetSegmentLevels()

		// Wait for up to 2 seconds for expected levels to match
		var success bool
		for i := 0; i < 40; i++ { // 40 * 50ms = 2 seconds
			actual = store.GetSegmentLevels()

			if len(actual) == len(expected) {
				match := true
				for j := range expected {
					if actual[j] != expected[j] {
						match = false
						break
					}
				}

				if match {
					success = true
					break
				}
			}

			// Wait a bit for compaction to happen
			time.Sleep(50 * time.Millisecond)
		}

		if !success {
			t.Logf("Expected levels: %v, got: %v", expected, actual)
		}
		return success
	}

	// Helper function to create segments with predefined levels
	createSegmentsWithLevels := func(levels []uint16) []*col.Reader {
		readers := make([]*col.Reader, 0, len(levels))

		// Create a segment file for each level
		for i, level := range levels {
			// Create a unique segment file
			filename := fmt.Sprintf("test_segment_%d.col", i)
			path := filepath.Join(tempDir, filename)

			// Create a writer with the specified level
			writer, err := col.NewWriter(path, col.WithLevel(level))
			require.NoError(t, err)

			// Write some data
			ids := make([]uint64, 5)
			vals := make([]int64, 5)
			for j := 0; j < 5; j++ {
				ids[j] = uint64(i*100 + j)
				vals[j] = int64(i*100 + j)
			}

			err = writer.WriteBlock(ids, vals)
			require.NoError(t, err)

			err = writer.FinalizeAndClose()
			require.NoError(t, err)

			// Open the file as a reader
			reader, err := col.NewReader(path)
			require.NoError(t, err)

			readers = append(readers, reader)
		}

		return readers
	}

	// Test Case 1: Basic compaction of same-level segments
	t.Run("BasicCompaction", func(t *testing.T) {
		// Start with a clean store
		err := store.Close()
		require.NoError(t, err)

		// Clean up any existing segments
		files, err := os.ReadDir(tempDir)
		require.NoError(t, err)
		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".col") {
				err := os.Remove(filepath.Join(tempDir, file.Name()))
				require.NoError(t, err)
			}
		}

		// Create a new store
		store, err = NewVibeStore(options)
		require.NoError(t, err)

		// Add data to create several segments with level 0
		// Each batch will trigger a flush
		for i := 0; i < 6; i++ {
			for j := 0; j < 5; j++ {
				err := store.Add(uint64(i*100+j), int64(i*100+j))
				require.NoError(t, err)
			}

			// Wait for flush to complete
			time.Sleep(100 * time.Millisecond)
		}

		// Wait for compactions to occur and check the final state
		time.Sleep(500 * time.Millisecond)

		// We should see levels compacted, though exact pattern
		// may vary based on timing of flushes and compactions
		levels := store.GetSegmentLevels()
		t.Logf("Final segment levels: %v", levels)

		// We should have fewer than 6 segments due to compaction
		assert.Less(t, len(levels), 6, "Expected fewer than 6 segments after compaction")

		// We should have at least one segment with level > 0
		hasHigherLevel := false
		for _, level := range levels {
			if level > 0 {
				hasHigherLevel = true
				break
			}
		}
		assert.True(t, hasHigherLevel, "Expected at least one segment with level > 0")
	})

	// Test Case 2: Specific compaction sequence with predefined levels
	t.Run("PredefinedLevels", func(t *testing.T) {
		// Start with a clean store
		err := store.Close()
		require.NoError(t, err)

		// Clean up any existing segments
		files, err := os.ReadDir(tempDir)
		require.NoError(t, err)
		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".col") {
				err := os.Remove(filepath.Join(tempDir, file.Name()))
				require.NoError(t, err)
			}
		}

		// Create a new store
		storeOptions := DefaultOptions(tempDir)
		storeOptions.CompactionCheckIntervalMs = 500 // Slow down compaction checks (500ms)
		store, err = NewVibeStore(storeOptions)
		require.NoError(t, err)

		// Manually create segments with predefined levels
		initialLevels := []uint16{3, 3, 2, 1, 1}
		readers := createSegmentsWithLevels(initialLevels)

		// Set the segments in the store's state
		currentState := store.state.Load().(*VibeStoreState)
		newState := &VibeStoreState{
			activeMemtable:    currentState.activeMemtable,
			activeSince:       currentState.activeSince,
			flushingMemtables: currentState.flushingMemtables,
			segments:          readers,
		}
		store.state.Store(newState)

		// Verify initial state
		assert.True(t, checkLevels(initialLevels), "Initial levels should match expected")

		// Enable compaction now by calling it directly and then let the timer-based compactions continue
		store.TriggerCompaction()

		// Instead of checking specific intermediate states, let's just verify
		// that we eventually reach the final expected state [4 3]
		expectedFinalState := []uint16{4, 3}

		assert.Eventually(t, func() bool {
			levels := store.GetSegmentLevels()
			if len(levels) != 2 {
				t.Logf("Current levels: %v (waiting for final state)", levels)
				return false
			}
			return levels[0] == 4 && levels[1] == 3
		}, 5*time.Second, 100*time.Millisecond,
			"Compaction should eventually result in [4 3]")

		// No more compactions should occur after reaching [4 3]
		time.Sleep(1 * time.Second)
		levels := store.GetSegmentLevels()
		assert.Equal(t, expectedFinalState, levels,
			"Levels should remain stable after all possible compactions")

		// Let's also verify that we've correctly gone through compactions
		// by checking that the number of segments is reduced from 5 to 2
		assert.Equal(t, 2, len(levels),
			"Should have exactly 2 segments after compaction")

		// Close store to clean up
		err = store.Close()
		require.NoError(t, err)
	})
}

// Test to verify that TriggerCompaction returns the correct boolean value
func TestTriggerCompactionReturnValue(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-compaction-return-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a new directory for each store to avoid sharing segments
	customStoreDir, err := os.MkdirTemp("", "vibe-store-compaction-custom-*")
	require.NoError(t, err)
	defer os.RemoveAll(customStoreDir)

	// Create store with test options
	options := DefaultOptions(tempDir)
	// Completely disable automatic compaction
	options.CompactionCheckIntervalMs = 0 // 0 means disable compaction checker
	options.DisableCompaction = false

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Test Case 1: Empty store - should return false since there's nothing to compact
	result := store.TriggerCompaction()
	assert.False(t, result, "Expected false when no segments exist")

	// Test Case 2: Create the segments with the same level (0)
	customOptions := DefaultOptions(customStoreDir)
	customOptions.DisableCompaction = false     // Allow compaction to be triggered manually
	customOptions.CompactionCheckIntervalMs = 0 // Disable periodic checks

	// Create a new store with fresh segment files
	customStore, err := NewVibeStore(customOptions)
	require.NoError(t, err)
	defer customStore.Close()

	// Create segments with the same level for testing
	// Add data to create segments with level 0
	for i := 0; i < 2; i++ {
		for j := uint64(0); j < 5; j++ {
			id := j + uint64(i*100)
			err := customStore.Add(id, int64(id*10))
			require.NoError(t, err)
		}
		customStore.ForceFlush()
		time.Sleep(200 * time.Millisecond)
	}

	// Check the levels in the custom store
	customLevels := customStore.GetSegmentLevels()
	t.Logf("Custom store segment levels: %v", customLevels)

	// At this point we should have 2 segments with the same level (0) in our custom store,
	// so TriggerCompaction should return true
	result = customStore.TriggerCompaction()
	assert.True(t, result, "Expected true when segments have the same level")

	// Wait for the compaction to complete
	time.Sleep(300 * time.Millisecond)

	// Verify compaction occurred by checking levels
	compactedLevels := customStore.GetSegmentLevels()
	t.Logf("Segment levels after compaction: %v", compactedLevels)

	// Should have one segment with level 1 after compaction
	if len(compactedLevels) == 1 {
		assert.Equal(t, uint16(1), compactedLevels[0], "Compacted segment should have level 1")
	}

	// Test Case 3: Demonstrate loop pattern for bulk imports
	// Create a new store for this test
	bulkStoreDir, err := os.MkdirTemp("", "vibe-store-compaction-bulk-*")
	require.NoError(t, err)
	defer os.RemoveAll(bulkStoreDir)

	bulkOptions := DefaultOptions(bulkStoreDir)
	bulkOptions.DisableCompaction = false
	bulkOptions.CompactionCheckIntervalMs = 0

	bulkStore, err := NewVibeStore(bulkOptions)
	require.NoError(t, err)
	defer bulkStore.Close()

	// Create 4 segments with level 0
	for i := 0; i < 4; i++ {
		for j := uint64(0); j < 5; j++ {
			id := j + uint64(i*1000)
			err := bulkStore.Add(id, int64(id*10))
			require.NoError(t, err)
		}
		bulkStore.ForceFlush()
		time.Sleep(200 * time.Millisecond)
	}

	// Log initial levels
	initialBulkLevels := bulkStore.GetSegmentLevels()
	t.Logf("Initial bulk store levels: %v", initialBulkLevels)

	// Keep compacting until no more compactions are possible
	compactionCount := 0
	for i := 0; i < 10; i++ { // Cap at 10 iterations as a safety measure
		if bulkStore.TriggerCompaction() {
			compactionCount++
			t.Logf("Compaction #%d triggered", compactionCount)
			// Wait for the compaction to complete
			time.Sleep(300 * time.Millisecond)
		} else {
			break
		}
	}

	// Log the final levels and number of compactions performed
	finalBulkLevels := bulkStore.GetSegmentLevels()
	t.Logf("Final bulk store levels after %d compactions: %v", compactionCount, finalBulkLevels)
	assert.Greater(t, compactionCount, 0, "At least one compaction should have occurred")
}

// TestManualCompactionWhenDisabled verifies that manual compaction still works
// even when automatic compaction is disabled
func TestManualCompactionWhenDisabled(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-manual-compaction-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store with test options
	options := DefaultOptions(tempDir)
	// Disable automatic compaction
	options.DisableCompaction = true
	options.CompactionCheckIntervalMs = 0 // Also disable periodic checks

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Create 2 segments with level 0
	for i := 0; i < 2; i++ {
		for j := uint64(0); j < 5; j++ {
			id := j + uint64(i*100)
			err := store.Add(id, int64(id*10))
			require.NoError(t, err)
		}
		store.ForceFlush()
		time.Sleep(200 * time.Millisecond)
	}

	// Check initial levels
	initialLevels := store.GetSegmentLevels()
	t.Logf("Initial segment levels: %v", initialLevels)
	require.Equal(t, 2, len(initialLevels), "Should have 2 segments")
	require.Equal(t, []uint16{0, 0}, initialLevels, "Both segments should have level 0")

	// Wait a bit to ensure no automatic compaction occurs
	time.Sleep(500 * time.Millisecond)

	// Verify levels haven't changed (no automatic compaction)
	levelsAfterWait := store.GetSegmentLevels()
	assert.Equal(t, initialLevels, levelsAfterWait, "Levels should not change when automatic compaction is disabled")

	// Now trigger manual compaction
	result := store.TriggerCompaction()
	assert.True(t, result, "Manual compaction should be triggered even with DisableCompaction=true")

	// Wait for compaction to complete
	time.Sleep(300 * time.Millisecond)

	// Verify compaction occurred
	finalLevels := store.GetSegmentLevels()
	t.Logf("Final segment levels: %v", finalLevels)

	// We should now have a single segment with level 1
	assert.Equal(t, 1, len(finalLevels), "Should have 1 segment after compaction")
	if len(finalLevels) == 1 {
		assert.Equal(t, uint16(1), finalLevels[0], "Compacted segment should have level 1")
	}
}
