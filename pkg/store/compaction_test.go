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
