package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSegmentLevelOnFlush verifies that segments created by memtable flushes
// are always created with level 0
func TestSegmentLevelOnFlush(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-level-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store with small memtable size to trigger flushes easily
	options := DefaultOptions(tempDir)
	options.MemtableSize = 10        // Flush after 10 entries
	options.MemtableMaxAgeMs = 10000 // 10 seconds max age (won't be triggered)

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	// No defer close here since we'll manually close it later

	// Add data to trigger the first flush
	for i := uint64(1); i <= 15; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Wait for the flush to complete
	time.Sleep(100 * time.Millisecond)

	// Verify at least one segment was created
	currentState := store.state.Load().(*VibeStoreState)
	require.Greater(t, len(currentState.segments), 0, "Expected at least one segment after flush")

	// Check that all segments have level 0
	for i, segment := range currentState.segments {
		level := segment.Level()
		assert.Equal(t, uint16(0), level, "Segment %d should have level 0, got %d", i, level)
	}

	// Add more data to trigger another flush
	for i := uint64(100); i <= 120; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Wait for the flush to complete
	time.Sleep(100 * time.Millisecond)

	// Verify state was updated
	currentState = store.state.Load().(*VibeStoreState)
	require.GreaterOrEqual(t, len(currentState.segments), 2, "Expected at least two segments after second flush")

	// Check that all segments have level 0
	for i, segment := range currentState.segments {
		level := segment.Level()
		assert.Equal(t, uint16(0), level, "Segment %d should have level 0, got %d", i, level)
	}

	// Trigger a manual flush and verify
	store.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Check state after manual flush
	currentState = store.state.Load().(*VibeStoreState)
	for i, segment := range currentState.segments {
		level := segment.Level()
		assert.Equal(t, uint16(0), level, "Segment %d should have level 0 after manual flush, got %d", i, level)
	}

	// Test the manual flush on close
	err = store.Close()
	require.NoError(t, err)

	// Manually open the segment files to verify their levels
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)

	foundSegments := false
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".col" {
			foundSegments = true
			segmentPath := filepath.Join(tempDir, entry.Name())

			// Open the segment with col.Reader to check its level
			reader, err := col.NewReader(segmentPath)
			if err != nil {
				t.Logf("Failed to open segment %s: %v", entry.Name(), err)
				continue
			}
			defer reader.Close()

			level := reader.Level()
			assert.Equal(t, uint16(0), level, "Segment file %s should have level 0, got %d", entry.Name(), level)
		}
	}
	assert.True(t, foundSegments, "Expected to find segment files in the data directory")
}
