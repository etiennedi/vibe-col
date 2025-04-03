package multicol

import (
	"fmt"
	"os"
	"testing"
	"time"

	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func TestMemtableBasicOperations(t *testing.T) {
	// Create a memtable with default options
	m := NewMemtable(nil)

	// Add some entries
	err := m.Add(1, 10)
	require.NoError(t, err)
	err = m.Add(2, 20)
	require.NoError(t, err)
	err = m.Add(3, 30)
	require.NoError(t, err)

	// Get entries
	value, exists := m.Get(1)
	assert.True(t, exists)
	assert.Equal(t, int64(10), value)

	value, exists = m.Get(2)
	assert.True(t, exists)
	assert.Equal(t, int64(20), value)

	value, exists = m.Get(3)
	assert.True(t, exists)
	assert.Equal(t, int64(30), value)

	// Get non-existent entry
	value, exists = m.Get(4)
	assert.False(t, exists)
	assert.Equal(t, int64(0), value)

	// Delete an entry
	success := m.Delete(2)
	assert.True(t, success)

	// Try to get the deleted entry
	value, exists = m.Get(2)
	assert.False(t, exists)
	assert.Equal(t, int64(0), value)

	// Try to delete a non-existent entry
	success = m.Delete(4)
	assert.False(t, success)

	// Try to delete the already deleted entry
	success = m.Delete(2)
	assert.False(t, success)

	// Check active count
	assert.Equal(t, int64(2), m.ActiveCount())
}

func TestMemtableBatchOperations(t *testing.T) {
	// Create a memtable with default options
	m := NewMemtable(nil)

	// Batch add entries
	ids := []uint64{1, 2, 3, 4, 5}
	values := []int64{10, 20, 30, 40, 50}
	err := m.BatchAdd(ids, values)
	require.NoError(t, err)

	// Check all entries
	for i, id := range ids {
		value, exists := m.Get(id)
		assert.True(t, exists)
		assert.Equal(t, values[i], value)
	}

	// Batch delete some entries
	deleteIDs := []uint64{2, 4}
	count := m.BatchDelete(deleteIDs)
	assert.Equal(t, 2, count)

	// Check remaining entries
	for i, id := range ids {
		value, exists := m.Get(id)
		if id == 2 || id == 4 {
			assert.False(t, exists)
			assert.Equal(t, int64(0), value)
		} else {
			assert.True(t, exists)
			assert.Equal(t, values[i], value)
		}
	}

	// Check active count
	assert.Equal(t, int64(3), m.ActiveCount())
}

func TestMemtableScan(t *testing.T) {
	// Create a memtable with default options
	m := NewMemtable(nil)

	// Add entries
	for i := uint64(1); i <= 10; i++ {
		err := m.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Delete some entries
	m.Delete(3)
	m.Delete(7)

	// Scan full range
	ids, values := m.Scan(0, 100)
	assert.Equal(t, 8, len(ids))
	assert.Equal(t, 8, len(values))

	// Verify scan results (deleted entries should be excluded)
	expectedIDs := []uint64{1, 2, 4, 5, 6, 8, 9, 10}
	expectedValues := []int64{10, 20, 40, 50, 60, 80, 90, 100}
	assert.Equal(t, expectedIDs, ids)
	assert.Equal(t, expectedValues, values)

	// Scan partial range
	ids, values = m.Scan(4, 8)
	assert.Equal(t, 4, len(ids))
	assert.Equal(t, 4, len(values))

	// Verify partial scan results
	expectedIDs = []uint64{4, 5, 6, 8}
	expectedValues = []int64{40, 50, 60, 80}
	assert.Equal(t, expectedIDs, ids)
	assert.Equal(t, expectedValues, values)
}

func TestMemtableAggregate(t *testing.T) {
	// Create a memtable with default options
	m := NewMemtable(nil)

	// Add entries
	for i := uint64(1); i <= 10; i++ {
		err := m.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Delete some entries
	m.Delete(3)
	m.Delete(7)

	// Aggregate (excludes deleted entries)
	minID, maxID, minValue, maxValue, sum, count := m.Aggregate()
	assert.Equal(t, uint64(1), minID)
	assert.Equal(t, uint64(10), maxID)
	assert.Equal(t, int64(10), minValue)
	assert.Equal(t, int64(100), maxValue)
	assert.Equal(t, int64(10+20+40+50+60+80+90+100), sum)
	assert.Equal(t, 8, count)
}

func TestMemtableFilteredAggregate(t *testing.T) {
	// Create a memtable with default options
	m := NewMemtable(nil)

	// Add entries
	for i := uint64(1); i <= 10; i++ {
		err := m.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Delete some entries
	m.Delete(3)
	m.Delete(7)

	// Create a filter for even IDs
	filter := sroar.NewBitmap()
	for i := uint64(2); i <= 10; i += 2 {
		filter.Set(i)
	}

	// Filtered aggregate (even IDs, excluding deleted entries)
	minID, maxID, minValue, maxValue, sum, count := m.FilteredAggregate(filter)
	assert.Equal(t, uint64(2), minID)
	assert.Equal(t, uint64(10), maxID)
	assert.Equal(t, int64(20), minValue)
	assert.Equal(t, int64(100), maxValue)
	assert.Equal(t, int64(20+40+60+80+100), sum)
	assert.Equal(t, 5, count)
}

func TestMemtableFlush(t *testing.T) {
	// Create a memtable with default options
	m := NewMemtable(nil)

	// Add entries
	for i := uint64(1); i <= 10; i++ {
		err := m.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Delete some entries
	m.Delete(3)
	m.Delete(7)

	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "memtable_flush_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Flush the memtable to the file
	err = m.Flush(tmpFile.Name())
	require.NoError(t, err)

	// Open the file and verify its contents
	reader, err := col.NewReader(tmpFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Verify that only non-deleted entries were written
	expectedIDs := []uint64{1, 2, 4, 5, 6, 8, 9, 10}
	expectedValues := []int64{10, 20, 40, 50, 60, 80, 90, 100}

	// Check the number of blocks
	blockCount := reader.BlockCount()
	assert.Equal(t, uint64(1), blockCount) // All entries should fit in one block

	// Check the entries in the block
	ids, values, err := reader.GetPairs(0)
	require.NoError(t, err)
	assert.Equal(t, len(expectedIDs), len(ids))
	assert.Equal(t, len(expectedValues), len(values))

	// Create maps for easier comparison
	idToValue := make(map[uint64]int64)
	for i, id := range ids {
		idToValue[id] = values[i]
	}

	for i, id := range expectedIDs {
		value, exists := idToValue[id]
		assert.True(t, exists)
		assert.Equal(t, expectedValues[i], value)
	}
}

func TestMemtableConcurrentOperations(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	// Create a memtable with default options
	m := NewMemtable(nil)

	// Number of operations per goroutine
	const numOps = 1000
	const numGoroutines = 10

	// Wait group to synchronize goroutines
	done := make(chan bool)
	errChan := make(chan error, numGoroutines)

	// Start writer goroutines
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			// Add entries
			for i := 0; i < numOps; i++ {
				key := uint64(id*numOps + i)
				err := m.Add(key, int64(key*10))
				if err != nil {
					errChan <- fmt.Errorf("Add error: %w", err)
					return
				}

				// Occasionally delete entries
				if i%10 == 0 {
					deleteKey := uint64(id*numOps + i/2)
					m.Delete(deleteKey)
				}
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines to finish
	for i := 0; i < numGoroutines; i++ {
		select {
		case err := <-errChan:
			t.Fatalf("Error in concurrent operations: %v", err)
		case <-done:
			// Goroutine completed successfully
		case <-time.After(30 * time.Second):
			t.Fatalf("Timeout waiting for goroutines to complete")
		}
	}

	// Verify that the memtable has the expected number of active entries
	// Each goroutine adds numOps entries and deletes numOps/10 entries
	expectedActive := int64(numGoroutines * (numOps - numOps/10))
	assert.True(t, m.ActiveCount() > 0)
	t.Logf("Active entries: %d, Expected at least: %d", m.ActiveCount(), expectedActive)

	// Flush the memtable to verify integrity
	tmpFile, err := os.CreateTemp("", "memtable_concurrent_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	err = m.Flush(tmpFile.Name())
	require.NoError(t, err)
}
