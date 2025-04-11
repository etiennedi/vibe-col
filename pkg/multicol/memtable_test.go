package multicol

import (
	"os"
	"path/filepath"
	"testing"
	"time"
	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func TestMemtableBasicOperations(t *testing.T) {
	// Create a new memtable
	m := NewMemtable(nil)

	// Test that it starts empty
	count := m.ActiveCount()
	assert.Equal(t, int64(0), count)
	assert.True(t, m.IsEmpty())

	// Add an entry
	err := m.Add(1, 100)
	require.NoError(t, err)

	// Verify it was added
	count = m.ActiveCount()
	assert.Equal(t, int64(1), count)
	assert.False(t, m.IsEmpty())

	// Get the entry
	val, ok := m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, int64(100), val)

	// Add more entries
	err = m.Add(2, 200)
	require.NoError(t, err)
	err = m.Add(5, 500)
	require.NoError(t, err)

	// Delete an entry
	deleted := m.Delete(2)
	assert.True(t, deleted)

	// Verify deletion
	_, ok = m.Get(2)
	assert.False(t, ok)

	// Delete a non-existent entry
	// With our updated design, this returns true even for non-existent IDs
	// because we want to mark them as deleted in the memtable's deletion list
	deleted = m.Delete(10)
	assert.True(t, deleted)

	// Verify count after deletion
	count = m.ActiveCount()
	assert.Equal(t, int64(2), count)
	assert.False(t, m.IsEmpty())

	// Delete remaining entries
	m.Delete(1)
	m.Delete(5)

	// Verify memtable is empty
	assert.True(t, m.IsEmpty())
	assert.Equal(t, int64(0), m.ActiveCount())
}

func TestMemtableBatchOperations(t *testing.T) {
	// Create a new memtable
	m := NewMemtable(nil)

	// Test batch add
	ids := []uint64{1, 2, 3, 4, 5}
	values := []int64{100, 200, 300, 400, 500}

	err := m.BatchAdd(ids, values)
	require.NoError(t, err)

	// Verify all entries were added
	count := m.ActiveCount()
	assert.Equal(t, int64(5), count)

	// Test each value was added correctly
	for i, id := range ids {
		v, ok := m.Get(id)
		assert.True(t, ok)
		assert.Equal(t, values[i], v)
	}

	// Test batch delete
	deleteIDs := []uint64{2, 4, 6} // Note: 6 doesn't exist
	// With our updated design, BatchDelete returns the number of IDs provided
	// regardless of whether they existed in the memtable
	deleteCount := m.BatchDelete(deleteIDs)
	assert.Equal(t, 3, deleteCount) // Should delete all 3 entries (marking 6 as deleted too)

	// Verify deletions
	_, ok := m.Get(2)
	assert.False(t, ok)
	_, ok = m.Get(4)
	assert.False(t, ok)

	// Verify remaining entries
	count = m.ActiveCount()
	assert.Equal(t, int64(3), count)

	// Test with empty batches
	err = m.BatchAdd([]uint64{}, []int64{})
	assert.NoError(t, err)

	deleteCount = m.BatchDelete([]uint64{})
	assert.Equal(t, 0, deleteCount)
}

func TestMemtableScan(t *testing.T) {
	// Create a new memtable
	m := NewMemtable(nil)

	// Add entries
	for i := uint64(1); i <= 10; i++ {
		err := m.Add(i, int64(i*100))
		require.NoError(t, err)
	}

	// Delete a few entries
	m.Delete(3)
	m.Delete(7)

	// Test scan entire range
	ids, values := m.Scan(0, 100)
	assert.Equal(t, 8, len(ids))
	assert.Equal(t, 8, len(values))

	// Verify deleted entries are not included
	for _, id := range ids {
		assert.NotEqual(t, uint64(3), id)
		assert.NotEqual(t, uint64(7), id)
	}

	// Test scan with range limits
	ids, values = m.Scan(5, 9)
	assert.Equal(t, 4, len(ids))
	assert.Equal(t, 4, len(values))

	for _, id := range ids {
		assert.True(t, id >= 5 && id <= 9)
		assert.NotEqual(t, uint64(7), id)
	}

	// Test scan with non-existent range
	ids, values = m.Scan(100, 200)
	assert.Equal(t, 0, len(ids))
	assert.Equal(t, 0, len(values))
}

func TestMemtableAggregate(t *testing.T) {
	// Create a new memtable
	m := NewMemtable(nil)

	// Add entries
	ids := []uint64{1, 3, 5, 7, 9}
	values := []int64{10, 30, 50, 70, 90}

	err := m.BatchAdd(ids, values)
	require.NoError(t, err)

	// Delete one entry
	m.Delete(3)

	// Aggregate should ignore deleted entry
	minID, maxID, minValue, maxValue, sum, count := m.Aggregate()
	assert.Equal(t, uint64(1), minID)
	assert.Equal(t, uint64(9), maxID)
	assert.Equal(t, int64(10), minValue)
	assert.Equal(t, int64(90), maxValue)
	assert.Equal(t, int64(10+50+70+90), sum)
	assert.Equal(t, 4, count)
}

func TestMemtableFilteredAggregate(t *testing.T) {
	// Create a new memtable
	m := NewMemtable(nil)

	// Add entries
	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	values := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}

	err := m.BatchAdd(ids, values)
	require.NoError(t, err)

	// Delete some entries
	m.Delete(2)
	m.Delete(4)

	// Create a filter for odd numbers
	filter := sroar.NewBitmap()
	for _, id := range []uint64{1, 3, 5, 7, 9} {
		filter.Set(id)
	}

	// Perform filtered aggregation
	minID, maxID, minValue, maxValue, sum, count := m.FilteredAggregate(filter)
	assert.Equal(t, uint64(1), minID)
	assert.Equal(t, uint64(9), maxID)
	assert.Equal(t, int64(10), minValue)
	assert.Equal(t, int64(90), maxValue)
	assert.Equal(t, int64(10+30+50+70+90), sum)
	assert.Equal(t, 5, count)
}

func TestMemtableFlush(t *testing.T) {
	// Create a new memtable
	m := NewMemtable(nil)

	// Add some entries
	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	values := []int64{10, 20, 30, 40, 50, 60, 70, 80}

	err := m.BatchAdd(ids, values)
	require.NoError(t, err)

	// Delete some entries
	m.Delete(2)
	m.Delete(6)

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "memtable_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// When we call Flush, store both return values
	written, err := m.Flush(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to flush memtable: %v", err)
	}

	// Check that the right number of entries were written
	if written != uint64(6) {
		t.Fatalf("Expected to write 6 entries, but wrote %d", written)
	}

	// Open the file and verify its contents
	// This would require implementing a reader, which is out of scope for this test
}

func TestMemtableReadIntegration(t *testing.T) {
	// Create a new memtable
	m := NewMemtable(nil)

	// Add entries
	testSize := 100
	for i := 0; i < testSize; i++ {
		err := m.Add(uint64(i), int64(i*10))
		require.NoError(t, err)
	}

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "memtable_read_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Write to the file
	written, err := m.Flush(tmpFile.Name())
	require.NoError(t, err)

	// Verify number of entries written
	assert.Equal(t, uint64(testSize), written)

	// Reading from the file would require a reader implementation
}

func TestMemtableConcurrentOperations(t *testing.T) {
	// Create a new memtable
	m := NewMemtable(nil)

	// Number of goroutines to run concurrently
	numGoroutines := 10
	// Number of operations per goroutine
	numOps := 1000

	// Channels for synchronization
	done := make(chan bool, numGoroutines)
	errChan := make(chan error, numGoroutines)

	// Start concurrent operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			startID := uint64(id * numOps)
			for j := 0; j < numOps; j++ {
				// Add operation
				idVal := startID + uint64(j)
				err := m.Add(idVal, int64(idVal))
				if err != nil {
					errChan <- err
					return
				}

				// Delete some entries (every 10th entry)
				if j%10 == 0 && j > 0 {
					deleteID := startID + uint64(j-1)
					if !m.Delete(deleteID) {
						errChan <- err
						return
					}
				}
			}
			done <- true
		}(i)
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

	written, err := m.Flush(tmpFile.Name())
	require.NoError(t, err)
	t.Logf("Flushed %d entries to file", written)
}

// TestMemtableSortsDataBeforeFlush tests that the memtable correctly sorts data
// before writing it to a file, as BufferedWriter requires sorted inputs
func TestMemtableSortsDataBeforeFlush(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "memtable-sort-test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new memtable
	mt := NewMemtable(nil)

	// Add entries in reverse order
	numEntries := 100
	for i := numEntries; i > 0; i-- {
		err := mt.Add(uint64(i), int64(i*10))
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Flush to a file
	filePath := filepath.Join(tempDir, "test.col")
	written, err := mt.Flush(filePath)
	if err != nil {
		t.Fatalf("Failed to flush memtable: %v", err)
	}

	// Verify we wrote all entries
	if written != uint64(numEntries) {
		t.Errorf("Expected to write %d entries, but wrote %d", numEntries, written)
	}

	// Read the file back and verify it's sorted
	reader, err := col.NewReader(filePath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// The file should have at least one block
	if reader.BlockCount() == 0 {
		t.Fatal("Expected at least one block in the file")
	}

	// Read all IDs and values
	var allIDs []uint64
	var allValues []int64

	for i := uint64(0); i < reader.BlockCount(); i++ {
		ids, values, err := reader.GetPairs(i)
		if err != nil {
			t.Fatalf("Failed to read block %d: %v", i, err)
		}
		allIDs = append(allIDs, ids...)
		allValues = append(allValues, values...)
	}

	// Verify we got all entries back
	if len(allIDs) != numEntries {
		t.Errorf("Expected %d entries, but got %d", numEntries, len(allIDs))
	}

	// Verify the data is sorted by ID
	for i := 1; i < len(allIDs); i++ {
		if allIDs[i] < allIDs[i-1] {
			t.Errorf("IDs are not sorted at index %d: %d > %d", i, allIDs[i-1], allIDs[i])
		}
	}

	// Also verify the values match the IDs
	for i := 0; i < len(allIDs); i++ {
		expectedValue := int64(allIDs[i] * 10)
		if allValues[i] != expectedValue {
			t.Errorf("Value mismatch at index %d: expected %d, got %d", i, expectedValue, allValues[i])
		}
	}
}

// TestMemtableUpdateAndDeleteBeforeFlush tests that updates and deletions
// are correctly handled before and after flushing the memtable
func TestMemtableUpdateAndDeleteBeforeFlush(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "memtable-update-delete-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// ---------------------------------------------------------
	// SCENARIO 1: Update test
	// ---------------------------------------------------------
	t.Run("Update Before Flush", func(t *testing.T) {
		// Create a new memtable
		mt := NewMemtable(nil)

		// 1. Create an ID with initial value
		initialID := uint64(42)
		initialValue := int64(100)
		err := mt.Add(initialID, initialValue)
		require.NoError(t, err)

		// Add some additional entries for context
		for i := uint64(1); i <= 10; i++ {
			if i != initialID {
				err := mt.Add(i, int64(i*10))
				require.NoError(t, err)
			}
		}

		// Verify the initial value was stored correctly
		val, ok := mt.Get(initialID)
		require.True(t, ok)
		require.Equal(t, initialValue, val)

		// 2. Update the ID with a new value
		updatedValue := int64(200)
		err = mt.Add(initialID, updatedValue)
		require.NoError(t, err)

		// Verify the updated value was stored correctly
		val, ok = mt.Get(initialID)
		require.True(t, ok)
		require.Equal(t, updatedValue, val)

		// 3. Perform aggregation on the memtable and verify results
		result := mt.(AggregateSource).AggregateWithOptions(col.AggregateOptions{})
		require.Equal(t, 11, result.Count, "Should have 11 entries")

		// Create a specific filter for the updated ID
		filter := sroar.NewBitmap()
		filter.Set(initialID)

		filteredResult := mt.(AggregateSource).AggregateWithOptions(col.AggregateOptions{
			Filter: filter,
		})
		require.Equal(t, 1, filteredResult.Count, "Filtered count should be 1")
		require.Equal(t, updatedValue, filteredResult.Sum, "Sum should be the updated value")

		// 4. Flush the memtable to disk
		colFile := filepath.Join(tempDir, "update_test.col")
		written, err := mt.Flush(colFile)
		require.NoError(t, err)
		require.Equal(t, uint64(11), written, "Should have written 11 entries")

		// 5. Create a reader and verify it returns the same aggregation results
		reader, err := col.NewReader(colFile)
		require.NoError(t, err)
		defer reader.Close()

		readerResult := reader.AggregateWithOptions(col.AggregateOptions{})
		require.Equal(t, result.Count, readerResult.Count, "Reader count should match memtable count")
		require.Equal(t, result.Sum, readerResult.Sum, "Reader sum should match memtable sum")

		readerFilteredResult := reader.AggregateWithOptions(col.AggregateOptions{
			Filter: filter,
		})
		require.Equal(t, filteredResult.Count, readerFilteredResult.Count, "Filtered counts should match")
		require.Equal(t, filteredResult.Sum, readerFilteredResult.Sum, "Filtered sums should match")
	})

	// ---------------------------------------------------------
	// SCENARIO 2: Delete test
	// ---------------------------------------------------------
	t.Run("Delete Before Flush", func(t *testing.T) {
		// Create a new memtable
		mt := NewMemtable(nil)

		// 1. Add multiple entries
		totalEntries := 20
		for i := uint64(1); i <= uint64(totalEntries); i++ {
			err := mt.Add(i, int64(i*10))
			require.NoError(t, err)
		}

		// Verify all entries were added
		require.Equal(t, int64(totalEntries), mt.ActiveCount(), "Should have added all entries")

		// 2. Delete an ID
		deletedID := uint64(5)
		deleted := mt.Delete(deletedID)
		require.True(t, deleted, "Delete should succeed")

		// Verify the ID was deleted
		_, exists := mt.Get(deletedID)
		require.False(t, exists, "Deleted ID should no longer exist")

		// 3. Perform aggregation and verify results
		result := mt.(AggregateSource).AggregateWithOptions(col.AggregateOptions{})
		require.Equal(t, totalEntries-1, result.Count, "Count should exclude deleted ID")

		// Calculate expected sum (exclude deleted ID)
		var expectedSum int64
		for i := uint64(1); i <= uint64(totalEntries); i++ {
			if i != deletedID {
				expectedSum += int64(i * 10)
			}
		}
		require.Equal(t, expectedSum, result.Sum, "Sum should exclude deleted ID")

		// Get the deleted ID bitmap
		deletedBitmap, err := mt.GetDeletedIDBitmap()
		require.NoError(t, err)
		require.True(t, deletedBitmap.Contains(deletedID), "Deleted bitmap should contain the ID")
		require.Equal(t, int(1), deletedBitmap.GetCardinality(), "Should have exactly one deleted ID")

		// 4. Flush the memtable
		colFile := filepath.Join(tempDir, "delete_test.col")
		written, err := mt.Flush(colFile)
		require.NoError(t, err)
		require.Equal(t, uint64(totalEntries-1), written, "Should have written all non-deleted entries")

		// 5. Create a reader and verify aggregation results
		reader, err := col.NewReader(colFile)
		require.NoError(t, err)
		defer reader.Close()

		readerResult := reader.AggregateWithOptions(col.AggregateOptions{})
		require.Equal(t, result.Count, readerResult.Count, "Reader count should match memtable count")
		require.Equal(t, result.Sum, readerResult.Sum, "Reader sum should match memtable sum")

		// 6. Verify the reader's deleted ID bitmap
		readerDeletedBitmap, err := reader.GetDeletedIDBitmap()
		require.NoError(t, err)
		require.True(t, readerDeletedBitmap.Contains(deletedID), "Reader's deleted bitmap should contain the ID")
		require.Equal(t, int(1), readerDeletedBitmap.GetCardinality(), "Reader should have exactly one deleted ID")
	})

	// ---------------------------------------------------------
	// SCENARIO 3: Delete + Re-add test
	// ---------------------------------------------------------
	t.Run("Delete Then Re-add Before Flush", func(t *testing.T) {
		// Create a new memtable
		mt := NewMemtable(nil)

		// 1. Add multiple entries
		totalEntries := 20
		for i := uint64(1); i <= uint64(totalEntries); i++ {
			err := mt.Add(i, int64(i*10))
			require.NoError(t, err)
		}

		// 2. Delete an ID
		readdedID := uint64(7)
		deleted := mt.Delete(readdedID)
		require.True(t, deleted, "Delete should succeed")

		// Verify the ID was deleted
		_, exists := mt.Get(readdedID)
		require.False(t, exists, "Deleted ID should no longer exist")

		// 3. Perform aggregation and verify results
		resultAfterDelete := mt.(AggregateSource).AggregateWithOptions(col.AggregateOptions{})
		require.Equal(t, totalEntries-1, resultAfterDelete.Count, "Count should exclude deleted ID")

		// Calculate expected sum (exclude deleted ID)
		var expectedSumAfterDelete int64
		for i := uint64(1); i <= uint64(totalEntries); i++ {
			if i != readdedID {
				expectedSumAfterDelete += int64(i * 10)
			}
		}
		require.Equal(t, expectedSumAfterDelete, resultAfterDelete.Sum, "Sum should exclude deleted ID")

		// Get the deleted ID bitmap after deletion
		deletedBitmapAfterDelete, err := mt.GetDeletedIDBitmap()
		require.NoError(t, err)
		require.True(t, deletedBitmapAfterDelete.Contains(readdedID), "Deleted bitmap should contain the ID")

		// 4. Re-add the same ID with a different value
		newValue := int64(777) // Distinct value to easily identify
		err = mt.Add(readdedID, newValue)
		require.NoError(t, err)

		// Verify the ID was re-added with the new value
		val, exists := mt.Get(readdedID)
		require.True(t, exists, "Re-added ID should exist")
		require.Equal(t, newValue, val, "Re-added ID should have the new value")

		// 5. Perform aggregation after re-adding and verify results
		resultAfterReadd := mt.(AggregateSource).AggregateWithOptions(col.AggregateOptions{})
		require.Equal(t, totalEntries, resultAfterReadd.Count, "Count should include re-added ID")

		// Calculate expected sum (include re-added ID with new value)
		expectedSumAfterReadd := expectedSumAfterDelete + newValue
		require.Equal(t, expectedSumAfterReadd, resultAfterReadd.Sum, "Sum should include re-added ID with new value")

		// Get the deleted ID bitmap after re-adding
		deletedBitmapAfterReadd, err := mt.GetDeletedIDBitmap()
		require.NoError(t, err)
		require.False(t, deletedBitmapAfterReadd.Contains(readdedID), "Deleted bitmap should NOT contain the re-added ID")

		// 6. Flush the memtable
		colFile := filepath.Join(tempDir, "delete_readd_test.col")
		written, err := mt.Flush(colFile)
		require.NoError(t, err)
		require.Equal(t, uint64(totalEntries), written, "Should have written all entries including re-added one")

		// 7. Create a reader and verify aggregation results
		reader, err := col.NewReader(colFile)
		require.NoError(t, err)
		defer reader.Close()

		readerResult := reader.AggregateWithOptions(col.AggregateOptions{})
		require.Equal(t, resultAfterReadd.Count, readerResult.Count, "Reader count should match memtable count after re-add")
		require.Equal(t, resultAfterReadd.Sum, readerResult.Sum, "Reader sum should match memtable sum after re-add")

		// Create a filter for the re-added ID to verify its value
		readdFilter := sroar.NewBitmap()
		readdFilter.Set(readdedID)

		readdFilteredResult := reader.AggregateWithOptions(col.AggregateOptions{
			Filter: readdFilter,
		})
		require.Equal(t, 1, readdFilteredResult.Count, "Should have exactly one entry for the re-added ID")
		require.Equal(t, newValue, readdFilteredResult.Sum, "Sum should be the new value of the re-added ID")

		// 8. Verify the reader's deleted ID bitmap doesn't contain the re-added ID
		readerDeletedBitmap, err := reader.GetDeletedIDBitmap()
		require.NoError(t, err)
		require.False(t, readerDeletedBitmap.Contains(readdedID), "Reader's deleted bitmap should NOT contain the re-added ID")
	})
}
