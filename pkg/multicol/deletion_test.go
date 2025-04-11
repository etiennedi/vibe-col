package multicol

import (
	"os"
	"testing"

	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiReaderWithDeletions tests that the MultiReader correctly handles
// deleted IDs across multiple readers.
func TestMultiReaderWithDeletions(t *testing.T) {
	// Create three temporary files for testing
	file1, err := os.CreateTemp("", "multicol_deletion_test_file1_*.col")
	require.NoError(t, err)
	defer os.Remove(file1.Name())
	file1.Close()

	file2, err := os.CreateTemp("", "multicol_deletion_test_file2_*.col")
	require.NoError(t, err)
	defer os.Remove(file2.Name())
	file2.Close()

	file3, err := os.CreateTemp("", "multicol_deletion_test_file3_*.col")
	require.NoError(t, err)
	defer os.Remove(file3.Name())
	file3.Close()

	// Setup file 1 (t=0) with initial data
	// IDs: 1-10 with values 10-100
	writer1, err := col.NewWriter(file1.Name())
	require.NoError(t, err)

	ids1 := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	values1 := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	err = writer1.WriteBlock(ids1, values1)
	require.NoError(t, err)

	err = writer1.FinalizeAndClose()
	require.NoError(t, err)

	// Setup file 2 (t=1) with some deletions and new data
	// Deletions: IDs 3, 4, 5 (these should now be ignored from file 1)
	// New: IDs 11-15 with values 110-150
	writer2, err := col.NewWriter(file2.Name())
	require.NoError(t, err)

	// New IDs (11-15)
	newIDs := []uint64{11, 12, 13, 14, 15}
	newValues := []int64{110, 120, 130, 140, 150}
	err = writer2.WriteBlock(newIDs, newValues)
	require.NoError(t, err)

	// Add some IDs to the deleted bitmap (3, 4, 5)
	deletedIDs := []uint64{3, 4, 5}
	writer2.BatchAddDeletedIDs(deletedIDs)

	err = writer2.FinalizeAndClose()
	require.NoError(t, err)

	// Setup file 3 (t=2) with updates, more deletions, and new data
	// Updates: IDs 6-7 with new values 600-700
	// Deletions: IDs 8, 9 (these should now be ignored from file 1)
	// Deletions: IDs 11, 12 (these should now be ignored from file 2)
	// New: IDs 16-20 with values 160-200
	writer3, err := col.NewWriter(file3.Name())
	require.NoError(t, err)

	// Updated IDs (6-7)
	updatedIDs := []uint64{6, 7}
	updatedValues := []int64{600, 700}
	err = writer3.WriteBlock(updatedIDs, updatedValues)
	require.NoError(t, err)

	// New IDs (16-20)
	newIDs2 := []uint64{16, 17, 18, 19, 20}
	newValues2 := []int64{160, 170, 180, 190, 200}
	err = writer3.WriteBlock(newIDs2, newValues2)
	require.NoError(t, err)

	// Add more IDs to the deleted bitmap
	moreDeletedIDs := []uint64{8, 9, 11, 12}
	writer3.BatchAddDeletedIDs(moreDeletedIDs)

	err = writer3.FinalizeAndClose()
	require.NoError(t, err)

	// Open all files for reading
	reader1, err := col.NewReader(file1.Name())
	require.NoError(t, err)
	defer reader1.Close()

	reader2, err := col.NewReader(file2.Name())
	require.NoError(t, err)
	defer reader2.Close()

	reader3, err := col.NewReader(file3.Name())
	require.NoError(t, err)
	defer reader3.Close()

	// Create a MultiReader with all readers (ordered from oldest to newest)
	readers := []*col.Reader{reader1, reader2, reader3}
	multiReader := NewColReaderMultiReader(readers)
	defer multiReader.Close()

	// Aggregate across all readers
	result, err := multiReader.Aggregate(AggregateOptions{})
	require.NoError(t, err)

	// Expected results:
	// - From file 1: IDs 1-2, 10 with values 10-20, 100 (3 items)
	//   (IDs 3-5 deleted in file 2, IDs 6-7 updated in file 3, IDs 8-9 deleted in file 3)
	// - From file 2: IDs 13-15 with values 130-150 (3 items)
	//   (IDs 11-12 deleted in file 3)
	// - From file 3: IDs 6-7, 16-20 with values 600-700, 160-200 (7 items)
	// - Total: 13 items

	// Validate count
	assert.Equal(t, 13, result.Count, "Count should be 13")

	// Calculate expected sum
	expectedSum := int64(0)
	// From file 1 (non-deleted, non-updated)
	expectedSum += int64(10 + 20 + 100) // IDs 1-2, 10
	// From file 2 (non-deleted)
	expectedSum += int64(130 + 140 + 150) // IDs 13-15
	// From file 3 (all)
	expectedSum += int64(600 + 700 + 160 + 170 + 180 + 190 + 200) // IDs 6-7, 16-20

	// Validate sum
	assert.Equal(t, expectedSum, result.Sum, "Sum should match expected value")

	// Validate average
	expectedAvg := float64(expectedSum) / 13.0
	assert.InDelta(t, expectedAvg, result.Avg, 0.01, "Average should match expected value")

	// Check min and max
	assert.Equal(t, int64(10), result.Min, "Min should be 10")
	assert.Equal(t, int64(700), result.Max, "Max should be 700")
}
