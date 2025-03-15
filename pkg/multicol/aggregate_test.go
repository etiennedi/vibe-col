package multicol

import (
	"os"
	"testing"

	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// TestMultiReaderAggregate tests the aggregation functionality across multiple readers.
func TestMultiReaderAggregate(t *testing.T) {
	// Create three temporary files for testing
	file1, err := os.CreateTemp("", "multicol_test_file1_*.col")
	require.NoError(t, err)
	defer os.Remove(file1.Name())
	file1.Close()

	file2, err := os.CreateTemp("", "multicol_test_file2_*.col")
	require.NoError(t, err)
	defer os.Remove(file2.Name())
	file2.Close()

	file3, err := os.CreateTemp("", "multicol_test_file3_*.col")
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

	// Setup file 2 (t=1) with updates and new data
	// Updates: IDs 5-7 with new values 500-700
	// New: IDs 11-15 with values 110-150
	writer2, err := col.NewWriter(file2.Name())
	require.NoError(t, err)

	// Updated IDs (5-7)
	updatedIDs1 := []uint64{5, 6, 7}
	updatedValues1 := []int64{500, 600, 700}
	err = writer2.WriteBlock(updatedIDs1, updatedValues1)
	require.NoError(t, err)

	// New IDs (11-15)
	newIDs1 := []uint64{11, 12, 13, 14, 15}
	newValues1 := []int64{110, 120, 130, 140, 150}
	err = writer2.WriteBlock(newIDs1, newValues1)
	require.NoError(t, err)

	err = writer2.FinalizeAndClose()
	require.NoError(t, err)

	// Setup file 3 (t=2) with more updates and new data
	// Updates: IDs 3-4 with new values 300-400
	// Updates: IDs 11-12 with new values 1100-1200
	// New: IDs 16-20 with values 160-200
	writer3, err := col.NewWriter(file3.Name())
	require.NoError(t, err)

	// Updated IDs (3-4, 11-12)
	updatedIDs2 := []uint64{3, 4, 11, 12}
	updatedValues2 := []int64{300, 400, 1100, 1200}
	err = writer3.WriteBlock(updatedIDs2, updatedValues2)
	require.NoError(t, err)

	// New IDs (16-20)
	newIDs2 := []uint64{16, 17, 18, 19, 20}
	newValues2 := []int64{160, 170, 180, 190, 200}
	err = writer3.WriteBlock(newIDs2, newValues2)
	require.NoError(t, err)

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
	multiReader := NewMultiReader(readers)
	defer multiReader.Close()

	// Aggregate across all readers
	result, err := multiReader.Aggregate(AggregateOptions{})
	require.NoError(t, err)

	// Expected results:
	// - From file 1: IDs 1-2, 8-10 with values 10-20, 80-100 (5 items)
	// - From file 2: IDs 5-7, 13-15 with values 500-700, 130-150 (6 items)
	// - From file 3: IDs 3-4, 11-12, 16-20 with values 300-400, 1100-1200, 160-200 (9 items)
	// - Total: 20 items

	// Validate count
	assert.Equal(t, 20, result.Count, "Count should be 20")

	// Validate min
	assert.Equal(t, int64(10), result.Min, "Min should be 10")

	// Validate max
	assert.Equal(t, int64(1200), result.Max, "Max should be 1200")

	// Calculate expected sum
	expectedSum := int64(0)
	// From file 1 (non-updated)
	expectedSum += int64(10 + 20 + 80 + 90 + 100)
	// From file 2 (non-updated)
	expectedSum += int64(500 + 600 + 700 + 130 + 140 + 150)
	// From file 3 (all)
	expectedSum += int64(300 + 400 + 1100 + 1200 + 160 + 170 + 180 + 190 + 200)

	// Validate sum
	assert.Equal(t, expectedSum, result.Sum, "Sum should match expected value")

	// Validate average
	expectedAvg := float64(expectedSum) / 20.0
	assert.InDelta(t, expectedAvg, result.Avg, 0.01, "Average should match expected value")

	// Additional validation: test with a filter
	filter := sroar.NewBitmap()
	// Only include IDs 1-10
	for i := uint64(1); i <= 10; i++ {
		filter.Set(i)
	}

	// Aggregate with filter
	filteredResult, err := multiReader.Aggregate(AggregateOptions{
		Filter: filter,
	})
	require.NoError(t, err)

	// Expected filtered results:
	// - From file 1: IDs 1-2, 8-10 with values 10-20, 80-100 (5 items)
	// - From file 2: IDs 5-7 with values 500-700 (3 items)
	// - From file 3: IDs 3-4 with values 300-400 (2 items)
	// - Total: 10 items

	// Validate filtered count
	assert.Equal(t, 10, filteredResult.Count, "Filtered count should be 10")

	// Calculate expected filtered sum
	expectedFilteredSum := int64(0)
	// From file 1 (non-updated)
	expectedFilteredSum += int64(10 + 20 + 80 + 90 + 100)
	// From file 2 (non-updated)
	expectedFilteredSum += int64(500 + 600 + 700)
	// From file 3 (all)
	expectedFilteredSum += int64(300 + 400)

	// Validate filtered sum
	assert.Equal(t, expectedFilteredSum, filteredResult.Sum, "Filtered sum should match expected value")
}

// TestMultiReaderAggregateEmpty tests the aggregation functionality with empty readers.
func TestMultiReaderAggregateEmpty(t *testing.T) {
	// Create a MultiReader with no readers
	multiReader := NewMultiReader([]*col.Reader{})

	// Aggregate should return an empty result
	result, err := multiReader.Aggregate(AggregateOptions{})
	require.NoError(t, err)
	assert.Equal(t, 0, result.Count, "Count should be 0 for empty MultiReader")
	assert.Equal(t, int64(0), result.Sum, "Sum should be 0 for empty MultiReader")
	assert.Equal(t, 0.0, result.Avg, "Average should be 0 for empty MultiReader")
}
