package col

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// TestUpdateExperiment tests the approach of using global ID bitmaps as deny lists
// for handling updates across multiple files.
func TestUpdateExperiment(t *testing.T) {
	// Create two temporary files for testing
	file1, err := os.CreateTemp("", "update_experiment_file1_*.col")
	require.NoError(t, err)
	defer os.Remove(file1.Name())
	file1.Close()

	file2, err := os.CreateTemp("", "update_experiment_file2_*.col")
	require.NoError(t, err)
	defer os.Remove(file2.Name())
	file2.Close()

	// Setup file 1 (t=0) with initial data
	// IDs: 1-10 with values 10-100
	writer1, err := NewWriter(file1.Name())
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
	writer2, err := NewWriter(file2.Name())
	require.NoError(t, err)

	// Updated IDs (5-7)
	updatedIDs := []uint64{5, 6, 7}
	updatedValues := []int64{500, 600, 700}
	err = writer2.WriteBlock(updatedIDs, updatedValues)
	require.NoError(t, err)

	// New IDs (11-15)
	newIDs := []uint64{11, 12, 13, 14, 15}
	newValues := []int64{110, 120, 130, 140, 150}
	err = writer2.WriteBlock(newIDs, newValues)
	require.NoError(t, err)

	err = writer2.FinalizeAndClose()
	require.NoError(t, err)

	// Open both files for reading
	reader1, err := NewReader(file1.Name())
	require.NoError(t, err)
	defer reader1.Close()

	reader2, err := NewReader(file2.Name())
	require.NoError(t, err)
	defer reader2.Close()

	// Step 1: Aggregate all values from file 2
	result2 := reader2.AggregateWithOptions(AggregateOptions{})

	// Step 2: Extract the bitmap from file 2
	bitmap2, err := reader2.GetGlobalIDBitmap()
	require.NoError(t, err)

	// Step 3: Use bitmap2 as a deny list for file 1
	result1 := reader1.AggregateWithOptions(AggregateOptions{
		DenyFilter: bitmap2,
	})

	// Step 4: Merge the two aggregation results
	mergedResult := AggregateResult{
		Count: result1.Count + result2.Count,
		Min:   minInt64(result1.Min, result2.Min),
		Max:   maxInt64(result1.Max, result2.Max),
		Sum:   result1.Sum + result2.Sum,
	}
	if mergedResult.Count > 0 {
		mergedResult.Avg = float64(mergedResult.Sum) / float64(mergedResult.Count)
	}

	// Step 5: Validate the results
	// Expected results:
	// - From file 1: IDs 1-4, 8-10 with values 10-40, 80-100 (7 items)
	// - From file 2: IDs 5-7, 11-15 with values 500-700, 110-150 (8 items)
	// - Total: 15 items, min=10, max=700, sum=2590, avg=172.67

	// Validate count
	assert.Equal(t, 15, mergedResult.Count, "Merged count should be 15")

	// Validate min
	assert.Equal(t, int64(10), mergedResult.Min, "Merged min should be 10")

	// Validate max
	assert.Equal(t, int64(700), mergedResult.Max, "Merged max should be 700")

	// Validate sum
	expectedTotalSum := int64(10+20+30+40+80+90+100) + // From file 1 (non-updated)
		int64(500+600+700) + // Updated values from file 2
		int64(110+120+130+140+150) // New values from file 2
	assert.Equal(t, expectedTotalSum, mergedResult.Sum, "Merged sum should be 2590")

	// Validate average
	expectedAvg := float64(expectedTotalSum) / 15.0
	assert.InDelta(t, expectedAvg, mergedResult.Avg, 0.01, "Merged average should be approximately 172.67")

	// Additional validation: check that we're correctly excluding updated IDs from file 1
	// We'll do this by manually reading all blocks and checking

	// Read all IDs and values from file 1
	allIDs1 := make([]uint64, 0)
	allValues1 := make([]int64, 0)
	for i := uint64(0); i < reader1.BlockCount(); i++ {
		ids, values, err := reader1.GetPairs(i)
		require.NoError(t, err)
		allIDs1 = append(allIDs1, ids...)
		allValues1 = append(allValues1, values...)
	}

	// Read all IDs and values from file 2
	allIDs2 := make([]uint64, 0)
	allValues2 := make([]int64, 0)
	for i := uint64(0); i < reader2.BlockCount(); i++ {
		ids, values, err := reader2.GetPairs(i)
		require.NoError(t, err)
		allIDs2 = append(allIDs2, ids...)
		allValues2 = append(allValues2, values...)
	}

	// Create a map of all IDs and their values
	idToValue := make(map[uint64]int64)

	// First add all IDs from file 1
	for i, id := range allIDs1 {
		idToValue[id] = allValues1[i]
	}

	// Then overwrite with IDs from file 2 (simulating updates)
	for i, id := range allIDs2 {
		idToValue[id] = allValues2[i]
	}

	// Calculate expected aggregation results
	expectedCount := len(idToValue)
	var expectedMin int64 = 1<<63 - 1 // Max int64 value
	var expectedMax int64 = -1 << 63  // Min int64 value
	var expectedSum int64 = 0

	for _, value := range idToValue {
		if value < expectedMin {
			expectedMin = value
		}
		if value > expectedMax {
			expectedMax = value
		}
		expectedSum += value
	}
	expectedAverage := float64(expectedSum) / float64(expectedCount)

	// Compare with our merged results
	assert.Equal(t, expectedCount, mergedResult.Count, "Merged count should match manual calculation")
	assert.Equal(t, expectedMin, mergedResult.Min, "Merged min should match manual calculation")
	assert.Equal(t, expectedMax, mergedResult.Max, "Merged max should match manual calculation")
	assert.Equal(t, expectedSum, mergedResult.Sum, "Merged sum should match manual calculation")
	assert.InDelta(t, expectedAverage, mergedResult.Avg, 0.01, "Merged average should match manual calculation")
}

// Helper function to find the minimum of two int64 values
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Helper function to find the maximum of two int64 values
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// TestDenyFilterExperiment tests the deny filter functionality specifically
func TestDenyFilterExperiment(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "deny_filter_test_*.col")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create a writer
	writer, err := NewWriter(tmpFile.Name())
	require.NoError(t, err)

	// Write a block with IDs 1-10
	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	values := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	err = writer.WriteBlock(ids, values)
	require.NoError(t, err)

	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(tmpFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	// Create a deny filter for IDs 5-7
	denyFilter := sroar.NewBitmap()
	denyFilter.Set(5)
	denyFilter.Set(6)
	denyFilter.Set(7)

	// Aggregate with the deny filter
	result := reader.AggregateWithOptions(AggregateOptions{
		DenyFilter: denyFilter,
	})

	// Expected results: IDs 1-4, 8-10 with values 10-40, 80-100 (7 items)
	// Count: 7, min=10, max=100, sum=370, avg=52.86
	assert.Equal(t, 7, result.Count, "Count should be 7")
	assert.Equal(t, int64(10), result.Min, "Min should be 10")
	assert.Equal(t, int64(100), result.Max, "Max should be 100")
	assert.Equal(t, int64(10+20+30+40+80+90+100), result.Sum, "Sum should be 370")
	assert.InDelta(t, float64(10+20+30+40+80+90+100)/7.0, result.Avg, 0.01, "Average should be approximately 52.86")
}
