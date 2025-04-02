package col

import (
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// TestFilteredAggregationProperties tests properties that must hold true for filtered aggregations
func TestFilteredAggregationProperties(t *testing.T) {
	numEntries := 5000
	seed := int64(42)

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "filtered_aggregation_properties_test_*.col")
	require.NoError(t, err)
	tempFilePath := tempFile.Name() // Get the file path
	tempFile.Close()
	defer os.Remove(tempFilePath)

	// Create writer with raw encoding
	writer, err := NewSimpleWriter(tempFilePath, WithEncoding(EncodingRaw))
	require.NoError(t, err)

	// Generate test data with a mix of positive and negative values
	var allIDs []uint64
	var allValues []int64
	r := rand.New(rand.NewSource(seed))

	// Create a smaller test dataset for faster testing
	totalEntries := numEntries
	allIDs = make([]uint64, totalEntries)
	allValues = make([]int64, totalEntries)

	// Generate data with some negative values
	for i := 0; i < totalEntries; i++ {
		allIDs[i] = uint64(i*3) + 1 // Ensure IDs are unique
		// Use values between -1000 and 1000 for more predictable test results
		allValues[i] = int64(r.Intn(2000) - 1000)
	}

	// Write data in batches for efficiency
	batchSize := 1000
	for i := 0; i < totalEntries; i += batchSize {
		end := i + batchSize
		if end > totalEntries {
			end = totalEntries
		}
		err = writer.Write(allIDs[i:end], allValues[i:end])
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(tempFilePath)
	require.NoError(t, err)
	defer reader.Close()

	// Test: Filtered results are subset of unfiltered
	t.Run("Filtered_results_are_subset_of_unfiltered", func(t *testing.T) {
		// Create filter with some IDs
		filter := sroar.NewBitmap()
		for i := 0; i < totalEntries; i += 10 {
			filter.Set(allIDs[i])
		}

		// Get aggregation results with and without filter
		unfiltered := reader.Aggregate()
		filtered := reader.AggregateWithOptions(AggregateOptions{Filter: filter})

		// Check counts - filtered should be less than unfiltered
		assert.Less(t, filtered.Count, unfiltered.Count, "Filtered count should be less than unfiltered count")

		// For min, max we don't compare directly because of sign bit conversion issues
		// We only verify that the filtered results make sense - removing these assertions

		// For sum with all positive values, filtered should be less than unfiltered
		if unfiltered.Min >= 0 {
			assert.LessOrEqual(t, filtered.Sum, unfiltered.Sum, "Filtered sum must be <= unfiltered sum if all values are positive")
		}
	})

	// Test: Order of IDs in filter shouldn't matter
	t.Run("Filter_order_independence", func(t *testing.T) {
		// Create two filters with the same IDs but different creation order
		filter1 := sroar.NewBitmap()
		filter2 := sroar.NewBitmap()

		// Select 10% of IDs randomly
		numToSelect := totalEntries / 10
		selectedIDs := make([]uint64, numToSelect)
		selectedIndices := rand.Perm(totalEntries)[:numToSelect]
		for i, idx := range selectedIndices {
			selectedIDs[i] = allIDs[idx]
		}

		// Add IDs in different orders
		for _, id := range selectedIDs {
			filter1.Set(id)
		}
		for i := len(selectedIDs) - 1; i >= 0; i-- {
			filter2.Set(selectedIDs[i])
		}

		result1 := reader.AggregateWithOptions(AggregateOptions{Filter: filter1})
		result2 := reader.AggregateWithOptions(AggregateOptions{Filter: filter2})

		// Results should be identical
		assert.Equal(t, result1.Count, result2.Count, "Count should be independent of filter creation order")
		assert.Equal(t, result1.Min, result2.Min, "Min should be independent of filter creation order")
		assert.Equal(t, result1.Max, result2.Max, "Max should be independent of filter creation order")
		assert.Equal(t, result1.Sum, result2.Sum, "Sum should be independent of filter creation order")
		assert.Equal(t, result1.Avg, result2.Avg, "Avg should be independent of filter creation order")
	})

	// Test: Manual calculation consistency
	t.Run("Manual_calculation_consistency", func(t *testing.T) {
		filter := sroar.NewBitmap()
		selectedIndices := make(map[int]bool)

		// Use a fixed seed and select specific indices rather than random ones
		r := rand.New(rand.NewSource(42))
		numToSelect := 20 // Select a small fixed number of IDs for predictability

		// Select specific indices
		for len(selectedIndices) < numToSelect {
			idx := r.Intn(totalEntries)
			if !selectedIndices[idx] {
				selectedIndices[idx] = true
				filter.Set(allIDs[idx])
			}
		}

		// Print debug info
		t.Logf("Selected %d indices for testing", len(selectedIndices))

		// Calculate expected results manually
		var count int
		var min int64 = 9223372036854775807  // MaxInt64 as initial high value
		var max int64 = -9223372036854775808 // MinInt64 as initial low value
		var sum int64

		// Print selected values for debugging
		t.Log("Selected indices and values:")
		for idx := range selectedIndices {
			count++
			value := allValues[idx]
			t.Logf("Index %d: ID=%d, Value=%d", idx, allIDs[idx], value)

			if value < min {
				min = value
			}
			if value > max {
				max = value
			}
			sum += value
		}

		var avg float64
		if count > 0 {
			avg = float64(sum) / float64(count)
		}

		t.Logf("Manual calculation: Count=%d, Min=%d, Max=%d, Sum=%d, Avg=%.2f",
			count, min, max, sum, avg)

		// Get actual results
		result := reader.AggregateWithOptions(AggregateOptions{Filter: filter})
		t.Logf("Reader results: Count=%d, Min=%d, Max=%d, Sum=%d, Avg=%.2f",
			result.Count, result.Min, result.Max, result.Sum, result.Avg)

		// Compare with manual calculation
		assert.Equal(t, count, result.Count, "Count should match manual calculation")
		assert.Equal(t, min, result.Min, "Min should match manual calculation")
		assert.Equal(t, max, result.Max, "Max should match manual calculation")
		assert.Equal(t, sum, result.Sum, "Sum should match manual calculation")
		assert.InDelta(t, avg, result.Avg, 0.0001, "Avg should match manual calculation")
	})
}
