package col

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/sroar"
)

// TestFilteredAggregationProperties tests properties that must hold true for filtered aggregations
func TestFilteredAggregationProperties(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "filtered-agg-prop-*.col")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	filename := tmpFile.Name()
	defer os.Remove(filename)

	// Create random test data
	rand.Seed(time.Now().UnixNano())
	numBlocks := 5
	entriesPerBlock := 1000
	totalEntries := numBlocks * entriesPerBlock

	// Generate random but sorted IDs and values
	allIDs := make([]uint64, totalEntries)
	allValues := make([]int64, totalEntries)
	currentID := uint64(1)

	for i := 0; i < totalEntries; i++ {
		// Generate IDs with random gaps
		currentID += uint64(rand.Intn(5) + 1)
		allIDs[i] = currentID
		// Generate random values between -1000 and 1000
		allValues[i] = int64(rand.Intn(2001) - 1000)
	}

	// Create writer and write blocks
	writer, err := NewWriter(filename)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	for i := 0; i < numBlocks; i++ {
		start := i * entriesPerBlock
		end := start + entriesPerBlock
		if err := writer.WriteBlock(allIDs[start:end], allValues[start:end]); err != nil {
			t.Fatalf("Failed to write block %d: %v", i, err)
		}
	}

	if err := writer.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize file: %v", err)
	}

	// Create reader
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer reader.Close()

	// Property 1: Filtered results must be a subset of unfiltered results
	t.Run("Filtered results are subset of unfiltered", func(t *testing.T) {
		// Get unfiltered results first
		unfilteredResult := reader.Aggregate()

		// Test with different filter densities
		filterDensities := []float64{0.01, 0.1, 0.5} // 1%, 10%, 50%
		for _, density := range filterDensities {
			filter := sroar.NewBitmap()
			numToSelect := int(float64(totalEntries) * density)

			// Randomly select IDs
			selectedIndices := rand.Perm(totalEntries)[:numToSelect]
			for _, idx := range selectedIndices {
				filter.Set(allIDs[idx])
			}

			filteredResult := reader.AggregateWithOptions(AggregateOptions{
				Filter: filter,
			})

			// Verify properties
			assert.LessOrEqual(t, filteredResult.Count, unfilteredResult.Count,
				"Filtered count must be <= unfiltered count")

			// For min/max, we can only make assertions if we have results
			if filteredResult.Count > 0 {
				assert.GreaterOrEqual(t, filteredResult.Min, unfilteredResult.Min,
					"Filtered min must be >= unfiltered min")
				assert.LessOrEqual(t, filteredResult.Max, unfilteredResult.Max,
					"Filtered max must be <= unfiltered max")
			}

			// For sum with negative values, we can't make assumptions about the relationship
			// between filtered and unfiltered sums
		}
	})

	// Property 2: Results must be consistent regardless of filter creation order
	t.Run("Filter order independence", func(t *testing.T) {
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

	// Property 3: Verify that combining filters works correctly
	t.Run("Filter combination correctness", func(t *testing.T) {
		// Create two non-overlapping filters
		filter1 := sroar.NewBitmap()
		filter2 := sroar.NewBitmap()

		midPoint := totalEntries / 2
		for i := 0; i < midPoint; i++ {
			if rand.Float64() < 0.2 { // 20% chance to select each ID
				filter1.Set(allIDs[i])
			}
		}
		for i := midPoint; i < totalEntries; i++ {
			if rand.Float64() < 0.2 { // 20% chance to select each ID
				filter2.Set(allIDs[i])
			}
		}

		// Get results for individual filters
		result1 := reader.AggregateWithOptions(AggregateOptions{Filter: filter1})
		result2 := reader.AggregateWithOptions(AggregateOptions{Filter: filter2})

		// Get result for combined filter
		combinedFilter := filter1.Or(filter2)
		combinedResult := reader.AggregateWithOptions(AggregateOptions{Filter: combinedFilter})

		// Verify properties of combined results
		assert.Equal(t, result1.Count+result2.Count, combinedResult.Count,
			"Combined count should equal sum of individual counts for non-overlapping filters")

		// For min, we want the smaller of the two values
		expectedMin := result1.Min
		if result2.Min < expectedMin {
			expectedMin = result2.Min
		}
		assert.Equal(t, expectedMin, combinedResult.Min,
			"Combined min should be the minimum of individual mins")

		// For max, we want the larger of the two values
		expectedMax := result1.Max
		if result2.Max > expectedMax {
			expectedMax = result2.Max
		}
		assert.Equal(t, expectedMax, combinedResult.Max,
			"Combined max should be the maximum of individual maxs")

		assert.Equal(t, result1.Sum+result2.Sum, combinedResult.Sum,
			"Combined sum should equal sum of individual sums for non-overlapping filters")
	})

	// Property 4: Verify that empty filters return empty results
	t.Run("Empty filter properties", func(t *testing.T) {
		emptyFilter := sroar.NewBitmap()
		result := reader.AggregateWithOptions(AggregateOptions{Filter: emptyFilter})

		assert.Equal(t, 0, result.Count, "Empty filter should return count of 0")
		assert.Equal(t, int64(0), result.Min, "Empty filter should return min of 0")
		assert.Equal(t, int64(0), result.Max, "Empty filter should return max of 0")
		assert.Equal(t, int64(0), result.Sum, "Empty filter should return sum of 0")
		assert.Equal(t, float64(0), result.Avg, "Empty filter should return avg of 0")
	})

	// Property 5: Verify that results are consistent with manual calculation
	t.Run("Manual calculation consistency", func(t *testing.T) {
		// Create a filter with random IDs
		filter := sroar.NewBitmap()
		selectedIndices := make(map[int]bool)
		numToSelect := totalEntries / 5 // Select 20% of IDs

		// Randomly select indices
		for len(selectedIndices) < numToSelect {
			idx := rand.Intn(totalEntries)
			if !selectedIndices[idx] {
				selectedIndices[idx] = true
				filter.Set(allIDs[idx])
			}
		}

		// Calculate expected results manually
		var count int
		var min int64 = 9223372036854775807  // Max int64
		var max int64 = -9223372036854775808 // Min int64
		var sum int64

		for idx := range selectedIndices {
			count++
			value := allValues[idx]
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

		// Get actual results
		result := reader.AggregateWithOptions(AggregateOptions{Filter: filter})

		// Compare with manual calculation
		assert.Equal(t, count, result.Count, "Count should match manual calculation")
		assert.Equal(t, min, result.Min, "Min should match manual calculation")
		assert.Equal(t, max, result.Max, "Max should match manual calculation")
		assert.Equal(t, sum, result.Sum, "Sum should match manual calculation")
		assert.InDelta(t, avg, result.Avg, 0.0001, "Avg should match manual calculation")
	})
}
