package store

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestColumnStoreAggregations(t *testing.T) {
	// Create a new column store with a flush trigger of 1000 entries
	store := NewColumnStore(1000)

	// Generate 1000 random int64 values between -1000 and 1000
	const numValues = 1000
	values := make([]int64, numValues)
	
	// Calculate expected aggregations
	var sum int64
	min := int64(math.MaxInt64)
	max := int64(math.MinInt64)
	
	// Generate data and calculate expected results
	for i := 0; i < numValues; i++ {
		// Generate random value between -1000 and 1000
		val := rand.Int63n(2001) - 1000
		values[i] = val
		
		// Insert into store with sequential IDs
		err := store.Put(uint64(i), val)
		if err != nil {
			t.Fatalf("Failed to put value: %v", err)
		}
		
		// Update expected aggregations
		if val < min {
			min = val
		}
		if val > max {
			max = val
		}
		sum += val
	}
	
	expectedMean := float64(sum) / float64(numValues)
	
	// Force a flush to create a segment
	err := store.Flush()
	if err != nil {
		t.Fatalf("Failed to flush store: %v", err)
	}
	
	// Test minimum aggregation
	minResult, err := store.Aggregate(Min)
	if err != nil {
		t.Fatalf("Failed to get minimum: %v", err)
	}
	if minResult != float64(min) {
		t.Errorf("Expected minimum %f, got %f", float64(min), minResult)
	}
	
	// Test maximum aggregation
	maxResult, err := store.Aggregate(Max)
	if err != nil {
		t.Fatalf("Failed to get maximum: %v", err)
	}
	if maxResult != float64(max) {
		t.Errorf("Expected maximum %f, got %f", float64(max), maxResult)
	}
	
	// Test mean aggregation
	meanResult, err := store.Aggregate(Mean)
	if err != nil {
		t.Fatalf("Failed to get mean: %v", err)
	}
	if math.Abs(meanResult-expectedMean) > 0.00001 {
		t.Errorf("Expected mean %f, got %f", expectedMean, meanResult)
	}
	
	// Test count aggregation
	countResult, err := store.Aggregate(Count)
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}
	if countResult != float64(numValues) {
		t.Errorf("Expected count %d, got %f", numValues, countResult)
	}
	
	// Test sum aggregation
	sumResult, err := store.Aggregate(Sum)
	if err != nil {
		t.Fatalf("Failed to get sum: %v", err)
	}
	if sumResult != float64(sum) {
		t.Errorf("Expected sum %d, got %f", sum, sumResult)
	}
	
	// Test median calculation
	// To calculate expected median
	valuesCopy := make([]int64, len(values))
	copy(valuesCopy, values)
	sort.Slice(valuesCopy, func(i, j int) bool {
		return valuesCopy[i] < valuesCopy[j]
	})
	
	var expectedMedian float64
	mid := len(valuesCopy) / 2
	if len(valuesCopy)%2 == 0 {
		expectedMedian = float64(valuesCopy[mid-1]+valuesCopy[mid]) / 2.0
	} else {
		expectedMedian = float64(valuesCopy[mid])
	}
	
	medianResult, err := store.Aggregate(Median)
	if err != nil {
		t.Fatalf("Failed to get median: %v", err)
	}
	if medianResult != expectedMedian {
		t.Errorf("Expected median %f, got %f", expectedMedian, medianResult)
	}
}