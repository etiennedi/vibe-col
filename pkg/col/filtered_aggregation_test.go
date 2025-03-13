package col

import (
	"os"
	"testing"

	"github.com/weaviate/sroar"
)

// TestFilteredAggregation tests the filtered aggregation functionality
func TestFilteredAggregation(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "filtered-agg-*.col")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	filename := tmpFile.Name()
	defer os.Remove(filename)

	// Create a writer
	writer, err := NewWriter(filename)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write multiple blocks with different ID ranges
	// Block 1: IDs 1-100, values = id*10
	ids1 := make([]uint64, 100)
	values1 := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids1[i] = uint64(i + 1)
		values1[i] = int64((i + 1) * 10)
	}
	if err := writer.WriteBlock(ids1, values1); err != nil {
		t.Fatalf("Failed to write block 1: %v", err)
	}

	// Block 2: IDs 101-200, values = id*5
	ids2 := make([]uint64, 100)
	values2 := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids2[i] = uint64(i + 101)
		values2[i] = int64((i + 101) * 5)
	}
	if err := writer.WriteBlock(ids2, values2); err != nil {
		t.Fatalf("Failed to write block 2: %v", err)
	}

	// Block 3: IDs 201-300, values = id*2
	ids3 := make([]uint64, 100)
	values3 := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids3[i] = uint64(i + 201)
		values3[i] = int64((i + 201) * 2)
	}
	if err := writer.WriteBlock(ids3, values3); err != nil {
		t.Fatalf("Failed to write block 3: %v", err)
	}

	// Finalize the file
	if err := writer.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize file: %v", err)
	}

	// Create a reader
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Test cases
	tests := []struct {
		name        string
		filterIDs   []uint64
		expectCount int
		expectMin   int64
		expectMax   int64
		expectSum   int64
		expectAvg   float64
	}{
		{
			name:        "No filter",
			filterIDs:   nil,
			expectCount: 300,
			expectMin:   10,
			expectMax:   1000,
			expectSum:   175850,
			expectAvg:   586.17,
		},
		{
			name:        "Filter block 1",
			filterIDs:   []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectCount: 10,
			expectMin:   10,
			expectMax:   100,
			expectSum:   550,
			expectAvg:   55,
		},
		{
			name:        "Filter across blocks",
			filterIDs:   []uint64{50, 150, 250},
			expectCount: 3,
			expectMin:   500,
			expectMax:   750,
			expectSum:   1750,
			expectAvg:   583.33,
		},
		{
			name:        "Filter non-existent IDs",
			filterIDs:   []uint64{1000, 2000, 3000},
			expectCount: 0,
			expectMin:   0,
			expectMax:   0,
			expectSum:   0,
			expectAvg:   0,
		},
		{
			name:        "Filter sparse IDs",
			filterIDs:   []uint64{10, 110, 210, 20, 120, 220},
			expectCount: 6,
			expectMin:   100,
			expectMax:   600,
			expectSum:   2310,
			expectAvg:   385.00,
		},
		{
			name:        "Filter dense range",
			filterIDs:   generateRange(150, 250),
			expectCount: 101,
			expectMin:   402,
			expectMax:   1000,
			expectSum:   67175,
			expectAvg:   665.10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var filter *sroar.Bitmap
			if tc.filterIDs != nil {
				filter = sroar.NewBitmap()
				for _, id := range tc.filterIDs {
					filter.Set(id)
				}
			}

			opts := AggregateOptions{
				SkipPreCalculated: true, // Force reading all blocks to test filtering
				Filter:            filter,
			}

			result := reader.AggregateWithOptions(opts)

			if result.Count != tc.expectCount {
				t.Errorf("Count: got %d, want %d", result.Count, tc.expectCount)
			}

			// Only check min/max/sum/avg if count > 0
			if tc.expectCount > 0 {
				if result.Min != tc.expectMin {
					t.Errorf("Min: got %d, want %d", result.Min, tc.expectMin)
				}
				if result.Max != tc.expectMax {
					t.Errorf("Max: got %d, want %d", result.Max, tc.expectMax)
				}
				if result.Sum != tc.expectSum {
					t.Errorf("Sum: got %d, want %d", result.Sum, tc.expectSum)
				}
				// Allow small floating point differences in average
				if !floatEquals(result.Avg, tc.expectAvg, 0.01) {
					t.Errorf("Avg: got %.2f, want %.2f", result.Avg, tc.expectAvg)
				}
			}
		})
	}

	// Test with cached values
	t.Run("With cached values", func(t *testing.T) {
		filter := sroar.NewBitmap()
		for _, id := range []uint64{50, 150, 250} {
			filter.Set(id)
		}

		opts := AggregateOptions{
			SkipPreCalculated: false, // Use cached values
			Filter:            filter,
		}

		result := reader.AggregateWithOptions(opts)

		if result.Count != 3 {
			t.Errorf("Count: got %d, want %d", result.Count, 3)
		}
		if result.Min != 500 {
			t.Errorf("Min: got %d, want %d", result.Min, 500)
		}
		if result.Max != 750 {
			t.Errorf("Max: got %d, want %d", result.Max, 750)
		}
		if result.Sum != 1750 {
			t.Errorf("Sum: got %d, want %d", result.Sum, 1750)
		}
		if !floatEquals(result.Avg, 583.33, 0.01) {
			t.Errorf("Avg: got %.2f, want %.2f", result.Avg, 583.33)
		}
	})

	// Test FilteredBlockIterator
	t.Run("FilteredBlockIterator", func(t *testing.T) {
		// Filter that only matches block 1
		filter1 := sroar.NewBitmap()
		filter1.Set(50)
		blocks1 := reader.FilteredBlockIterator(filter1)
		if len(blocks1) != 1 || blocks1[0] != 0 {
			t.Errorf("Expected [0], got %v", blocks1)
		}

		// Filter that matches blocks 2 and 3
		filter2 := sroar.NewBitmap()
		filter2.Set(150)
		filter2.Set(250)
		blocks2 := reader.FilteredBlockIterator(filter2)
		if len(blocks2) != 2 || blocks2[0] != 1 || blocks2[1] != 2 {
			t.Errorf("Expected [1, 2], got %v", blocks2)
		}

		// Filter that doesn't match any block
		filter3 := sroar.NewBitmap()
		filter3.Set(1000)
		blocks3 := reader.FilteredBlockIterator(filter3)
		if len(blocks3) != 0 {
			t.Errorf("Expected [], got %v", blocks3)
		}
	})

	// Test readBlockFiltered
	t.Run("readBlockFiltered", func(t *testing.T) {
		// Filter that matches some IDs in block 1
		filter := sroar.NewBitmap()
		filter.Set(10)
		filter.Set(20)
		filter.Set(30)

		ids, values, err := reader.readBlockFiltered(0, filter)
		if err != nil {
			t.Fatalf("readBlockFiltered failed: %v", err)
		}

		if len(ids) != 3 || len(values) != 3 {
			t.Errorf("Expected 3 IDs and values, got %d IDs and %d values", len(ids), len(values))
		}

		expectedIDs := []uint64{10, 20, 30}
		expectedValues := []int64{100, 200, 300}
		for i := 0; i < len(ids); i++ {
			if ids[i] != expectedIDs[i] {
				t.Errorf("ID[%d]: got %d, want %d", i, ids[i], expectedIDs[i])
			}
			if values[i] != expectedValues[i] {
				t.Errorf("Value[%d]: got %d, want %d", i, values[i], expectedValues[i])
			}
		}
	})
}

// Helper function to generate a range of IDs
func generateRange(start, end uint64) []uint64 {
	result := make([]uint64, end-start+1)
	for i := range result {
		result[i] = start + uint64(i)
	}
	return result
}

// Helper function to compare floats with a tolerance
func floatEquals(a, b, tolerance float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}
