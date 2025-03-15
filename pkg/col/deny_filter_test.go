package col

import (
	"os"
	"testing"

	"github.com/weaviate/sroar"
)

// TestDenyFilter tests the deny filter functionality
func TestDenyFilter(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "deny-filter-*.col")
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
		allowIDs    []uint64
		denyIDs     []uint64
		expectCount int
		expectMin   int64
		expectMax   int64
		expectSum   int64
		expectAvg   float64
	}{
		{
			name:        "Only deny filter",
			allowIDs:    nil,
			denyIDs:     []uint64{1, 2, 3, 4, 5},
			expectCount: 295,
			expectMin:   60,
			expectMax:   1000,
			expectSum:   175700,
			expectAvg:   595.59,
		},
		{
			name:        "Allow and deny filters",
			allowIDs:    []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			denyIDs:     []uint64{1, 3, 5, 7, 9},
			expectCount: 5,
			expectMin:   20,
			expectMax:   100,
			expectSum:   300,
			expectAvg:   60.0,
		},
		{
			name:        "Deny all allowed IDs",
			allowIDs:    []uint64{1, 2, 3, 4, 5},
			denyIDs:     []uint64{1, 2, 3, 4, 5},
			expectCount: 0,
			expectMin:   0,
			expectMax:   0,
			expectSum:   0,
			expectAvg:   0,
		},
		{
			name:        "Deny across blocks",
			allowIDs:    nil,
			denyIDs:     []uint64{50, 150, 250},
			expectCount: 297,
			expectMin:   10,
			expectMax:   1000,
			expectSum:   174100,
			expectAvg:   586.19,
		},
		{
			name:        "Allow specific, deny subset",
			allowIDs:    generateRange(1, 100),
			denyIDs:     generateRange(50, 100),
			expectCount: 49,
			expectMin:   10,
			expectMax:   490,
			expectSum:   12250,
			expectAvg:   250.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var allowFilter *sroar.Bitmap
			if tc.allowIDs != nil {
				allowFilter = sroar.NewBitmap()
				for _, id := range tc.allowIDs {
					allowFilter.Set(id)
				}
			}

			var denyFilter *sroar.Bitmap
			if tc.denyIDs != nil {
				denyFilter = sroar.NewBitmap()
				for _, id := range tc.denyIDs {
					denyFilter.Set(id)
				}
			}

			opts := AggregateOptions{
				SkipPreCalculated: true, // Force reading all blocks to test filtering
				Filter:            allowFilter,
				DenyFilter:        denyFilter,
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

	// Test readBlockFiltered with deny filter
	t.Run("readBlockFiltered with deny filter", func(t *testing.T) {
		// Allow filter that matches some IDs in block 1
		allowFilter := sroar.NewBitmap()
		for i := uint64(1); i <= 10; i++ {
			allowFilter.Set(i)
		}

		// Deny filter that excludes some of the allowed IDs
		denyFilter := sroar.NewBitmap()
		denyFilter.Set(2)
		denyFilter.Set(4)
		denyFilter.Set(6)
		denyFilter.Set(8)
		denyFilter.Set(10)

		ids, values, err := reader.readBlockFiltered(0, allowFilter, denyFilter)
		if err != nil {
			t.Fatalf("readBlockFiltered failed: %v", err)
		}

		if len(ids) != 5 || len(values) != 5 {
			t.Errorf("Expected 5 IDs and values, got %d IDs and %d values", len(ids), len(values))
		}

		expectedIDs := []uint64{1, 3, 5, 7, 9}
		expectedValues := []int64{10, 30, 50, 70, 90}
		for i := 0; i < len(ids); i++ {
			if ids[i] != expectedIDs[i] {
				t.Errorf("ID[%d]: got %d, want %d", i, ids[i], expectedIDs[i])
			}
			if values[i] != expectedValues[i] {
				t.Errorf("Value[%d]: got %d, want %d", i, values[i], expectedValues[i])
			}
		}
	})

	// Test only deny filter
	t.Run("Only deny filter", func(t *testing.T) {
		// Deny filter that excludes some IDs
		denyFilter := sroar.NewBitmap()
		denyFilter.Set(1)
		denyFilter.Set(101)
		denyFilter.Set(201)

		ids, values, err := reader.readBlockFiltered(0, nil, denyFilter)
		if err != nil {
			t.Fatalf("readBlockFiltered failed: %v", err)
		}

		if len(ids) != 99 || len(values) != 99 {
			t.Errorf("Expected 99 IDs and values, got %d IDs and %d values", len(ids), len(values))
		}

		// First ID should be 2 since 1 is denied
		if ids[0] != 2 {
			t.Errorf("First ID: got %d, want %d", ids[0], 2)
		}
	})
}

// Helper function to compare floats with a tolerance
