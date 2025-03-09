package col

import (
	"os"
	"testing"
)

func TestWriteAndReadSimpleFile(t *testing.T) {
	// Create a temporary file
	tempFile := "test_example.col"
	defer os.Remove(tempFile)

	// Create test data
	ids := []uint64{1, 5, 10, 15, 20, 25, 30, 35, 40, 45}
	values := []int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

	// Write the file
	writer, err := NewWriter(tempFile)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write a block of data
	if err := writer.WriteBlock(ids, values); err != nil {
		t.Fatalf("Failed to write block: %v", err)
	}

	// Finalize and close the file
	if err := writer.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize file: %v", err)
	}
	
	// Debug code removed after fixing the implementation

	// Open the file for reading
	reader, err := NewReader(tempFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer reader.Close()

	// Check file metadata
	if reader.Version() != Version {
		t.Errorf("Expected version %d, got %d", Version, reader.Version())
	}
	if reader.BlockCount() != 1 {
		t.Errorf("Expected 1 block, got %d", reader.BlockCount())
	}

	// Check aggregations
	agg := reader.Aggregate()
	expectedAgg := AggregateResult{
		Count: 10,
		Min:   100,
		Max:   1000,
		Sum:   5500,
		Avg:   550.0,
	}

	if agg.Count != expectedAgg.Count {
		t.Errorf("Expected count %d, got %d", expectedAgg.Count, agg.Count)
	}
	if agg.Min != expectedAgg.Min {
		t.Errorf("Expected min %d, got %d", expectedAgg.Min, agg.Min)
	}
	if agg.Max != expectedAgg.Max {
		t.Errorf("Expected max %d, got %d", expectedAgg.Max, agg.Max)
	}
	if agg.Sum != expectedAgg.Sum {
		t.Errorf("Expected sum %d, got %d", expectedAgg.Sum, agg.Sum)
	}
	if agg.Avg != expectedAgg.Avg {
		t.Errorf("Expected avg %.2f, got %.2f", expectedAgg.Avg, agg.Avg)
	}

	// Read the data
	readIds, readValues, err := reader.GetPairs(0)
	if err != nil {
		t.Fatalf("Failed to read pairs: %v", err)
	}

	// Check data integrity
	if len(readIds) != len(ids) {
		t.Errorf("Expected %d IDs, got %d", len(ids), len(readIds))
	}
	if len(readValues) != len(values) {
		t.Errorf("Expected %d values, got %d", len(values), len(readValues))
	}

	for i := 0; i < len(ids); i++ {
		if readIds[i] != ids[i] {
			t.Errorf("ID mismatch at index %d: expected %d, got %d", i, ids[i], readIds[i])
		}
		if readValues[i] != values[i] {
			t.Errorf("Value mismatch at index %d: expected %d, got %d", i, values[i], readValues[i])
		}
	}
}