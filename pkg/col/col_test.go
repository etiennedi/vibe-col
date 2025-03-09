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

// TestDifferentDataFile tests writing and reading a file with different data 
// to ensure the implementation doesn't rely on hardcoded values
func TestDifferentDataFile(t *testing.T) {
	// Create a temporary file
	tempFile := "test_different.col"
	defer os.Remove(tempFile)

	// Create test data with different sizes and values
	ids := []uint64{100, 200, 300, 400, 500}
	values := []int64{10, 20, 30, 40, 50}

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
		Count: 5,
		Min:   10,
		Max:   50,
		Sum:   150,
		Avg:   30.0,
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

// TestMultipleBlocks tests writing and reading a file with multiple blocks
func TestMultipleBlocks(t *testing.T) {
	// Create a temporary file
	tempFile := "test_multi_block.col"
	defer os.Remove(tempFile)

	// Create writer
	writer, err := NewWriter(tempFile)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// First block of data
	ids1 := []uint64{1, 2, 3, 4, 5}
	values1 := []int64{10, 20, 30, 40, 50}

	// Second block of data
	ids2 := []uint64{6, 7, 8, 9, 10}
	values2 := []int64{60, 70, 80, 90, 100}

	// Write blocks
	if err := writer.WriteBlock(ids1, values1); err != nil {
		t.Fatalf("Failed to write first block: %v", err)
	}
	if err := writer.WriteBlock(ids2, values2); err != nil {
		t.Fatalf("Failed to write second block: %v", err)
	}

	// Finalize and close the file
	if err := writer.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize file: %v", err)
	}

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
	if reader.BlockCount() != 2 {
		t.Errorf("Expected 2 blocks, got %d", reader.BlockCount())
	}

	// Check aggregations (should combine both blocks)
	agg := reader.Aggregate()
	expectedAgg := AggregateResult{
		Count: 10,
		Min:   10,
		Max:   100,
		Sum:   500,
		Avg:   50.0,
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

	// Read and check first block
	readIds1, readValues1, err := reader.GetPairs(0)
	if err != nil {
		t.Fatalf("Failed to read first block pairs: %v", err)
	}

	// Check data integrity for first block
	if len(readIds1) != len(ids1) {
		t.Errorf("Expected %d IDs in first block, got %d", len(ids1), len(readIds1))
	}
	for i := 0; i < len(ids1); i++ {
		if readIds1[i] != ids1[i] {
			t.Errorf("ID mismatch in first block at index %d: expected %d, got %d", i, ids1[i], readIds1[i])
		}
		if readValues1[i] != values1[i] {
			t.Errorf("Value mismatch in first block at index %d: expected %d, got %d", i, values1[i], readValues1[i])
		}
	}

	// Read and check second block
	readIds2, readValues2, err := reader.GetPairs(1)
	if err != nil {
		t.Fatalf("Failed to read second block pairs: %v", err)
	}

	// Check data integrity for second block
	if len(readIds2) != len(ids2) {
		t.Errorf("Expected %d IDs in second block, got %d", len(ids2), len(readIds2))
	}
	for i := 0; i < len(ids2); i++ {
		if readIds2[i] != ids2[i] {
			t.Errorf("ID mismatch in second block at index %d: expected %d, got %d", i, ids2[i], readIds2[i])
		}
		if readValues2[i] != values2[i] {
			t.Errorf("Value mismatch in second block at index %d: expected %d, got %d", i, values2[i], readValues2[i])
		}
	}
}