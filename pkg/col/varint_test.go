package col

import (
	"math"
	"math/rand"
	"os"
	"testing"
)

func TestVarintEncoding_WriteRead(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "test_varint.col"
	defer os.Remove(tempFile)

	// Test data
	ids := []uint64{1, 5, 10, 15, 20, 30, 50, 100, 1000, 10000}
	values := []int64{-100, -50, -10, -1, 0, 1, 10, 100, 1000, 10000}

	// Test with varint-only encoding first
	t.Run("VarInt_Only", func(t *testing.T) {
		// Create a writer with variable-length encoding
		writer, err := NewWriter(tempFile, WithEncoding(EncodingVarInt))
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Print debug info about our test data
		t.Logf("Test data: %d IDs and %d values", len(ids), len(values))

		// Write the data
		if err := writer.WriteBlock(ids, values); err != nil {
			t.Fatalf("Failed to write block: %v", err)
		}

		// Finalize and close the file
		if err := writer.FinalizeAndClose(); err != nil {
			t.Fatalf("Failed to finalize file: %v", err)
		}

		// Read the file back
		reader, err := NewReader(tempFile)
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer reader.Close()

		// Check encoding type
		if !reader.IsVarIntEncoded() {
			t.Errorf("File should be reported as varint encoded")
		}

		// Check block count
		if reader.BlockCount() != 1 {
			t.Errorf("Expected 1 block, got %d", reader.BlockCount())
		}

		// Read the data
		readIds, readValues, err := reader.GetPairs(0)
		if err != nil {
			t.Fatalf("Failed to read pairs: %v", err)
		}

		// Verify data integrity
		if len(readIds) != len(ids) {
			t.Errorf("Expected %d IDs, got %d", len(ids), len(readIds))
		}
		if len(readValues) != len(values) {
			t.Errorf("Expected %d values, got %d", len(values), len(readValues))
		}

		// Check a few values
		for i := 0; i < len(ids); i++ {
			if readIds[i] != ids[i] {
				t.Errorf("ID mismatch at index %d: expected %d, got %d", i, ids[i], readIds[i])
			}
			if readValues[i] != values[i] {
				t.Errorf("Value mismatch at index %d: expected %d, got %d", i, values[i], readValues[i])
			}
		}
	})

	// Test with combined delta + varint encoding
	t.Run("VarInt_Delta", func(t *testing.T) {
		// Create a writer with combined encoding
		writer, err := NewWriter(tempFile, WithEncoding(EncodingVarIntBoth))
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Write the data
		if err := writer.WriteBlock(ids, values); err != nil {
			t.Fatalf("Failed to write block: %v", err)
		}

		// Finalize and close the file
		if err := writer.FinalizeAndClose(); err != nil {
			t.Fatalf("Failed to finalize file: %v", err)
		}

		// Read the file back
		reader, err := NewReader(tempFile)
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer reader.Close()

		// Check encoding type
		if !reader.IsVarIntEncoded() {
			t.Errorf("File should be reported as varint encoded")
		}

		// Read the data
		readIds, readValues, err := reader.GetPairs(0)
		if err != nil {
			t.Fatalf("Failed to read pairs: %v", err)
		}

		// Verify data integrity
		if len(readIds) != len(ids) {
			t.Errorf("Expected %d IDs, got %d", len(ids), len(readIds))
		}
		if len(readValues) != len(values) {
			t.Errorf("Expected %d values, got %d", len(values), len(readValues))
		}

		// Check that values match
		for i := 0; i < len(ids); i++ {
			if readIds[i] != ids[i] {
				t.Errorf("ID mismatch at index %d: expected %d, got %d", i, ids[i], readIds[i])
			}
			if readValues[i] != values[i] {
				t.Errorf("Value mismatch at index %d: expected %d, got %d", i, values[i], readValues[i])
			}
		}
	})
}

// TestVarintEncodingCompression tests that varint encoding compresses data better than raw encoding
func TestVarintEncodingCompression(t *testing.T) {
	// Create temporary files
	tempFileRaw, err := os.CreateTemp("", "col-raw-*.col")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFileRaw.Name())
	tempFileRawName := tempFileRaw.Name()
	tempFileRaw.Close()

	tempFileVarInt, err := os.CreateTemp("", "col-varint-*.col")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFileVarInt.Name())
	tempFileVarIntName := tempFileVarInt.Name()
	tempFileVarInt.Close()

	// Create test data
	count := 2000
	ids := make([]uint64, count)
	values := make([]int64, count)

	// Sequential IDs (1, 2, 3, ...) - delta encoding will be efficient
	// Small values (0, 1, 2, ...) - varint encoding will be efficient
	for i := 0; i < count; i++ {
		ids[i] = uint64(i + 1)     // Start from 1
		values[i] = int64(i % 100) // Small values (0-99)
	}

	// Write with raw encoding first
	rawWriter, err := NewWriter(tempFileRawName, WithEncoding(EncodingRaw))
	if err != nil {
		t.Fatalf("Failed to create raw writer: %v", err)
	}

	// Write all items, handling BlockFullError if needed
	remainingIDs := ids
	remainingValues := values
	for len(remainingIDs) > 0 {
		err := rawWriter.WriteBlock(remainingIDs, remainingValues)
		if blockFullErr, ok := err.(*BlockFullError); ok {
			// Some items were written, continue with the rest
			itemsWritten := blockFullErr.ItemsWritten
			remainingIDs = remainingIDs[itemsWritten:]
			remainingValues = remainingValues[itemsWritten:]
		} else if err != nil {
			t.Fatalf("Failed to write raw block: %v", err)
			break
		} else {
			// All items were written
			remainingIDs = nil
			remainingValues = nil
		}
	}

	if err := rawWriter.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize raw file: %v", err)
	}

	// Write with varint encoding
	varIntWriter, err := NewWriter(tempFileVarIntName, WithEncoding(EncodingVarIntBoth))
	if err != nil {
		t.Fatalf("Failed to create varint writer: %v", err)
	}

	// Write all items, handling BlockFullError if needed
	remainingIDs = ids
	remainingValues = values
	for len(remainingIDs) > 0 {
		err := varIntWriter.WriteBlock(remainingIDs, remainingValues)
		if blockFullErr, ok := err.(*BlockFullError); ok {
			// Some items were written, continue with the rest
			itemsWritten := blockFullErr.ItemsWritten
			remainingIDs = remainingIDs[itemsWritten:]
			remainingValues = remainingValues[itemsWritten:]
		} else if err != nil {
			t.Fatalf("Failed to write varint block: %v", err)
			break
		} else {
			// All items were written
			remainingIDs = nil
			remainingValues = nil
		}
	}

	if err := varIntWriter.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize varint file: %v", err)
	}

	// Compare file sizes
	rawInfo, err := os.Stat(tempFileRawName)
	if err != nil {
		t.Fatalf("Failed to get raw file info: %v", err)
	}

	varIntInfo, err := os.Stat(tempFileVarIntName)
	if err != nil {
		t.Fatalf("Failed to get varint file info: %v", err)
	}

	t.Logf("Raw file size: %d bytes", rawInfo.Size())
	t.Logf("VarInt file size: %d bytes", varIntInfo.Size())
	t.Logf("Compression ratio: %.2f%%", float64(varIntInfo.Size())/float64(rawInfo.Size())*100)

	// VarInt should be smaller
	if varIntInfo.Size() >= rawInfo.Size() {
		t.Errorf("VarInt encoding should produce smaller file, but got: raw=%d, varint=%d",
			rawInfo.Size(), varIntInfo.Size())
	}

	// Read and verify both files
	rawReader, err := NewReader(tempFileRawName)
	if err != nil {
		t.Fatalf("Failed to open raw file: %v", err)
	}
	defer rawReader.Close()

	varIntReader, err := NewReader(tempFileVarIntName)
	if err != nil {
		t.Fatalf("Failed to open varint file: %v", err)
	}
	defer varIntReader.Close()

	// Verify encoding types
	if rawReader.EncodingType() != EncodingRaw {
		t.Errorf("Expected raw encoding, got %v", rawReader.EncodingType())
	}

	if varIntReader.EncodingType() != EncodingVarIntBoth {
		t.Errorf("Expected varint encoding, got %v", varIntReader.EncodingType())
	}

	// Verify data in both files
	verifyReaderData(t, rawReader, ids, values)
	verifyReaderData(t, varIntReader, ids, values)
}

func TestVarIntEncodeDecode(t *testing.T) {
	testCases := []uint64{
		0, 1, 2, 3, 4, 5, 10, 100, 127, 128, 129, 255, 256,
		1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
		0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF,
	}

	for _, tc := range testCases {
		encoded := encodeVarInt(tc)
		decoded, bytesRead := decodeVarInt(encoded)

		if decoded != tc {
			t.Errorf("Value mismatch: expected %d, got %d", tc, decoded)
		}

		if bytesRead != len(encoded) {
			t.Errorf("Bytes read mismatch: expected %d, got %d", len(encoded), bytesRead)
		}
	}
}

func TestSignedVarIntEncodeDecode(t *testing.T) {
	testCases := []int64{
		0, 1, -1, 2, -2, 3, -3, 10, -10, 100, -100, 127, -127, 128, -128, 129, -129,
		255, -255, 256, -256, 1000, -1000, 10000, -10000, 100000, -100000,
		1000000, -1000000, 10000000, -10000000, 100000000, -100000000, 1000000000, -1000000000,
		0x7FFFFFFFFFFFFFFF, -0x7FFFFFFFFFFFFFFF,
	}

	for _, tc := range testCases {
		encoded := encodeSignedVarInt(tc)
		decoded, bytesRead := decodeSignedVarInt(encoded)

		if decoded != tc {
			t.Errorf("Value mismatch: expected %d, got %d", tc, decoded)
		}

		if bytesRead != len(encoded) {
			t.Errorf("Bytes read mismatch: expected %d, got %d", len(encoded), bytesRead)
		}
	}
}

// TestVarintEncodingCompression_RealWorldData tests varint encoding with real-world data patterns
func TestVarintEncodingCompression_RealWorldData(t *testing.T) {
	// Create temporary files
	tempFileRaw, err := os.CreateTemp("", "col-raw-real-*.col")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFileRaw.Name())
	tempFileRawName := tempFileRaw.Name()
	tempFileRaw.Close()

	tempFileVarInt, err := os.CreateTemp("", "col-varint-real-*.col")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFileVarInt.Name())
	tempFileVarIntName := tempFileVarInt.Name()
	tempFileVarInt.Close()

	// Create test data with realistic patterns
	count := 2000
	ids := make([]uint64, count)
	values := make([]int64, count)

	// Scenario 1: Sparse IDs with small values
	// This simulates a sparse dataset where IDs are far apart but values are small
	for i := 0; i < count; i++ {
		ids[i] = uint64(i * 1000)  // Sparse IDs (0, 1000, 2000, ...)
		values[i] = int64(i % 100) // Small values (0-99)
	}

	// Write with raw encoding first
	rawWriter, err := NewWriter(tempFileRawName, WithEncoding(EncodingRaw))
	if err != nil {
		t.Fatalf("Failed to create raw writer: %v", err)
	}

	// Write all items, handling BlockFullError if needed
	remainingIDs := ids
	remainingValues := values
	for len(remainingIDs) > 0 {
		err := rawWriter.WriteBlock(remainingIDs, remainingValues)
		if blockFullErr, ok := err.(*BlockFullError); ok {
			// Some items were written, continue with the rest
			itemsWritten := blockFullErr.ItemsWritten
			remainingIDs = remainingIDs[itemsWritten:]
			remainingValues = remainingValues[itemsWritten:]
		} else if err != nil {
			t.Fatalf("Failed to write raw block: %v", err)
			break
		} else {
			// All items were written
			remainingIDs = nil
			remainingValues = nil
		}
	}

	if err := rawWriter.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize raw file: %v", err)
	}

	// Write with varint encoding
	varIntWriter, err := NewWriter(tempFileVarIntName, WithEncoding(EncodingVarIntBoth))
	if err != nil {
		t.Fatalf("Failed to create varint writer: %v", err)
	}

	// Write all items, handling BlockFullError if needed
	remainingIDs = ids
	remainingValues = values
	for len(remainingIDs) > 0 {
		err := varIntWriter.WriteBlock(remainingIDs, remainingValues)
		if blockFullErr, ok := err.(*BlockFullError); ok {
			// Some items were written, continue with the rest
			itemsWritten := blockFullErr.ItemsWritten
			remainingIDs = remainingIDs[itemsWritten:]
			remainingValues = remainingValues[itemsWritten:]
		} else if err != nil {
			t.Fatalf("Failed to write varint block: %v", err)
			break
		} else {
			// All items were written
			remainingIDs = nil
			remainingValues = nil
		}
	}

	if err := varIntWriter.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize varint file: %v", err)
	}

	// Compare file sizes
	rawInfo, err := os.Stat(tempFileRawName)
	if err != nil {
		t.Fatalf("Failed to get raw file info: %v", err)
	}

	varIntInfo, err := os.Stat(tempFileVarIntName)
	if err != nil {
		t.Fatalf("Failed to get varint file info: %v", err)
	}

	t.Logf("Raw file size: %d bytes", rawInfo.Size())
	t.Logf("VarInt file size: %d bytes", varIntInfo.Size())
	t.Logf("Compression ratio: %.2f%%", float64(varIntInfo.Size())/float64(rawInfo.Size())*100)

	// Read and verify both files
	rawReader, err := NewReader(tempFileRawName)
	if err != nil {
		t.Fatalf("Failed to open raw file: %v", err)
	}
	defer rawReader.Close()

	varIntReader, err := NewReader(tempFileVarIntName)
	if err != nil {
		t.Fatalf("Failed to open varint file: %v", err)
	}
	defer varIntReader.Close()

	// Verify encoding types
	if rawReader.EncodingType() != EncodingRaw {
		t.Errorf("Expected raw encoding, got %v", rawReader.EncodingType())
	}

	if varIntReader.EncodingType() != EncodingVarIntBoth {
		t.Errorf("Expected varint encoding, got %v", varIntReader.EncodingType())
	}

	// Verify data in both files
	verifyReaderData(t, rawReader, ids, values)
	verifyReaderData(t, varIntReader, ids, values)
}

// TestAggregateWithoutPreCalculated verifies that aggregation works correctly
// when skipping pre-calculated values in the footer
func TestAggregateWithoutPreCalculated(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "test_aggregate_no_precalc.col"
	defer os.Remove(tempFile)

	// Generate 10k values with some variability
	const numValues = 10000
	ids := make([]uint64, numValues)
	values := make([]int64, numValues)

	// Track expected aggregation results
	var expectedSum int64
	var expectedMin int64 = 9223372036854775807  // Max int64
	var expectedMax int64 = -9223372036854775808 // Min int64

	// Generate data with variable patterns
	r := rand.New(rand.NewSource(42)) // Use fixed seed for reproducibility
	for i := 0; i < numValues; i++ {
		// Generate IDs with some gaps
		if i == 0 {
			ids[i] = 1000
		} else {
			// Add a random gap between 1 and 10
			ids[i] = ids[i-1] + uint64(r.Intn(10)+1)
		}

		// Generate values with both positive and negative numbers
		// Use a mix of small and large values
		var val int64
		switch r.Intn(5) {
		case 0:
			// Small negative (-100 to -1)
			val = -int64(r.Intn(100) + 1)
		case 1:
			// Small positive (1 to 100)
			val = int64(r.Intn(100) + 1)
		case 2:
			// Medium negative (-10000 to -101)
			val = -int64(r.Intn(9900) + 101)
		case 3:
			// Medium positive (101 to 10000)
			val = int64(r.Intn(9900) + 101)
		case 4:
			// Large (could be positive or negative, up to ±1M)
			val = int64(r.Intn(2000000) - 1000000)
		}
		values[i] = val

		// Update expected aggregation values
		expectedSum += val
		if val < expectedMin {
			expectedMin = val
		}
		if val > expectedMax {
			expectedMax = val
		}
	}

	// Create a writer with variable-length encoding
	writer, err := NewWriter(tempFile, WithEncoding(EncodingVarIntBoth))
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write the data in multiple blocks (1000 values per block)
	const blockSize = 1000
	for i := 0; i < numValues; i += blockSize {
		end := i + blockSize
		if end > numValues {
			end = numValues
		}

		blockIDs := ids[i:end]
		blockValues := values[i:end]

		if err := writer.WriteBlock(blockIDs, blockValues); err != nil {
			t.Fatalf("Failed to write block at index %d: %v", i, err)
		}
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

	// Verify file metadata
	if reader.Version() != Version {
		t.Errorf("Expected version %d, got %d", Version, reader.Version())
	}
	if reader.BlockCount() != uint64((numValues+blockSize-1)/blockSize) {
		t.Errorf("Expected %d blocks, got %d", (numValues+blockSize-1)/blockSize, reader.BlockCount())
	}

	// Get aggregation using pre-calculated values (default)
	precalcAgg := reader.Aggregate()

	// Get aggregation by reading all values
	directAgg := reader.AggregateWithOptions(AggregateOptions{SkipPreCalculated: true})

	// Verify both methods produce the same results
	t.Logf("Pre-calculated aggregation: Count=%d, Min=%d, Max=%d, Sum=%d, Avg=%.2f",
		precalcAgg.Count, precalcAgg.Min, precalcAgg.Max, precalcAgg.Sum, precalcAgg.Avg)

	t.Logf("Direct aggregation: Count=%d, Min=%d, Max=%d, Sum=%d, Avg=%.2f",
		directAgg.Count, directAgg.Min, directAgg.Max, directAgg.Sum, directAgg.Avg)

	// Verify against expected values
	t.Logf("Expected values: Count=%d, Min=%d, Max=%d, Sum=%d, Avg=%.2f",
		numValues, expectedMin, expectedMax, expectedSum, float64(expectedSum)/float64(numValues))

	// Compare pre-calculated with direct
	if precalcAgg.Count != directAgg.Count {
		t.Errorf("Count mismatch: pre-calculated=%d, direct=%d", precalcAgg.Count, directAgg.Count)
	}
	if precalcAgg.Min != directAgg.Min {
		t.Errorf("Min mismatch: pre-calculated=%d, direct=%d", precalcAgg.Min, directAgg.Min)
	}
	if precalcAgg.Max != directAgg.Max {
		t.Errorf("Max mismatch: pre-calculated=%d, direct=%d", precalcAgg.Max, directAgg.Max)
	}
	if precalcAgg.Sum != directAgg.Sum {
		t.Errorf("Sum mismatch: pre-calculated=%d, direct=%d", precalcAgg.Sum, directAgg.Sum)
	}
	if precalcAgg.Avg != directAgg.Avg {
		t.Errorf("Avg mismatch: pre-calculated=%.2f, direct=%.2f", precalcAgg.Avg, directAgg.Avg)
	}

	// Compare with expected values
	if precalcAgg.Count != numValues {
		t.Errorf("Count incorrect: expected=%d, got=%d", numValues, precalcAgg.Count)
	}
	if precalcAgg.Min != expectedMin {
		t.Errorf("Min incorrect: expected=%d, got=%d", expectedMin, precalcAgg.Min)
	}
	if precalcAgg.Max != expectedMax {
		t.Errorf("Max incorrect: expected=%d, got=%d", expectedMax, precalcAgg.Max)
	}
	if precalcAgg.Sum != expectedSum {
		t.Errorf("Sum incorrect: expected=%d, got=%d", expectedSum, precalcAgg.Sum)
	}

	expectedAvg := float64(expectedSum) / float64(numValues)
	if math.Abs(precalcAgg.Avg-expectedAvg) > 0.001 {
		t.Errorf("Avg incorrect: expected=%.2f, got=%.2f", expectedAvg, precalcAgg.Avg)
	}
}

// verifyReaderData checks that the reader contains the expected IDs and values
func verifyReaderData(t *testing.T, reader *Reader, expectedIDs []uint64, expectedValues []int64) {
	// Read all blocks and combine the data
	var gotIDs []uint64
	var gotValues []int64

	for i := uint64(0); i < reader.BlockCount(); i++ {
		blockIDs, blockValues, err := reader.GetPairs(i)
		if err != nil {
			t.Fatalf("Failed to read block %d: %v", i, err)
		}
		gotIDs = append(gotIDs, blockIDs...)
		gotValues = append(gotValues, blockValues...)
	}

	// Check that we got the expected number of items
	if len(gotIDs) != len(expectedIDs) {
		t.Errorf("ID count mismatch: got %d, expected %d", len(gotIDs), len(expectedIDs))
	}
	if len(gotValues) != len(expectedValues) {
		t.Errorf("Value count mismatch: got %d, expected %d", len(gotValues), len(expectedValues))
	}

	// Check the first few and last few items
	checkCount := 10
	if len(gotIDs) < checkCount {
		checkCount = len(gotIDs)
	}

	for i := 0; i < checkCount; i++ {
		if gotIDs[i] != expectedIDs[i] {
			t.Errorf("ID mismatch at index %d: got %d, expected %d", i, gotIDs[i], expectedIDs[i])
		}
		if gotValues[i] != expectedValues[i] {
			t.Errorf("Value mismatch at index %d: got %d, expected %d", i, gotValues[i], expectedValues[i])
		}
	}

	// Check the last few items
	for i := len(gotIDs) - checkCount; i < len(gotIDs); i++ {
		if i < 0 {
			continue
		}
		expectedIndex := len(expectedIDs) - (len(gotIDs) - i)
		if expectedIndex < 0 || expectedIndex >= len(expectedIDs) {
			continue
		}
		if gotIDs[i] != expectedIDs[expectedIndex] {
			t.Errorf("ID mismatch at end index %d: got %d, expected %d", i, gotIDs[i], expectedIDs[expectedIndex])
		}
		if gotValues[i] != expectedValues[expectedIndex] {
			t.Errorf("Value mismatch at end index %d: got %d, expected %d", i, gotValues[i], expectedValues[expectedIndex])
		}
	}
}
