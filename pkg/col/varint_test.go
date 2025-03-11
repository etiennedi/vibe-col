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

func TestVarintEncodingCompression(t *testing.T) {
	// Create a temporary file for testing
	tempFileRaw := "test_raw.col"
	tempFileVarInt := "test_varint.col"
	defer os.Remove(tempFileRaw)
	defer os.Remove(tempFileVarInt)

	// Create sequential data that should compress well
	const count = 100000
	ids := make([]uint64, count)
	values := make([]int64, count)

	// Sequential IDs (1, 2, 3, ...) - delta encoding will be efficient
	// Small values (0, 1, 2, ...) - varint encoding will be efficient
	for i := 0; i < count; i++ {
		ids[i] = uint64(i + 1)     // Start from 1
		values[i] = int64(i % 100) // Small values (0-99)
	}

	// Write with raw encoding first
	rawWriter, err := NewWriter(tempFileRaw, WithEncoding(EncodingRaw))
	if err != nil {
		t.Fatalf("Failed to create raw writer: %v", err)
	}
	if err := rawWriter.WriteBlock(ids, values); err != nil {
		t.Fatalf("Failed to write raw block: %v", err)
	}
	if err := rawWriter.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize raw file: %v", err)
	}

	// Write with varint encoding
	varIntWriter, err := NewWriter(tempFileVarInt, WithEncoding(EncodingVarIntBoth))
	if err != nil {
		t.Fatalf("Failed to create varint writer: %v", err)
	}
	if err := varIntWriter.WriteBlock(ids, values); err != nil {
		t.Fatalf("Failed to write varint block: %v", err)
	}
	if err := varIntWriter.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize varint file: %v", err)
	}

	// Get file sizes
	rawInfo, err := os.Stat(tempFileRaw)
	if err != nil {
		t.Fatalf("Failed to get raw file info: %v", err)
	}
	rawSize := rawInfo.Size()

	varIntInfo, err := os.Stat(tempFileVarInt)
	if err != nil {
		t.Fatalf("Failed to get varint file info: %v", err)
	}
	varIntSize := varIntInfo.Size()

	// Verify that the varint file is significantly smaller
	if varIntSize >= rawSize {
		t.Errorf("VarInt encoding should result in smaller file, but got: raw=%d bytes, varint=%d bytes",
			rawSize, varIntSize)
	} else {
		compressionRatio := float64(rawSize) / float64(varIntSize)
		t.Logf("VarInt compression: raw=%d bytes, varint=%d bytes, ratio=%.2fx",
			rawSize, varIntSize, compressionRatio)

		// Add display in kilobytes
		rawSizeKB := float64(rawSize) / 1024.0
		varIntSizeKB := float64(varIntSize) / 1024.0
		t.Logf("Sizes in kB: raw=%.2f kB, varint=%.2f kB", rawSizeKB, varIntSizeKB)

		// For small sequential values, we expect at least 2x compression
		if compressionRatio < 2.0 {
			t.Errorf("VarInt compression ratio (%.2fx) is lower than expected (at least 2x)",
				compressionRatio)
		}

		// Calculate exact savings
		byteSavings := rawSize - varIntSize
		percentageSavings := (float64(byteSavings) / float64(rawSize)) * 100

		// Print exact savings
		t.Logf("Exact savings: %d bytes (%.2f%%)", byteSavings, percentageSavings)
	}

	// Verify that both files contain the correct data
	rawReader, err := NewReader(tempFileRaw)
	if err != nil {
		t.Fatalf("Failed to open raw file: %v", err)
	}
	defer rawReader.Close()

	varIntReader, err := NewReader(tempFileVarInt)
	if err != nil {
		t.Fatalf("Failed to open varint file: %v", err)
	}
	defer varIntReader.Close()

	// Read data from both files
	rawIds, rawValues, err := rawReader.GetPairs(0)
	if err != nil {
		t.Fatalf("Failed to read raw pairs: %v", err)
	}

	varIntIds, varIntValues, err := varIntReader.GetPairs(0)
	if err != nil {
		t.Fatalf("Failed to read varint pairs: %v", err)
	}

	// Check data integrity
	if len(rawIds) != count || len(varIntIds) != count {
		t.Errorf("ID count mismatch: raw=%d, varint=%d, expected=%d",
			len(rawIds), len(varIntIds), count)
	}

	if len(rawValues) != count || len(varIntValues) != count {
		t.Errorf("Value count mismatch: raw=%d, varint=%d, expected=%d",
			len(rawValues), len(varIntValues), count)
	}

	// Check some values to ensure they're identical
	for i := 0; i < count; i += count / 10 { // Check every 10% of entries
		if rawIds[i] != varIntIds[i] {
			t.Errorf("ID mismatch at index %d: raw=%d, varint=%d",
				i, rawIds[i], varIntIds[i])
		}
		if rawValues[i] != varIntValues[i] {
			t.Errorf("Value mismatch at index %d: raw=%d, varint=%d",
				i, rawValues[i], varIntValues[i])
		}
	}
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

// TestVarintEncodingCompression_RealWorldData tests VarInt encoding compression with more realistic data patterns
func TestVarintEncodingCompression_RealWorldData(t *testing.T) {
	// Create a temporary file for testing
	tempFileRaw := "test_raw_real.col"
	tempFileVarInt := "test_varint_real.col"
	defer os.Remove(tempFileRaw)
	defer os.Remove(tempFileVarInt)

	// Create less ideal data that represents more realistic scenarios
	const count = 100000
	ids := make([]uint64, count)
	values := make([]int64, count)

	// Initialize random number generator with a fixed seed for reproducibility
	rand.Seed(42)

	// Generate data with more realistic patterns
	// 1. IDs with gaps and non-uniform distribution
	// 2. Values with larger magnitudes and more variability
	var lastID uint64 = 1000 // Starting ID

	// Generate first value
	if rand.Intn(3) == 0 {
		// Small value
		values[0] = int64(rand.Intn(100))
	} else if rand.Intn(2) == 0 {
		// Medium value
		values[0] = int64(rand.Intn(10000))
	} else {
		// Large value, sometimes negative
		values[0] = int64(rand.Intn(1000000))
		if rand.Intn(2) == 0 {
			values[0] = -values[0]
		}
	}
	ids[0] = lastID

	// Generate remaining values
	for i := 1; i < count; i++ {
		// IDs with variable gaps (some small, some large)
		gap := uint64(1)
		if i%10 == 0 {
			// Every 10th element has a larger gap
			gap = uint64(rand.Intn(100) + 10)
		} else if i%100 == 0 {
			// Every 100th element has an even larger gap
			gap = uint64(rand.Intn(1000) + 100)
		}

		lastID += gap
		ids[i] = lastID

		// Generate value
		if i%3 == 0 {
			// Small values
			values[i] = int64(rand.Intn(100))
		} else if i%3 == 1 {
			// Medium values
			values[i] = int64(rand.Intn(10000))
		} else {
			// Large values, sometimes negative
			values[i] = int64(rand.Intn(1000000))
			if rand.Intn(2) == 0 {
				values[i] = -values[i]
			}
		}
	}

	// Write with raw encoding first
	rawWriter, err := NewWriter(tempFileRaw, WithEncoding(EncodingRaw))
	if err != nil {
		t.Fatalf("Failed to create raw writer: %v", err)
	}
	if err := rawWriter.WriteBlock(ids, values); err != nil {
		t.Fatalf("Failed to write raw block: %v", err)
	}
	if err := rawWriter.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize raw file: %v", err)
	}

	// Write with varint encoding
	varIntWriter, err := NewWriter(tempFileVarInt, WithEncoding(EncodingVarIntBoth))
	if err != nil {
		t.Fatalf("Failed to create varint writer: %v", err)
	}
	if err := varIntWriter.WriteBlock(ids, values); err != nil {
		t.Fatalf("Failed to write varint block: %v", err)
	}
	if err := varIntWriter.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize varint file: %v", err)
	}

	// Get file sizes
	rawInfo, err := os.Stat(tempFileRaw)
	if err != nil {
		t.Fatalf("Failed to get raw file info: %v", err)
	}
	rawSize := rawInfo.Size()

	varIntInfo, err := os.Stat(tempFileVarInt)
	if err != nil {
		t.Fatalf("Failed to get varint file info: %v", err)
	}
	varIntSize := varIntInfo.Size()

	// Verify that the varint file is smaller
	if varIntSize >= rawSize {
		t.Errorf("VarInt encoding should result in smaller file, but got: raw=%d bytes, varint=%d bytes",
			rawSize, varIntSize)
	} else {
		t.Logf("Real-world data test results:")
		compressionRatio := float64(rawSize) / float64(varIntSize)
		t.Logf("VarInt compression: raw=%d bytes, varint=%d bytes, ratio=%.2fx",
			rawSize, varIntSize, compressionRatio)

		// Add display in kilobytes
		rawSizeKB := float64(rawSize) / 1024.0
		varIntSizeKB := float64(varIntSize) / 1024.0
		t.Logf("Sizes in kB: raw=%.2f kB, varint=%.2f kB", rawSizeKB, varIntSizeKB)

		// Calculate exact savings
		byteSavings := rawSize - varIntSize
		percentageSavings := (float64(byteSavings) / float64(rawSize)) * 100

		// Print exact savings
		t.Logf("Exact savings: %d bytes (%.2f%%)", byteSavings, percentageSavings)
	}

	// Verify that the file can be read back correctly
	reader, err := NewReader(tempFileVarInt)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	gotIDs, gotValues, err := reader.GetPairs(0)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	// Validate the first few and last few entries
	for i := 0; i < 5 && i < len(ids); i++ {
		if ids[i] != gotIDs[i] {
			t.Errorf("ID mismatch at index %d: want %d, got %d", i, ids[i], gotIDs[i])
		}
		if values[i] != gotValues[i] {
			t.Errorf("Value mismatch at index %d: want %d, got %d", i, values[i], gotValues[i])
		}
	}

	for i := len(ids) - 5; i < len(ids); i++ {
		if i < 0 {
			continue
		}
		if ids[i] != gotIDs[i] {
			t.Errorf("ID mismatch at index %d: want %d, got %d", i, ids[i], gotIDs[i])
		}
		if values[i] != gotValues[i] {
			t.Errorf("Value mismatch at index %d: want %d, got %d", i, values[i], gotValues[i])
		}
	}
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
