package col

import (
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
