package col

import (
	"os"
	"testing"
)

func dumpFileContents(t *testing.T, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to open file for dumping: %v", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("Failed to get file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// Read the entire file
	data := make([]byte, fileSize)
	if _, err := file.Read(data); err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// Print file size
	t.Logf("File size: %d bytes", fileSize)

	// Print file header (first 64 bytes)
	t.Logf("File header (first 64 bytes):")
	for i := 0; i < 64 && i < len(data); i++ {
		if i%16 == 0 {
			t.Logf("%04x: ", i)
		}
		t.Logf("%02x ", data[i])
		if (i+1)%16 == 0 {
			t.Logf("\n")
		}
	}
	t.Logf("\n")

	// Print block header (next 64 bytes)
	t.Logf("Block header (next 64 bytes):")
	for i := 64; i < 128 && i < len(data); i++ {
		if i%16 == 0 {
			t.Logf("%04x: ", i)
		}
		t.Logf("%02x ", data[i])
		if (i+1)%16 == 0 {
			t.Logf("\n")
		}
	}
	t.Logf("\n")

	// Print block layout (next 16 bytes)
	t.Logf("Block layout (next 16 bytes):")
	for i := 128; i < 144 && i < len(data); i++ {
		if i%16 == 0 {
			t.Logf("%04x: ", i)
		}
		t.Logf("%02x ", data[i])
		if (i+1)%16 == 0 {
			t.Logf("\n")
		}
	}
	t.Logf("\n")

	// Print first part of data section
	t.Logf("Data section (first 32 bytes):")
	for i := 144; i < 176 && i < len(data); i++ {
		if i%16 == 0 {
			t.Logf("%04x: ", i)
		}
		t.Logf("%02x ", data[i])
		if (i+1)%16 == 0 {
			t.Logf("\n")
		}
	}
	t.Logf("\n")

	// Print footer (last 64 bytes)
	t.Logf("Footer (last 64 bytes):")
	start := len(data) - 64
	if start < 0 {
		start = 0
	}
	for i := start; i < len(data); i++ {
		if i%16 == 0 {
			t.Logf("%04x: ", i)
		}
		t.Logf("%02x ", data[i])
		if (i+1)%16 == 0 {
			t.Logf("\n")
		}
	}
	t.Logf("\n")
}

func TestVarIntIO(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "./test_varint_io.col"
	defer os.Remove(tempFile)

	// Test data - simple sequential IDs and values
	const count = 100
	ids := make([]uint64, count)
	values := make([]int64, count)

	for i := 0; i < count; i++ {
		ids[i] = uint64(i + 1)
		values[i] = int64(i * 10)
	}

	// Write with varint encoding
	writer, err := NewWriter(tempFile, WithEncoding(EncodingVarInt))
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

	// Dump file contents for debugging
	dumpFileContents(t, tempFile)

	// Read the file back
	reader, err := NewReader(tempFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer reader.Close()

	// Print debug info
	t.Logf("Reader debug info: %s", reader.DebugInfo())

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

	// Check all values that we have
	minLen := len(readIds)
	if len(readValues) < minLen {
		minLen = len(readValues)
	}
	if len(ids) < minLen {
		minLen = len(ids)
	}
	if len(values) < minLen {
		minLen = len(values)
	}

	for i := 0; i < minLen; i++ {
		if readIds[i] != ids[i] {
			t.Errorf("ID mismatch at index %d: expected %d, got %d", i, ids[i], readIds[i])
		}
		if readValues[i] != values[i] {
			t.Errorf("Value mismatch at index %d: expected %d, got %d", i, values[i], readValues[i])
		}
	}
}

func TestDeltaVarIntIO(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "test_delta_varint_io.col"
	defer os.Remove(tempFile)

	// Test data - simple sequential IDs and values
	const count = 100
	ids := make([]uint64, count)
	values := make([]int64, count)

	for i := 0; i < count; i++ {
		ids[i] = uint64(i*10 + 1) // Non-sequential IDs with gaps
		values[i] = int64(i * 10)
	}

	// Write with delta + varint encoding
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

	// Check all values
	for i := 0; i < len(ids); i++ {
		if readIds[i] != ids[i] {
			t.Errorf("ID mismatch at index %d: expected %d, got %d", i, ids[i], readIds[i])
		}
		if readValues[i] != values[i] {
			t.Errorf("Value mismatch at index %d: expected %d, got %d", i, values[i], readValues[i])
		}
	}
}
