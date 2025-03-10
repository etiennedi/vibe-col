package col

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestFileFormatDebug(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "./test_file_format.col"
	defer os.Remove(tempFile)

	// Test data - simple sequential IDs and values
	const count = 10
	ids := make([]uint64, count)
	values := make([]int64, count)

	for i := 0; i < count; i++ {
		ids[i] = uint64(i + 1)
		values[i] = int64(i * 10)
	}

	// Write with varint encoding
	writer, err := NewWriter(tempFile, WithEncoding(EncodingRaw))
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

	// Open the file and read it byte by byte
	file, err := os.Open(tempFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
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

	// Print the file contents
	fmt.Printf("File size: %d bytes\n", fileSize)
	fmt.Printf("File header (first 64 bytes):\n")
	for i := 0; i < 64 && i < len(data); i++ {
		fmt.Printf("%02x ", data[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Print the block header (next 64 bytes)
	fmt.Printf("Block header (next 64 bytes):\n")
	for i := 64; i < 128 && i < len(data); i++ {
		fmt.Printf("%02x ", data[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Parse the block header
	minID := binary.LittleEndian.Uint64(data[64:72])
	maxID := binary.LittleEndian.Uint64(data[72:80])
	headerCount := binary.LittleEndian.Uint32(data[104:108])
	encodingType := binary.LittleEndian.Uint32(data[108:112])
	uncompressedSize := binary.LittleEndian.Uint32(data[116:120])
	compressedSize := binary.LittleEndian.Uint32(data[120:124])
	fmt.Printf("Parsed block header: minID=%d, maxID=%d, count=%d, encodingType=%d, uncompressedSize=%d, compressedSize=%d\n",
		minID, maxID, headerCount, encodingType, uncompressedSize, compressedSize)

	// Print the block layout (next 16 bytes)
	fmt.Printf("Block layout (next 16 bytes):\n")
	for i := 128; i < 144 && i < len(data); i++ {
		fmt.Printf("%02x ", data[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Parse the block layout
	idOffset := binary.LittleEndian.Uint32(data[128:132])
	idSectionSize := binary.LittleEndian.Uint32(data[132:136])
	valOffset := binary.LittleEndian.Uint32(data[136:140])
	valueSectionSize := binary.LittleEndian.Uint32(data[140:144])
	fmt.Printf("Parsed block layout: idOffset=%d, idSectionSize=%d, valOffset=%d, valueSectionSize=%d\n",
		idOffset, idSectionSize, valOffset, valueSectionSize)

	// Print the ID section (next 10 bytes)
	fmt.Printf("ID section (next %d bytes):\n", count)
	for i := 144; i < 144+count && i < len(data); i++ {
		fmt.Printf("%02x ", data[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Print the value section (next 10 bytes)
	fmt.Printf("Value section (next %d bytes):\n", count)
	for i := 144 + count; i < 144+count*2 && i < len(data); i++ {
		fmt.Printf("%02x ", data[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Now read the file back using the Reader
	reader, err := NewReader(tempFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer reader.Close()

	// Read the data
	readIds, readValues, err := reader.GetPairs(0)
	if err != nil {
		t.Fatalf("Failed to read pairs: %v", err)
	}

	// Print the read data
	fmt.Printf("Read %d IDs and %d values\n", len(readIds), len(readValues))
	for i := 0; i < len(readIds) && i < len(readValues); i++ {
		fmt.Printf("ID[%d]: %d, Value[%d]: %d\n", i, readIds[i], i, readValues[i])
	}

	// Verify data integrity
	if len(readIds) != len(ids) {
		t.Errorf("Expected %d IDs, got %d", len(ids), len(readIds))
	}
	if len(readValues) != len(values) {
		t.Errorf("Expected %d values, got %d", len(values), len(readValues))
	}

	// Check all values
	for i := 0; i < len(readIds) && i < len(ids); i++ {
		if readIds[i] != ids[i] {
			t.Errorf("ID mismatch at index %d: expected %d, got %d", i, ids[i], readIds[i])
		}
	}

	for i := 0; i < len(readValues) && i < len(values); i++ {
		if readValues[i] != values[i] {
			t.Errorf("Value mismatch at index %d: expected %d, got %d", i, values[i], readValues[i])
		}
	}
}
