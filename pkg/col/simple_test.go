package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestSimpleColumnFile(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "./test_simple_column.col"
	defer os.Remove(tempFile)

	// Test data - a single ID-value pair
	ids := []uint64{42}
	values := []int64{123}

	// Write with raw encoding
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

	// Dump the file contents
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

	// Print the block layout (next 16 bytes)
	fmt.Printf("Block layout (next 16 bytes):\n")
	for i := 128; i < 144 && i < len(data); i++ {
		fmt.Printf("%02x ", data[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Print the data section (next 16 bytes)
	fmt.Printf("Data section (next 16 bytes):\n")
	for i := 144; i < 160 && i < len(data); i++ {
		fmt.Printf("%02x ", data[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Parse the data section
	idSectionSize := binary.LittleEndian.Uint32(data[144:148])
	valueSectionSize := binary.LittleEndian.Uint32(data[148:152])
	fmt.Printf("Data section header: idSectionSize=%d, valueSectionSize=%d\n", idSectionSize, valueSectionSize)

	// Read the ID section
	idSection := make([]byte, idSectionSize)
	file.Seek(160, os.SEEK_SET)
	if _, err := io.ReadFull(file, idSection); err != nil {
		t.Fatalf("Failed to read ID section: %v", err)
	}

	// Read the value section
	valueSection := make([]byte, valueSectionSize)
	if _, err := io.ReadFull(file, valueSection); err != nil {
		t.Fatalf("Failed to read value section: %v", err)
	}

	fmt.Printf("ID section bytes: ")
	for _, b := range idSection {
		fmt.Printf("%02x ", b)
	}
	fmt.Println()

	fmt.Printf("Value section bytes: ")
	for _, b := range valueSection {
		fmt.Printf("%02x ", b)
	}
	fmt.Println()

	// Interpret the bytes
	if len(idSection) == 8 {
		idBytes := binary.LittleEndian.Uint64(idSection)
		fmt.Printf("ID bytes as uint64: %d\n", idBytes)
	}

	if len(valueSection) == 8 {
		valueBytes := binary.LittleEndian.Uint64(valueSection)
		valueInt64 := int64(valueBytes)
		fmt.Printf("Value bytes as uint64: %d\n", valueBytes)
		fmt.Printf("Value bytes as int64: %d\n", valueInt64)
	}

	// Now read the file back using the Reader
	reader, err := NewReader(tempFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer reader.Close()

	// Check block count
	if reader.BlockCount() != 1 {
		t.Errorf("Expected 1 block, got %d", reader.BlockCount())
	}

	// Read the data using the reader
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

	// Print the values
	fmt.Printf("Original ID: %d, Reader ID: %d\n", ids[0], readIds[0])
	fmt.Printf("Original Value: %d, Reader Value: %d\n", values[0], readValues[0])

	// Check the values
	if len(readIds) > 0 && readIds[0] != ids[0] {
		t.Errorf("ID mismatch: expected %d, got %d", ids[0], readIds[0])
	}
	if len(readValues) > 0 && readValues[0] != values[0] {
		t.Errorf("Value mismatch: expected %d, got %d", values[0], readValues[0])
	}
}
