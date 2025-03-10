package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestSimpleWriteRead(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "./test_simple.col"
	defer os.Remove(tempFile)

	// Test data - a single ID-value pair
	id := uint64(42)
	value := int64(123)

	// Create a file
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write the ID
	if err := binary.Write(file, binary.LittleEndian, id); err != nil {
		t.Fatalf("Failed to write ID: %v", err)
	}

	// Write the value
	if err := binary.Write(file, binary.LittleEndian, value); err != nil {
		t.Fatalf("Failed to write value: %v", err)
	}

	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Open the file
	file, err = os.Open(tempFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Read the ID
	var readId uint64
	if err := binary.Read(file, binary.LittleEndian, &readId); err != nil {
		t.Fatalf("Failed to read ID: %v", err)
	}

	// Read the value
	var readValue int64
	if err := binary.Read(file, binary.LittleEndian, &readValue); err != nil {
		t.Fatalf("Failed to read value: %v", err)
	}

	// Print the values
	fmt.Printf("Original ID: %d, Read ID: %d\n", id, readId)
	fmt.Printf("Original Value: %d, Read Value: %d\n", value, readValue)

	// Check the values
	if readId != id {
		t.Errorf("ID mismatch: expected %d, got %d", id, readId)
	}
	if readValue != value {
		t.Errorf("Value mismatch: expected %d, got %d", value, readValue)
	}
}

func TestDirectWriteRead(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "./test_direct.col"
	defer os.Remove(tempFile)

	// Test data - a single ID-value pair
	id := uint64(42)
	value := int64(123)

	// Create a file
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write a header (64 bytes)
	header := make([]byte, 64)
	// Set the magic number
	copy(header[0:8], []byte{0x00, 0x4c, 0x4f, 0x43, 0x5f, 0x45, 0x42, 0x56})
	// Set the version
	binary.LittleEndian.PutUint32(header[8:12], 1)
	// Set the block count
	binary.LittleEndian.PutUint64(header[16:24], 1)
	if _, err := file.Write(header); err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	// Write a block header (64 bytes)
	blockHeader := make([]byte, 64)
	if _, err := file.Write(blockHeader); err != nil {
		t.Fatalf("Failed to write block header: %v", err)
	}

	// Write a block layout (16 bytes)
	blockLayout := make([]byte, 16)
	binary.LittleEndian.PutUint32(blockLayout[0:4], 0)
	binary.LittleEndian.PutUint32(blockLayout[4:8], 8)
	binary.LittleEndian.PutUint32(blockLayout[8:12], 8)
	binary.LittleEndian.PutUint32(blockLayout[12:16], 8)
	if _, err := file.Write(blockLayout); err != nil {
		t.Fatalf("Failed to write block layout: %v", err)
	}

	// Write the ID
	if err := binary.Write(file, binary.LittleEndian, id); err != nil {
		t.Fatalf("Failed to write ID: %v", err)
	}

	// Write the value
	if err := binary.Write(file, binary.LittleEndian, value); err != nil {
		t.Fatalf("Failed to write value: %v", err)
	}

	// Write a block index (4 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 = 64 bytes)
	blockIndex := make([]byte, 64)
	// Set the block count
	binary.LittleEndian.PutUint32(blockIndex[0:4], 1)
	// Set the block offset
	binary.LittleEndian.PutUint64(blockIndex[4:12], 64)
	// Set the block size
	binary.LittleEndian.PutUint32(blockIndex[12:16], 64+16+16)
	// Set the min ID
	binary.LittleEndian.PutUint64(blockIndex[16:24], id)
	// Set the max ID
	binary.LittleEndian.PutUint64(blockIndex[24:32], id)
	// Set the min value
	binary.LittleEndian.PutUint64(blockIndex[32:40], uint64(value))
	// Set the max value
	binary.LittleEndian.PutUint64(blockIndex[40:48], uint64(value))
	// Set the sum
	binary.LittleEndian.PutUint64(blockIndex[48:56], uint64(value))
	// Set the count
	binary.LittleEndian.PutUint32(blockIndex[56:60], 1)
	if _, err := file.Write(blockIndex); err != nil {
		t.Fatalf("Failed to write block index: %v", err)
	}

	// Write a footer (24 bytes)
	footer := make([]byte, 24)
	// Set the footer size
	binary.LittleEndian.PutUint32(footer[0:4], uint32(len(blockIndex)))
	// Set the footer magic number
	copy(footer[16:24], []byte{0x00, 0x4c, 0x4f, 0x43, 0x5f, 0x45, 0x42, 0x56})
	if _, err := file.Write(footer); err != nil {
		t.Fatalf("Failed to write footer: %v", err)
	}

	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Dump the file contents
	file, err = os.Open(tempFile)
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

	// Print the ID and value (next 16 bytes)
	fmt.Printf("ID and value (next 16 bytes):\n")
	for i := 144; i < 160 && i < len(data); i++ {
		fmt.Printf("%02x ", data[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Open the file with the reader
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

	// Print the values
	fmt.Printf("Original ID: %d, Reader ID: %d\n", id, readIds[0])
	fmt.Printf("Original Value: %d, Reader Value: %d\n", value, readValues[0])

	// Check the values
	if readIds[0] != id {
		t.Errorf("ID mismatch: expected %d, got %d", id, readIds[0])
	}
	if readValues[0] != value {
		t.Errorf("Value mismatch: expected %d, got %d", value, readValues[0])
	}
}

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
