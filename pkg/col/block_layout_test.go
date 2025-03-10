package col

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestBlockLayout(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "./test_block_layout.col"
	defer os.Remove(tempFile)

	// Create a file
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Create a layout buffer and fill it
	layoutBuf := make([]byte, 16)
	idSectionOffset := uint32(0)
	idSectionSize := uint32(10)
	valueSectionOffset := uint32(10)
	valueSectionSize := uint32(20)

	binary.LittleEndian.PutUint32(layoutBuf[0:4], idSectionOffset)
	binary.LittleEndian.PutUint32(layoutBuf[4:8], idSectionSize)
	binary.LittleEndian.PutUint32(layoutBuf[8:12], valueSectionOffset)
	binary.LittleEndian.PutUint32(layoutBuf[12:16], valueSectionSize)

	// Write the layout buffer to file
	if _, err := file.Write(layoutBuf); err != nil {
		t.Fatalf("Failed to write block layout: %v", err)
	}

	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Open the file and read it byte by byte
	file, err = os.Open(tempFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Read the layout buffer
	readLayoutBuf := make([]byte, 16)
	if _, err := file.Read(readLayoutBuf); err != nil {
		t.Fatalf("Failed to read block layout: %v", err)
	}

	// Parse the layout buffer
	readIdOffset := binary.LittleEndian.Uint32(readLayoutBuf[0:4])
	readIdSectionSize := binary.LittleEndian.Uint32(readLayoutBuf[4:8])
	readValOffset := binary.LittleEndian.Uint32(readLayoutBuf[8:12])
	readValueSectionSize := binary.LittleEndian.Uint32(readLayoutBuf[12:16])

	// Print the layout values
	fmt.Printf("Original layout: idOffset=%d, idSectionSize=%d, valOffset=%d, valueSectionSize=%d\n",
		idSectionOffset, idSectionSize, valueSectionOffset, valueSectionSize)
	fmt.Printf("Read layout: idOffset=%d, idSectionSize=%d, valOffset=%d, valueSectionSize=%d\n",
		readIdOffset, readIdSectionSize, readValOffset, readValueSectionSize)

	// Verify the layout values
	if readIdOffset != idSectionOffset {
		t.Errorf("ID offset mismatch: expected %d, got %d", idSectionOffset, readIdOffset)
	}
	if readIdSectionSize != idSectionSize {
		t.Errorf("ID section size mismatch: expected %d, got %d", idSectionSize, readIdSectionSize)
	}
	if readValOffset != valueSectionOffset {
		t.Errorf("Value offset mismatch: expected %d, got %d", valueSectionOffset, readValOffset)
	}
	if readValueSectionSize != valueSectionSize {
		t.Errorf("Value section size mismatch: expected %d, got %d", valueSectionSize, readValueSectionSize)
	}

	// Print the raw bytes
	fmt.Printf("Original layout bytes: ")
	for _, b := range layoutBuf {
		fmt.Printf("%02x ", b)
	}
	fmt.Println()

	fmt.Printf("Read layout bytes: ")
	for _, b := range readLayoutBuf {
		fmt.Printf("%02x ", b)
	}
	fmt.Println()
}

func TestBlockLayoutInFile(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "./test_block_layout_in_file.col"
	defer os.Remove(tempFile)

	// Create a file with some header data
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write some header data (64 bytes)
	headerData := make([]byte, 64)
	if _, err := file.Write(headerData); err != nil {
		t.Fatalf("Failed to write header data: %v", err)
	}

	// Write some block header data (64 bytes)
	blockHeaderData := make([]byte, 64)
	if _, err := file.Write(blockHeaderData); err != nil {
		t.Fatalf("Failed to write block header data: %v", err)
	}

	// Create a layout buffer and fill it
	layoutBuf := make([]byte, 16)
	idSectionOffset := uint32(0)
	idSectionSize := uint32(10)
	valueSectionOffset := uint32(10)
	valueSectionSize := uint32(20)

	binary.LittleEndian.PutUint32(layoutBuf[0:4], idSectionOffset)
	binary.LittleEndian.PutUint32(layoutBuf[4:8], idSectionSize)
	binary.LittleEndian.PutUint32(layoutBuf[8:12], valueSectionOffset)
	binary.LittleEndian.PutUint32(layoutBuf[12:16], valueSectionSize)

	// Write the layout buffer to file
	if _, err := file.Write(layoutBuf); err != nil {
		t.Fatalf("Failed to write block layout: %v", err)
	}

	// Write some data (30 bytes)
	data := make([]byte, 30)
	for i := 0; i < 30; i++ {
		data[i] = byte(i)
	}
	if _, err := file.Write(data); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Open the file and read it byte by byte
	file, err = os.Open(tempFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Read the entire file
	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("Failed to get file info: %v", err)
	}
	fileSize := fileInfo.Size()
	fileData := make([]byte, fileSize)
	if _, err := file.Read(fileData); err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// Print the file contents
	fmt.Printf("File size: %d bytes\n", fileSize)
	fmt.Printf("File header (first 64 bytes):\n")
	for i := 0; i < 64 && i < len(fileData); i++ {
		fmt.Printf("%02x ", fileData[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Print the block header (next 64 bytes)
	fmt.Printf("Block header (next 64 bytes):\n")
	for i := 64; i < 128 && i < len(fileData); i++ {
		fmt.Printf("%02x ", fileData[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Print the block layout (next 16 bytes)
	fmt.Printf("Block layout (next 16 bytes):\n")
	for i := 128; i < 144 && i < len(fileData); i++ {
		fmt.Printf("%02x ", fileData[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Parse the block layout
	readIdOffset := binary.LittleEndian.Uint32(fileData[128:132])
	readIdSectionSize := binary.LittleEndian.Uint32(fileData[132:136])
	readValOffset := binary.LittleEndian.Uint32(fileData[136:140])
	readValueSectionSize := binary.LittleEndian.Uint32(fileData[140:144])

	// Print the layout values
	fmt.Printf("Original layout: idOffset=%d, idSectionSize=%d, valOffset=%d, valueSectionSize=%d\n",
		idSectionOffset, idSectionSize, valueSectionOffset, valueSectionSize)
	fmt.Printf("Read layout: idOffset=%d, idSectionSize=%d, valOffset=%d, valueSectionSize=%d\n",
		readIdOffset, readIdSectionSize, readValOffset, readValueSectionSize)

	// Verify the layout values
	if readIdOffset != idSectionOffset {
		t.Errorf("ID offset mismatch: expected %d, got %d", idSectionOffset, readIdOffset)
	}
	if readIdSectionSize != idSectionSize {
		t.Errorf("ID section size mismatch: expected %d, got %d", idSectionSize, readIdSectionSize)
	}
	if readValOffset != valueSectionOffset {
		t.Errorf("Value offset mismatch: expected %d, got %d", valueSectionOffset, readValOffset)
	}
	if readValueSectionSize != valueSectionSize {
		t.Errorf("Value section size mismatch: expected %d, got %d", valueSectionSize, readValueSectionSize)
	}
}
