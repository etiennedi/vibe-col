package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"os"
	"time"
)

const (
	// Magic number to identify our file format
	MagicNumber uint64 = 0x5649424553434F4C // "VIBESCOL" in ASCII

	// File format version
	Version uint32 = 1

	// Data types
	DataTypeInt64 uint32 = 0

	// Encoding types
	EncodingRaw uint32 = 0

	// Compression types
	CompressionNone uint32 = 0
)

// writeFile writes a simple example column file with 10 int64 values
func writeFile(filename string) error {
	// Create the file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Sample data - 10 ID-value pairs
	ids := []uint64{1, 5, 10, 15, 20, 25, 30, 35, 40, 45}
	values := []int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	count := uint32(len(ids))

	// Calculate statistics
	minID := ids[0]
	maxID := ids[0]
	minValue := values[0]
	maxValue := values[0]
	var sum int64

	for i := 0; i < len(ids); i++ {
		if ids[i] < minID {
			minID = ids[i]
		}
		if ids[i] > maxID {
			maxID = ids[i]
		}
		if values[i] < minValue {
			minValue = values[i]
		}
		if values[i] > maxValue {
			maxValue = values[i]
		}
		sum += values[i]
	}

	// Write file header (64 bytes)
	binary.Write(file, binary.LittleEndian, MagicNumber)                 // 8 bytes
	binary.Write(file, binary.LittleEndian, Version)                     // 4 bytes
	binary.Write(file, binary.LittleEndian, DataTypeInt64)               // 4 bytes
	binary.Write(file, binary.LittleEndian, uint64(1))                   // 8 bytes: block count
	binary.Write(file, binary.LittleEndian, uint32(4*1024))              // 4 bytes: target block size
	binary.Write(file, binary.LittleEndian, CompressionNone)             // 4 bytes
	binary.Write(file, binary.LittleEndian, EncodingRaw)                 // 4 bytes
	binary.Write(file, binary.LittleEndian, uint64(time.Now().Unix()))   // 8 bytes: creation time
	binary.Write(file, binary.LittleEndian, make([]byte, 24))            // 24 bytes: reserved

	// Write block header (64 bytes)
	blockStart := int64(64) // After file header
	binary.Write(file, binary.LittleEndian, minID)                      // 8 bytes
	binary.Write(file, binary.LittleEndian, maxID)                      // 8 bytes
	binary.Write(file, binary.LittleEndian, minValue)                   // 8 bytes
	binary.Write(file, binary.LittleEndian, maxValue)                   // 8 bytes
	binary.Write(file, binary.LittleEndian, sum)                        // 8 bytes
	binary.Write(file, binary.LittleEndian, count)                      // 4 bytes
	binary.Write(file, binary.LittleEndian, EncodingRaw)                // 4 bytes
	binary.Write(file, binary.LittleEndian, CompressionNone)            // 4 bytes
	binary.Write(file, binary.LittleEndian, uint32(count*8*2))          // 4 bytes: uncompressed size
	binary.Write(file, binary.LittleEndian, uint32(count*8*2))          // 4 bytes: compressed size
	
	// We'll calculate the checksum later
	checksumPos, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}
	binary.Write(file, binary.LittleEndian, uint64(0))                  // 8 bytes: placeholder for checksum
	binary.Write(file, binary.LittleEndian, make([]byte, 8))            // 8 bytes: reserved

	// Write block data layout (16 bytes)
	binary.Write(file, binary.LittleEndian, uint32(0))                  // 4 bytes: ID section offset (0 = right after layout)
	binary.Write(file, binary.LittleEndian, uint32(count*8))            // 4 bytes: ID section size
	binary.Write(file, binary.LittleEndian, uint32(count*8))            // 4 bytes: Value section offset
	binary.Write(file, binary.LittleEndian, uint32(count*8))            // 4 bytes: Value section size

	// Write block data - IDs and values
	dataStart, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}
	
	// Write ID array
	for _, id := range ids {
		binary.Write(file, binary.LittleEndian, id)
	}
	
	// Write Value array
	for _, val := range values {
		binary.Write(file, binary.LittleEndian, val)
	}
	
	dataEnd, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}
	
	// Calculate block checksum
	_, err = file.Seek(dataStart, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}
	blockData := make([]byte, dataEnd-dataStart)
	_, err = file.Read(blockData)
	if err != nil {
		return fmt.Errorf("failed to read block data: %w", err)
	}
	blockChecksum := crc64.Checksum(blockData, crc64.MakeTable(crc64.ISO))
	
	// Write the checksum back to the header
	_, err = file.Seek(checksumPos, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}
	binary.Write(file, binary.LittleEndian, blockChecksum)
	
	// Move to the end to write the footer
	_, err = file.Seek(0, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}
	
	// Write footer
	footerStart, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}
	
	// Block index count
	binary.Write(file, binary.LittleEndian, uint32(1))  // 4 bytes: 1 block
	
	// Block index entry (48 bytes)
	binary.Write(file, binary.LittleEndian, uint64(blockStart))   // 8 bytes: block offset
	binary.Write(file, binary.LittleEndian, uint32(dataEnd-blockStart)) // 4 bytes: block size
	binary.Write(file, binary.LittleEndian, minID)              // 8 bytes
	binary.Write(file, binary.LittleEndian, maxID)              // 8 bytes
	binary.Write(file, binary.LittleEndian, minValue)           // 8 bytes
	binary.Write(file, binary.LittleEndian, maxValue)           // 8 bytes
	binary.Write(file, binary.LittleEndian, sum)                // 8 bytes
	binary.Write(file, binary.LittleEndian, count)              // 4 bytes
	
	// Footer size
	footerEnd, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}
	footerSize := footerEnd - footerStart
	binary.Write(file, binary.LittleEndian, uint64(footerSize)) // 8 bytes
	
	// Calculate file checksum (excluding the checksum field itself)
	_, err = file.Seek(0, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}
	fileData := make([]byte, footerEnd)
	_, err = file.Read(fileData)
	if err != nil {
		return fmt.Errorf("failed to read file data: %w", err)
	}
	fileChecksum := crc64.Checksum(fileData, crc64.MakeTable(crc64.ISO))
	binary.Write(file, binary.LittleEndian, fileChecksum)       // 8 bytes
	
	// Ending magic number
	binary.Write(file, binary.LittleEndian, MagicNumber)        // 8 bytes
	
	// Get final file size
	currentPos, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}
	
	fmt.Printf("Wrote example file with %d entries to %s\n", count, filename)
	fmt.Printf("File size: %d bytes\n", currentPos)
	
	return nil
}

func main() {
	filename := "example.col"
	if err := writeFile(filename); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}