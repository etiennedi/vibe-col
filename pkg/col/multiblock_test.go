package col

import (
	"encoding/binary"
	"os"
	"testing"
)

// TestDiagnoseMultiBlockIssue is a test to diagnose issues with multi-block files
func TestDiagnoseMultiBlockIssue(t *testing.T) {
	// Create a temporary file
	tempFile := "test_multiblock_issue.col"
	defer os.Remove(tempFile)

	// Create test data - reasonable size for debugging (1000 entries)
	const numEntries = 1000
	const blockSize = 100 // 10 blocks
	
	// Use our own test data generation for better debugging
	ids := make([]uint64, numEntries)
	values := make([]int64, numEntries)
	
	for i := 0; i < numEntries; i++ {
		ids[i] = uint64(1000 + i)      // Start at 1000 and increment by 1
		values[i] = int64(i * 10)      // Simple values that are easy to verify
	}
	
	t.Logf("Test data: IDs from %d to %d, Values from %d to %d",
		ids[0], ids[numEntries-1], values[0], values[numEntries-1])

	// Delete any existing test file to ensure we're starting fresh
	os.Remove(tempFile)
	
	// Create writer
	writer, err := NewWriter(tempFile)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write data in blocks
	blockCount := 0
	for start := 0; start < len(ids); start += blockSize {
		end := start + blockSize
		if end > len(ids) {
			end = len(ids)
		}
		
		t.Logf("Writing block %d: entries %d-%d", blockCount, start, end-1)
		
		// Write a block
		if err := writer.WriteBlock(ids[start:end], values[start:end]); err != nil {
			t.Fatalf("Failed to write block %d: %v", blockCount, err)
		}
		
		blockCount++
	}

	// Finalize and close
	if err := writer.FinalizeAndClose(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	
	// Get file info for diagnostics
	fileInfo, err := os.Stat(tempFile)
	if err != nil {
		t.Fatalf("Failed to get file info: %v", err)
	}
	t.Logf("File size: %d bytes", fileInfo.Size())
	
	// Get raw file bytes for inspection
	fileData, err := os.ReadFile(tempFile)
	if err != nil {
		t.Fatalf("Failed to read file data: %v", err)
	}
	
	// Inspect the file header and first block's layout
	if len(fileData) >= 100 {
		t.Logf("File header: % x", fileData[0:64])
		
		// Look at the actual header values
		blockCount := binary.LittleEndian.Uint64(fileData[16:24])
		t.Logf("Header shows block count = %d", blockCount)
		
		// Examine first block header
		const blockHeaderSize = 64
		const blockHeaderStart = 64 // First block starts after file header
		
		t.Logf("First block header: % x", fileData[blockHeaderStart:blockHeaderStart+blockHeaderSize])
		
		// Extract important values from block header
		minID := binary.LittleEndian.Uint64(fileData[blockHeaderStart:blockHeaderStart+8])
		maxID := binary.LittleEndian.Uint64(fileData[blockHeaderStart+8:blockHeaderStart+16])
		count := binary.LittleEndian.Uint32(fileData[blockHeaderStart+40:blockHeaderStart+44])
		encodingType := binary.LittleEndian.Uint32(fileData[blockHeaderStart+44:blockHeaderStart+48])
		
		t.Logf("First block: minID=%d, maxID=%d, count=%d, encoding=%d", 
			minID, maxID, count, encodingType)
		
		// Check block layout
		const layoutOffset = blockHeaderStart + blockHeaderSize
		const layoutSize = 16
		
		if len(fileData) >= int(layoutOffset + layoutSize) {
			layoutBytes := fileData[layoutOffset:layoutOffset+layoutSize]
			t.Logf("Layout section (raw bytes): % x", layoutBytes)
			
			idOffset := binary.LittleEndian.Uint32(layoutBytes[0:4])
			idSize := binary.LittleEndian.Uint32(layoutBytes[4:8])
			valueOffset := binary.LittleEndian.Uint32(layoutBytes[8:12])
			valueSize := binary.LittleEndian.Uint32(layoutBytes[12:16])
			
			t.Logf("First block layout: idOffset=%d, idSize=%d, valueOffset=%d, valueSize=%d",
				idOffset, idSize, valueOffset, valueSize)
			
			// We expect idSize to be count*8 (8 bytes per ID)
			expectedIdSize := count * 8
			if idSize != expectedIdSize {
				t.Logf("WARNING: idSize=%d doesn't match expected size=%d (count=%d * 8)", 
					idSize, expectedIdSize, count)
			}
			
			// Print the first few data bytes
			dataStartIdx := int(layoutOffset + layoutSize)
			dataEndIdx := dataStartIdx + 32
			if dataEndIdx > len(fileData) {
				dataEndIdx = len(fileData)
			}
			
			t.Logf("First bytes of data section (after layout): % x", fileData[dataStartIdx:dataEndIdx])
			
			// Calculate where IDs and values should be
			idStartIdx := dataStartIdx + int(idOffset)
			valueStartIdx := dataStartIdx + int(valueOffset)
			
			// Show first few bytes of ID and value sections
			if idStartIdx+16 <= len(fileData) {
				t.Logf("First few bytes of ID section: % x", fileData[idStartIdx:idStartIdx+16])
			}
			
			if valueStartIdx+16 <= len(fileData) {
				t.Logf("First few bytes of value section: % x", fileData[valueStartIdx:valueStartIdx+16])
			}
		}
	}
	
	// Try to read the file
	reader, err := NewReader(tempFile)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()
	
	// Check file metadata
	t.Logf("File metadata: Version=%d, BlockCount=%d", reader.Version(), reader.BlockCount())
	
	// Try to read each block
	for i := uint64(0); i < reader.BlockCount(); i++ {
		t.Logf("Reading block %d", i)
		readIds, readValues, err := reader.GetPairs(i)
		if err != nil {
			t.Fatalf("Failed to read block %d: %v", i, err)
		}
		
		t.Logf("Block %d: Read %d entries", i, len(readIds))
		
		// Verify data from this block
		startIdx := int(i) * blockSize
		endIdx := startIdx + blockSize
		if endIdx > numEntries {
			endIdx = numEntries
		}
		
		expectedCount := endIdx - startIdx
		if len(readIds) != expectedCount {
			t.Errorf("Block %d: Expected %d entries, got %d", i, expectedCount, len(readIds))
		}
		
		// Print the first few values for debugging
		if len(readIds) > 0 {
			expectedFirstID := ids[startIdx]
			actualFirstID := readIds[0]
			t.Logf("Block %d first entry: Expected ID=%d, got ID=%d", i, expectedFirstID, actualFirstID)
			
			expectedFirstValue := values[startIdx]
			actualFirstValue := readValues[0]
			t.Logf("Block %d first entry: Expected value=%d, got value=%d", i, expectedFirstValue, actualFirstValue)
			
			// Also check if values are consistent internally (even if not matching expected)
			if len(readIds) > 1 {
				delta := int64(readIds[1]) - int64(readIds[0])
				t.Logf("Block %d: Delta between first two IDs = %d", i, delta)
			}
		}
	}
	
	// Try to get aggregation
	agg := reader.Aggregate()
	t.Logf("Aggregation results: Count=%d, Min=%d, Max=%d, Sum=%d, Avg=%.2f",
		agg.Count, agg.Min, agg.Max, agg.Sum, agg.Avg)
		
	// Verify aggregation results
	if agg.Count != numEntries {
		t.Errorf("Aggregation count incorrect: expected %d, got %d", numEntries, agg.Count)
	}
}