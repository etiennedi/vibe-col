package col

import (
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