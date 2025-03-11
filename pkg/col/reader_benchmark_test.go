package col

import (
	"os"
	"testing"
)

// setupTestFile creates a test file with multiple blocks for benchmarking
func setupTestFile(b *testing.B, numBlocks int) string {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "benchmark-*.col")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	filename := tmpFile.Name()

	// Create a writer
	writer, err := NewWriter(filename)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}

	// Write multiple blocks
	for i := 0; i < numBlocks; i++ {
		// Create IDs and values
		ids := make([]uint64, 1000)
		values := make([]int64, 1000)
		for j := 0; j < 1000; j++ {
			ids[j] = uint64(i*1000 + j)
			values[j] = int64(i*1000 + j)
		}

		// Write the block
		if err := writer.WriteBlock(ids, values); err != nil {
			b.Fatalf("Failed to write block: %v", err)
		}
	}

	// Finalize the file
	if err := writer.FinalizeAndClose(); err != nil {
		b.Fatalf("Failed to finalize file: %v", err)
	}

	return filename
}

// BenchmarkReaderGetPairs benchmarks the GetPairs method
func BenchmarkReaderGetPairs(b *testing.B) {
	// Create a test file with 10 blocks
	filename := setupTestFile(b, 10)
	defer os.Remove(filename)

	// Create a reader
	reader, err := NewReader(filename)
	if err != nil {
		b.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Reset the timer
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Read each block
		for j := 0; j < 10; j++ {
			ids, values, err := reader.GetPairs(uint64(j))
			if err != nil {
				b.Fatalf("Failed to read block: %v", err)
			}
			if len(ids) != 1000 || len(values) != 1000 {
				b.Fatalf("Unexpected number of pairs: %d, %d", len(ids), len(values))
			}
		}
	}
}

// BenchmarkReaderReadAllBlocks benchmarks reading all blocks sequentially
func BenchmarkReaderReadAllBlocks(b *testing.B) {
	// Create a test file with 100 blocks
	filename := setupTestFile(b, 100)
	defer os.Remove(filename)

	// Reset the timer
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create a reader
		reader, err := NewReader(filename)
		if err != nil {
			b.Fatalf("Failed to create reader: %v", err)
		}

		// Read all blocks
		blockCount := reader.BlockCount()
		for j := uint64(0); j < blockCount; j++ {
			_, _, err := reader.GetPairs(j)
			if err != nil {
				b.Fatalf("Failed to read block: %v", err)
			}
		}

		reader.Close()
	}
}
