package col

import (
	"os"
	"strconv"
	"testing"
)

// BenchmarkWriteBlock tests the performance of the WriteBlock method
func BenchmarkWriteBlock(b *testing.B) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "benchmark-write-*.col")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Test with different batch sizes
	batchSizes := []int{1000, 10000, 100000}

	for _, batchSize := range batchSizes {
		b.Run(formatBatchSize(batchSize), func(b *testing.B) {
			// Generate test data
			ids := make([]uint64, batchSize)
			values := make([]int64, batchSize)

			for i := 0; i < batchSize; i++ {
				ids[i] = uint64(i + 1)
				values[i] = int64(i + 1)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Create a new writer for each iteration to avoid accumulating data
				writer, err := NewWriter(tempFile.Name())
				if err != nil {
					b.Fatalf("Failed to create writer: %v", err)
				}

				// Set a larger block size for the benchmark
				writer.blockSizeTarget = 1024 * 1024 // 1MB

				b.StartTimer()

				// Write data in batches if needed
				remaining := batchSize
				offset := 0

				for remaining > 0 {
					// Try to write the current batch
					err = writer.WriteBlock(ids[offset:offset+remaining], values[offset:offset+remaining])

					if err == nil {
						// All data written successfully
						break
					}

					// Check if we got a BlockFullError
					if blockFullErr, ok := err.(*BlockFullError); ok {
						if blockFullErr.ItemsWritten > 0 {
							// Some items were written, update our state
							offset += blockFullErr.ItemsWritten
							remaining -= blockFullErr.ItemsWritten
						} else {
							// No items were written, try with a smaller batch
							batchToWrite := remaining / 2
							if batchToWrite == 0 {
								b.Fatalf("Failed to write even a single item")
							}

							err = writer.WriteBlock(ids[offset:offset+batchToWrite], values[offset:offset+batchToWrite])
							if err != nil {
								b.Fatalf("Failed to write smaller batch: %v", err)
							}

							offset += batchToWrite
							remaining -= batchToWrite
						}
					} else {
						// Some other error occurred
						b.Fatalf("Failed to write block: %v", err)
					}
				}

				b.StopTimer()
				writer.FinalizeAndClose()
			}
		})
	}
}

// BenchmarkSimpleWriter tests the performance of the SimpleWriter
func BenchmarkBufferedWriterForBatch(b *testing.B) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "benchmark-buffered-batch-*.col")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Test with different batch sizes
	batchSizes := []int{1000, 10000, 100000}

	for _, batchSize := range batchSizes {
		b.Run(formatBatchSize(batchSize), func(b *testing.B) {
			// Generate test data
			ids := make([]uint64, batchSize)
			values := make([]int64, batchSize)

			for i := 0; i < batchSize; i++ {
				ids[i] = uint64(i + 1)
				values[i] = int64(i + 1)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Create a new writer for each iteration
				writer, err := NewBufferedWriter(tempFile.Name(), WithBufferedBlockSize(1024*1024)) // 1MB
				if err != nil {
					b.Fatalf("Failed to create writer: %v", err)
				}

				b.StartTimer()

				// Measure the time to write the data
				err = writer.BatchAdd(ids, values)

				b.StopTimer()
				writer.Close()
				if err != nil {
					b.Fatalf("Failed to write data: %v", err)
				}
			}
		})
	}
}

// Helper function to format batch sizes for benchmark names
func formatBatchSize(size int) string {
	if size >= 1000000 {
		return strconv.Itoa(size/1000000) + "M"
	} else if size >= 1000 {
		return strconv.Itoa(size/1000) + "K"
	}
	return strconv.Itoa(size)
}
