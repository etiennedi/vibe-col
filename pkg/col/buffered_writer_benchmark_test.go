package col

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

// BenchmarkBufferedWriter benchmarks the performance of the buffered writer
// with different block sizes and batch operations.
func BenchmarkBufferedWriter(b *testing.B) {
	// Define block sizes to benchmark
	blockSizes := []int{4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024} // 4KB to 64KB

	// Define batch sizes to test
	batchSizes := []int{1, 10, 100, 1000}

	// Create test data once
	const maxEntries = 100000
	var ids []uint64
	var values []int64

	for i := 0; i < maxEntries; i++ {
		ids = append(ids, uint64(i+1))
		values = append(values, int64(i*10))
	}

	for _, blockSize := range blockSizes {
		for _, batchSize := range batchSizes {
			b.Run(
				fmt.Sprintf("BlockSize_%dKB_Batch_%d", blockSize/1024, batchSize),
				func(b *testing.B) {
					// Limit the number of entries based on batch size to keep benchmark time reasonable
					entriesPerRun := maxEntries
					if batchSize == 1 && b.N > 1 {
						entriesPerRun = 10000 // Reduce entries for single-add to avoid excessive benchmark time
					}

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						// Create a temporary file
						f, err := os.CreateTemp("", "bench-bufferedwriter-*.col")
						if err != nil {
							b.Fatalf("Failed to create temp file: %v", err)
						}
						tempFilePath := f.Name()
						f.Close() // Close immediately as the writer will open it

						// Create a buffered writer
						bufferedWriter, err := NewBufferedWriter(tempFilePath, WithBufferedBlockSize(uint32(blockSize)))
						if err != nil {
							os.Remove(tempFilePath)
							b.Fatalf("Failed to create buffered writer: %v", err)
						}
						b.StartTimer()

						// With batch size 1, we're testing the normal Add() performance
						if batchSize == 1 {
							for j := 0; j < entriesPerRun; j++ {
								if err := bufferedWriter.Add(ids[j], values[j]); err != nil {
									b.Fatalf("Failed to add entry: %v", err)
								}
							}
						} else {
							// With larger batch sizes, use BatchAdd to add entries in batches
							for j := 0; j < entriesPerRun; j += batchSize {
								end := j + batchSize
								if end > entriesPerRun {
									end = entriesPerRun
								}

								// Use the new BatchAdd method
								if err := bufferedWriter.BatchAdd(ids[j:end], values[j:end]); err != nil {
									b.Fatalf("Failed to batch add entries: %v", err)
								}
							}
						}

						// Don't count closing time in the benchmark
						b.StopTimer()
						err = bufferedWriter.Close()
						if err != nil {
							os.Remove(tempFilePath)
							b.Fatalf("Failed to close writer: %v", err)
						}
						os.Remove(tempFilePath)
					}
				},
			)
		}
	}
}

// BenchmarkBufferedWriterVsStandard compares the performance of BufferedWriter with the standard Writer
func BenchmarkBufferedWriterVsStandard(b *testing.B) {
	// Create test data
	const numEntries = 100000
	var ids []uint64
	var values []int64

	for i := 0; i < numEntries; i++ {
		ids = append(ids, uint64(i+1))
		values = append(values, int64(i*10))
	}

	b.Run("StandardWriter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Create a temporary file
			f, err := os.CreateTemp("", "bench-standard-*.col")
			if err != nil {
				b.Fatalf("Failed to create temp file: %v", err)
			}
			tempFilePath := f.Name()
			f.Close() // Close immediately as the writer will open it

			// Create a standard writer with a large block size to hold all entries
			standardWriter, err := NewWriter(tempFilePath, WithBlockSize(2*1024*1024)) // 2MB block size
			if err != nil {
				os.Remove(tempFilePath)
				b.Fatalf("Failed to create standard writer: %v", err)
			}
			b.StartTimer()

			// Write all entries at once (as that's how standard writer works)
			err = standardWriter.WriteBlock(ids, values)
			if err != nil {
				b.Fatalf("Failed to write block: %v", err)
			}

			b.StopTimer()
			err = standardWriter.FinalizeAndClose()
			if err != nil {
				os.Remove(tempFilePath)
				b.Fatalf("Failed to close writer: %v", err)
			}
			os.Remove(tempFilePath)
		}
	})

	b.Run("BufferedWriter_SingleAdd", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Create a temporary file
			f, err := os.CreateTemp("", "bench-buffered-*.col")
			if err != nil {
				b.Fatalf("Failed to create temp file: %v", err)
			}
			tempFilePath := f.Name()
			f.Close() // Close immediately as the writer will open it

			// Create a buffered writer with 16KB blocks
			bufferedWriter, err := NewBufferedWriter(tempFilePath, WithBufferedBlockSize(16*1024))
			if err != nil {
				os.Remove(tempFilePath)
				b.Fatalf("Failed to create buffered writer: %v", err)
			}
			b.StartTimer()

			// Add each entry individually (the old inefficient way)
			for j := 0; j < numEntries; j++ {
				err = bufferedWriter.Add(ids[j], values[j])
				if err != nil {
					b.Fatalf("Failed to add entry: %v", err)
				}
			}

			b.StopTimer()
			err = bufferedWriter.Close()
			if err != nil {
				os.Remove(tempFilePath)
				b.Fatalf("Failed to close writer: %v", err)
			}
			os.Remove(tempFilePath)
		}
	})

	b.Run("BufferedWriter_BatchAdd", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Create a temporary file
			f, err := os.CreateTemp("", "bench-buffered-*.col")
			if err != nil {
				b.Fatalf("Failed to create temp file: %v", err)
			}
			tempFilePath := f.Name()
			f.Close() // Close immediately as the writer will open it

			// Create a buffered writer with 16KB blocks
			bufferedWriter, err := NewBufferedWriter(tempFilePath, WithBufferedBlockSize(16*1024))
			if err != nil {
				os.Remove(tempFilePath)
				b.Fatalf("Failed to create buffered writer: %v", err)
			}
			b.StartTimer()

			// Use the new BatchAdd method for all entries
			err = bufferedWriter.BatchAdd(ids, values)
			if err != nil {
				b.Fatalf("Failed to batch add entries: %v", err)
			}

			b.StopTimer()
			err = bufferedWriter.Close()
			if err != nil {
				os.Remove(tempFilePath)
				b.Fatalf("Failed to close writer: %v", err)
			}
			os.Remove(tempFilePath)
		}
	})
}

// Helper function to prepare BlockData for testing
func prepareTestBlockData(ids []uint64, values []int64, encodingType uint32) (*BlockData, error) {
	// Calculate min/max/sum
	minID, maxID := calculateMinMaxUint64(ids)
	minValue, maxValue := calculateMinMaxInt64(values)
	sum := calculateSumInt64(values)
	count := uint32(len(ids))

	// For simplicity, use raw encoding (no delta or varint)
	idSection := make([]byte, len(ids)*8)
	valueSection := make([]byte, len(values)*8)

	// Serialize IDs and values using proper binary encoding
	for i, id := range ids {
		offset := i * 8
		binary.LittleEndian.PutUint64(idSection[offset:offset+8], id)

		// Value
		value := values[i]
		valueU64 := int64ToUint64(value)
		binary.LittleEndian.PutUint64(valueSection[offset:offset+8], valueU64)
	}

	return &BlockData{
		MinID:                  minID,
		MaxID:                  maxID,
		MinValue:               minValue,
		MaxValue:               maxValue,
		Sum:                    sum,
		Count:                  count,
		IDSectionSize:          uint32(len(idSection)),
		ValueSectionSize:       uint32(len(valueSection)),
		SerializedIDSection:    idSection,
		SerializedValueSection: valueSection,
	}, nil
}
