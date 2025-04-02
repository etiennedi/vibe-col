package col

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkBufferedWriter(b *testing.B) {
	// Create temp directory for benchmarks
	tempDir := b.TempDir()

	// Generate test data with 1,000 ID-value pairs (smaller to avoid block limits)
	numEntries := 1000
	ids := make([]uint64, numEntries)
	values := make([]int64, numEntries)

	for i := 0; i < numEntries; i++ {
		ids[i] = uint64(i + 1)
		values[i] = int64(i * 10)
	}

	// Use a large block size to accommodate all entries
	blockSize := uint32(1024 * 1024) // 1MB

	b.Run("Add1K", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Reset the timer while we set up
			b.StopTimer()
			filename := filepath.Join(tempDir, "buffered_add_benchmark.col")
			writer, err := NewBufferedWriter(filename, WithBufferedBlockSize(blockSize))
			if err != nil {
				b.Fatalf("Failed to create writer: %v", err)
			}

			// Start timing
			b.StartTimer()

			// Add all entries
			for j := 0; j < numEntries; j++ {
				if err := writer.Add(ids[j], values[j]); err != nil {
					b.Fatalf("Failed to add entry: %v", err)
				}
			}

			// Stop timing for cleanup
			b.StopTimer()

			// Clean up
			writer.Close()
			os.Remove(filename)
		}
	})

	b.Run("StandardWriteBlock1K", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Reset the timer while we set up
			b.StopTimer()
			filename := filepath.Join(tempDir, "standard_block_benchmark.col")
			writer, err := NewWriter(filename, WithBlockSize(blockSize))
			if err != nil {
				b.Fatalf("Failed to create writer: %v", err)
			}

			// Start timing
			b.StartTimer()

			// Write a single block with all entries
			if err := writer.WriteBlock(ids, values); err != nil {
				b.Fatalf("Failed to write block: %v", err)
			}

			// Finalize and close
			if err := writer.FinalizeAndClose(); err != nil {
				b.Fatalf("Failed to finalize and close: %v", err)
			}

			// Stop timing for cleanup
			b.StopTimer()

			// Clean up
			os.Remove(filename)
		}
	})

	b.Run("BufferedWriteBlock1K", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Reset the timer while we set up
			b.StopTimer()
			filename := filepath.Join(tempDir, "buffered_block_benchmark.col")

			// Create new BlockData from IDs and values
			blockData, _ := prepareTestBlockData(ids, values, EncodingRaw)

			writer, err := NewBufferedWriter(filename, WithBufferedBlockSize(blockSize))
			if err != nil {
				b.Fatalf("Failed to create writer: %v", err)
			}

			// Start timing
			b.StartTimer()

			// Write block
			if err := writer.WriteBlock(blockData); err != nil {
				b.Fatalf("Failed to write block: %v", err)
			}

			// Close
			if err := writer.Close(); err != nil {
				b.Fatalf("Failed to close: %v", err)
			}

			// Stop timing for cleanup
			b.StopTimer()

			// Clean up
			os.Remove(filename)
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
