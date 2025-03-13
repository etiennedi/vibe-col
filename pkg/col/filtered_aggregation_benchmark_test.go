package col

import (
	"os"
	"testing"

	"github.com/weaviate/sroar"
)

// BenchmarkFilteredAggregation benchmarks the filtered aggregation functionality
func BenchmarkFilteredAggregation(b *testing.B) {
	// Create a test file with 100 blocks, each with 1000 values
	filename := setupBenchmarkFile(b, 100, 1000)
	defer os.Remove(filename)

	// Create a reader
	reader, err := NewReader(filename)
	if err != nil {
		b.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Benchmark cases
	benchmarks := []struct {
		name       string
		filterFunc func() *sroar.Bitmap
	}{
		{
			name: "No filter",
			filterFunc: func() *sroar.Bitmap {
				return nil
			},
		},
		{
			name: "Sparse filter (0.1%)",
			filterFunc: func() *sroar.Bitmap {
				// Create a bitmap with 0.1% of IDs (100 out of 100,000)
				filter := sroar.NewBitmap()
				for i := uint64(0); i < 100; i++ {
					filter.Set(i * 1000) // Every 1000th ID
				}
				return filter
			},
		},
		{
			name: "Medium filter (10%)",
			filterFunc: func() *sroar.Bitmap {
				// Create a bitmap with 10% of IDs (10,000 out of 100,000)
				filter := sroar.NewBitmap()
				for i := uint64(0); i < 10000; i++ {
					filter.Set(i * 10) // Every 10th ID
				}
				return filter
			},
		},
		{
			name: "Dense filter (50%)",
			filterFunc: func() *sroar.Bitmap {
				// Create a bitmap with 50% of IDs (50,000 out of 100,000)
				filter := sroar.NewBitmap()
				for i := uint64(0); i < 50000; i++ {
					filter.Set(i * 2) // Every 2nd ID
				}
				return filter
			},
		},
		{
			name: "Single block filter",
			filterFunc: func() *sroar.Bitmap {
				// Create a bitmap that only matches IDs in the first block
				filter := sroar.NewBitmap()
				for i := uint64(0); i < 1000; i++ {
					filter.Set(i)
				}
				return filter
			},
		},
		{
			name: "Range filter",
			filterFunc: func() *sroar.Bitmap {
				// Create a bitmap with a range of IDs (25% of total)
				filter := sroar.NewBitmap()
				for i := uint64(25000); i < 50000; i++ {
					filter.Set(i)
				}
				return filter
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			filter := bm.filterFunc()

			// Reset the timer before the benchmark loop
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				opts := AggregateOptions{
					SkipPreCalculated: true, // Force reading all blocks to benchmark filtering
					Filter:            filter,
				}

				result := reader.AggregateWithOptions(opts)

				// Prevent the compiler from optimizing away the result
				if result.Count < 0 {
					b.Fatalf("Unexpected result")
				}
			}
		})

		// Also benchmark with cached values
		b.Run(bm.name+" (cached)", func(b *testing.B) {
			filter := bm.filterFunc()

			// Reset the timer before the benchmark loop
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				opts := AggregateOptions{
					SkipPreCalculated: false, // Use cached values
					Filter:            filter,
				}

				result := reader.AggregateWithOptions(opts)

				// Prevent the compiler from optimizing away the result
				if result.Count < 0 {
					b.Fatalf("Unexpected result")
				}
			}
		})
	}
}

// setupBenchmarkFile creates a test file with the specified number of blocks and values per block
func setupBenchmarkFile(b *testing.B, numBlocks, valuesPerBlock int) string {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "benchmark-filtered-*.col")
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

	// Write blocks
	for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
		// Create IDs and values
		ids := make([]uint64, valuesPerBlock)
		values := make([]int64, valuesPerBlock)

		for i := 0; i < valuesPerBlock; i++ {
			ids[i] = uint64(blockIdx*valuesPerBlock + i)
			values[i] = int64(ids[i] * 2) // Simple formula for values
		}

		// Write the block
		if err := writer.WriteBlock(ids, values); err != nil {
			b.Fatalf("Failed to write block %d: %v", blockIdx, err)
		}
	}

	// Finalize the file
	if err := writer.FinalizeAndClose(); err != nil {
		b.Fatalf("Failed to finalize file: %v", err)
	}

	return filename
}
