package col

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// TestParallelAggregation tests that parallel aggregation produces the same results as sequential aggregation
func TestParallelAggregation(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-parallel-agg-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file with multiple blocks
	filePath := filepath.Join(tempDir, "parallel_test.col")

	// Create a writer with a smaller block size to ensure multiple blocks
	writer, err := NewWriter(filePath, WithBlockSize(32*1024)) // 32KB blocks
	require.NoError(t, err)

	// Generate test data with 100,000 items
	const numItems = 100000
	ids := make([]uint64, numItems)
	values := make([]int64, numItems)

	// Use a fixed seed for reproducibility
	r := rand.New(rand.NewSource(42))

	// Generate random IDs and values
	for i := 0; i < numItems; i++ {
		ids[i] = uint64(i)
		values[i] = int64(r.Intn(1000000)) // Random values between 0 and 999,999
	}

	// Write data in smaller batches to create multiple blocks
	batchSize := 10000
	for i := 0; i < numItems; i += batchSize {
		end := i + batchSize
		if end > numItems {
			end = numItems
		}

		// Write the batch, handling BlockFullError if needed
		remainingIDs := ids[i:end]
		remainingValues := values[i:end]

		for len(remainingIDs) > 0 {
			err := writer.WriteBlock(remainingIDs, remainingValues)
			if blockFullErr, ok := err.(*BlockFullError); ok {
				// Some items were written, continue with the rest
				itemsWritten := blockFullErr.ItemsWritten
				remainingIDs = remainingIDs[itemsWritten:]
				remainingValues = remainingValues[itemsWritten:]
			} else if err != nil {
				require.NoError(t, err, "Failed to write block")
				break
			} else {
				// All items were written
				break
			}
		}
	}

	// Finalize and close the writer
	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(filePath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify we have multiple blocks
	blockCount := reader.BlockCount()
	t.Logf("Created file with %d blocks", blockCount)
	require.Greater(t, blockCount, uint64(1), "Test requires multiple blocks")

	// Run sequential aggregation as baseline
	seqResult := reader.Aggregate()

	// Test with different parallelization factors
	parallelFactors := []int{
		2,                     // 2 workers
		4,                     // 4 workers
		runtime.GOMAXPROCS(0), // GOMAXPROCS workers
		-1,                    // Auto (GOMAXPROCS)
	}

	for _, parallel := range parallelFactors {
		t.Run(fmt.Sprintf("Parallel=%d", parallel), func(t *testing.T) {
			opts := AggregateOptions{
				Parallel: parallel,
			}

			// Run parallel aggregation
			parallelResult := reader.AggregateWithOptions(opts)

			// Verify results match sequential aggregation
			assert.Equal(t, seqResult.Count, parallelResult.Count, "Count should match")
			assert.Equal(t, seqResult.Min, parallelResult.Min, "Min should match")
			assert.Equal(t, seqResult.Max, parallelResult.Max, "Max should match")
			assert.Equal(t, seqResult.Sum, parallelResult.Sum, "Sum should match")
			assert.InDelta(t, seqResult.Avg, parallelResult.Avg, 0.0001, "Avg should match")
		})
	}
}

// BenchmarkParallelAggregation benchmarks different aggregation methods
func BenchmarkParallelAggregation(b *testing.B) {
	// Create a temporary directory for the benchmark
	tempDir, err := os.MkdirTemp("", "col-parallel-agg-bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test files with different numbers of blocks
	fileSizes := []struct {
		name      string
		numItems  int
		blockSize uint32
	}{
		{"Small_10Blocks", 100000, 32 * 1024},   // ~10 blocks
		{"Medium_50Blocks", 500000, 32 * 1024},  // ~50 blocks
		{"Large_100Blocks", 1000000, 32 * 1024}, // ~100 blocks
	}

	for _, fileSize := range fileSizes {
		// Create the test file
		filePath := filepath.Join(tempDir, fileSize.name+".col")

		// Create a writer with the specified block size
		writer, err := NewWriter(filePath, WithBlockSize(fileSize.blockSize))
		if err != nil {
			b.Fatalf("Failed to create writer: %v", err)
		}

		// Generate test data
		ids := make([]uint64, fileSize.numItems)
		values := make([]int64, fileSize.numItems)

		// Use a fixed seed for reproducibility
		r := rand.New(rand.NewSource(42))

		// Generate sequential IDs and random values
		for i := 0; i < fileSize.numItems; i++ {
			ids[i] = uint64(i)
			values[i] = int64(r.Intn(1000000)) // Random values between 0 and 999,999
		}

		// Write data in smaller batches to create multiple blocks
		batchSize := 10000
		for i := 0; i < fileSize.numItems; i += batchSize {
			end := i + batchSize
			if end > fileSize.numItems {
				end = fileSize.numItems
			}

			// Write the batch, handling BlockFullError if needed
			remainingIDs := ids[i:end]
			remainingValues := values[i:end]

			for len(remainingIDs) > 0 {
				err := writer.WriteBlock(remainingIDs, remainingValues)
				if blockFullErr, ok := err.(*BlockFullError); ok {
					// Some items were written, continue with the rest
					itemsWritten := blockFullErr.ItemsWritten
					remainingIDs = remainingIDs[itemsWritten:]
					remainingValues = remainingValues[itemsWritten:]
				} else if err != nil {
					b.Fatalf("Failed to write block: %v", err)
					break
				} else {
					// All items were written
					break
				}
			}
		}

		// Finalize and close the writer
		err = writer.FinalizeAndClose()
		if err != nil {
			b.Fatalf("Failed to finalize file: %v", err)
		}

		// Open the file for reading
		reader, err := NewReader(filePath)
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}
		defer reader.Close()

		// Log the actual number of blocks created
		blockCount := reader.BlockCount()
		b.Logf("%s: Created file with %d blocks", fileSize.name, blockCount)

		// Benchmark sequential aggregation
		b.Run(fmt.Sprintf("%s_Sequential", fileSize.name), func(b *testing.B) {
			opts := AggregateOptions{
				SkipPreCalculated: true, // Force reading all values
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = reader.AggregateWithOptions(opts)
			}
		})

		// Benchmark parallel aggregation with different worker counts
		parallelFactors := []int{2, 4, runtime.GOMAXPROCS(0), -1}
		for _, parallel := range parallelFactors {
			name := "Auto"
			if parallel > 0 {
				name = fmt.Sprintf("%d", parallel)
			}

			b.Run(fmt.Sprintf("%s_Parallel_%s", fileSize.name, name), func(b *testing.B) {
				opts := AggregateOptions{
					Parallel:          parallel,
					SkipPreCalculated: true, // Force reading all values
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = reader.AggregateWithOptions(opts)
				}
			})
		}
	}
}

// TestParallelAggregationWithFilter tests that parallel aggregation works correctly with filters
func TestParallelAggregationWithFilter(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-parallel-filter-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file with multiple blocks
	filePath := filepath.Join(tempDir, "parallel_filter_test.col")

	// Create a writer with a smaller block size to ensure multiple blocks
	writer, err := NewWriter(filePath, WithBlockSize(32*1024)) // 32KB blocks
	require.NoError(t, err)

	// Generate test data with 100,000 items
	const numItems = 100000
	ids := make([]uint64, numItems)
	values := make([]int64, numItems)

	// Use a fixed seed for reproducibility
	r := rand.New(rand.NewSource(42))

	// Generate sequential IDs and random values
	for i := 0; i < numItems; i++ {
		ids[i] = uint64(i)
		values[i] = int64(r.Intn(1000000)) // Random values between 0 and 999,999
	}

	// Write data in smaller batches to create multiple blocks
	batchSize := 10000
	for i := 0; i < numItems; i += batchSize {
		end := i + batchSize
		if end > numItems {
			end = numItems
		}

		// Write the batch, handling BlockFullError if needed
		remainingIDs := ids[i:end]
		remainingValues := values[i:end]

		for len(remainingIDs) > 0 {
			err := writer.WriteBlock(remainingIDs, remainingValues)
			if blockFullErr, ok := err.(*BlockFullError); ok {
				// Some items were written, continue with the rest
				itemsWritten := blockFullErr.ItemsWritten
				remainingIDs = remainingIDs[itemsWritten:]
				remainingValues = remainingValues[itemsWritten:]
			} else if err != nil {
				require.NoError(t, err, "Failed to write block")
				break
			} else {
				// All items were written
				break
			}
		}
	}

	// Finalize and close the writer
	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(filePath)
	require.NoError(t, err)
	defer reader.Close()

	// Create a filter that includes only even IDs
	filter := NewBitmap()
	for i := uint64(0); i < uint64(numItems); i += 2 {
		filter.Set(i)
	}

	// Run sequential filtered aggregation as baseline
	seqOpts := AggregateOptions{
		Filter: filter,
	}
	seqResult := reader.AggregateWithOptions(seqOpts)

	// Test with different parallelization factors
	parallelFactors := []int{
		2,                     // 2 workers
		4,                     // 4 workers
		runtime.GOMAXPROCS(0), // GOMAXPROCS workers
		-1,                    // Auto (GOMAXPROCS)
	}

	for _, parallel := range parallelFactors {
		t.Run(fmt.Sprintf("FilteredParallel=%d", parallel), func(t *testing.T) {
			opts := AggregateOptions{
				Filter:   filter,
				Parallel: parallel,
			}

			// Run parallel filtered aggregation
			parallelResult := reader.AggregateWithOptions(opts)

			// Verify results match sequential filtered aggregation
			assert.Equal(t, seqResult.Count, parallelResult.Count, "Count should match")
			assert.Equal(t, seqResult.Min, parallelResult.Min, "Min should match")
			assert.Equal(t, seqResult.Max, parallelResult.Max, "Max should match")
			assert.Equal(t, seqResult.Sum, parallelResult.Sum, "Sum should match")
			assert.InDelta(t, seqResult.Avg, parallelResult.Avg, 0.0001, "Avg should match")
		})
	}
}

// NewBitmap creates a new bitmap for testing
func NewBitmap() *sroar.Bitmap {
	return sroar.NewBitmap()
}
