package col

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkGlobalIDBitmap(b *testing.B) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "global_id_bitmap_benchmark_*.col")
	require.NoError(b, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create a writer
	writer, err := NewWriter(tmpFile.Name())
	require.NoError(b, err)

	// Generate test data with 100,000 IDs
	const numIDs = 100000
	ids := make([]uint64, numIDs)
	values := make([]int64, numIDs)
	for i := 0; i < numIDs; i++ {
		ids[i] = uint64(i)
		values[i] = int64(i * 10)
	}

	// Write the data in chunks of 1000 IDs
	const chunkSize = 1000
	for i := 0; i < numIDs; i += chunkSize {
		end := i + chunkSize
		if end > numIDs {
			end = numIDs
		}
		err = writer.WriteBlock(ids[i:end], values[i:end])
		require.NoError(b, err)
	}

	// Reset the timer before measuring the finalization
	b.ResetTimer()

	// Benchmark the finalization, which includes writing the global ID bitmap
	for i := 0; i < b.N; i++ {
		// We can't actually finalize multiple times, so we'll just measure the bitmap serialization
		_, _, err := writer.writeGlobalIDBitmap()
		require.NoError(b, err)
	}
}

func BenchmarkGlobalIDBitmapWithLargeIDs(b *testing.B) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "global_id_bitmap_large_benchmark_*.col")
	require.NoError(b, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create a writer
	writer, err := NewWriter(tmpFile.Name())
	require.NoError(b, err)

	// Generate test data with 100,000 IDs with large values
	const numIDs = 100000
	ids := make([]uint64, numIDs)
	values := make([]int64, numIDs)
	for i := 0; i < numIDs; i++ {
		ids[i] = uint64(i * 1000000) // Large IDs
		values[i] = int64(i * 10)
	}

	// Write the data in chunks of 1000 IDs
	const chunkSize = 1000
	for i := 0; i < numIDs; i += chunkSize {
		end := i + chunkSize
		if end > numIDs {
			end = numIDs
		}
		err = writer.WriteBlock(ids[i:end], values[i:end])
		require.NoError(b, err)
	}

	// Reset the timer before measuring the finalization
	b.ResetTimer()

	// Benchmark the finalization, which includes writing the global ID bitmap
	for i := 0; i < b.N; i++ {
		// We can't actually finalize multiple times, so we'll just measure the bitmap serialization
		_, _, err := writer.writeGlobalIDBitmap()
		require.NoError(b, err)
	}
}
