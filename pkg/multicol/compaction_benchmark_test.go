package multicol

import (
	"fmt"
	"os"
	"testing"
	"time"
	"vibe-lsm/pkg/col"
)

// BenchmarkCompaction benchmarks the compaction process
func BenchmarkCompaction(b *testing.B) {
	benchSizes := []struct {
		name      string
		leftSize  int
		rightSize int
	}{
		{"Small_10K", 10_000, 10_000},
		{"Medium_100K", 100_000, 100_000},
		{"Large_1M", 1_000_000, 1_000_000},
	}

	for _, size := range benchSizes {
		b.Run(size.name, func(b *testing.B) {
			// Skip larger benchmarks in short mode
			if testing.Short() && (size.leftSize > 100_000 || size.rightSize > 100_000) {
				b.Skip("Skipping large benchmark in short mode")
			}

			// Create test files only once for all benchmark iterations
			leftFilePath, err := prepareSegmentFile("bench_left", size.leftSize, true, col.EncodingVarIntBoth)
			if err != nil {
				b.Fatalf("Failed to create left segment: %v", err)
			}
			defer os.Remove(leftFilePath)

			rightFilePath, err := prepareSegmentFile("bench_right", size.rightSize, false, col.EncodingVarIntBoth)
			if err != nil {
				b.Fatalf("Failed to create right segment: %v", err)
			}
			defer os.Remove(rightFilePath)

			// Benchmark the actual compaction operation
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a temporary output file for each iteration
				outputFile, err := os.CreateTemp("", "bench_compaction_*.col")
				if err != nil {
					b.Fatalf("Failed to create output file: %v", err)
				}
				outputPath := outputFile.Name()
				outputFile.Close()

				// Open input files
				leftReader, err := col.NewReader(leftFilePath)
				if err != nil {
					os.Remove(outputPath)
					b.Fatalf("Failed to open left reader: %v", err)
				}

				rightReader, err := col.NewReader(rightFilePath)
				if err != nil {
					leftReader.Close()
					os.Remove(outputPath)
					b.Fatalf("Failed to open right reader: %v", err)
				}

				// Configure compaction options
				opts := DefaultCompactionOptions()
				opts.EncodingType = col.EncodingVarIntBoth

				// Run the compaction
				startTime := time.Now()
				err = Compact(leftReader, rightReader, outputPath, opts)
				if err != nil {
					leftReader.Close()
					rightReader.Close()
					os.Remove(outputPath)
					b.Fatalf("Compaction failed: %v", err)
				}

				compactionTime := time.Since(startTime)
				b.ReportMetric(float64(size.leftSize+size.rightSize)/compactionTime.Seconds(), "entries/sec")

				// Clean up
				leftReader.Close()
				rightReader.Close()
				os.Remove(outputPath)
			}
		})
	}
}

// BenchmarkCompactionComparison benchmarks and compares both compaction implementations
func BenchmarkCompactionComparison(b *testing.B) {
	benchSizes := []struct {
		name      string
		leftSize  int
		rightSize int
	}{
		{"Small_10K", 10_000, 10_000},
		{"Medium_100K", 100_000, 100_000},
	}

	for _, size := range benchSizes {
		// Create test files once for both implementations
		leftFilePath, err := prepareSegmentFile("comp_left", size.leftSize, true, col.EncodingVarIntBoth)
		if err != nil {
			b.Fatalf("Failed to create left segment: %v", err)
		}
		defer os.Remove(leftFilePath)

		rightFilePath, err := prepareSegmentFile("comp_right", size.rightSize, false, col.EncodingVarIntBoth)
		if err != nil {
			b.Fatalf("Failed to create right segment: %v", err)
		}
		defer os.Remove(rightFilePath)

		// Create the comparison benchmarks
		b.Run(fmt.Sprintf("%s/Old_With_Batches", size.name), func(b *testing.B) {
			benchmarkImplementation(b, leftFilePath, rightFilePath, size.leftSize+size.rightSize, true)
		})

		b.Run(fmt.Sprintf("%s/New_No_Batches", size.name), func(b *testing.B) {
			benchmarkImplementation(b, leftFilePath, rightFilePath, size.leftSize+size.rightSize, false)
		})
	}
}

// Helper function to benchmark a specific implementation
func benchmarkImplementation(b *testing.B, leftPath, rightPath string, totalEntries int, useBatches bool) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a temporary output file
		outputFile, err := os.CreateTemp("", "bench_comp_impl_*.col")
		if err != nil {
			b.Fatalf("Failed to create output file: %v", err)
		}
		outputPath := outputFile.Name()
		outputFile.Close()

		// Open input files
		leftReader, err := col.NewReader(leftPath)
		if err != nil {
			os.Remove(outputPath)
			b.Fatalf("Failed to open left reader: %v", err)
		}

		rightReader, err := col.NewReader(rightPath)
		if err != nil {
			leftReader.Close()
			os.Remove(outputPath)
			b.Fatalf("Failed to open right reader: %v", err)
		}

		// Configure compaction options
		opts := DefaultCompactionOptions()
		opts.EncodingType = col.EncodingVarIntBoth

		// Run the appropriate implementation
		startTime := time.Now()
		if useBatches {
			err = CompactWithBatches(leftReader, rightReader, outputPath, opts)
		} else {
			err = Compact(leftReader, rightReader, outputPath, opts)
		}

		if err != nil {
			leftReader.Close()
			rightReader.Close()
			os.Remove(outputPath)
			b.Fatalf("Compaction failed: %v", err)
		}

		compactionTime := time.Since(startTime)
		b.ReportMetric(float64(totalEntries)/compactionTime.Seconds(), "entries/sec")

		// Clean up
		leftReader.Close()
		rightReader.Close()
		os.Remove(outputPath)
	}
}

// Helper function to prepare a segment file for benchmarking
func prepareSegmentFile(namePrefix string, numEntries int, isLeft bool, encodingType uint32) (string, error) {
	file, err := os.CreateTemp("", fmt.Sprintf("%s_*.col", namePrefix))
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	filePath := file.Name()
	file.Close()

	// Create writer
	writer, err := col.NewBufferedWriter(filePath, col.WithBufferedEncoding(encodingType))
	if err != nil {
		os.Remove(filePath)
		return "", fmt.Errorf("failed to create writer: %w", err)
	}

	// Use a large batch size for efficiency
	const batchSize = 100_000
	multiplier := int64(10)
	if !isLeft {
		multiplier = 11 // Different multiplier for right segments
	}

	// Write data in batches
	for offset := 0; offset < numEntries; offset += batchSize {
		currentBatchSize := min(batchSize, numEntries-offset)
		ids := make([]uint64, currentBatchSize)
		values := make([]int64, currentBatchSize)

		for i := 0; i < currentBatchSize; i++ {
			id := uint64(offset + i + 1)

			// For right segments, make half the entries unique
			if !isLeft && i%2 == 0 {
				id += uint64(numEntries)
			}

			ids[i] = id
			values[i] = int64(id) * multiplier
		}

		// Write the batch
		if err := writer.BatchAdd(ids, values); err != nil {
			writer.Close()
			os.Remove(filePath)
			return "", fmt.Errorf("failed to write batch: %w", err)
		}
	}

	// Close the writer
	if err := writer.Close(); err != nil {
		os.Remove(filePath)
		return "", fmt.Errorf("failed to close writer: %w", err)
	}

	return filePath, nil
}
