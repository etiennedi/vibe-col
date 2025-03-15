package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"vibe-lsm/pkg/col"
)

const (
	defaultNumValues = 10_000_000
	defaultBlockSize = 10_000
	defaultFilename  = "loadtest.col"
)

func main() {
	// Define commands
	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	aggregateCmd := flag.NewFlagSet("aggregate", flag.ExitOnError)

	// Import command flags
	importNumValues := importCmd.Int("n", defaultNumValues, "Number of values to import")
	importBlockSize := importCmd.Int("block-size", defaultBlockSize, "Target block size")
	importFilename := importCmd.String("file", defaultFilename, "Output file name")
	importSeed := importCmd.Int64("seed", time.Now().UnixNano(), "Random seed")
	importMaxValue := importCmd.Int64("max-value", 1000000, "Maximum value")
	importMaxID := importCmd.Uint64("max-id", 20000000, "Maximum ID")

	// Aggregate command flags
	aggregateFilename := aggregateCmd.String("file", defaultFilename, "Input file name")
	aggregateSkipCache := aggregateCmd.Bool("skip-cache", true, "Skip using cached sums")
	aggregateParallel := aggregateCmd.Int("parallel", 0, "Parallel factor (0=sequential, <0=auto/GOMAXPROCS, >0=specific number of workers)")

	// Check if a command is provided
	if len(os.Args) < 2 {
		fmt.Println("Expected 'import' or 'aggregate' subcommand")
		os.Exit(1)
	}

	// Parse the command
	switch os.Args[1] {
	case "import":
		importCmd.Parse(os.Args[2:])
		runImport(*importNumValues, *importBlockSize, *importFilename, *importSeed, *importMaxValue, *importMaxID)
	case "aggregate":
		aggregateCmd.Parse(os.Args[2:])
		runAggregate(*aggregateFilename, *aggregateSkipCache, *aggregateParallel)
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		fmt.Println("Expected 'import' or 'aggregate' subcommand")
		os.Exit(1)
	}
}

func runImport(numValues, blockSize int, filename string, seed int64, maxValue int64, maxID uint64) {
	fmt.Printf("Importing %d values with block size %d to %s\n", numValues, blockSize, filename)

	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("Error creating directory: %v\n", err)
			os.Exit(1)
		}
	}

	// Initialize random number generator
	rng := rand.New(rand.NewSource(seed))

	// Create SimpleWriter with VarInt encoding for both IDs and values
	writer, err := col.NewSimpleWriter(filename,
		col.WithBlockSize(uint32(blockSize)),
		col.WithEncoding(col.EncodingVarIntBoth))
	if err != nil {
		fmt.Printf("Error creating writer: %v\n", err)
		os.Exit(1)
	}
	defer writer.Close()

	// Track progress
	startTime := time.Now()
	lastReportTime := startTime
	valuesWritten := 0
	blockCount := 0

	// Generate and write values in batches
	batchSize := 10000 // Use a reasonable batch size for SimpleWriter

	for valuesWritten < numValues {
		// Determine batch size for this iteration
		currentBatchSize := batchSize
		if valuesWritten+currentBatchSize > numValues {
			currentBatchSize = numValues - valuesWritten
		}

		// Generate IDs and values
		ids := make([]uint64, currentBatchSize)
		values := make([]int64, currentBatchSize)

		for i := 0; i < currentBatchSize; i++ {
			// Generate random IDs with some locality (increasing with occasional jumps)
			if i > 0 && rng.Float64() < 0.9 {
				ids[i] = ids[i-1] + uint64(rng.Intn(10)+1)
			} else {
				ids[i] = uint64(rng.Int63n(int64(maxID)))
			}

			// Generate random values with some correlation to IDs
			if rng.Float64() < 0.7 {
				// 70% of values are somewhat correlated with IDs
				values[i] = int64(ids[i] % uint64(maxValue))
				// Add some noise
				values[i] += rng.Int63n(maxValue/10) - maxValue/20
			} else {
				// 30% are completely random
				values[i] = rng.Int63n(maxValue)
			}
		}

		// Write the batch to the SimpleWriter
		if err := writer.Write(ids, values); err != nil {
			fmt.Printf("Error writing batch: %v\n", err)
			os.Exit(1)
		}

		// Update counters
		valuesWritten += currentBatchSize

		// Get current total items to track progress
		currentTotalItems := writer.TotalItems()
		if currentTotalItems > uint64(blockCount) {
			blockCount = int(currentTotalItems)
		}

		// Report progress every second
		now := time.Now()
		if now.Sub(lastReportTime) >= time.Second {
			elapsed := now.Sub(startTime).Seconds()
			fmt.Printf("Progress: %d/%d values (%.2f%%), %d items written, %.2f values/sec\n",
				valuesWritten, numValues, float64(valuesWritten)/float64(numValues)*100,
				currentTotalItems, float64(valuesWritten)/elapsed)
			lastReportTime = now
		}
	}

	// Close the file (this will finalize it)
	if err := writer.Close(); err != nil {
		fmt.Printf("Error closing file: %v\n", err)
		os.Exit(1)
	}

	// Report final statistics
	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("\nImport completed in %.2f seconds\n", elapsed)
	fmt.Printf("Total values: %d\n", valuesWritten)
	fmt.Printf("Total items written: %d\n", writer.TotalItems())
	fmt.Printf("Average throughput: %.2f values/sec\n", float64(valuesWritten)/elapsed)

	// Get file size
	fileInfo, err := os.Stat(filename)
	if err == nil {
		fileSizeMB := float64(fileInfo.Size()) / (1024 * 1024)
		fmt.Printf("File size: %.2f MB\n", fileSizeMB)
		fmt.Printf("Bytes per value: %.2f\n", float64(fileInfo.Size())/float64(valuesWritten))
	}
}

func runAggregate(filename string, skipCache bool, parallel int) {
	fmt.Printf("Running aggregations on %s (skip cache: %v, parallel: %v)\n", filename, skipCache, parallel)

	// Open the file
	reader, err := col.NewReader(filename)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	// Print file info
	fmt.Printf("File version: %d\n", reader.Version())
	fmt.Printf("Encoding type: %d\n", reader.EncodingType())
	fmt.Printf("Block count: %d\n", reader.BlockCount())

	// Run different aggregation operations
	runAggregations(reader, skipCache, parallel)
}

func runAggregations(reader *col.Reader, skipCache bool, parallel int) {
	// Track overall time
	startTime := time.Now()

	// Create aggregate options
	opts := col.AggregateOptions{
		SkipPreCalculated: skipCache,
		Parallel:          parallel,
	}

	// Run aggregation
	aggStart := time.Now()
	result := reader.AggregateWithOptions(opts)
	aggDuration := time.Since(aggStart)

	// Print results
	fmt.Printf("Count: %d\n", result.Count)
	fmt.Printf("Min: %d\n", result.Min)
	fmt.Printf("Max: %d\n", result.Max)
	fmt.Printf("Sum: %d\n", result.Sum)
	fmt.Printf("Average: %.2f\n", result.Avg)
	fmt.Printf("Aggregation time: %.2f ms\n", aggDuration.Seconds()*1000)

	// Print parallel info if used
	if parallel != 0 {
		actualWorkers := parallel
		if parallel < 0 {
			actualWorkers = runtime.GOMAXPROCS(0)
		}
		fmt.Printf("Parallel workers: %d\n", actualWorkers)
	}

	// Run full scan (read all blocks)
	scanStart := time.Now()
	var totalValues int64
	for i := uint64(0); i < reader.BlockCount(); i++ {
		_, values, err := reader.GetPairs(i)
		if err != nil {
			fmt.Printf("Error reading block %d: %v\n", i, err)
			return
		}
		totalValues += int64(len(values))
	}
	scanDuration := time.Since(scanStart)
	fmt.Printf("Full scan: %d values (%.2f ms, %.2f values/sec)\n",
		totalValues,
		scanDuration.Seconds()*1000,
		float64(totalValues)/scanDuration.Seconds())

	// Report total time
	totalDuration := time.Since(startTime)
	fmt.Printf("\nTotal time: %.2f ms\n", totalDuration.Seconds()*1000)
}
