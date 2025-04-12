package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"vibe-lsm/pkg/col"
	"vibe-lsm/pkg/multicol"
	"vibe-lsm/pkg/store"
)

const (
	defaultNumValues    = 10_000_000
	defaultBlockSize    = 256 * 1024 // 256KB
	defaultFilename     = "loadtest.col"
	defaultMemtableSize = 125_000 // Flush after 125,000 items
)

func main() {
	// Define commands
	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	aggregateCmd := flag.NewFlagSet("aggregate", flag.ExitOnError)

	// Import command flags
	importNumValues := importCmd.Int("n", defaultNumValues, "Number of values to import")
	importDataDir := importCmd.String("dir", "data", "Data directory for store")
	importSeed := importCmd.Int64("seed", time.Now().UnixNano(), "Random seed")
	importMaxValue := importCmd.Int64("max-value", 1000000, "Maximum value")
	importMaxID := importCmd.Uint64("max-id", 20000000, "Maximum ID")
	importCPUProfile := importCmd.String("cpuprofile", "", "Write CPU profile to file")
	importMemProfile := importCmd.String("memprofile", "", "Write memory profile to file")

	// Aggregate command flags
	aggregateDataDir := aggregateCmd.String("dir", "data", "Data directory for store")
	aggregateSkipCache := aggregateCmd.Bool("skip-cache", true, "Skip using cached sums")
	aggregateParallel := aggregateCmd.Int("parallel", 0, "Parallel factor (0=sequential, <0=auto/GOMAXPROCS, >0=specific number of workers)")
	aggregateCPUProfile := aggregateCmd.String("cpuprofile", "", "Write CPU profile to file")
	aggregateMemProfile := aggregateCmd.String("memprofile", "", "Write memory profile to file")
	aggregateMemProfileType := aggregateCmd.String("memprofiletype", "heap", "Memory profile type: 'heap' or 'allocs'")

	// Check if a command is provided
	if len(os.Args) < 2 {
		fmt.Println("Expected 'import' or 'aggregate' subcommand")
		os.Exit(1)
	}

	// Parse the command
	switch os.Args[1] {
	case "import":
		importCmd.Parse(os.Args[2:])
		runImport(*importNumValues, *importDataDir, *importSeed, *importMaxValue, *importMaxID, *importCPUProfile, *importMemProfile)
	case "aggregate":
		aggregateCmd.Parse(os.Args[2:])
		runAggregate(*aggregateDataDir, *aggregateSkipCache, *aggregateParallel, *aggregateCPUProfile, *aggregateMemProfile, *aggregateMemProfileType)
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		fmt.Println("Expected 'import' or 'aggregate' subcommand")
		os.Exit(1)
	}
}

func runImport(numValues int, dataDir string, seed int64, maxValue int64, maxID uint64, cpuProfile, memProfile string) {
	// Start CPU profiling if requested
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			fmt.Printf("Error creating CPU profile file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("Error starting CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
		fmt.Printf("CPU profiling enabled, writing to %s\n", cpuProfile)
	}

	fmt.Printf("Importing %d values with VibeStore to %s\n", numValues, dataDir)
	fmt.Printf("Configuration: Memtable size = %d items, Target block size = %d KB\n",
		defaultMemtableSize, defaultBlockSize/1024)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		os.Exit(1)
	}

	// Initialize random number generator
	rng := rand.New(rand.NewSource(seed))

	// Configure store options
	options := store.DefaultOptions(dataDir)
	options.MemtableSize = defaultMemtableSize                  // Flush after 125,000 items
	options.MemtableOptions = multicol.DefaultMemtableOptions() // Use default memtable options
	options.CompactionOptions = multicol.CompactionOptions{
		TargetBlockSize: defaultBlockSize,       // 256KB target block size for compaction
		EncodingType:    col.EncodingVarIntBoth, // Use VarInt encoding
	}

	// Create the store
	vibeStore, err := store.NewVibeStore(options)
	if err != nil {
		fmt.Printf("Error creating store: %v\n", err)
		os.Exit(1)
	}
	defer vibeStore.Close()

	// Track progress
	startTime := time.Now()
	lastReportTime := startTime
	valuesWritten := 0

	// For tracking items written
	totalItemsWritten := uint64(0)

	// Generate and write values in batches
	batchSize := 10000 // Use a reasonable batch size

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

		// Write the batch to the store
		if err := vibeStore.BatchAdd(ids, values); err != nil {
			fmt.Printf("Error writing batch: %v\n", err)
			os.Exit(1)
		}

		// Update counters
		valuesWritten += currentBatchSize
		totalItemsWritten += uint64(currentBatchSize)

		// Report progress every second
		now := time.Now()
		if now.Sub(lastReportTime) >= time.Second {
			elapsed := now.Sub(startTime).Seconds()
			fmt.Printf("Progress: %d/%d values (%.2f%%), %d items written, %.2f values/sec\n",
				valuesWritten, numValues, float64(valuesWritten)/float64(numValues)*100,
				totalItemsWritten, float64(valuesWritten)/elapsed)
			lastReportTime = now
		}
	}

	// Report final statistics
	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("\nImport completed in %.2f seconds\n", elapsed)
	fmt.Printf("Total values: %d\n", valuesWritten)
	fmt.Printf("Total items written: %d\n", totalItemsWritten)
	fmt.Printf("Average throughput: %.2f values/sec\n", float64(valuesWritten)/elapsed)

	// Compaction phase
	fmt.Println("\nStarting compaction phase...")

	// Force flush any remaining memtable data
	vibeStore.ForceFlush()
	time.Sleep(500 * time.Millisecond) // Wait for flush to complete

	// Get initial segment levels
	levels := vibeStore.GetSegmentLevels()
	fmt.Printf("Initial segment levels: %v\n", levels)

	// Keep compacting until no more compactions are possible
	compactionCount := 0
	startCompactionTime := time.Now()

	for vibeStore.TriggerCompaction() {
		compactionCount++
		fmt.Printf("Compaction #%d triggered\n", compactionCount)
		// Wait for the compaction to complete
		time.Sleep(100 * time.Millisecond)
	}

	compactionElapsed := time.Since(startCompactionTime).Seconds()
	fmt.Printf("\nCompaction phase completed in %.2f seconds\n", compactionElapsed)
	fmt.Printf("Performed %d compactions\n", compactionCount)

	// Get final segment levels
	finalLevels := vibeStore.GetSegmentLevels()
	fmt.Printf("Final segment levels: %v\n", finalLevels)

	// Perform a final aggregation to verify the data
	fmt.Println("\nVerifying data by running aggregation...")
	aggResult, err := vibeStore.AggregateWithOptions(store.AggregateOptions{})
	if err != nil {
		fmt.Printf("Error during aggregation: %v\n", err)
	} else {
		fmt.Printf("Total count: %d\n", aggResult.Count)
		fmt.Printf("Min value: %d\n", aggResult.Min)
		fmt.Printf("Max value: %d\n", aggResult.Max)
		fmt.Printf("Sum: %d\n", aggResult.Sum)
		fmt.Printf("Average: %.2f\n", aggResult.Avg)
	}

	// Print detailed segment info
	fmt.Println("\nDetailed segment information:")
	fmt.Println("-----------------------------")

	// List all files in the data directory to get segment files
	files, err := os.ReadDir(dataDir)
	if err != nil {
		fmt.Printf("Error reading data directory: %v\n", err)
	} else {
		// Get all .col files (both compacted and regular segments)
		type segmentInfo struct {
			path     string
			filename string
			info     os.FileInfo
		}

		var segments []segmentInfo
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".col") {
				path := filepath.Join(dataDir, file.Name())
				info, err := os.Stat(path)
				if err != nil {
					continue
				}
				segments = append(segments, segmentInfo{
					path:     path,
					filename: file.Name(),
					info:     info,
				})
			}
		}

		// Sort by modification time (oldest first to match the store's ordering)
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].info.ModTime().Before(segments[j].info.ModTime())
		})

		if len(segments) == 0 {
			fmt.Println("No segment files found")
		} else {
			// Compare with the levels we have
			if len(segments) != len(finalLevels) {
				fmt.Printf("Note: Found %d segment files but store reports %d segments\n",
					len(segments), len(finalLevels))
				fmt.Println("(This may happen if the store has in-memory segments or if files were deleted)")
			}

			// Show stats for each segment
			for i, segment := range segments {
				segmentSize := segment.info.Size()

				// Get level and block info
				var level uint16
				var blockCount uint64
				var avgBlockSize float64

				// Open the segment to get more info
				reader, err := col.NewReader(segment.path)
				if err != nil {
					fmt.Printf("Error opening segment %s: %v\n", segment.filename, err)
					continue
				}

				level = reader.Level()
				blockCount = reader.BlockCount()
				if blockCount > 0 {
					avgBlockSize = float64(segmentSize) / float64(blockCount)
				}
				reader.Close()

				// Determine if this is a compacted segment or a regular segment
				segmentType := "Regular"
				if strings.HasPrefix(segment.filename, "compacted_") {
					segmentType = "Compacted"
				}

				// Print segment details
				fmt.Printf("Segment #%d: %s\n", i, segment.filename)
				fmt.Printf("  Type: %s\n", segmentType)
				fmt.Printf("  Level: %d\n", level)
				fmt.Printf("  Size: %.2f MB\n", float64(segmentSize)/(1024*1024))
				fmt.Printf("  Block count: %d\n", blockCount)
				fmt.Printf("  Avg block size: %.2f KB\n", avgBlockSize/1024)
				fmt.Println()
			}
		}
	}

	// Write memory profile if requested
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			fmt.Printf("Error creating memory profile file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		// Run garbage collection to get accurate memory profile
		runtime.GC()

		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Printf("Error writing memory profile: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Heap profile written to %s\n", memProfile)
	}
}

func runAggregate(dataDir string, skipCache bool, parallel int, cpuProfile, memProfile, memProfileType string) {
	// Track if profiling is enabled
	isProfilingEnabled := cpuProfile != "" || memProfile != ""

	// Start CPU profiling if requested
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			fmt.Printf("Error creating CPU profile file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("Error starting CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
		fmt.Printf("CPU profiling enabled, writing to %s\n", cpuProfile)
	}

	fmt.Printf("Running aggregations on store in %s (skip cache: %v, parallel: %v)\n",
		dataDir, skipCache, parallel)

	// First, examine the segment files in the directory
	fmt.Println("\nSegment files found in directory:")
	files, err := os.ReadDir(dataDir)
	if err != nil {
		fmt.Printf("Error reading data directory: %v\n", err)
	} else {
		segmentCount := 0
		compactedCount := 0
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".col") {
				segmentPath := filepath.Join(dataDir, file.Name())
				fileInfo, err := os.Stat(segmentPath)
				if err != nil {
					fmt.Printf("Error getting file info for %s: %v\n", file.Name(), err)
					continue
				}

				// Try to open as segment to get level
				reader, err := col.NewReader(segmentPath)
				if err != nil {
					fmt.Printf("Error opening segment %s: %v\n", file.Name(), err)
					continue
				}

				level := reader.Level()
				blockCount := reader.BlockCount()
				reader.Close()

				fileType := "Regular"
				if strings.HasPrefix(file.Name(), "compacted_") {
					fileType = "Compacted"
					compactedCount++
				} else {
					segmentCount++
				}

				fmt.Printf("  %s: %s (Level: %d, Blocks: %d, Size: %.2f MB)\n",
					fileType, file.Name(), level, blockCount,
					float64(fileInfo.Size())/(1024*1024))
			}
		}
		fmt.Printf("Found %d regular segments and %d compacted segments\n",
			segmentCount, compactedCount)
	}

	// Open the store
	fmt.Println("\nOpening store...")
	options := store.DefaultOptions(dataDir)
	vibeStore, err := store.NewVibeStore(options)
	if err != nil {
		fmt.Printf("Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer vibeStore.Close()

	// Print store info
	levels := vibeStore.GetSegmentLevels()
	fmt.Printf("Segment levels in store: %v\n", levels)
	fmt.Printf("Segment count in store: %d\n", len(levels))

	// Create aggregate options
	opts := store.AggregateOptions{
		SkipPreCalculated: skipCache,
		Parallel:          parallel,
	}

	// Track overall time
	startTime := time.Now()

	// Run a single iteration first
	aggStart := time.Now()
	result, err := vibeStore.AggregateWithOptions(opts)
	if err != nil {
		fmt.Printf("Error during aggregation: %v\n", err)
		os.Exit(1)
	}
	singleIterationDuration := time.Since(aggStart)

	// Determine if we need to run more iterations for profiling
	iterations := 1
	if isProfilingEnabled {
		// Target duration for profiling (1 second)
		targetDuration := time.Second

		if singleIterationDuration < targetDuration {
			// Run more iterations when profiling to get meaningful data
			// Calculate how many iterations we need to run to reach the target duration
			estimatedIterations := int(targetDuration / singleIterationDuration)

			// Ensure we run at least 10 iterations for stability
			if estimatedIterations < 10 {
				estimatedIterations = 10
			}

			fmt.Printf("Profiling enabled, running ~%d iterations to reach 1 second of profiling data\n", estimatedIterations)

			// Run the remaining iterations
			remainingIterations := estimatedIterations - 1 // We already ran one
			iterationStart := time.Now()

			for i := 0; i < remainingIterations; i++ {
				result, _ = vibeStore.AggregateWithOptions(opts)
			}

			// Update total iterations and duration
			iterations = estimatedIterations
			aggDuration := singleIterationDuration + time.Since(iterationStart)

			// Adjust duration for reporting
			reportedDuration := aggDuration / time.Duration(iterations)

			// Print results
			fmt.Printf("\nAggregation results:\n")
			fmt.Printf("Count: %d\n", result.Count)
			fmt.Printf("Min: %d\n", result.Min)
			fmt.Printf("Max: %d\n", result.Max)
			fmt.Printf("Sum: %d\n", result.Sum)
			fmt.Printf("Average: %.2f\n", result.Avg)
			fmt.Printf("Ran %d iterations in %.2f ms (%.2f ms per iteration)\n",
				iterations,
				aggDuration.Seconds()*1000,
				reportedDuration.Seconds()*1000)
		} else {
			// Single iteration already took more than the target duration
			fmt.Printf("Single iteration took %.2f ms, no need for additional iterations\n",
				singleIterationDuration.Seconds()*1000)

			// Print results
			fmt.Printf("\nAggregation results:\n")
			fmt.Printf("Count: %d\n", result.Count)
			fmt.Printf("Min: %d\n", result.Min)
			fmt.Printf("Max: %d\n", result.Max)
			fmt.Printf("Sum: %d\n", result.Sum)
			fmt.Printf("Average: %.2f\n", result.Avg)
			fmt.Printf("Aggregation time: %.2f ms\n", singleIterationDuration.Seconds()*1000)
		}
	} else {
		// Not profiling, just print the results from the single iteration
		fmt.Printf("\nAggregation results:\n")
		fmt.Printf("Count: %d\n", result.Count)
		fmt.Printf("Min: %d\n", result.Min)
		fmt.Printf("Max: %d\n", result.Max)
		fmt.Printf("Sum: %d\n", result.Sum)
		fmt.Printf("Average: %.2f\n", result.Avg)
		fmt.Printf("Aggregation time: %.2f ms\n", singleIterationDuration.Seconds()*1000)
	}

	// Print parallel info if used
	if parallel != 0 {
		actualWorkers := parallel
		if parallel < 0 {
			actualWorkers = runtime.GOMAXPROCS(0)
		}
		fmt.Printf("Parallel workers: %d\n", actualWorkers)
	}

	// Report total time
	totalDuration := time.Since(startTime)
	fmt.Printf("\nTotal time: %.2f ms\n", totalDuration.Seconds()*1000)

	// Write memory profile if requested
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			fmt.Printf("Error creating memory profile file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		// Run garbage collection to get accurate memory profile
		runtime.GC()

		if memProfileType == "allocs" {
			if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
				fmt.Printf("Error writing allocation profile: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Allocation profile written to %s\n", memProfile)
		} else {
			// Default to heap profile
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Printf("Error writing heap profile: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Heap profile written to %s\n", memProfile)
		}
	}
}
