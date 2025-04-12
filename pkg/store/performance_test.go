package store

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteLatencyWithFlushes tests the latency of write operations while
// ensuring multiple flushes occur, to verify flush operations don't
// significantly impact write latency
func TestWriteLatencyWithFlushes(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-write-latency-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create options with smaller memtable size to trigger multiple flushes
	options := DefaultOptions(tempDir)
	options.MemtableSize = 1000     // Flush after 1000 entries
	options.MemtableMaxAgeMs = 1000 // Flush after 1 second

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Test parameters
	numOperations := 10000 // Total number of write operations
	reportEvery := 1000    // Print progress every N operations
	latencies := make([]time.Duration, numOperations)

	// Track segment count for validation
	initialState := store.state.Load().(*VibeStoreState)
	initialSegmentCount := len(initialState.segments)

	// Monitor flush progress by checking files periodically
	go monitorSegments(t, tempDir, reportEvery)

	// Random source for generating values
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Perform write operations and measure latencies
	t.Logf("Starting write latency test with %d operations...", numOperations)
	for i := 0; i < numOperations; i++ {
		id := uint64(i + 1)
		value := int64(r.Intn(10000))

		start := time.Now()
		err := store.Add(id, value)
		latency := time.Since(start)
		latencies[i] = latency

		require.NoError(t, err)

		// Report progress periodically
		if (i+1)%reportEvery == 0 {
			currentState := store.state.Load().(*VibeStoreState)
			currentSegmentCount := len(currentState.segments)
			t.Logf("Progress: %d/%d operations, %d segments so far",
				i+1, numOperations, currentSegmentCount)
		}
	}

	// Wait for any pending flushes to complete
	time.Sleep(2 * time.Second)

	// Count final segments to confirm flushes
	finalState := store.state.Load().(*VibeStoreState)
	finalSegmentCount := len(finalState.segments)
	t.Logf("Final segment count: %d (initial: %d)", finalSegmentCount, initialSegmentCount)
	assert.Greater(t, finalSegmentCount, initialSegmentCount, "Number of segments should have increased")

	// Verify multiple flushes occurred by counting segment files
	segmentFiles, err := countSegmentFiles(tempDir)
	require.NoError(t, err)
	t.Logf("Test completed with %d segment files created", segmentFiles)
	assert.GreaterOrEqual(t, segmentFiles, 3, "At least 3 segments should have been created")

	// Calculate latency percentiles
	calculateLatencyPercentiles(t, latencies)

	// Optional: Check if there's a correlation between high latencies and flush events
	// This is a basic check to see if flushes caused spikes
	checkForLatencySpikes(t, latencies, numOperations/segmentFiles)
}

// countSegmentFiles counts the number of segment files in the directory
func countSegmentFiles(dir string) (int, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".col") {
			// Count both regular segments and compacted segments
			if strings.HasPrefix(file.Name(), "segment_") || strings.HasPrefix(file.Name(), "compacted_") {
				count++
			}
		}
	}
	return count, nil
}

// monitorSegments periodically checks the directory for segment files
func monitorSegments(t *testing.T, dir string, checkInterval int) {
	ticker := time.NewTicker(time.Duration(checkInterval) * time.Millisecond)
	defer ticker.Stop()

	lastCount := 0
	for range ticker.C {
		count, err := countSegmentFiles(dir)
		if err != nil {
			continue
		}

		if count > lastCount {
			t.Logf("New segments detected: %d (previously %d)", count, lastCount)
			lastCount = count
		}
	}
}

// calculateLatencyPercentiles calculates and reports latency percentiles
func calculateLatencyPercentiles(t *testing.T, latencies []time.Duration) {
	// Sort latencies for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate percentiles
	p50 := percentile(latencies, 50)
	p90 := percentile(latencies, 90)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	p999 := percentile(latencies, 99.9)

	min := latencies[0]
	max := latencies[len(latencies)-1]
	mean := mean(latencies)

	// Report results
	t.Logf("Write Latency Statistics:")
	t.Logf("  Min: %v", min)
	t.Logf("  P50 (Median): %v", p50)
	t.Logf("  P90: %v", p90)
	t.Logf("  P95: %v", p95)
	t.Logf("  P99: %v", p99)
	t.Logf("  P99.9: %v", p999)
	t.Logf("  Max: %v", max)
	t.Logf("  Mean: %v", mean)
}

// percentile calculates the given percentile from sorted latencies
func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	rank := p / 100.0 * float64(len(latencies)-1)
	rankInt := int(rank)
	rankFrac := rank - float64(rankInt)

	if rankInt >= len(latencies)-1 {
		return latencies[len(latencies)-1]
	}

	return time.Duration(float64(latencies[rankInt]) + rankFrac*float64(latencies[rankInt+1]-latencies[rankInt]))
}

// mean calculates the average latency
func mean(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	var sum time.Duration
	for _, d := range latencies {
		sum += d
	}
	return sum / time.Duration(len(latencies))
}

// checkForLatencySpikes tries to identify if there are latency spikes
// that correlate with expected flush points
func checkForLatencySpikes(t *testing.T, latencies []time.Duration, estimatedFlushInterval int) {
	if estimatedFlushInterval <= 0 {
		t.Logf("Cannot check for latency spikes: invalid flush interval")
		return
	}

	// Create windows around estimated flush points and check for latency spikes
	windowSize := 10 // Look at operations right before and after predicted flush

	flushPoints := []int{}
	for i := estimatedFlushInterval; i < len(latencies); i += estimatedFlushInterval {
		flushPoints = append(flushPoints, i)
	}

	// Report on potential flush points
	t.Logf("Checking latency around estimated flush points:")
	for _, flushPoint := range flushPoints {
		// Skip if we're too close to the beginning or end
		if flushPoint < windowSize || flushPoint >= len(latencies)-windowSize {
			continue
		}

		// Get latencies before, during, and after the potential flush
		before := mean(latencies[flushPoint-windowSize : flushPoint])
		after := mean(latencies[flushPoint : flushPoint+windowSize])
		flushLatency := latencies[flushPoint]

		// Calculate the percent increase
		percentIncrease := float64(after)/float64(before)*100.0 - 100.0

		// Report significant changes
		if percentIncrease > 50 { // More than 50% increase
			t.Logf("  Potential latency spike at operation %d: before=%v, after=%v, spike=%v (+%.2f%%)",
				flushPoint, before, after, flushLatency, percentIncrease)
		}
	}

	// Find the top 10 highest latencies
	type indexedLatency struct {
		index   int
		latency time.Duration
	}

	allLatencies := make([]indexedLatency, len(latencies))
	for i, l := range latencies {
		allLatencies[i] = indexedLatency{i, l}
	}

	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i].latency > allLatencies[j].latency
	})

	t.Logf("Top 10 highest latencies:")
	for i := 0; i < 10 && i < len(allLatencies); i++ {
		t.Logf("  Operation %d: %v", allLatencies[i].index, allLatencies[i].latency)
	}
}

// TestLatencyDistribution runs a longer test to generate a detailed
// latency distribution, useful for more thorough analysis and visualization
func TestLatencyDistribution(t *testing.T) {
	// Skip in normal test runs, only run explicitly
	if testing.Short() || !testing.Verbose() {
		t.Skip("Skipping extended latency distribution test")
	}

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-latency-distribution-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create options with smaller memtable size to trigger multiple flushes
	options := DefaultOptions(tempDir)
	options.MemtableSize = 500     // Flush after 500 entries (more frequent flushes)
	options.MemtableMaxAgeMs = 500 // Flush after 0.5 seconds

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Test parameters
	numOperations := 20000 // More operations for better distribution
	latencies := make([]time.Duration, numOperations)

	// Create result directory
	resultDir := filepath.Join(tempDir, "results")
	err = os.MkdirAll(resultDir, 0755)
	require.NoError(t, err)

	resultFile := filepath.Join(resultDir, "latency_data.csv")
	file, err := os.Create(resultFile)
	require.NoError(t, err)
	defer file.Close()

	// Write CSV header
	_, err = file.WriteString("operation,latency_ns,timestamp_ms\n")
	require.NoError(t, err)

	// Random source for generating values
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	startTime := time.Now()

	// Perform write operations and record latencies
	for i := 0; i < numOperations; i++ {
		id := uint64(i + 1)
		value := int64(r.Intn(10000))

		opStart := time.Now()
		err := store.Add(id, value)
		latency := time.Since(opStart)
		latencies[i] = latency
		require.NoError(t, err)

		// Record to CSV
		timestamp := time.Since(startTime).Milliseconds()
		_, err = file.WriteString(fmt.Sprintf("%d,%d,%d\n", i, latency.Nanoseconds(), timestamp))
		require.NoError(t, err)

		// Optional: Add a small delay between operations
		if i%10 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	// Close file before analysis
	file.Close()

	// Wait for any pending operations
	time.Sleep(1 * time.Second)

	// Final segment count
	finalState := store.state.Load().(*VibeStoreState)
	t.Logf("Final segment count: %d", len(finalState.segments))

	// Calculate percentiles
	calculateLatencyPercentiles(t, latencies)

	t.Logf("Latency data written to: %s", resultFile)
	t.Logf("You can analyze this data using your preferred plotting tool")
}

// TestWriteLatencyCorrelation runs a test to specifically track write latency
// and correlate it with segment creation times to identify if flushes cause
// latency spikes
func TestWriteLatencyCorrelation(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping performance correlation test in short mode")
	}

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-latency-correlation-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create options with smaller memtable size to trigger multiple flushes
	options := DefaultOptions(tempDir)
	options.MemtableSize = 500     // More frequent flushes for better correlation
	options.MemtableMaxAgeMs = 500 // Flush after 0.5 seconds

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Test parameters
	numOperations := 5000 // More manageable number of operations
	latencies := make([]time.Duration, numOperations)
	timestamps := make([]time.Time, numOperations)

	// Keep track of segment creation events
	segmentEvents := make([]time.Time, 0, 20)
	segmentChan := make(chan time.Time, 20)

	// Start monitoring segment creation
	go func() {
		segmentMonitor(t, tempDir, segmentChan)
	}()

	// Start timestamp
	testStart := time.Now()

	// Perform write operations and measure latencies
	t.Logf("Starting correlation test with %d operations...", numOperations)
	for i := 0; i < numOperations; i++ {
		id := uint64(i + 1)
		value := int64(i * 10) // Deterministic values for easier debugging

		// Record pre-operation timestamp
		timestamps[i] = time.Now()

		// Perform operation and record latency
		start := time.Now()
		err := store.Add(id, value)
		latency := time.Since(start)
		latencies[i] = latency

		require.NoError(t, err)

		// Occasional progress update
		if i > 0 && i%500 == 0 {
			t.Logf("Progress: %d/%d operations", i, numOperations)
		}

		// Collect segment events if available
		select {
		case timestamp := <-segmentChan:
			segmentEvents = append(segmentEvents, timestamp)
			t.Logf("Segment created at operation ~%d (elapsed: %v)",
				i, timestamp.Sub(testStart))
		default:
			// No segment event, continue
		}

		// Small delay to spread out operations
		if i%10 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	// Wait to collect any remaining segment events
	time.Sleep(1 * time.Second)
	close(segmentChan)

	// Collect any remaining segment events
	for timestamp := range segmentChan {
		segmentEvents = append(segmentEvents, timestamp)
	}

	// Calculate latency statistics
	calculateLatencyPercentiles(t, latencies)

	// Sort segment events by time
	sort.Slice(segmentEvents, func(i, j int) bool {
		return segmentEvents[i].Before(segmentEvents[j])
	})

	// Analyze latency near segment creation times
	if len(segmentEvents) > 0 {
		t.Logf("Found %d segment creation events", len(segmentEvents))

		// For each segment creation, examine operations around that time
		windowSize := 20 // Look at operations before and after the segment

		// Find spikes around segment creation times
		for i, segmentTime := range segmentEvents {
			// Find the closest operation to this segment creation time
			closestOp := findClosestOperation(timestamps, segmentTime)

			// Skip if too close to beginning or end
			if closestOp < windowSize || closestOp >= len(latencies)-windowSize {
				continue
			}

			// Calculate latencies before and after segment creation
			before := make([]time.Duration, 0, windowSize)
			for j := closestOp - windowSize; j < closestOp; j++ {
				before = append(before, latencies[j])
			}

			after := make([]time.Duration, 0, windowSize)
			for j := closestOp; j < closestOp+windowSize; j++ {
				after = append(after, latencies[j])
			}

			// Calculate statistics
			beforeAvg := mean(before)
			afterAvg := mean(after)

			// Calculate percentage change
			percentChange := float64(afterAvg)/float64(beforeAvg)*100.0 - 100.0

			t.Logf("Segment %d (near op %d): Before avg=%v, After avg=%v, Change: %.2f%%",
				i+1, closestOp, beforeAvg, afterAvg, percentChange)

			// If significant change, show the highest latencies
			if percentChange > 30 {
				// Find highest latencies after segment creation
				highLatencies := make([]time.Duration, len(after))
				copy(highLatencies, after)
				sort.Slice(highLatencies, func(i, j int) bool {
					return highLatencies[i] > highLatencies[j]
				})

				t.Logf("  Highest latencies after segment %d: %v, %v, %v",
					i+1, highLatencies[0], highLatencies[1], highLatencies[2])
			}
		}
	}
}

// findClosestOperation finds the operation index closest to the given timestamp
func findClosestOperation(timestamps []time.Time, target time.Time) int {
	closest := 0
	minDiff := time.Duration(1<<63 - 1) // Max duration

	for i, ts := range timestamps {
		diff := absDuration(ts.Sub(target))
		if diff < minDiff {
			minDiff = diff
			closest = i
		}
	}

	return closest
}

// absDuration returns the absolute value of a duration
func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// segmentMonitor watches for new segment files and reports their creation times
func segmentMonitor(t *testing.T, dir string, eventChan chan<- time.Time) {
	seen := make(map[string]bool)
	checkInterval := 50 * time.Millisecond

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		files, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, file := range files {
			name := file.Name()
			if !seen[name] && strings.HasPrefix(name, "segment_") && strings.HasSuffix(name, ".col") {
				seen[name] = true

				// Extract timestamp from filename for more accurate timing
				timeStr := strings.TrimPrefix(name, "segment_")
				timeStr = strings.TrimSuffix(timeStr, ".col")
				if ns, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
					eventTime := time.Unix(0, ns)
					eventChan <- eventTime
				} else {
					// Fallback to current time if parsing fails
					eventChan <- time.Now()
				}
			}
		}
	}
}
