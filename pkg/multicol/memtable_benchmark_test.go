package multicol

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// BenchmarkMemtableConcurrentWrites tests how fast we can write into the memtable concurrently
// with different striping factors.
func BenchmarkMemtableConcurrentWrites(b *testing.B) {
	// Number of elements to write
	const numElements = 100_000

	// Number of concurrent writers
	const numWriters = 36

	// Test different striping factors
	stripingFactors := []int{1, 2, 4, 8, 16, 32, 64}

	for _, stripes := range stripingFactors {
		b.Run(fmt.Sprintf("Stripes_%d", stripes), func(b *testing.B) {
			// Reset the timer for setup
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Create a memtable with the specified number of stripes
				opts := &MemtableOptions{
					NumStripes: stripes,
					MaxHeight:  DefaultMaxHeight,
					Seed:       time.Now().UnixNano(),
				}

				m := NewMemtable(opts)

				// Create all IDs beforehand
				ids := make([]uint64, numElements)
				for i := 0; i < numElements; i++ {
					ids[i] = uint64(i)
				}

				// Shuffle the IDs to ensure random access patterns
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				r.Shuffle(len(ids), func(i, j int) {
					ids[i], ids[j] = ids[j], ids[i]
				})

				// Divide IDs among writers
				idsPerWriter := numElements / numWriters

				// Start timer for actual benchmark
				b.StartTimer()

				// Use a WaitGroup to synchronize goroutines
				var wg sync.WaitGroup
				wg.Add(numWriters)

				// Start writer goroutines
				for w := 0; w < numWriters; w++ {
					start := w * idsPerWriter
					end := start + idsPerWriter
					if w == numWriters-1 {
						end = numElements // Ensure we use all IDs
					}

					go func(startIdx, endIdx int) {
						defer wg.Done()

						// Each goroutine writes its portion of IDs
						for i := startIdx; i < endIdx; i++ {
							id := ids[i]
							value := int64(id * 10) // Simple value based on ID
							err := m.Add(id, value)
							if err != nil {
								b.Fatalf("Error adding to memtable: %v", err)
							}
						}
					}(start, end)
				}

				// Wait for all writers to finish
				wg.Wait()

				// Verify that all elements were written
				count := m.ActiveCount()
				if count != int64(numElements) {
					b.Fatalf("Expected %d elements, got %d", numElements, count)
				}
			}
		})
	}
}

// TestMemtableStripingPerformance runs a similar test to the benchmark but with direct
// measurement and reporting to make it easy to analyze results without benchmark tooling
func TestMemtableStripingPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping striping performance test in short mode")
	}

	// Number of elements to write
	const numElements = 100_000

	// Number of concurrent writers
	const numWriters = 36

	// Test different striping factors
	stripingFactors := []int{1, 2, 4, 8, 16, 32, 64, 128, 256}

	// Run tests with different access patterns
	testStripingWithPattern(t, stripingFactors, numElements, numWriters, "random")
	testStripingWithPattern(t, stripingFactors, numElements, numWriters, "sorted")
	testStripingWithPattern(t, stripingFactors, numElements, numWriters, "sequential")
}

// testStripingWithPattern tests striping performance with different access patterns
func testStripingWithPattern(t *testing.T, stripingFactors []int, numElements, numWriters int, pattern string) {
	t.Logf("===== Testing with %s access pattern =====", pattern)

	results := make(map[int]time.Duration)

	for _, stripes := range stripingFactors {
		// Create a memtable with the specified number of stripes
		opts := &MemtableOptions{
			NumStripes: stripes,
			MaxHeight:  DefaultMaxHeight,
			Seed:       time.Now().UnixNano(),
		}

		m := NewMemtable(opts)

		// Create all IDs beforehand
		ids := make([]uint64, numElements)
		for i := 0; i < numElements; i++ {
			ids[i] = uint64(i)
		}

		// Apply the access pattern
		switch pattern {
		case "random":
			// Shuffle the IDs to ensure random access patterns
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			r.Shuffle(len(ids), func(i, j int) {
				ids[i], ids[j] = ids[j], ids[i]
			})
		case "sorted":
			// IDs are already sorted
		case "sequential":
			// Each writer gets sequential chunks, but writers can interleave
			// This is a common pattern in many applications
			chunks := make([][]uint64, numWriters)
			for w := 0; w < numWriters; w++ {
				chunkSize := numElements / numWriters
				if w == numWriters-1 {
					chunkSize = numElements - (numWriters-1)*(numElements/numWriters)
				}
				chunks[w] = make([]uint64, chunkSize)
				startIdx := w * (numElements / numWriters)
				for i := 0; i < chunkSize; i++ {
					chunks[w][i] = uint64(startIdx + i)
				}
			}

			// Interleave the chunks
			idx := 0
			for w := 0; w < numWriters; w++ {
				for i := 0; i < len(chunks[w]); i++ {
					ids[idx] = chunks[w][i]
					idx++
				}
			}
		}

		// Divide IDs among writers
		idsPerWriter := numElements / numWriters

		// Use a WaitGroup to synchronize goroutines
		var wg sync.WaitGroup
		wg.Add(numWriters)

		// Start timer
		start := time.Now()

		// Start writer goroutines
		for w := 0; w < numWriters; w++ {
			startIdx := w * idsPerWriter
			endIdx := startIdx + idsPerWriter
			if w == numWriters-1 {
				endIdx = numElements // Ensure we use all IDs
			}

			go func(startIdx, endIdx int) {
				defer wg.Done()

				// Each goroutine writes its portion of IDs
				for i := startIdx; i < endIdx; i++ {
					id := ids[i]
					value := int64(id * 10) // Simple value based on ID
					err := m.Add(id, value)
					if err != nil {
						t.Fatalf("Error adding to memtable: %v", err)
					}
				}
			}(startIdx, endIdx)
		}

		// Wait for all writers to finish
		wg.Wait()

		// Record elapsed time
		elapsed := time.Since(start)
		results[stripes] = elapsed

		// Verify that all elements were written
		count := m.ActiveCount()
		if count != int64(numElements) {
			t.Fatalf("Expected %d elements, got %d", numElements, count)
		}

		// Report the result
		t.Logf("Stripes: %d, Time: %v, Throughput: %.2f ops/sec",
			stripes, elapsed, float64(numElements)/elapsed.Seconds())
	}

	// Find the fastest configuration
	var fastestStripes int
	var fastestTime time.Duration = time.Hour // Start with a large value

	for stripes, duration := range results {
		if duration < fastestTime {
			fastestTime = duration
			fastestStripes = stripes
		}
	}

	t.Logf("Fastest configuration: %d stripes, Time: %v, Throughput: %.2f ops/sec",
		fastestStripes, fastestTime, float64(numElements)/fastestTime.Seconds())

	// Compare all to the fastest
	for stripes, duration := range results {
		if stripes != fastestStripes {
			slowdown := float64(duration) / float64(fastestTime)
			t.Logf("Stripes: %d is %.2fx slower than the fastest configuration", stripes, slowdown)
		}
	}

	t.Logf("===== End of %s access pattern test =====\n", pattern)
}
