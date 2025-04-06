package multicol

import (
	"math/rand"
	"sync"
	"testing"
)

// BenchmarkMemtableRandomAccess benchmarks concurrent writes to the memtable implementation
// with random access patterns
func BenchmarkMemtableRandomAccess(b *testing.B) {
	const numElements = 100000
	const numWriters = 36

	// Create IDs
	ids := make([]uint64, numElements)
	for i := 0; i < numElements; i++ {
		ids[i] = uint64(i + 1)
	}

	b.ResetTimer()
	b.Run("Memtable", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Initialize a new memtable for each iteration
			mt := NewMemtable(nil)

			// Shuffle IDs for random access pattern
			rand.Shuffle(len(ids), func(i, j int) {
				ids[i], ids[j] = ids[j], ids[i]
			})

			// Divide IDs among writers
			idsPerWriter := numElements / numWriters
			var wg sync.WaitGroup
			b.StartTimer()

			// Start goroutines to write to the memtable concurrently
			for w := 0; w < numWriters; w++ {
				wg.Add(1)
				start := w * idsPerWriter
				end := start + idsPerWriter
				if w == numWriters-1 {
					end = numElements // Last writer takes any remainder
				}

				go func(startIdx, endIdx int) {
					defer wg.Done()
					for j := startIdx; j < endIdx; j++ {
						id := ids[j]
						err := mt.Add(id, int64(id*10)) // Simple value based on ID
						if err != nil {
							b.Error("Failed to add entry:", err)
							return
						}
					}
				}(start, end)
			}

			// Wait for all writers to complete
			wg.Wait()

			// Verify count
			active := mt.ActiveCount()
			if active != int64(numElements) {
				b.Errorf("Expected %d active elements, got %d", numElements, active)
			}
		}
	})
}

// TestCompareImplementations is now obsolete since we only have one implementation
// We'll reimplement this as a simple benchmark test

// BenchmarkSortedMemtableOperation benchmarks the memtable with sorted IDs
func BenchmarkSortedMemtableOperation(b *testing.B) {
	const numElements = 100000
	const numWriters = 36

	// Create IDs - already sorted
	ids := make([]uint64, numElements)
	for i := 0; i < numElements; i++ {
		ids[i] = uint64(i + 1)
	}

	// Divide IDs among writers
	idsPerWriter := numElements / numWriters

	// Benchmark with sorted IDs
	b.Run("Memtable-Sorted", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mt := NewMemtable(nil)
			var wg sync.WaitGroup

			// Start goroutines to write to the memtable concurrently
			for w := 0; w < numWriters; w++ {
				wg.Add(1)
				start := w * idsPerWriter
				end := start + idsPerWriter
				if w == numWriters-1 {
					end = numElements // Last writer takes any remainder
				}

				go func(startIdx, endIdx int) {
					defer wg.Done()
					for j := startIdx; j < endIdx; j++ {
						id := ids[j]
						err := mt.Add(id, int64(id*10)) // Simple value based on ID
						if err != nil {
							b.Error("Failed to add entry:", err)
							return
						}
					}
				}(start, end)
			}

			// Wait for all writers to complete
			wg.Wait()

			// Verify count
			active := mt.ActiveCount()
			if active != int64(numElements) {
				b.Errorf("Expected %d active elements, got %d", numElements, active)
			}
		}
	})
}
