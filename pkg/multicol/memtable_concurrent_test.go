package multicol

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestMemtableConcurrency tests if the memtable implementation
// correctly handles concurrent operations
func TestMemtableConcurrency(t *testing.T) {
	m := NewMemtable(nil)

	// Number of concurrent writers
	const numWriters = 36

	// Number of operations per writer
	const numOps = 10000

	// Use a WaitGroup to wait for all goroutines
	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Start time
	start := time.Now()

	// Launch writers
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()

			// Create a local random source
			r := rand.New(rand.NewSource(time.Now().UnixNano()))

			// Each writer adds entries in its own ID range
			idBase := writerID * numOps

			for j := 0; j < numOps; j++ {
				id := uint64(idBase + j)
				value := int64(r.Intn(10000))

				err := m.Add(id, value)
				if err != nil {
					t.Errorf("Error adding to memtable: %v", err)
				}

				// Occasionally delete entries
				if j > 0 && j%10 == 0 {
					deleteID := uint64(idBase + j - 1)
					m.Delete(deleteID)
				}
			}
		}(i)
	}

	// Wait for all writers to finish
	wg.Wait()

	// Record elapsed time
	elapsed := time.Since(start)

	// Calculate operations per second
	opsPerSecond := float64(numWriters*numOps) / elapsed.Seconds()

	t.Logf("Completed %d operations in %v (%.2f ops/sec)",
		numWriters*numOps, elapsed, opsPerSecond)

	// Verify expected active count
	// Each writer adds numOps entries and deletes numOps/10 entries
	expectedActive := int64(numWriters * (numOps - numOps/10))
	actualActive := m.ActiveCount()

	t.Logf("Active entries: %d, Expected: %d", actualActive, expectedActive)

	// Flush entries to a file to make sure they can be persisted
	written, err := m.Flush("dummy_path.col")
	if err != nil {
		t.Errorf("Error flushing memtable: %v", err)
	}

	t.Logf("Flushed %d entries", written)
}
