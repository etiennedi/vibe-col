package store

import (
	"os"
	"testing"
	"time"
	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// TestAggregationAcrossSegments tests aggregation operations across multiple segments
func TestAggregationAcrossSegments(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-aggregation-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store with small memtable size for easier testing
	options := DefaultOptions(tempDir)
	options.MemtableSize = 50 // Flush after 50 entries

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Add the first batch of data (1-50)
	for i := uint64(1); i <= 50; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Wait for flush to complete since we hit the memtable size limit
	time.Sleep(100 * time.Millisecond)

	// Add the second batch of data (51-100) to create a second segment
	for i := uint64(51); i <= 100; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Wait for flush to complete
	time.Sleep(100 * time.Millisecond)

	// Update some values from the first segment
	for i := uint64(25); i <= 75; i++ {
		err := store.Add(i, int64(i*20)) // Double the values
		require.NoError(t, err)
	}

	// Force flush to create a third segment with updates
	store.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Test unfiltered aggregation
	t.Run("Unfiltered aggregation", func(t *testing.T) {
		result, err := store.Aggregate(EmptyAggregateOptions())
		require.NoError(t, err)

		// Calculate expected results
		expectedCount := 100

		expectedSum := int64(0)
		for i := int64(1); i <= 24; i++ {
			expectedSum += i * 10
		}
		for i := int64(25); i <= 75; i++ {
			expectedSum += i * 20
		}
		for i := int64(76); i <= 100; i++ {
			expectedSum += i * 10
		}

		assert.Equal(t, expectedCount, result.Count)
		assert.Equal(t, expectedSum, result.Sum)
		assert.Equal(t, float64(expectedSum)/float64(expectedCount), result.Avg)
		assert.Equal(t, int64(10), result.Min)   // Value of ID 1
		assert.Equal(t, int64(1500), result.Max) // Value of ID 75 (75*20)
	})

	// Test filtered aggregation
	t.Run("Filtered aggregation", func(t *testing.T) {
		// Create filter for IDs 10-30
		filter := sroar.NewBitmap()
		for i := uint64(10); i <= 30; i++ {
			filter.Set(i)
		}

		result, err := store.Aggregate(col.AggregateOptions{
			Filter: filter,
		})
		require.NoError(t, err)

		// Calculate expected results
		expectedCount := 21 // IDs 10-30 (inclusive)

		expectedSum := int64(0)
		for i := int64(10); i <= 24; i++ {
			expectedSum += i * 10
		}
		for i := int64(25); i <= 30; i++ {
			expectedSum += i * 20
		}

		assert.Equal(t, expectedCount, result.Count)
		assert.Equal(t, expectedSum, result.Sum)
		assert.Equal(t, float64(expectedSum)/float64(expectedCount), result.Avg)
		assert.Equal(t, int64(100), result.Min) // Value of ID 10
		assert.Equal(t, int64(600), result.Max) // Value of ID 30 (30*20)
	})

	// Test advanced aggregation options
	t.Run("AggregateWithOptions", func(t *testing.T) {
		// Create filter for IDs divisible by 10
		filter := sroar.NewBitmap()
		for i := uint64(10); i <= 100; i += 10 {
			filter.Set(i)
		}

		result, err := store.AggregateWithOptions(AggregateOptions{
			Filter: filter,
		})
		require.NoError(t, err)

		// Calculate expected results
		expectedCount := 10 // 10, 20, 30, 40, 50, 60, 70, 80, 90, 100

		expectedSum := int64(0)
		// IDs 10, 20 have original values
		expectedSum += 10 * 10
		expectedSum += 20 * 10
		// IDs 30, 40, 50, 60, 70 have doubled values
		expectedSum += 30 * 20
		expectedSum += 40 * 20
		expectedSum += 50 * 20
		expectedSum += 60 * 20
		expectedSum += 70 * 20
		// IDs 80, 90, 100 have original values
		expectedSum += 80 * 10
		expectedSum += 90 * 10
		expectedSum += 100 * 10

		assert.Equal(t, expectedCount, result.Count)
		assert.Equal(t, expectedSum, result.Sum)
		assert.InDelta(t, float64(expectedSum)/float64(expectedCount), result.Avg, 0.01)
	})
}

// TestDeletionsWithAggregation tests how deletions affect aggregation results
func TestDeletionsWithAggregation(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-deletion-aggregation-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store
	options := DefaultOptions(tempDir)
	options.MemtableSize = 50 // Flush after 50 entries

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Add data
	for i := uint64(1); i <= 100; i++ {
		err := store.Add(i, int64(i*10))
		require.NoError(t, err)
	}

	// Force flush to ensure all data is in segments
	store.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Delete some entries
	for i := uint64(25); i <= 50; i++ {
		err := store.Delete(i)
		require.NoError(t, err)
	}

	// Force flush the deletions
	store.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Test aggregation with deletions
	t.Run("Aggregate after deletions", func(t *testing.T) {
		result, err := store.Aggregate(EmptyAggregateOptions())
		require.NoError(t, err)

		// We deleted IDs 25-50, so we should have 74 entries (100 - 26)
		expectedCount := 74

		expectedSum := int64(0)
		for i := int64(1); i <= 24; i++ {
			expectedSum += i * 10
		}
		for i := int64(51); i <= 100; i++ {
			expectedSum += i * 10
		}

		assert.Equal(t, expectedCount, result.Count)
		assert.Equal(t, expectedSum, result.Sum)
		assert.Equal(t, float64(expectedSum)/float64(expectedCount), result.Avg)
	})

	// Add more data and test with a mix of original, deleted, and new values
	for i := uint64(30); i <= 40; i++ {
		// Re-add some of the deleted values
		err := store.Add(i, int64(i*30)) // Triple the original values
		require.NoError(t, err)
	}

	// Force flush
	store.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	t.Run("Aggregate after re-adding some deleted entries", func(t *testing.T) {
		result, err := store.Aggregate(EmptyAggregateOptions())
		require.NoError(t, err)

		// Now we should have 85 entries (74 + 11 re-added)
		expectedCount := 85

		expectedSum := int64(0)
		for i := int64(1); i <= 24; i++ {
			expectedSum += i * 10
		}
		for i := int64(30); i <= 40; i++ {
			expectedSum += i * 30 // These were re-added with triple values
		}
		for i := int64(51); i <= 100; i++ {
			expectedSum += i * 10
		}

		assert.Equal(t, expectedCount, result.Count)
		assert.Equal(t, expectedSum, result.Sum)
		assert.Equal(t, float64(expectedSum)/float64(expectedCount), result.Avg)
	})
}

// TestParallelAggregation tests aggregation with parallel processing
func TestParallelAggregation(t *testing.T) {
	// Skip for short test runs
	if testing.Short() {
		t.Skip("Skipping parallel aggregation test in short mode")
	}

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "vibe-store-parallel-aggregation-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create store
	options := DefaultOptions(tempDir)
	options.MemtableSize = 1000 // Larger size for meaningful parallel testing

	store, err := NewVibeStore(options)
	require.NoError(t, err)
	defer store.Close()

	// Add a large amount of data to make parallel processing worthwhile
	for i := uint64(1); i <= 5000; i++ {
		err := store.Add(i, int64(i))
		require.NoError(t, err)
	}

	// Force flush
	store.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Add more data with updates
	for i := uint64(2500); i <= 7500; i++ {
		err := store.Add(i, int64(i*2))
		require.NoError(t, err)
	}

	// Force flush
	store.ForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Calculate expected results
	expectedCount := 7500
	expectedSum := int64(0)
	for i := int64(1); i <= 2499; i++ {
		expectedSum += i
	}
	for i := int64(2500); i <= 7500; i++ {
		expectedSum += i * 2
	}

	// Test sequential aggregation first as baseline
	t.Run("Sequential aggregation", func(t *testing.T) {
		start := time.Now()
		result, err := store.AggregateWithOptions(AggregateOptions{
			Parallel: 0, // Sequential
		})
		sequentialDuration := time.Since(start)
		t.Logf("Sequential aggregation took %v", sequentialDuration)

		require.NoError(t, err)
		assert.Equal(t, expectedCount, result.Count)
		assert.Equal(t, expectedSum, result.Sum)
	})

	// Test parallel aggregation
	t.Run("Parallel aggregation", func(t *testing.T) {
		start := time.Now()
		result, err := store.AggregateWithOptions(AggregateOptions{
			Parallel: -1, // Use GOMAXPROCS
		})
		parallelDuration := time.Since(start)
		t.Logf("Parallel aggregation took %v", parallelDuration)

		require.NoError(t, err)
		assert.Equal(t, expectedCount, result.Count)
		assert.Equal(t, expectedSum, result.Sum)

		// We don't strictly check for performance improvement here because
		// in small test environments the overhead might outweigh the benefits
	})
}
