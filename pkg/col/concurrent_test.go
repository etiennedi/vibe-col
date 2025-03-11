package col

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConcurrentReads verifies that the Reader can handle concurrent reads
// from multiple goroutines without issues.
func TestConcurrentReads(t *testing.T) {
	// Create a temporary file
	filename := "concurrent_test.col"
	defer os.Remove(filename)

	// Create a writer with raw encoding
	writer, err := NewWriter(filename, WithEncoding(EncodingRaw))
	assert.NoError(t, err)

	// Create 10 blocks with 100 entries each
	numBlocks := 10
	entriesPerBlock := 100
	for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
		ids := make([]uint64, entriesPerBlock)
		values := make([]int64, entriesPerBlock)

		// Fill with sequential data
		baseID := uint64(blockIdx * entriesPerBlock)
		baseValue := int64(blockIdx * entriesPerBlock * 10)
		for i := 0; i < entriesPerBlock; i++ {
			ids[i] = baseID + uint64(i)
			values[i] = baseValue + int64(i*10)
		}

		// Write the block
		err = writer.WriteBlock(ids, values)
		assert.NoError(t, err)
	}

	// Finalize and close the writer
	err = writer.FinalizeAndClose()
	assert.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(filename)
	assert.NoError(t, err)
	defer reader.Close()

	// Verify the block count
	assert.Equal(t, uint64(numBlocks), reader.BlockCount())

	// Create a wait group to synchronize goroutines
	var wg sync.WaitGroup
	// Create an error channel to collect errors
	errChan := make(chan error, numBlocks)
	// Create a mutex to protect the results map
	var resultsMutex sync.Mutex
	// Map to store results from each goroutine
	results := make(map[int][]int64)

	// Launch a goroutine for each block
	for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Read the block
			ids, values, err := reader.GetPairs(uint64(idx))
			if err != nil {
				errChan <- err
				return
			}

			// Verify the number of entries
			if len(ids) != entriesPerBlock || len(values) != entriesPerBlock {
				errChan <- err
				return
			}

			// Store the first few values for verification
			resultsMutex.Lock()
			results[idx] = values[:5]
			resultsMutex.Unlock()
		}(blockIdx)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		assert.NoError(t, err)
	}

	// Verify the results
	assert.Equal(t, numBlocks, len(results))
	for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
		baseValue := int64(blockIdx * entriesPerBlock * 10)
		expected := []int64{
			baseValue,
			baseValue + 10,
			baseValue + 20,
			baseValue + 30,
			baseValue + 40,
		}
		assert.Equal(t, expected, results[blockIdx])
	}
}

// TestConcurrentAggregation verifies that multiple goroutines can
// perform aggregation operations concurrently.
func TestConcurrentAggregation(t *testing.T) {
	// Create a temporary file
	filename := "concurrent_agg_test.col"
	defer os.Remove(filename)

	// Create a writer with raw encoding
	writer, err := NewWriter(filename, WithEncoding(EncodingRaw))
	assert.NoError(t, err)

	// Create 5 blocks with different data
	numBlocks := 5
	entriesPerBlock := 100
	totalEntries := numBlocks * entriesPerBlock
	expectedSum := int64(0)

	for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
		ids := make([]uint64, entriesPerBlock)
		values := make([]int64, entriesPerBlock)

		// Fill with sequential data
		baseID := uint64(blockIdx * entriesPerBlock)
		baseValue := int64(blockIdx * 1000)
		for i := 0; i < entriesPerBlock; i++ {
			ids[i] = baseID + uint64(i)
			values[i] = baseValue + int64(i)
			expectedSum += values[i]
		}

		// Write the block
		err = writer.WriteBlock(ids, values)
		assert.NoError(t, err)
	}

	// Finalize and close the writer
	err = writer.FinalizeAndClose()
	assert.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(filename)
	assert.NoError(t, err)
	defer reader.Close()

	// Verify the block count
	assert.Equal(t, uint64(numBlocks), reader.BlockCount())

	// Number of concurrent operations
	numConcurrent := 10
	var wg sync.WaitGroup
	results := make([]AggregateResult, numConcurrent)

	// Launch multiple goroutines to perform aggregation
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = reader.Aggregate()
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify all results are the same
	expected := AggregateResult{
		Count: totalEntries,
		Min:   0,                                               // First value in first block
		Max:   int64((numBlocks-1)*1000 + entriesPerBlock - 1), // Last value in last block
		Sum:   expectedSum,
		Avg:   float64(expectedSum) / float64(totalEntries),
	}

	for i := 0; i < numConcurrent; i++ {
		assert.Equal(t, expected.Count, results[i].Count)
		assert.Equal(t, expected.Min, results[i].Min)
		assert.Equal(t, expected.Max, results[i].Max)
		assert.Equal(t, expected.Sum, results[i].Sum)
		assert.InDelta(t, expected.Avg, results[i].Avg, 0.001)
	}
}
