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

	// FIRST: Test sequential reading of each block
	// This validates the basic functionality before attempting concurrent access
	sequentialResults := make(map[int][]int64)
	for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
		// Read the block
		ids, values, err := reader.GetPairs(uint64(blockIdx))
		assert.NoError(t, err)

		// Verify the number of entries
		assert.Equal(t, entriesPerBlock, len(ids))
		assert.Equal(t, entriesPerBlock, len(values))

		// Store first 5 values for comparison
		valuesCopy := make([]int64, 5)
		copy(valuesCopy, values[:5])
		sequentialResults[blockIdx] = valuesCopy

		// Verify the actual data for the first few entries
		baseValue := int64(blockIdx * entriesPerBlock * 10)
		expected := []int64{
			baseValue,
			baseValue + 10,
			baseValue + 20,
			baseValue + 30,
			baseValue + 40,
		}
		actual := values[:5]
		assert.Equal(t, expected, actual, "Sequential block %d data mismatch", blockIdx)
	}

	// Now test true concurrent reads
	// Create a channel to collect results and a wait group to synchronize
	type resultPair struct {
		blockIdx int
		values   []int64
	}
	resultChan := make(chan resultPair, numBlocks)
	var wg sync.WaitGroup

	// Launch a goroutine for each block
	for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Read the block
			ids, values, err := reader.GetPairs(uint64(idx))
			if err != nil {
				t.Errorf("Error reading block %d: %v", idx, err)
				return
			}

			// Verify the number of entries
			if len(ids) != entriesPerBlock || len(values) != entriesPerBlock {
				t.Errorf("Block %d: expected %d entries, got %d ids and %d values",
					idx, entriesPerBlock, len(ids), len(values))
				return
			}

			// Make a copy of the first few values to return
			valuesCopy := make([]int64, 5)
			copy(valuesCopy, values[:5])

			// Send results to the channel
			resultChan <- resultPair{idx, valuesCopy}
		}(blockIdx)
	}

	// Close the channel once all goroutines are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect and verify results
	concurrentResults := make(map[int][]int64)
	for result := range resultChan {
		concurrentResults[result.blockIdx] = result.values
	}

	// Verify all blocks were processed
	assert.Equal(t, numBlocks, len(concurrentResults))

	// Compare concurrent results with sequential results
	for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
		baseValue := int64(blockIdx * entriesPerBlock * 10)
		expected := []int64{
			baseValue,
			baseValue + 10,
			baseValue + 20,
			baseValue + 30,
			baseValue + 40,
		}

		// Results should match both expected values and sequential results
		assert.Equal(t, expected, concurrentResults[blockIdx],
			"Block %d: Expected vs concurrent mismatch", blockIdx)
		assert.Equal(t, sequentialResults[blockIdx], concurrentResults[blockIdx],
			"Block %d: Sequential vs concurrent mismatch", blockIdx)
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
