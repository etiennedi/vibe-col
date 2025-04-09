package multicol

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"vibe-lsm/pkg/col"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockIterator(t *testing.T) {
	testCases := []struct {
		name         string
		numEntries   int
		encodingType uint32
		blockSize    int
	}{
		{"SmallFileRawEncoding", 100, col.EncodingRaw, 1024},
		{"SmallFileVarIntEncoding", 100, col.EncodingVarIntBoth, 1024},
		{"MultiBlockRawEncoding", 10000, col.EncodingRaw, 16 * 1024},           // Multiple blocks with raw encoding
		{"MultiBlockVarIntEncoding", 10000, col.EncodingVarIntBoth, 16 * 1024}, // Multiple blocks with VarInt encoding
		{"LargeFileRawEncoding", 100000, col.EncodingRaw, 16 * 1024},           // Many blocks with raw encoding
		{"LargeFileVarIntEncoding", 100000, col.EncodingVarIntBoth, 16 * 1024}, // Many blocks with VarInt encoding
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a temporary file for this test
			tempFile, err := os.CreateTemp("", fmt.Sprintf("block_iterator_%s_*.col", tc.name))
			require.NoError(t, err)
			tempFilePath := tempFile.Name()
			tempFile.Close()
			defer os.Remove(tempFilePath)

			// Create test data
			createTestData(t, tempFilePath, tc.numEntries, tc.encodingType, tc.blockSize)

			// Open the file for reading
			reader, err := col.NewReader(tempFilePath)
			require.NoError(t, err)
			defer reader.Close()

			// Verify the file has the expected number of blocks
			blockCount := reader.BlockCount()
			t.Logf("Created file with %d entries in %d blocks using %s encoding",
				tc.numEntries, blockCount, getEncodingName(tc.encodingType))

			// Create a block iterator
			iterator := NewBlockIterator(reader)

			// Variables to track the iteration state
			var count int
			var prevID uint64
			var expectedValue int64

			// Iterate through all entries
			for iterator.Next() {
				id := iterator.CurrentID()
				value := iterator.CurrentValue()

				// Verify the ID and value relationship
				expectedValue = int64(id * 10)
				assert.Equal(t, expectedValue, value, "Value should match expected for ID %d", id)

				// Verify IDs are in ascending order
				if count > 0 {
					assert.Greater(t, id, prevID, "IDs should be in ascending order")
				}

				prevID = id
				count++
			}

			// Verify we iterated through all entries
			assert.Equal(t, tc.numEntries, count, "Iterator should iterate through all entries")

			// Run a full scan to verify the same count
			var totalEntries int
			for blockIdx := uint64(0); blockIdx < blockCount; blockIdx++ {
				ids, _, err := reader.GetPairs(blockIdx)
				require.NoError(t, err)
				totalEntries += len(ids)
			}
			assert.Equal(t, tc.numEntries, totalEntries, "Total entries from direct block access should match")
		})
	}
}

// TestBlockIteratorAdvancedScenarios tests the block iterator with more complex scenarios
func TestBlockIteratorAdvancedScenarios(t *testing.T) {
	// We'll adapt to what the col package actually does rather than our expectations
	testScenarios := []struct {
		name         string
		entries      [][2]uint64 // Array of [id, value] pairs
		encodingType uint32
	}{
		{
			name: "SparseIDs",
			entries: [][2]uint64{
				{1, 10}, {10, 100}, {100, 1000}, {1000, 10000}, {10000, 100000},
			},
			encodingType: col.EncodingRaw,
		},
		{
			name: "SparseIDsVarInt",
			entries: [][2]uint64{
				{1, 10}, {10, 100}, {100, 1000}, {1000, 10000}, {10000, 100000},
			},
			encodingType: col.EncodingVarIntBoth,
		},
		{
			name: "NonSequentialIDs",
			entries: [][2]uint64{
				{5, 50}, {3, 30}, {7, 70}, {2, 20}, {9, 90},
			},
			encodingType: col.EncodingRaw,
		},
		{
			name: "NonSequentialIDsVarInt",
			entries: [][2]uint64{
				{5, 50}, {3, 30}, {7, 70}, {2, 20}, {9, 90},
			},
			encodingType: col.EncodingVarIntBoth,
		},
		{
			name: "DuplicateIDs", // Writer behavior with duplicates can vary
			entries: [][2]uint64{
				{1, 10}, {2, 20}, {2, 25}, {3, 30}, {3, 35}, {4, 40},
			},
			encodingType: col.EncodingRaw,
		},
		{
			name: "DuplicateIDsVarInt", // Writer behavior with duplicates can vary
			entries: [][2]uint64{
				{1, 10}, {2, 20}, {2, 25}, {3, 30}, {3, 35}, {4, 40},
			},
			encodingType: col.EncodingVarIntBoth,
		},
	}

	for _, scenario := range testScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Create a temporary file
			tempFile, err := os.CreateTemp("", fmt.Sprintf("block_iterator_%s_*.col", scenario.name))
			require.NoError(t, err)
			tempFilePath := tempFile.Name()
			tempFile.Close()
			defer os.Remove(tempFilePath)

			// Create writer with specified encoding
			writer, err := col.NewBufferedWriter(tempFilePath, col.WithBufferedEncoding(scenario.encodingType))
			require.NoError(t, err)

			// Add entries
			for _, entry := range scenario.entries {
				err := writer.Add(entry[0], int64(entry[1]))
				require.NoError(t, err)
			}

			// Close the writer
			err = writer.Close()
			require.NoError(t, err)

			// Open reader
			reader, err := col.NewReader(tempFilePath)
			require.NoError(t, err)
			defer reader.Close()

			// Get all entries directly for comparison
			var allEntries []struct {
				ID    uint64
				Value int64
			}

			for blockIdx := uint64(0); blockIdx < reader.BlockCount(); blockIdx++ {
				ids, values, err := reader.GetPairs(blockIdx)
				require.NoError(t, err)

				for i := 0; i < len(ids); i++ {
					allEntries = append(allEntries, struct {
						ID    uint64
						Value int64
					}{
						ID:    ids[i],
						Value: values[i],
					})
				}
			}

			// Count unique IDs from what we read
			idMap := make(map[uint64]int64)
			for _, entry := range allEntries {
				idMap[entry.ID] = entry.Value
			}

			t.Logf("File contains %d entries (%d unique IDs) in %d blocks (%s)",
				len(allEntries), len(idMap), reader.BlockCount(), getEncodingName(scenario.encodingType))

			// Create and use a block iterator
			iterator := NewBlockIterator(reader)

			iteratedEntries := make([]struct {
				ID    uint64
				Value int64
			}, 0, len(allEntries))

			// Collect all entries from the iterator
			for iterator.Next() {
				iteratedEntries = append(iteratedEntries, struct {
					ID    uint64
					Value int64
				}{
					ID:    iterator.CurrentID(),
					Value: iterator.CurrentValue(),
				})
			}

			// Verify iterating through the BlockIterator gives us the same entries
			assert.Equal(t, len(allEntries), len(iteratedEntries), "Iterator should return all entries")

			// Verify entries match (both ordering and content)
			for i := 0; i < len(allEntries); i++ {
				assert.Equal(t, allEntries[i].ID, iteratedEntries[i].ID,
					"Entry %d ID mismatch - BlockIterator should preserve order", i)
				assert.Equal(t, allEntries[i].Value, iteratedEntries[i].Value,
					"Entry %d Value mismatch", i)
			}

			// For specific tests, verify basic ordering properties
			if strings.Contains(scenario.name, "Sparse") {
				// For sparse IDs, verify monotonically increasing
				for i := 1; i < len(iteratedEntries); i++ {
					assert.Greater(t, iteratedEntries[i].ID, iteratedEntries[i-1].ID,
						"Sparse IDs should be in ascending order")
				}
			}

			// When we know the writer sorts IDs, verify order
			if !strings.Contains(scenario.name, "NonSequential") && !strings.Contains(scenario.name, "Duplicate") {
				var lastID uint64
				for i, entry := range iteratedEntries {
					if i > 0 {
						assert.Greater(t, entry.ID, lastID, "IDs should be in ascending order")
					}
					lastID = entry.ID
				}
			}
		})
	}
}

// TestBlockIteratorEmpty tests the behavior of BlockIterator with a minimal file
func TestBlockIteratorEmpty(t *testing.T) {
	// Create a temporary file
	tempFile, err := os.CreateTemp("", "block_iterator_empty_*.col")
	require.NoError(t, err)
	tempFilePath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempFilePath)

	// Create writer with a minimal entry since completely empty files aren't allowed
	writer, err := col.NewBufferedWriter(tempFilePath)
	require.NoError(t, err)

	// Add a single entry
	err = writer.Add(1, 1)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Open reader
	reader, err := col.NewReader(tempFilePath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify the reader has one block
	assert.Equal(t, uint64(1), reader.BlockCount(), "Reader should have one block")

	// Create block iterator
	iterator := NewBlockIterator(reader)

	// Verify we can read the entry
	assert.True(t, iterator.Next(), "Iterator.Next() should return true for the first entry")
	assert.Equal(t, uint64(1), iterator.CurrentID(), "ID should be 1")
	assert.Equal(t, int64(1), iterator.CurrentValue(), "Value should be 1")

	// There should be no more entries
	assert.False(t, iterator.Next(), "Iterator.Next() should return false after the last entry")
}

// createTestData creates a file with test data
func createTestData(t *testing.T, filePath string, numEntries int, encodingType uint32, blockSize int) {
	// Create writer with specified encoding and block size
	writer, err := col.NewBufferedWriter(filePath,
		col.WithBufferedEncoding(encodingType),
		col.WithBufferedBlockSize(uint32(blockSize)))
	require.NoError(t, err)

	// Add entries to test with sequential IDs (using batches for efficiency)
	const batchSize = 10000
	for offset := 0; offset < numEntries; offset += batchSize {
		batchEndIdx := minInt(offset+batchSize, numEntries)
		batchSize := batchEndIdx - offset

		ids := make([]uint64, batchSize)
		values := make([]int64, batchSize)

		for i := 0; i < batchSize; i++ {
			id := uint64(offset + i + 1)
			ids[i] = id
			values[i] = int64(id * 10) // Simple value = id * 10 relationship
		}

		err := writer.BatchAdd(ids, values)
		require.NoError(t, err)
	}

	// Close the writer
	err = writer.Close()
	require.NoError(t, err)
}

// getEncodingName returns a human-readable name for the encoding type
func getEncodingName(encodingType uint32) string {
	switch encodingType {
	case col.EncodingRaw:
		return "Raw"
	case col.EncodingVarIntBoth:
		return "VarInt"
	default:
		return fmt.Sprintf("Unknown(%d)", encodingType)
	}
}

// min returns the minimum of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
