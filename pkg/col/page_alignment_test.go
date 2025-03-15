package col

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageAlignment(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "col-page-alignment-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a test file
	filePath := filepath.Join(tempDir, "test.col")

	// Create a writer with correct options
	writer, err := NewWriter(filePath, WithEncoding(EncodingRaw))
	require.NoError(t, err)

	// Write 3 blocks with varying sizes
	// Block 0: Small block
	ids1 := make([]uint64, 10)
	values1 := make([]int64, 10)
	for i := 0; i < 10; i++ {
		ids1[i] = uint64(i)
		values1[i] = int64(i * 100)
	}
	err = writer.WriteBlock(ids1, values1)
	require.NoError(t, err)

	// Block 1: Medium block
	ids2 := make([]uint64, 50)
	values2 := make([]int64, 50)
	for i := 0; i < 50; i++ {
		ids2[i] = uint64(i + 100)
		values2[i] = int64((i + 100) * 100)
	}
	err = writer.WriteBlock(ids2, values2)
	require.NoError(t, err)

	// Block 2: Large block
	ids3 := make([]uint64, 100)
	values3 := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids3[i] = uint64(i + 1000)
		values3[i] = int64((i + 1000) * 100)
	}
	err = writer.WriteBlock(ids3, values3)
	require.NoError(t, err)

	// Finalize the file
	err = writer.Finalize()
	require.NoError(t, err)

	// Open the file for reading
	reader, err := NewReader(filePath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify block count
	assert.Equal(t, uint64(3), reader.BlockCount(), "Expected 3 blocks")

	// Access block positions through reader.blockIndex
	var blockPositions []uint64
	for i := 0; i < int(reader.BlockCount()); i++ {
		// We need to add a method to get block positions
		ids, _, err := reader.GetPairs(uint64(i))
		require.NoError(t, err)
		require.NotEmpty(t, ids, "Block should contain data")

		// Use direct field access for testing (not ideal but works for test)
		blockOffset := reader.blockIndex[i].BlockOffset
		blockPositions = append(blockPositions, blockOffset)

		t.Logf("Block %d: offset=%d", i, blockOffset)
	}

	// Verify page alignment of blocks
	for i, blockOffset := range blockPositions {
		if i == 0 {
			// First block starts at header size (64), not page-aligned
			assert.Equal(t, uint64(headerSize), blockOffset, "Block 0 should start at header size")
		} else {
			// Check if block offset is page-aligned (multiple of PageSize)
			remainder := blockOffset % uint64(PageSize)
			assert.Equal(t, uint64(0), remainder,
				"Block %d offset %d should be page-aligned (remainder: %d)",
				i, blockOffset, remainder)
		}
	}
}

func TestCalculatePadding(t *testing.T) {
	testCases := []struct {
		position int64
		pageSize int64
		expected int64
	}{
		{0, 4096, 0},               // Already aligned
		{4096, 4096, 0},            // Already aligned
		{4097, 4096, 4095},         // Need 4095 bytes to align
		{8191, 4096, 1},            // Need 1 byte to align
		{8192, 4096, 0},            // Already aligned
		{1234, 4096, 4096 - 1234},  // Need (4096 - 1234) bytes to align
		{4096*3 + 100, 4096, 3996}, // Need 3996 bytes to align
	}

	for i, tc := range testCases {
		result := calculatePadding(tc.position, tc.pageSize)
		if result != tc.expected {
			t.Errorf("Test case %d: calculatePadding(%d, %d) = %d, expected %d",
				i, tc.position, tc.pageSize, result, tc.expected)
		}
	}
}
