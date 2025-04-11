package col

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLevelField(t *testing.T) {
	// Create a temporary directory for our test files
	tempDir := t.TempDir()

	testCases := []struct {
		name     string
		level    uint16
		useLevel bool
	}{
		{
			name:     "Default Level (0)",
			level:    0,
			useLevel: false,
		},
		{
			name:     "Set Level 1",
			level:    1,
			useLevel: true,
		},
		{
			name:     "Set Level 2",
			level:    2,
			useLevel: true,
		},
		{
			name:     "Set Level 10",
			level:    10,
			useLevel: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test with standard Writer
			t.Run("StandardWriter", func(t *testing.T) {
				filePath := filepath.Join(tempDir, "level_test_standard_"+tc.name+".col")

				// Create writer options
				options := []WriterOption{WithEncoding(EncodingRaw)}
				if tc.useLevel {
					options = append(options, WithLevel(tc.level))
				}

				// Create writer
				writer, err := NewWriter(filePath, options...)
				require.NoError(t, err)

				// Check level is set correctly in writer
				assert.Equal(t, tc.level, writer.Level())

				// Write some data
				ids := []uint64{1, 2, 3, 4, 5}
				values := []int64{10, 20, 30, 40, 50}
				err = writer.WriteBlock(ids, values)
				require.NoError(t, err)

				// Close writer
				err = writer.FinalizeAndClose()
				require.NoError(t, err)

				// Open file for reading
				reader, err := NewReader(filePath)
				require.NoError(t, err)
				defer reader.Close()

				// Verify level is preserved
				assert.Equal(t, tc.level, reader.Level())
			})

			// Test with BufferedWriter
			t.Run("BufferedWriter", func(t *testing.T) {
				filePath := filepath.Join(tempDir, "level_test_buffered_"+tc.name+".col")

				// Create writer options
				options := []BufferedWriterOption{WithBufferedEncoding(EncodingRaw)}
				if tc.useLevel {
					options = append(options, WithBufferedLevel(tc.level))
				}

				// Create writer
				writer, err := NewBufferedWriter(filePath, options...)
				require.NoError(t, err)

				// Check level is set correctly in writer
				assert.Equal(t, tc.level, writer.Level())

				// Write some data
				for i := uint64(1); i <= 5; i++ {
					err = writer.Add(i, int64(i*10))
					require.NoError(t, err)
				}

				// Close writer
				err = writer.Close()
				require.NoError(t, err)

				// Open file for reading
				reader, err := NewReader(filePath)
				require.NoError(t, err)
				defer reader.Close()

				// Verify level is preserved
				assert.Equal(t, tc.level, reader.Level())
			})
		})
	}
}

// TestLevelBackwardCompatibility verifies that older files without a level field
// are read with a default level of 0.
func TestLevelBackwardCompatibility(t *testing.T) {
	// Create a temporary directory for our test files
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "backwards_compat_test.col")

	// Write a file with a standard writer
	writer, err := NewWriter(filePath)
	require.NoError(t, err)

	// Write some data
	ids := []uint64{1, 2, 3}
	values := []int64{10, 20, 30}
	err = writer.WriteBlock(ids, values)
	require.NoError(t, err)

	// Close writer
	err = writer.FinalizeAndClose()
	require.NoError(t, err)

	// Open the file and manually zero out the level bytes at the correct offset
	// to simulate an old file without the level field
	fileBytes, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// The level field is after DeletedBitmapSize, which is 8 bytes and starts at offset 76
	// So level starts at offset 84
	levelOffset := 84
	// Zero out two bytes (uint16)
	fileBytes[levelOffset] = 0
	fileBytes[levelOffset+1] = 0

	// Write the modified bytes back
	err = os.WriteFile(filePath, fileBytes, 0644)
	require.NoError(t, err)

	// Now open and read the file
	reader, err := NewReader(filePath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify the level is read as 0
	assert.Equal(t, uint16(0), reader.Level())
}

// TestLevelInHeaderFormat ensures the level field is at the correct position
// in the header and doesn't interfere with other fields
func TestLevelInHeaderFormat(t *testing.T) {
	// Create a test header
	header := NewFileHeader(100, 4096, EncodingRaw)

	// Set a non-zero level
	header.Level = 5

	// Serialize the header
	buf := header.Serialize()

	// Create a new header from the buffer
	var newHeader FileHeader
	err := newHeader.Deserialize(buf)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, header.Magic, newHeader.Magic)
	assert.Equal(t, header.Version, newHeader.Version)
	assert.Equal(t, header.BlockCount, newHeader.BlockCount)
	assert.Equal(t, header.BlockSizeTarget, newHeader.BlockSizeTarget)
	assert.Equal(t, header.CompressionType, newHeader.CompressionType)
	assert.Equal(t, header.EncodingType, newHeader.EncodingType)
	assert.Equal(t, header.CreationTime, newHeader.CreationTime)
	assert.Equal(t, header.BitmapOffset, newHeader.BitmapOffset)
	assert.Equal(t, header.BitmapSize, newHeader.BitmapSize)
	assert.Equal(t, header.DeletedBitmapOffset, newHeader.DeletedBitmapOffset)
	assert.Equal(t, header.DeletedBitmapSize, newHeader.DeletedBitmapSize)
	assert.Equal(t, header.Level, newHeader.Level)
}
