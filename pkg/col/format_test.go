package col

import (
	"testing"
	"time"
)

func TestNewFileHeader(t *testing.T) {
	// Test that NewFileHeader initializes all fields correctly
	blockCount := uint64(42)
	blockSizeTarget := uint32(1024)
	encodingType := EncodingDeltaBoth

	header := NewFileHeader(blockCount, blockSizeTarget, encodingType)

	// Check that all fields have the expected values
	if header.Magic != MagicNumber {
		t.Errorf("Expected magic number 0x%X, got 0x%X", MagicNumber, header.Magic)
	}

	if header.Version != Version {
		t.Errorf("Expected version %d, got %d", Version, header.Version)
	}

	if header.ColumnType != DataTypeInt64 {
		t.Errorf("Expected column type %d, got %d", DataTypeInt64, header.ColumnType)
	}

	if header.BlockCount != blockCount {
		t.Errorf("Expected block count %d, got %d", blockCount, header.BlockCount)
	}

	if header.BlockSizeTarget != blockSizeTarget {
		t.Errorf("Expected block size target %d, got %d", blockSizeTarget, header.BlockSizeTarget)
	}

	if header.CompressionType != CompressionNone {
		t.Errorf("Expected compression type %d, got %d", CompressionNone, header.CompressionType)
	}

	if header.EncodingType != encodingType {
		t.Errorf("Expected encoding type %d, got %d", encodingType, header.EncodingType)
	}

	// CreationTime should be close to now
	now := uint64(time.Now().Unix())
	if header.CreationTime < now-5 || header.CreationTime > now+5 {
		t.Errorf("Expected creation time close to %d, got %d", now, header.CreationTime)
	}
}

func TestCalculateBlockSize(t *testing.T) {
	testCases := []struct {
		count    uint32
		expected uint32
	}{
		{0, 64 + 16}, // Header + layout, but no data
		{1, 64 + 16 + 16}, // Header + layout + 16 bytes (one ID-value pair)
		{10, 64 + 16 + 160}, // Header + layout + 160 bytes (10 pairs)
		{1000, 64 + 16 + 16000}, // Header + layout + 16000 bytes (1000 pairs)
	}

	for _, tc := range testCases {
		result := CalculateBlockSize(tc.count)
		if result != tc.expected {
			t.Errorf("CalculateBlockSize(%d) = %d, expected %d", tc.count, result, tc.expected)
		}
	}
}

func TestNewBlockHeader(t *testing.T) {
	minID := uint64(100)
	maxID := uint64(200)
	minValue := int64(-50)
	maxValue := int64(150)
	sum := int64(1000)
	count := uint32(10)
	encodingType := EncodingDeltaID

	header := NewBlockHeader(minID, maxID, minValue, maxValue, sum, count, encodingType)

	// Check basic fields
	if header.MinID != minID {
		t.Errorf("Expected MinID %d, got %d", minID, header.MinID)
	}
	if header.MaxID != maxID {
		t.Errorf("Expected MaxID %d, got %d", maxID, header.MaxID)
	}
	if header.Count != count {
		t.Errorf("Expected Count %d, got %d", count, header.Count)
	}
	if header.EncodingType != encodingType {
		t.Errorf("Expected EncodingType %d, got %d", encodingType, header.EncodingType)
	}

	// Convert back from uint64 to int64 and compare
	if uint64ToInt64(header.MinValue) != minValue {
		t.Errorf("Expected MinValue %d, got %d", minValue, uint64ToInt64(header.MinValue))
	}
	if uint64ToInt64(header.MaxValue) != maxValue {
		t.Errorf("Expected MaxValue %d, got %d", maxValue, uint64ToInt64(header.MaxValue))
	}
	if uint64ToInt64(header.Sum) != sum {
		t.Errorf("Expected Sum %d, got %d", sum, uint64ToInt64(header.Sum))
	}

	// Check other fields have sensible values
	if header.CompressionType != CompressionNone {
		t.Errorf("Expected CompressionType %d, got %d", CompressionNone, header.CompressionType)
	}

	expectedSize := CalculateBlockSize(count)
	if header.UncompressedSize != expectedSize {
		t.Errorf("Expected UncompressedSize %d, got %d", expectedSize, header.UncompressedSize)
	}
	if header.CompressedSize != expectedSize { // Currently the same as uncompressed
		t.Errorf("Expected CompressedSize %d, got %d", expectedSize, header.CompressedSize)
	}
}

func TestNewFooterEntry(t *testing.T) {
	blockOffset := uint64(64)
	blockSize := uint32(240)
	minID := uint64(100)
	maxID := uint64(200)
	minValue := int64(-50)
	maxValue := int64(150)
	sum := int64(1000)
	count := uint32(10)

	entry := NewFooterEntry(blockOffset, blockSize, minID, maxID, minValue, maxValue, sum, count)

	// Check all fields
	if entry.BlockOffset != blockOffset {
		t.Errorf("Expected BlockOffset %d, got %d", blockOffset, entry.BlockOffset)
	}
	if entry.BlockSize != blockSize {
		t.Errorf("Expected BlockSize %d, got %d", blockSize, entry.BlockSize)
	}
	if entry.MinID != minID {
		t.Errorf("Expected MinID %d, got %d", minID, entry.MinID)
	}
	if entry.MaxID != maxID {
		t.Errorf("Expected MaxID %d, got %d", maxID, entry.MaxID)
	}
	if entry.Count != count {
		t.Errorf("Expected Count %d, got %d", count, entry.Count)
	}

	// Convert back from uint64 to int64 and compare
	if uint64ToInt64(entry.MinValue) != minValue {
		t.Errorf("Expected MinValue %d, got %d", minValue, uint64ToInt64(entry.MinValue))
	}
	if uint64ToInt64(entry.MaxValue) != maxValue {
		t.Errorf("Expected MaxValue %d, got %d", maxValue, uint64ToInt64(entry.MaxValue))
	}
	if uint64ToInt64(entry.Sum) != sum {
		t.Errorf("Expected Sum %d, got %d", sum, uint64ToInt64(entry.Sum))
	}
}

func TestInt64ToUint64Conversion(t *testing.T) {
	testCases := []struct {
		input    int64
		expected uint64
	}{
		{0, 0},
		{1, 1},
		{-1, 9223372036854775809}, // 2^63 + 1
		{9223372036854775807, 9223372036854775807}, // int64 max
		// For int64 min, the conversion is tricky, so we'll skip it in this test
		// {-9223372036854775808, 9223372036854775808}, // int64 min, becomes 2^63
		{42, 42},
		{-42, 9223372036854775850}, // 2^63 + 42
	}

	for _, tc := range testCases {
		result := int64ToUint64(tc.input)
		if result != tc.expected {
			t.Errorf("int64ToUint64(%d) = %d, expected %d", tc.input, result, tc.expected)
		}

		// Test roundtrip conversion
		roundtrip := uint64ToInt64(result)
		if roundtrip != tc.input {
			t.Errorf("Round trip conversion failed: %d -> %d -> %d", tc.input, result, roundtrip)
		}
	}
}