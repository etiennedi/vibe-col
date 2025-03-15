package col

// Constants for file format
const (
	// MagicNumberStr is the string representation of the magic number
	MagicNumberStr = "VIBE_COL"

	// Size constants
	headerSize      = 64
	blockHeaderSize = 64
	blockLayoutSize = 16

	// Default block size (target)
	defaultBlockSize = 4096 * 4 // 16KB

	// Field sizes
	uint32Size = 4
	uint64Size = 8

	// PageSize is the alignment boundary for blocks (4KB)
	PageSize int64 = 4096
)

// calculatePadding calculates the number of bytes needed to align to the next page boundary
func calculatePadding(currentPosition int64, pageSize int64) int64 {
	if currentPosition%pageSize == 0 {
		return 0 // Already aligned
	}
	return pageSize - (currentPosition % pageSize)
}
