package col

const (
	headerSize       = 64       // Size of file header in bytes
	blockHeaderSize  = 96       // Size of block header in bytes
	blockLayoutSize  = 16       // Size of block layout section in bytes
	uint64Size       = 8        // Size of uint64 in bytes
	uint32Size       = 4        // Size of uint32 in bytes
	defaultBlockSize = 4 * 1024 // Default target block size (4KB)
)

// BlockStats holds statistics for a block
type BlockStats struct {
	MinID    uint64
	MaxID    uint64
	MinValue int64
	MaxValue int64
	Sum      int64
	Count    uint32
}
