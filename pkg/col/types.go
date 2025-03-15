package col

// BlockStats holds statistics for a block
type BlockStats struct {
	MinID    uint64
	MaxID    uint64
	MinValue int64
	MaxValue int64
	Sum      int64
	Count    uint32
}
