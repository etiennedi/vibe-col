package multicol

import "vibe-lsm/pkg/col"

// BlockIterator provides an iterator interface over column file blocks
type BlockIterator struct {
	reader        *col.Reader
	currentBlock  uint64
	blockCount    uint64
	currentIndex  int
	currentIDs    []uint64
	currentValues []int64
}

// NewBlockIterator creates a new block iterator for the given reader
func NewBlockIterator(reader *col.Reader) *BlockIterator {
	return &BlockIterator{
		reader:        reader,
		currentBlock:  0,
		blockCount:    reader.BlockCount(),
		currentIndex:  -1,
		currentIDs:    nil,
		currentValues: nil,
	}
}

// Next advances to the next entry
// Returns true if there's a valid entry to read, false if we've reached the end
func (it *BlockIterator) Next() bool {
	it.currentIndex++

	// If we need to load a new block
	if it.currentIDs == nil || it.currentIndex >= len(it.currentIDs) {
		// Reset the current index for the new block
		it.currentIndex = 0

		// Check if we've processed all blocks
		if it.currentBlock >= it.blockCount {
			return false
		}

		// Load the next block
		var err error
		it.currentIDs, it.currentValues, err = it.reader.GetPairs(it.currentBlock)

		// Move to the next block for the next time we need to load
		it.currentBlock++

		// Skip empty blocks or blocks with errors
		if err != nil || len(it.currentIDs) == 0 {
			// Try the next block recursively
			return it.Next()
		}
	}

	return it.currentIndex < len(it.currentIDs)
}

// CurrentID returns the current entry's ID
func (it *BlockIterator) CurrentID() uint64 {
	return it.currentIDs[it.currentIndex]
}

// CurrentValue returns the current entry's value
func (it *BlockIterator) CurrentValue() int64 {
	return it.currentValues[it.currentIndex]
}
