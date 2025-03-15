// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"fmt"
	"vibe-lsm/pkg/col"
)

// CompactionOptions contains options for the compaction process
type CompactionOptions struct {
	// TargetBlockSize specifies the target size of the output blocks
	TargetBlockSize int
}

// DefaultCompactionOptions returns the default options for compaction
func DefaultCompactionOptions() CompactionOptions {
	return CompactionOptions{
		TargetBlockSize: 1024, // Default to 1024 entries per block
	}
}

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
		// If we're at the first call, adjust to -1 so we'll load block 0
		if it.currentIndex == 0 && it.currentBlock == 0 && it.currentIDs == nil {
			it.currentBlock = ^uint64(0) // Set to max value so when we increment, it becomes 0
		}

		it.currentBlock++

		// Check if we've processed all blocks
		if it.currentBlock >= it.blockCount {
			return false
		}

		// Load the next block
		ids, values, err := it.reader.GetPairs(it.currentBlock)
		if err != nil || len(ids) == 0 {
			return it.Next() // Skip empty/error blocks
		}

		it.currentIDs = ids
		it.currentValues = values
		it.currentIndex = 0
	}

	return true
}

// CurrentID returns the current entry's ID
func (it *BlockIterator) CurrentID() uint64 {
	return it.currentIDs[it.currentIndex]
}

// CurrentValue returns the current entry's value
func (it *BlockIterator) CurrentValue() int64 {
	return it.currentValues[it.currentIndex]
}

// BlockBuffer collects entries before writing a block
type BlockBuffer struct {
	ids        []uint64
	values     []int64
	targetSize int
}

// NewBlockBuffer creates a new block buffer with the given target size
func NewBlockBuffer(targetSize int) *BlockBuffer {
	return &BlockBuffer{
		ids:        make([]uint64, 0, targetSize),
		values:     make([]int64, 0, targetSize),
		targetSize: targetSize,
	}
}

// Add adds an entry to the buffer
func (b *BlockBuffer) Add(id uint64, value int64) {
	b.ids = append(b.ids, id)
	b.values = append(b.values, value)
}

// ShouldFlush returns true if the buffer should be flushed
func (b *BlockBuffer) ShouldFlush() bool {
	return len(b.ids) >= b.targetSize
}

// GetIDs returns the IDs in the buffer
func (b *BlockBuffer) GetIDs() []uint64 {
	return b.ids
}

// GetValues returns the values in the buffer
func (b *BlockBuffer) GetValues() []int64 {
	return b.values
}

// Clear clears the buffer
func (b *BlockBuffer) Clear() {
	b.ids = b.ids[:0]
	b.values = b.values[:0]
}

// IsEmpty returns true if the buffer is empty
func (b *BlockBuffer) IsEmpty() bool {
	return len(b.ids) == 0
}

// Compact compacts two column file segments using a merge-sort approach
// The leftReader contains older data, and the rightReader contains newer data.
// When the same ID appears in both segments, the value from the rightReader takes precedence.
func Compact(leftReader, rightReader *col.Reader, outputPath string, opts CompactionOptions) error {
	// Create the output writer
	writer, err := col.NewSimpleWriter(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %w", err)
	}
	defer writer.Close()

	// Set target block size
	if opts.TargetBlockSize > 0 {
		writer.SetTargetBlockSize(opts.TargetBlockSize)
	}

	// Create buffer for collecting entries
	buffer := NewBlockBuffer(opts.TargetBlockSize)

	// Create iterators for both segments
	leftIter := NewBlockIterator(leftReader)
	rightIter := NewBlockIterator(rightReader)

	// Initialize state variables
	var leftHasNext, rightHasNext bool
	var leftID, rightID uint64
	var leftValue, rightValue int64

	// Prime the iterators
	leftHasNext = leftIter.Next()
	rightHasNext = rightIter.Next()

	if leftHasNext {
		leftID = leftIter.CurrentID()
		leftValue = leftIter.CurrentValue()
	}

	if rightHasNext {
		rightID = rightIter.CurrentID()
		rightValue = rightIter.CurrentValue()
	}

	// Main merge loop - continue while either iterator has more entries
	for leftHasNext || rightHasNext {
		// Determine which entry to process next
		if !leftHasNext {
			// Only right has more entries
			buffer.Add(rightID, rightValue)
			rightHasNext = rightIter.Next()
			if rightHasNext {
				rightID = rightIter.CurrentID()
				rightValue = rightIter.CurrentValue()
			}
		} else if !rightHasNext {
			// Only left has more entries
			buffer.Add(leftID, leftValue)
			leftHasNext = leftIter.Next()
			if leftHasNext {
				leftID = leftIter.CurrentID()
				leftValue = leftIter.CurrentValue()
			}
		} else {
			// Both have more entries, compare IDs
			if rightID < leftID {
				// Process right entry (lower ID)
				buffer.Add(rightID, rightValue)
				rightHasNext = rightIter.Next()
				if rightHasNext {
					rightID = rightIter.CurrentID()
					rightValue = rightIter.CurrentValue()
				}
			} else if leftID < rightID {
				// Process left entry (lower ID)
				buffer.Add(leftID, leftValue)
				leftHasNext = leftIter.Next()
				if leftHasNext {
					leftID = leftIter.CurrentID()
					leftValue = leftIter.CurrentValue()
				}
			} else {
				// Equal IDs - take right value, advance both
				buffer.Add(rightID, rightValue)

				leftHasNext = leftIter.Next()
				if leftHasNext {
					leftID = leftIter.CurrentID()
					leftValue = leftIter.CurrentValue()
				}

				rightHasNext = rightIter.Next()
				if rightHasNext {
					rightID = rightIter.CurrentID()
					rightValue = rightIter.CurrentValue()
				}
			}
		}

		// Check if buffer should be flushed
		if buffer.ShouldFlush() {
			if err := writer.Write(buffer.GetIDs(), buffer.GetValues()); err != nil {
				return fmt.Errorf("failed to write block: %w", err)
			}
			buffer.Clear()
		}
	}

	// Final flush for any remaining entries
	if !buffer.IsEmpty() {
		if err := writer.Write(buffer.GetIDs(), buffer.GetValues()); err != nil {
			return fmt.Errorf("failed to write final block: %w", err)
		}
	}

	return nil
}
