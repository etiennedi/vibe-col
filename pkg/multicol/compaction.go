// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"fmt"
	"sort"
	"vibe-lsm/pkg/col"
)

// CompactionOptions defines options for compaction
type CompactionOptions struct {
	TargetBlockSize int    // Target block size in number of entries (0 means use default)
	EncodingType    uint32 // Encoding type to use for the output file (0 means use default)
}

// DefaultCompactionOptions returns the default compaction options
func DefaultCompactionOptions() CompactionOptions {
	return CompactionOptions{
		TargetBlockSize: 0, // Use the default block size from BufferedWriter (128KB)
		EncodingType:    0, // Use default encoding
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

// Compact merges two column file segments using a merge-sort approach.
// It uses a BufferedWriter to write the data, with options for target block size and encoding type.
// The rightReader is assumed to contain newer data than leftReader, so its values take precedence
// for the same ID.
func Compact(leftReader, rightReader *col.Reader, outputPath string, opts CompactionOptions) error {
	// Create the SimpleWriter with the specified encoding options
	writerOptions := []col.BufferedWriterOption{}

	// If an encoding type is specified, use it
	if opts.EncodingType != 0 {
		writerOptions = append(writerOptions, col.WithBufferedEncoding(opts.EncodingType))
	}

	// If a target block size is specified, use it
	if opts.TargetBlockSize > 0 {
		writerOptions = append(writerOptions, col.WithBufferedBlockSize(uint32(opts.TargetBlockSize)))
	}

	// Create the writer with the configured options
	writer, err := col.NewBufferedWriter(outputPath, writerOptions...)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %w", err)
	}
	defer writer.Close()

	// First collect all IDs and values from both readers
	allEntries := make(map[uint64]int64)

	// Process left reader (older data)
	leftIter := NewBlockIterator(leftReader)
	for leftIter.Next() {
		allEntries[leftIter.CurrentID()] = leftIter.CurrentValue()
	}

	// Process right reader (newer data - takes precedence)
	rightIter := NewBlockIterator(rightReader)
	for rightIter.Next() {
		allEntries[rightIter.CurrentID()] = rightIter.CurrentValue()
	}

	// Get sorted IDs to maintain order
	sortedIDs := make([]uint64, 0, len(allEntries))
	for id := range allEntries {
		sortedIDs = append(sortedIDs, id)
	}

	// Sort the IDs in ascending order
	sort.Slice(sortedIDs, func(i, j int) bool {
		return sortedIDs[i] < sortedIDs[j]
	})

	// Write all entries in sorted order
	for _, id := range sortedIDs {
		if err := writer.Add(id, allEntries[id]); err != nil {
			return fmt.Errorf("failed to write entry for ID %d: %w", id, err)
		}
	}

	return nil
}
