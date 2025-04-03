// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"fmt"
	"vibe-lsm/pkg/col"
)

// CompactWithBatches is the old implementation of compaction using batching
// It uses a BufferedWriter to write the data, with options for target block size and encoding type,
// but with an explicit batch buffering layer on top.
func CompactWithBatches(leftReader, rightReader *col.Reader, outputPath string, opts CompactionOptions) error {
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

	// Create iterators for both readers
	leftIter := NewBlockIterator(leftReader)
	rightIter := NewBlockIterator(rightReader)

	// Prime the iterators
	leftHasData := leftIter.Next()
	rightHasData := rightIter.Next()

	// Use a smaller buffer size to avoid issues with large batches
	// This is a balance between memory usage and write efficiency
	const bufferCap = 100 // Reduced from 10000 to 100 for more efficient block sizing
	batchIDs := make([]uint64, 0, bufferCap)
	batchValues := make([]int64, 0, bufferCap)

	// Process all entries from both readers using merge-sort algorithm
	for leftHasData || rightHasData {
		// Determine which entry (or entries) to add to the result next
		if !leftHasData {
			// Only right reader has more data
			batchIDs = append(batchIDs, rightIter.CurrentID())
			batchValues = append(batchValues, rightIter.CurrentValue())
			rightHasData = rightIter.Next()
		} else if !rightHasData {
			// Only left reader has more data
			batchIDs = append(batchIDs, leftIter.CurrentID())
			batchValues = append(batchValues, leftIter.CurrentValue())
			leftHasData = leftIter.Next()
		} else {
			// Both readers have more data, need to compare IDs
			leftID := leftIter.CurrentID()
			rightID := rightIter.CurrentID()

			if rightID < leftID {
				// Take right entry (lower ID)
				batchIDs = append(batchIDs, rightID)
				batchValues = append(batchValues, rightIter.CurrentValue())
				rightHasData = rightIter.Next()
			} else if leftID < rightID {
				// Take left entry (lower ID)
				batchIDs = append(batchIDs, leftID)
				batchValues = append(batchValues, leftIter.CurrentValue())
				leftHasData = leftIter.Next()
			} else {
				// Same ID - take right value, advance both iterators
				batchIDs = append(batchIDs, rightID)
				batchValues = append(batchValues, rightIter.CurrentValue())
				leftHasData = leftIter.Next()
				rightHasData = rightIter.Next()
			}
		}

		// Flush to writer when buffer reaches capacity
		if len(batchIDs) >= bufferCap {
			if err := writer.BatchAdd(batchIDs, batchValues); err != nil {
				return fmt.Errorf("failed to write batch: %w", err)
			}
			// Reset the batch buffers
			batchIDs = batchIDs[:0]
			batchValues = batchValues[:0]
		}
	}

	// Write any remaining entries
	if len(batchIDs) > 0 {
		if err := writer.BatchAdd(batchIDs, batchValues); err != nil {
			return fmt.Errorf("failed to write final batch: %w", err)
		}
	}

	return nil
}
