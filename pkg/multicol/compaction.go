// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"fmt"
	"os"

	"vibe-lsm/pkg/col"
)

// CompactionOptions defines options for compaction
type CompactionOptions struct {
	TargetBlockSize int    // Target block size in number of entries (0 means use default)
	EncodingType    uint32 // Encoding type to use for the output file (0 means use default)
	Level           uint16 // Compaction level (0 is base level)
}

// DefaultCompactionOptions returns the default compaction options
func DefaultCompactionOptions() CompactionOptions {
	return CompactionOptions{
		TargetBlockSize: 0, // Use the default block size from BufferedWriter (128KB)
		EncodingType:    0, // Use default encoding
		Level:           0, // Default level is 0 (base level)
	}
}

// Reader is an interface for a column reader
type Reader interface {
	GetBlock(blockIdx int) ([]uint64, []int64, error)
	NumBlocks() int
}

// WriterOptions contains configuration for the writer
type WriterOptions struct {
	EncodingOptions EncodingOptions
	TargetBlockSize int
	Level           uint16 // Compaction level (0 is base level)
}

// EncodingOptions contains configuration for encoding
type EncodingOptions struct {
	Type uint32 // Changed from int to uint32 to match col.WithBufferedEncoding expectation
}

// NewWriter creates a new column writer
func NewWriter(file *os.File, opts EncodingOptions, targetBlockSize int, level uint16) (*col.BufferedWriter, error) {
	writerOptions := []col.BufferedWriterOption{}
	if opts.Type != 0 {
		writerOptions = append(writerOptions, col.WithBufferedEncoding(opts.Type))
	}
	if targetBlockSize > 0 {
		writerOptions = append(writerOptions, col.WithBufferedBlockSize(uint32(targetBlockSize)))
	}
	if level > 0 {
		writerOptions = append(writerOptions, col.WithBufferedLevel(level))
	}

	return col.NewBufferedWriter(file.Name(), writerOptions...)
}

// Compact merges two column file segments, preferring values from the right (newer) reader when IDs conflict
// It writes the result to the specified output file path
func Compact(left, right *col.Reader, outputPath string, options CompactionOptions) error {
	// Create the output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer outputFile.Close()

	// Create writer options
	writerOpts := []col.BufferedWriterOption{}
	if options.EncodingType != 0 {
		writerOpts = append(writerOpts, col.WithBufferedEncoding(options.EncodingType))
	}
	if options.TargetBlockSize > 0 {
		writerOpts = append(writerOpts, col.WithBufferedBlockSize(uint32(options.TargetBlockSize)))
	}
	// Add the level option
	writerOpts = append(writerOpts, col.WithBufferedLevel(options.Level))

	// Create our writer with the specified encoding options and target block size
	writer, err := col.NewBufferedWriter(outputPath, writerOpts...)
	if err != nil {
		return fmt.Errorf("error creating writer: %w", err)
	}
	defer writer.Close()

	// Get the deleted ID bitmap from the right (newer) file
	deletedIDs, err := right.GetDeletedIDBitmap()
	if err != nil {
		return fmt.Errorf("error getting deleted ID bitmap: %w", err)
	}

	leftIt := NewBlockIterator(left)
	rightIt := NewBlockIterator(right)

	var leftID, rightID uint64
	var leftValue, rightValue int64
	var hasLeft, hasRight bool

	hasLeft, hasRight = leftIt.Next(), rightIt.Next()

	if hasLeft {
		leftID, leftValue = leftIt.CurrentID(), leftIt.CurrentValue()
	}
	if hasRight {
		rightID, rightValue = rightIt.CurrentID(), rightIt.CurrentValue()
	}

	for hasLeft || hasRight {
		if hasLeft && (!hasRight || leftID < rightID) {
			// Only in left file
			// Skip if ID is in the deleted bitmap
			if !deletedIDs.Contains(leftID) {
				if err := writer.Add(leftID, leftValue); err != nil {
					return fmt.Errorf("error adding entry: %w", err)
				}
			}
			hasLeft = leftIt.Next()
			if hasLeft {
				leftID, leftValue = leftIt.CurrentID(), leftIt.CurrentValue()
			}
			continue
		}

		if hasRight && (!hasLeft || rightID < leftID) {
			// Only in right file
			if !deletedIDs.Contains(rightID) {
				if err := writer.Add(rightID, rightValue); err != nil {
					return fmt.Errorf("error adding entry: %w", err)
				}
			}
			hasRight = rightIt.Next()
			if hasRight {
				rightID, rightValue = rightIt.CurrentID(), rightIt.CurrentValue()
			}
			continue
		}

		if hasLeft && hasRight && leftID == rightID {
			// discard left, use right, advance both
			if !deletedIDs.Contains(rightID) {
				if err := writer.Add(rightID, rightValue); err != nil {
					return fmt.Errorf("error adding entry: %w", err)
				}
			}

			hasLeft = leftIt.Next()
			hasRight = rightIt.Next()
			if hasLeft {
				leftID, leftValue = leftIt.CurrentID(), leftIt.CurrentValue()
			}
			if hasRight {
				rightID, rightValue = rightIt.CurrentID(), rightIt.CurrentValue()
			}
			continue
		}
	}

	// Get the deleted ID bitmap from the left (older) file
	leftDeletedIDs, err := left.GetDeletedIDBitmap()
	if err != nil {
		return fmt.Errorf("error getting deleted ID bitmap from left file: %w", err)
	}

	// Merge deleted IDs from both files and add to writer
	// This preserves all deletions when we compact
	writer.AddDeletedIDBitmap(deletedIDs)
	writer.AddDeletedIDBitmap(leftDeletedIDs)

	// We don't need to explicitly flush because the Close method will do it for us
	// But we will call Close explicitly to ensure proper finalization
	if err := writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %w", err)
	}

	return nil
}
