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
}

// DefaultCompactionOptions returns the default compaction options
func DefaultCompactionOptions() CompactionOptions {
	return CompactionOptions{
		TargetBlockSize: 0, // Use the default block size from BufferedWriter (128KB)
		EncodingType:    0, // Use default encoding
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
}

// EncodingOptions contains configuration for encoding
type EncodingOptions struct {
	Type uint32 // Changed from int to uint32 to match col.WithBufferedEncoding expectation
}

// NewWriter creates a new column writer
func NewWriter(file *os.File, opts EncodingOptions, targetBlockSize int) (*col.BufferedWriter, error) {
	writerOptions := []col.BufferedWriterOption{}
	if opts.Type != 0 {
		writerOptions = append(writerOptions, col.WithBufferedEncoding(opts.Type))
	}
	if targetBlockSize > 0 {
		writerOptions = append(writerOptions, col.WithBufferedBlockSize(uint32(targetBlockSize)))
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

	// Create our writer with the specified encoding options and target block size
	writer, err := col.NewBufferedWriter(outputPath, writerOpts...)
	if err != nil {
		return fmt.Errorf("error creating writer: %w", err)
	}
	defer writer.Close()

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
			if err := writer.Add(leftID, leftValue); err != nil {
				return fmt.Errorf("error adding entry: %w", err)
			}
			hasLeft = leftIt.Next()
			if hasLeft {
				leftID, leftValue = leftIt.CurrentID(), leftIt.CurrentValue()
			}
			continue
		}

		if hasRight && (!hasLeft || rightID < leftID) {
			if err := writer.Add(rightID, rightValue); err != nil {
				return fmt.Errorf("error adding entry: %w", err)
			}
			hasRight = rightIt.Next()
			if hasRight {
				rightID, rightValue = rightIt.CurrentID(), rightIt.CurrentValue()
			}
			continue
		}

		if hasLeft && hasRight && leftID == rightID {
			// discard left, use right, advance both
			if err := writer.Add(rightID, rightValue); err != nil {
				return fmt.Errorf("error adding entry: %w", err)
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

	// We don't need to explicitly flush because the Close method will do it for us
	// But we will call Close explicitly to ensure proper finalization
	if err := writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %w", err)
	}

	return nil
}
