// Package multicol provides functionality for working with multiple column files.
package multicol

import (
	"fmt"
	"os"
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

	// First, collect all entries from both readers into a map to handle duplicates
	// The map key is the ID, and the value is the corresponding value
	// This ensures that if there are duplicate IDs, the last one (which has precedence) wins
	entries := make(map[uint64]int64)

	// Read all entries from the left reader
	for i := uint64(0); i < left.BlockCount(); i++ {
		ids, values, err := left.GetPairs(i)
		if err != nil {
			return fmt.Errorf("error reading block %d from left reader: %w", i, err)
		}

		for j := 0; j < len(ids); j++ {
			entries[ids[j]] = values[j]
		}
	}

	// Read all entries from the right reader, overwriting any duplicates
	// This ensures right (newer) values take precedence
	for i := uint64(0); i < right.BlockCount(); i++ {
		ids, values, err := right.GetPairs(i)
		if err != nil {
			return fmt.Errorf("error reading block %d from right reader: %w", i, err)
		}

		for j := 0; j < len(ids); j++ {
			entries[ids[j]] = values[j]
		}
	}

	// Convert map to sorted array of IDs for consistent output
	sortedIDs := make([]uint64, 0, len(entries))
	for id := range entries {
		sortedIDs = append(sortedIDs, id)
	}

	// Sort the IDs for consistent and efficient storage
	sort.Slice(sortedIDs, func(i, j int) bool {
		return sortedIDs[i] < sortedIDs[j]
	})

	// Write all entries in sorted order
	for _, id := range sortedIDs {
		if err := writer.Add(id, entries[id]); err != nil {
			return fmt.Errorf("error adding entry: %w", err)
		}
	}

	// We don't need to explicitly flush because the Close method will do it for us
	// But we will call Close explicitly to ensure proper finalization
	if err := writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %w", err)
	}

	return nil
}
