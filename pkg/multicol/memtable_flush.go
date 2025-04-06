package multicol

import (
	"fmt"

	"vibe-lsm/pkg/col"
)

// Flush writes the non-deleted contents of the memtable to the specified path.
// It returns the number of pairs written to the file.
func (mt *MemtableImpl) Flush(path string) (uint64, error) {
	// Create a new writer
	writer, err := col.NewWriter(path)
	if err != nil {
		return 0, fmt.Errorf("failed to create writer: %w", err)
	}
	defer writer.Close()

	// Iterate through the memtable using ScanIterator
	iter := mt.ScanIterator(0, 0) // Scan all entries
	defer iter.Close()

	// Process entries in batches to improve performance
	const batchSize = 10000
	var totalWritten uint64
	batchIDs := make([]uint64, 0, batchSize)
	batchValues := make([]int64, 0, batchSize)

	// Process entries in batches
	for {
		// Get the next batch
		for i := 0; i < batchSize && iter.Next(); i++ {
			id, value := iter.Entry()
			batchIDs = append(batchIDs, id)
			batchValues = append(batchValues, value)
		}

		// If we have entries to write
		if len(batchIDs) > 0 {
			// Write the batch using WriteBlock method
			if err := writer.WriteBlock(batchIDs, batchValues); err != nil {
				return totalWritten, fmt.Errorf("failed to write batch: %w", err)
			}

			totalWritten += uint64(len(batchIDs))

			// Clear the batch for the next iteration
			batchIDs = batchIDs[:0]
			batchValues = batchValues[:0]
		}

		// Check if we've processed all entries
		if !iter.HasNext() {
			break
		}
	}

	// Finalize the file
	if err := writer.Finalize(); err != nil {
		return totalWritten, fmt.Errorf("failed to finalize file: %w", err)
	}

	return totalWritten, nil
}
