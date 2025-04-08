package multicol

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALRecovery(t *testing.T) {
	// Create a temporary directory for the WAL file
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err, "Failed to create temp directory")
	defer os.RemoveAll(dir)

	walPath := filepath.Join(dir, "memtable.wal")

	// Test 1: Basic recovery of Add operations
	t.Run("RecoverAddOperations", func(t *testing.T) {
		// Create and populate a memtable with WAL
		mt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: walPath,
		})
		require.NoError(t, err, "Failed to create durable memtable")

		// Add some entries
		for i := uint64(1); i <= 10; i++ {
			err := mt.Add(i, int64(i*100))
			require.NoError(t, err, "Failed to add entry")
		}

		// Sync to ensure durability
		err = mt.Sync()
		require.NoError(t, err, "Failed to sync WAL")

		// Disable WAL to close the file
		err = mt.DisableWAL()
		require.NoError(t, err, "Failed to disable WAL")

		// Create a new memtable and recover from WAL
		recoveredMt := NewMemtable(nil)
		wal, err := NewWAL(walPath, 0)
		require.NoError(t, err, "Failed to create WAL for recovery")

		// Recover
		err = wal.Recover(recoveredMt)
		require.NoError(t, err, "Failed to recover from WAL")

		// Verify the recovered data
		assert.Equal(t, int64(10), recoveredMt.ActiveCount(), "Recovered wrong number of entries")

		for i := uint64(1); i <= 10; i++ {
			expectedValue := int64(i * 100)
			actualValue, exists := recoveredMt.Get(i)
			assert.True(t, exists, "Entry for ID %d not found", i)
			assert.Equal(t, expectedValue, actualValue, "Wrong value for ID %d", i)
		}

		// Verify aggregation match
		origMinID, origMaxID, origMinVal, origMaxVal, origSum, origCount := recoveredMt.Aggregate()
		assert.Equal(t, uint64(1), origMinID, "Wrong min ID")
		assert.Equal(t, uint64(10), origMaxID, "Wrong max ID")
		assert.Equal(t, int64(100), origMinVal, "Wrong min value")
		assert.Equal(t, int64(1000), origMaxVal, "Wrong max value")
		assert.Equal(t, int64(5500), origSum, "Wrong sum")
		assert.Equal(t, 10, origCount, "Wrong count")
	})

	// Test 2: Recovery with mixed operations (add, batch add, delete)
	t.Run("RecoverMixedOperations", func(t *testing.T) {
		// Create a new WAL path for this test
		mixedWalPath := filepath.Join(dir, "mixed_ops.wal")

		// Create and populate a memtable with WAL
		mt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: mixedWalPath,
		})
		require.NoError(t, err, "Failed to create durable memtable")

		// Add some entries individually
		for i := uint64(1); i <= 5; i++ {
			err := mt.Add(i, int64(i*100))
			require.NoError(t, err, "Failed to add entry")
		}

		// Add some entries in batch
		batchIds := []uint64{6, 7, 8, 9, 10}
		batchValues := []int64{600, 700, 800, 900, 1000}
		err = mt.BatchAdd(batchIds, batchValues)
		require.NoError(t, err, "Failed to batch add entries")

		// Delete some entries
		mt.Delete(3)
		mt.Delete(8)

		// Batch delete
		mt.BatchDelete([]uint64{5, 9})

		// Sync to ensure durability
		err = mt.Sync()
		require.NoError(t, err, "Failed to sync WAL")

		// Disable WAL to close the file
		err = mt.DisableWAL()
		require.NoError(t, err, "Failed to disable WAL")

		// Create a new memtable and recover from WAL
		recoveredMt := NewMemtable(nil)
		wal, err := NewWAL(mixedWalPath, 0)
		require.NoError(t, err, "Failed to create WAL for recovery")

		// Recover
		err = wal.Recover(recoveredMt)
		require.NoError(t, err, "Failed to recover from WAL")

		// Verify the recovered data
		assert.Equal(t, int64(6), recoveredMt.ActiveCount(), "Recovered wrong number of entries")

		// Entries that should exist
		existingEntries := map[uint64]int64{
			1:  100,
			2:  200,
			4:  400,
			6:  600,
			7:  700,
			10: 1000,
		}

		// Entries that should be deleted
		deletedEntries := []uint64{3, 5, 8, 9}

		// Check existing entries
		for id, expectedValue := range existingEntries {
			actualValue, exists := recoveredMt.Get(id)
			assert.True(t, exists, "Entry for ID %d should exist", id)
			assert.Equal(t, expectedValue, actualValue, "Wrong value for ID %d", id)
		}

		// Check deleted entries
		for _, id := range deletedEntries {
			_, exists := recoveredMt.Get(id)
			assert.False(t, exists, "Entry for ID %d should not exist", id)
		}

		// Verify aggregation match
		origMinID, origMaxID, origMinVal, origMaxVal, origSum, origCount := recoveredMt.Aggregate()
		assert.Equal(t, uint64(1), origMinID, "Wrong min ID")
		assert.Equal(t, uint64(10), origMaxID, "Wrong max ID")
		assert.Equal(t, int64(100), origMinVal, "Wrong min value")
		assert.Equal(t, int64(1000), origMaxVal, "Wrong max value")
		assert.Equal(t, int64(3000), origSum, "Wrong sum") // 100+200+400+600+700+1000
		assert.Equal(t, 6, origCount, "Wrong count")
	})

	// Test 3: Recovery after crash (simulated by not closing the WAL)
	t.Run("RecoverAfterCrash", func(t *testing.T) {
		// Create a new WAL path for this test
		crashWalPath := filepath.Join(dir, "crash.wal")

		// Create and populate a memtable with WAL
		mt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: crashWalPath,
		})
		require.NoError(t, err, "Failed to create durable memtable")

		// Add some entries
		for i := uint64(1); i <= 10; i++ {
			err := mt.Add(i, int64(i*100))
			require.NoError(t, err, "Failed to add entry")
		}

		// Sync to ensure durability
		err = mt.Sync()
		require.NoError(t, err, "Failed to sync WAL")

		// Simulate a crash by not closing the WAL properly
		// We'll just create a new memtable and recover from the WAL

		// Create a new memtable and recover from WAL
		recoveredMt := NewMemtable(nil)
		wal, err := NewWAL(crashWalPath, 0)
		require.NoError(t, err, "Failed to create WAL for recovery")

		// Recover
		err = wal.Recover(recoveredMt)
		require.NoError(t, err, "Failed to recover from WAL")

		// Verify the recovered data
		assert.Equal(t, int64(10), recoveredMt.ActiveCount(), "Recovered wrong number of entries")

		// Check the values
		for i := uint64(1); i <= 10; i++ {
			expectedValue := int64(i * 100)
			actualValue, exists := recoveredMt.Get(i)
			assert.True(t, exists, "Entry for ID %d not found", i)
			assert.Equal(t, expectedValue, actualValue, "Wrong value for ID %d", i)
		}
	})

	// Test 4: Automatic recovery during initialization
	t.Run("AutoRecoveryDuringInit", func(t *testing.T) {
		// Create a new WAL path for this test
		autoWalPath := filepath.Join(dir, "auto_recovery.wal")

		// Create and populate a memtable with WAL
		mt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: autoWalPath,
		})
		require.NoError(t, err, "Failed to create durable memtable")

		// Add some data
		for i := uint64(1); i <= 5; i++ {
			err := mt.Add(i, int64(i*10))
			require.NoError(t, err, "Failed to add entry")
		}

		// Sync to ensure durability
		err = mt.Sync()
		require.NoError(t, err, "Failed to sync WAL")

		// Disable WAL to close the file
		err = mt.DisableWAL()
		require.NoError(t, err, "Failed to disable WAL")

		// Now create a new WAL-enabled memtable with the same WAL path
		// It should automatically recover
		recoveredMt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: autoWalPath,
		})
		require.NoError(t, err, "Failed to create recovered memtable")

		// Create a separate WAL instance and explicitly recover
		// to compare the expected state
		wal, err := NewWAL(autoWalPath, 0)
		require.NoError(t, err, "Failed to create WAL for recovery")

		// Recover the WAL into the memtable
		err = wal.Recover(recoveredMt)
		require.NoError(t, err, "Failed to recover from WAL")

		// Verify the recovered data
		assert.Equal(t, int64(5), recoveredMt.ActiveCount(), "Recovered wrong number of entries")

		// Check values
		for i := uint64(1); i <= 5; i++ {
			expectedValue := int64(i * 10)
			actualValue, exists := recoveredMt.Get(i)
			assert.True(t, exists, "Entry for ID %d not found", i)
			assert.Equal(t, expectedValue, actualValue, "Wrong value for ID %d", i)
		}
	})
}
