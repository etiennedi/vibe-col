package multicol

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkMemtableWithWAL(b *testing.B) {
	// Create a temporary directory for the WAL files
	dir, err := os.MkdirTemp("", "wal_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Benchmark without WAL
	b.Run("NoWAL", func(b *testing.B) {
		mt := NewMemtable(nil)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := uint64(i)
			value := int64(i * 10)
			if err := mt.Add(id, value); err != nil {
				b.Fatalf("Failed to add entry: %v", err)
			}
		}
	})

	// Benchmark with WAL but no sync
	b.Run("WAL_NoSync", func(b *testing.B) {
		walPath := filepath.Join(dir, "benchmark_no_sync.wal")
		// Remove any existing WAL file from previous runs
		os.Remove(walPath)

		mt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: walPath,
		})
		if err != nil {
			b.Fatalf("Failed to create durable memtable: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := uint64(i)
			value := int64(i * 10)
			if err := mt.Add(id, value); err != nil {
				b.Fatalf("Failed to add entry: %v", err)
			}
		}

		b.StopTimer()
		// Properly close the WAL to ensure integrity
		if dm, ok := mt.(DurableMemtable); ok {
			if err := dm.DisableWAL(); err != nil {
				b.Logf("Warning: failed to close WAL: %v", err)
			}
		}
	})

	// Benchmark with WAL and periodic sync (every 1000 entries)
	b.Run("WAL_PeriodicSync", func(b *testing.B) {
		walPath := filepath.Join(dir, "benchmark_periodic_sync.wal")
		// Remove any existing WAL file from previous runs
		os.Remove(walPath)

		mt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: walPath,
		})
		if err != nil {
			b.Fatalf("Failed to create durable memtable: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := uint64(i)
			value := int64(i * 10)
			if err := mt.Add(id, value); err != nil {
				b.Fatalf("Failed to add entry: %v", err)
			}

			// Sync every 1000 entries
			if i > 0 && i%1000 == 0 {
				if err := mt.Sync(); err != nil {
					b.Fatalf("Failed to sync WAL: %v", err)
				}
			}
		}

		b.StopTimer()
		// Properly close the WAL to ensure integrity
		if dm, ok := mt.(DurableMemtable); ok {
			if err := dm.DisableWAL(); err != nil {
				b.Logf("Warning: failed to close WAL: %v", err)
			}
		}
	})

	// Benchmark with WAL and sync on every operation
	b.Run("WAL_SyncEveryOp", func(b *testing.B) {
		walPath := filepath.Join(dir, "benchmark_sync_every_op.wal")
		// Remove any existing WAL file from previous runs
		os.Remove(walPath)

		mt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: walPath,
		})
		if err != nil {
			b.Fatalf("Failed to create durable memtable: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := uint64(i)
			value := int64(i * 10)
			if err := mt.Add(id, value); err != nil {
				b.Fatalf("Failed to add entry: %v", err)
			}

			// Sync after every operation
			if err := mt.Sync(); err != nil {
				b.Fatalf("Failed to sync WAL: %v", err)
			}
		}

		b.StopTimer()
		// Properly close the WAL to ensure integrity
		if dm, ok := mt.(DurableMemtable); ok {
			if err := dm.DisableWAL(); err != nil {
				b.Logf("Warning: failed to close WAL: %v", err)
			}
		}
	})

	// Benchmark with batch adds (1000 entries per batch)
	b.Run("WAL_BatchAdd", func(b *testing.B) {
		walPath := filepath.Join(dir, "benchmark_batch.wal")
		// Remove any existing WAL file from previous runs
		os.Remove(walPath)

		mt, err := NewDurableMemtable(&MemtableOptions{
			WalPath: walPath,
		})
		if err != nil {
			b.Fatalf("Failed to create durable memtable: %v", err)
		}

		batchSize := 1000
		iterations := b.N / batchSize
		if iterations < 1 {
			iterations = 1
		}

		b.ResetTimer()
		for j := 0; j < iterations; j++ {
			baseID := uint64(j * batchSize)

			// Prepare batch
			ids := make([]uint64, batchSize)
			values := make([]int64, batchSize)

			for i := 0; i < batchSize; i++ {
				ids[i] = baseID + uint64(i)
				values[i] = int64(ids[i] * 10)
			}

			// Add batch
			if err := mt.BatchAdd(ids, values); err != nil {
				b.Fatalf("Failed to batch add entries: %v", err)
			}

			// Sync after each batch
			if err := mt.Sync(); err != nil {
				b.Fatalf("Failed to sync WAL: %v", err)
			}
		}

		b.StopTimer()
		// Properly close the WAL to ensure integrity
		if dm, ok := mt.(DurableMemtable); ok {
			if err := dm.DisableWAL(); err != nil {
				b.Logf("Warning: failed to close WAL: %v", err)
			}
		}

		// Adjust the reported N to match actual operations
		b.SetBytes(int64(batchSize * 16)) // 16 bytes per entry (8 for ID, 8 for value)
	})
}
