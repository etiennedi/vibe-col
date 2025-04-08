package multicol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// RecordType represents the type of operation recorded in the WAL
type RecordType byte

const (
	// Record types
	RecordTypeAdd RecordType = iota + 1
	RecordTypeBatchAdd
	RecordTypeDelete
	RecordTypeBatchDelete

	// Magic number for WAL records - "VWAL" in ASCII
	walMagicNumber uint32 = 0x5657414C

	// Default buffer size (256KB)
	defaultBufferSize = 256 * 1024
)

// WALManager manages the write-ahead log for the memtable
type WALManager interface {
	// Log a single add operation
	LogAdd(id uint64, value int64) error

	// Log a batch of add operations
	LogBatchAdd(ids []uint64, values []int64) error

	// Log a delete operation
	LogDelete(id uint64) error

	// Log a batch of delete operations
	LogBatchDelete(ids []uint64) error

	// Sync ensures all logged operations are durably persisted to disk
	Sync() error

	// Close the WAL, flushing any pending data
	Close() error
}

// WALRecoverer is the interface for recovering from a WAL
type WALRecoverer interface {
	// Recover rebuilds the memtable state from the WAL
	Recover(memtable Memtable) error
}

// WAL implements both WALManager and WALRecoverer
type WAL struct {
	file       *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
	path       string
	seqNum     uint64
	bufferSize int
}

// NewWAL creates a new WAL at the specified path
func NewWAL(path string, bufferSize int) (*WAL, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Open the file for writing
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// If buffer size is 0 or negative, use default
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}

	wal := &WAL{
		file:       file,
		writer:     bufio.NewWriterSize(file, bufferSize),
		path:       path,
		seqNum:     0,
		bufferSize: bufferSize,
	}

	// Add finalizer to ensure WAL is closed when garbage collected
	runtime.SetFinalizer(wal, func(w *WAL) {
		// This is a safety net, proper code should call Close explicitly
		_ = w.Close()
	})

	return wal, nil
}

// LogAdd logs a single add operation
func (w *WAL) LogAdd(id uint64, value int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Prepare the payload: id (8 bytes) + value (8 bytes)
	payload := make([]byte, 16)
	binary.LittleEndian.PutUint64(payload[:8], id)
	binary.LittleEndian.PutUint64(payload[8:], uint64(value))

	// Write the record
	return w.writeRecord(RecordTypeAdd, payload)
}

// LogBatchAdd logs a batch of add operations
func (w *WAL) LogBatchAdd(ids []uint64, values []int64) error {
	if len(ids) != len(values) {
		return errors.New("ids and values must have the same length")
	}
	if len(ids) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Prepare the payload: count (4 bytes) + entries (16 bytes each)
	payloadSize := 4 + (len(ids) * 16)
	payload := make([]byte, payloadSize)

	// Write the count
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(ids)))

	// Write each entry (id + value)
	offset := 4
	for i := 0; i < len(ids); i++ {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], ids[i])
		binary.LittleEndian.PutUint64(payload[offset+8:offset+16], uint64(values[i]))
		offset += 16
	}

	// Write the record
	return w.writeRecord(RecordTypeBatchAdd, payload)
}

// LogDelete logs a delete operation
func (w *WAL) LogDelete(id uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Prepare the payload: id (8 bytes)
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, id)

	// Write the record
	return w.writeRecord(RecordTypeDelete, payload)
}

// LogBatchDelete logs a batch of delete operations
func (w *WAL) LogBatchDelete(ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Prepare the payload: count (4 bytes) + ids (8 bytes each)
	payloadSize := 4 + (len(ids) * 8)
	payload := make([]byte, payloadSize)

	// Write the count
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(ids)))

	// Write each id
	offset := 4
	for i := 0; i < len(ids); i++ {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], ids[i])
		offset += 8
	}

	// Write the record
	return w.writeRecord(RecordTypeBatchDelete, payload)
}

// Sync ensures all logged operations are durably persisted to disk
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush the buffer to the OS
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}

	// Sync to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	return nil
}

// Close flushes any pending data and closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush buffer
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer during close: %w", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	return nil
}

// writeRecord writes a record to the WAL
func (w *WAL) writeRecord(recordType RecordType, payload []byte) error {
	// Increment sequence number
	w.seqNum++
	seqNum := w.seqNum

	// Calculate record components size
	headerSize := 9 // 1 byte type + 8 bytes sequence
	checksumSize := 4
	recordSize := headerSize + len(payload) + checksumSize

	// Prepare the record header
	header := make([]byte, headerSize)
	header[0] = byte(recordType)
	binary.LittleEndian.PutUint64(header[1:], seqNum)

	// Calculate checksum over header and payload
	checksum := crc32.NewIEEE()
	checksum.Write(header)
	checksum.Write(payload)
	checksumValue := checksum.Sum32()

	// Convert checksum to bytes
	checksumBytes := make([]byte, checksumSize)
	binary.LittleEndian.PutUint32(checksumBytes, checksumValue)

	// Write the record parts

	// Write magic number
	magicBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(magicBytes, walMagicNumber)
	if _, err := w.writer.Write(magicBytes); err != nil {
		return fmt.Errorf("failed to write WAL record magic: %w", err)
	}

	// Write record length
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(recordSize))
	if _, err := w.writer.Write(lengthBytes); err != nil {
		return fmt.Errorf("failed to write WAL record length: %w", err)
	}

	// Write header
	if _, err := w.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write WAL record header: %w", err)
	}

	// Write payload
	if _, err := w.writer.Write(payload); err != nil {
		return fmt.Errorf("failed to write WAL record payload: %w", err)
	}

	// Write checksum
	if _, err := w.writer.Write(checksumBytes); err != nil {
		return fmt.Errorf("failed to write WAL record checksum: %w", err)
	}

	return nil
}

// Recover rebuilds the memtable state from the WAL
func (w *WAL) Recover(memtable Memtable) error {
	// Open the WAL file for reading
	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			// No WAL file, nothing to recover
			return nil
		}
		return fmt.Errorf("failed to open WAL file for recovery: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var lastSeqNum uint64
	var recoveryErrors int

	// Read and process each record
	for {
		// Try to read a record
		recordType, seqNum, payload, err := w.readRecord(reader)
		if err != nil {
			if err == io.EOF {
				// End of file, recovery complete
				break
			}

			// For corrupted records, just note error and stop recovery
			// This prevents continuing with potentially corrupted data
			if err == io.ErrUnexpectedEOF ||
				strings.Contains(err.Error(), "magic number mismatch") ||
				strings.Contains(err.Error(), "checksum verification failed") {

				// Only report first few errors to avoid flooding logs during benchmarks
				recoveryErrors++
				if recoveryErrors <= 3 {
					fmt.Printf("WAL recovery stopping due to corruption: %v\n", err)
				}

				// We've hit corruption, stop recovery but don't return error
				break
			}

			// For other errors, log and continue
			fmt.Printf("WAL recovery error: %v\n", err)
			continue
		}

		// Track the highest sequence number
		if seqNum > lastSeqNum {
			lastSeqNum = seqNum
		}

		// Apply the operation to the memtable
		if err := w.applyRecord(memtable, recordType, payload); err != nil {
			fmt.Printf("Failed to apply WAL record: %v\n", err)
			// Continue processing other records
			continue
		}
	}

	// Update the sequence number to the highest seen
	if lastSeqNum > w.seqNum {
		w.seqNum = lastSeqNum
	}

	return nil
}

// readRecord reads a single record from the WAL file
func (w *WAL) readRecord(reader *bufio.Reader) (RecordType, uint64, []byte, error) {
	// Check if there's any data available - this helps with incomplete files
	_, err := reader.Peek(1)
	if err != nil {
		return 0, 0, nil, err
	}

	// Read and verify magic number
	magicBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, magicBytes); err != nil {
		return 0, 0, nil, err
	}
	magic := binary.LittleEndian.Uint32(magicBytes)
	if magic != walMagicNumber {
		return 0, 0, nil, fmt.Errorf("invalid WAL record: magic number mismatch")
	}

	// Read record length
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBytes); err != nil {
		return 0, 0, nil, err
	}
	recordSize := int(binary.LittleEndian.Uint32(lengthBytes))

	// Sanity check record size to avoid allocation attacks
	// A reasonable WAL record shouldn't be larger than a few KB
	if recordSize <= 0 || recordSize > 1024*1024 {
		return 0, 0, nil, fmt.Errorf("invalid record size: %d", recordSize)
	}

	// Read the full record data
	recordData := make([]byte, recordSize)
	if _, err := io.ReadFull(reader, recordData); err != nil {
		return 0, 0, nil, err
	}

	// Parse the record components
	headerSize := 9 // 1 byte type + 8 bytes sequence
	checksumSize := 4

	// Ensure we have enough data for minimum record
	if len(recordData) < headerSize+checksumSize {
		return 0, 0, nil, fmt.Errorf("record too small")
	}

	// Extract record type and validate
	recordType := RecordType(recordData[0])
	if recordType < RecordTypeAdd || recordType > RecordTypeBatchDelete {
		return 0, 0, nil, fmt.Errorf("invalid record type: %d", recordType)
	}

	// Extract sequence number
	seqNum := binary.LittleEndian.Uint64(recordData[1:headerSize])

	// Extract payload
	payload := recordData[headerSize : recordSize-checksumSize]

	// Extract and verify checksum
	storedChecksum := binary.LittleEndian.Uint32(recordData[recordSize-checksumSize:])

	// Calculate checksum over header and payload
	checksum := crc32.NewIEEE()
	checksum.Write(recordData[:recordSize-checksumSize])
	calculatedChecksum := checksum.Sum32()

	if calculatedChecksum != storedChecksum {
		return 0, 0, nil, fmt.Errorf("WAL record checksum verification failed")
	}

	return recordType, seqNum, payload, nil
}

// applyRecord applies a WAL record to the memtable
func (w *WAL) applyRecord(memtable Memtable, recordType RecordType, payload []byte) error {
	switch recordType {
	case RecordTypeAdd:
		// Extract id and value
		id := binary.LittleEndian.Uint64(payload[:8])
		value := int64(binary.LittleEndian.Uint64(payload[8:16]))

		// Apply to memtable
		return memtable.Add(id, value)

	case RecordTypeBatchAdd:
		// Extract count
		count := int(binary.LittleEndian.Uint32(payload[:4]))

		// Extract ids and values
		ids := make([]uint64, count)
		values := make([]int64, count)

		offset := 4
		for i := 0; i < count; i++ {
			ids[i] = binary.LittleEndian.Uint64(payload[offset : offset+8])
			values[i] = int64(binary.LittleEndian.Uint64(payload[offset+8 : offset+16]))
			offset += 16
		}

		// Apply to memtable
		return memtable.BatchAdd(ids, values)

	case RecordTypeDelete:
		// Extract id
		id := binary.LittleEndian.Uint64(payload[:8])

		// Apply to memtable
		memtable.Delete(id)
		return nil

	case RecordTypeBatchDelete:
		// Extract count
		count := int(binary.LittleEndian.Uint32(payload[:4]))

		// Extract ids
		ids := make([]uint64, count)

		offset := 4
		for i := 0; i < count; i++ {
			ids[i] = binary.LittleEndian.Uint64(payload[offset : offset+8])
			offset += 8
		}

		// Apply to memtable
		memtable.BatchDelete(ids)
		return nil

	default:
		return fmt.Errorf("unknown WAL record type: %d", recordType)
	}
}
