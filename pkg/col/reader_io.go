package col

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// bufferPool is a pool of byte slices for reuse
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Start with a reasonable size buffer
		return make([]byte, 4096)
	},
}

// readBytesAt reads bytes at a specific offset
func (r *Reader) readBytesAt(offset int64, size int) ([]byte, error) {
	// Get a buffer from the pool
	bufInterface := bufferPool.Get()
	buf := bufInterface.([]byte)

	// Ensure the buffer is large enough
	if cap(buf) < size {
		// If the buffer is too small, create a new one
		buf = make([]byte, size)
	} else {
		// Otherwise, resize the existing buffer
		buf = buf[:size]
	}

	// Read data into the buffer
	n, err := r.file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		// Return the buffer to the pool before returning an error
		bufferPool.Put(buf)
		return nil, fmt.Errorf("failed to read bytes at offset %d: %w", offset, err)
	}
	if n < size && err != io.EOF {
		// Return the buffer to the pool before returning an error
		bufferPool.Put(buf)
		return nil, fmt.Errorf("incomplete read at offset %d: got %d bytes, expected %d", offset, n, size)
	}

	// Create a copy of the data to return
	// This is necessary because we're returning the buffer to the pool
	result := make([]byte, n)
	copy(result, buf[:n])

	// Return the buffer to the pool
	bufferPool.Put(buf)

	return result, nil
}

// readUint64At reads a uint64 at a specific offset
func (r *Reader) readUint64At(offset int64) (uint64, error) {
	buf, err := r.readBytesAt(offset, 8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

// readUint32At reads a uint32 at a specific offset
func (r *Reader) readUint32At(offset int64) (uint32, error) {
	buf, err := r.readBytesAt(offset, 4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

// readBufferedUint64 reads a uint64 from a buffer at a specific offset
func readBufferedUint64(buf []byte, offset int) uint64 {
	return binary.LittleEndian.Uint64(buf[offset : offset+8])
}

// readBufferedUint32 reads a uint32 from a buffer at a specific offset
func readBufferedUint32(buf []byte, offset int) uint32 {
	return binary.LittleEndian.Uint32(buf[offset : offset+4])
}
