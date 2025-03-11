package col

import (
	"encoding/binary"
	"fmt"
	"io"
)

// readBytesAt reads bytes at a specific offset
func (r *Reader) readBytesAt(offset int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	n, err := r.file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read bytes at offset %d: %w", offset, err)
	}
	if n < size && err != io.EOF {
		return nil, fmt.Errorf("incomplete read at offset %d: got %d bytes, expected %d", offset, n, size)
	}
	return buf, nil
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
