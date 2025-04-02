package col

import (
	"encoding/binary"
)

// serializeVarIntIDs serializes IDs using variable-length encoding
func (bw *BufferedWriter) serializeVarIntIDs(ids []uint64) ([]byte, error) {
	var encodedBytes [][]byte
	for _, id := range ids {
		encoded := encodeVarInt(id)
		encodedBytes = append(encodedBytes, encoded)
	}

	// Pre-calculate total buffer size
	totalSize := 0
	for i := range encodedBytes {
		totalSize += len(encodedBytes[i])
	}

	// Create a single buffer for all IDs
	buffer := make([]byte, totalSize)
	offset := 0

	// Copy all encoded bytes into the buffer
	for i := range encodedBytes {
		copy(buffer[offset:], encodedBytes[i])
		offset += len(encodedBytes[i])
	}

	return buffer, nil
}

// serializeFixedLengthIDs serializes IDs using fixed-length encoding
func (bw *BufferedWriter) serializeFixedLengthIDs(ids []uint64) ([]byte, error) {
	// Pre-calculate buffer size: 8 bytes per ID
	buffer := make([]byte, len(ids)*8)

	// Write each ID into the buffer
	for i, id := range ids {
		binary.LittleEndian.PutUint64(buffer[i*8:], id)
	}

	return buffer, nil
}

// serializeVarIntValues serializes values using variable-length encoding
func (bw *BufferedWriter) serializeVarIntValues(values []int64) ([]byte, error) {
	var encodedBytes [][]byte
	for _, value := range values {
		encoded := encodeSignedVarInt(value)
		encodedBytes = append(encodedBytes, encoded)
	}

	// Pre-calculate total buffer size
	totalSize := 0
	for i := range encodedBytes {
		totalSize += len(encodedBytes[i])
	}

	// Create a single buffer for all values
	buffer := make([]byte, totalSize)
	offset := 0

	// Copy all encoded bytes into the buffer
	for i := range encodedBytes {
		copy(buffer[offset:], encodedBytes[i])
		offset += len(encodedBytes[i])
	}

	return buffer, nil
}

// serializeFixedLengthValues serializes values using fixed-length encoding
func (bw *BufferedWriter) serializeFixedLengthValues(values []int64) ([]byte, error) {
	// Pre-calculate buffer size: 8 bytes per value
	buffer := make([]byte, len(values)*8)

	// Write each value into the buffer
	for i, value := range values {
		binary.LittleEndian.PutUint64(buffer[i*8:], int64ToUint64(value))
	}

	return buffer, nil
}
