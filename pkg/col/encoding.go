package col

// DeltaEncoder handles delta encoding for a sequence of values
type DeltaEncoder interface {
	Encode(values interface{}) interface{}
	Decode(values interface{}) interface{}
}

// deltaEncode calculates delta-encoded values from original values
func deltaEncode(values []uint64) []uint64 {
	if len(values) == 0 {
		return []uint64{}
	}
	
	result := make([]uint64, len(values))
	// First value is stored as-is
	result[0] = values[0]
	
	// For remaining values, store delta from previous value
	for i := 1; i < len(values); i++ {
		result[i] = values[i] - values[i-1]
	}
	
	return result
}

// deltaDecode reconstructs original values from delta-encoded values
func deltaDecode(deltas []uint64) []uint64 {
	if len(deltas) == 0 {
		return []uint64{}
	}
	
	result := make([]uint64, len(deltas))
	// First value is stored as-is
	result[0] = deltas[0]
	
	// For remaining values, add the delta to the previous value
	for i := 1; i < len(deltas); i++ {
		result[i] = result[i-1] + deltas[i]
	}
	
	return result
}

// deltaEncodeInt64 calculates delta-encoded values from original int64 values
func deltaEncodeInt64(values []int64) []int64 {
	if len(values) == 0 {
		return []int64{}
	}
	
	result := make([]int64, len(values))
	// First value is stored as-is
	result[0] = values[0]
	
	// For remaining values, store delta from previous value
	for i := 1; i < len(values); i++ {
		result[i] = values[i] - values[i-1]
	}
	
	return result
}

// deltaDecodeInt64 reconstructs original int64 values from delta-encoded values
func deltaDecodeInt64(deltas []int64) []int64 {
	if len(deltas) == 0 {
		return []int64{}
	}
	
	result := make([]int64, len(deltas))
	// First value is stored as-is
	result[0] = deltas[0]
	
	// For remaining values, add the delta to the previous value
	for i := 1; i < len(deltas); i++ {
		result[i] = result[i-1] + deltas[i]
	}
	
	return result
}

// int64ToUint64 converts an int64 to uint64 for binary storage
// This preserves the bit pattern while allowing storage in uint64 fields
func int64ToUint64(value int64) uint64 {
	if value >= 0 {
		return uint64(value)
	}
	// Handle negative values by converting bits directly
	return uint64(uint64(^value+1) | (1 << 63))
}

// uint64ToInt64 converts a uint64 back to int64 after reading from storage
// This is the inverse of int64ToUint64
func uint64ToInt64(value uint64) int64 {
	if value&(1<<63) == 0 {
		return int64(value)
	}
	// Handle negative values by converting bits back
	return ^int64(value&^(1<<63)) + 1
}

// encodeVarInt encodes an unsigned integer as a variable-length byte array
// using a variable-length encoding scheme similar to Protocol Buffers:
// - Numbers between 0-127 are encoded in a single byte
// - Larger numbers use more bytes with 7 bits per byte for the value and
//   the 8th bit (MSB) as a continuation flag (1 = more bytes, 0 = final byte)
func encodeVarInt(value uint64) []byte {
	// Special case for 0
	if value == 0 {
		return []byte{0}
	}
	
	// Estimate bytes needed: 
	// - Each byte holds 7 bits of the number
	// - Maximum would be 10 bytes for uint64 (64 bits / 7 bits per byte, rounded up)
	result := make([]byte, 0, 10)
	
	// Break the number into 7-bit chunks
	for value > 0 {
		// Get the 7 least significant bits
		b := byte(value & 0x7F)
		
		// Shift value right by 7 bits for next iteration
		value >>= 7
		
		// If there are more bytes to encode, set the continuation bit
		if value > 0 {
			b |= 0x80 // Set the most significant bit (continuation flag)
		}
		
		// Add this byte to the result
		result = append(result, b)
	}
	
	return result
}

// decodeVarInt decodes a variable-length byte array back to uint64
// It reads bytes until it finds one without the continuation bit set
func decodeVarInt(data []byte) (uint64, int) {
	var result uint64
	var shift uint
	var bytesRead int
	
	for _, b := range data {
		bytesRead++
		
		// Extract the 7 value bits from this byte
		value := uint64(b & 0x7F)
		
		// Add these 7 bits to our result, shifted to the correct position
		result |= value << shift
		
		// If the continuation bit is not set, we're done
		if (b & 0x80) == 0 {
			return result, bytesRead
		}
		
		// Move to next 7-bit segment
		shift += 7
		
		// Safeguard against malformed data: uint64 uses max 10 bytes (70 bits)
		if shift >= 70 {
			return result, bytesRead
		}
	}
	
	// If we get here, the continuation bit was set on the last byte but we have no more data
	// Return what we've got
	return result, bytesRead
}

// encodeSignedVarInt encodes a signed int64 as a variable-length byte array
// It uses ZigZag encoding to convert signed integers to unsigned integers
// before applying variable-length encoding
func encodeSignedVarInt(value int64) []byte {
	// ZigZag encoding: map signed integers to unsigned integers
	// in a way that numbers with small absolute values map to small
	// unsigned numbers. This works well with variable-length encoding.
	// (value << 1) ^ (value >> 63) encodes:
	// 0 -> 0
	// -1 -> 1
	// 1 -> 2
	// -2 -> 3
	// ...and so on
	zigzag := uint64((value << 1) ^ (value >> 63))
	return encodeVarInt(zigzag)
}

// decodeSignedVarInt decodes a variable-length byte array back to int64
// It first decodes the ZigZag-encoded unsigned integer, then converts it back
// to a signed integer
func decodeSignedVarInt(data []byte) (int64, int) {
	zigzag, bytesRead := decodeVarInt(data)
	
	// ZigZag decoding: convert unsigned back to signed
	// (zigzag >> 1) ^ (-(zigzag & 1)) decodes:
	// 0 -> 0
	// 1 -> -1
	// 2 -> 1
	// 3 -> -2
	// ...and so on
	value := int64((zigzag >> 1) ^ (-(zigzag & 1)))
	return value, bytesRead
}