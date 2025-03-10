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