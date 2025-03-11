package col

import (
	"fmt"
)

// encodeData is a helper function to encode data based on the encoding type
func encodeData[T any](encodingType uint32, data []T, deltaEncodeFunc func([]T) []T, encodeVarIntFunc func(T) []byte) ([]T, [][]byte, uint32, error) {
	var encodedData []T
	var encodedDataBytes [][]byte
	var sectionSize uint32

	// First apply delta encoding if needed
	switch encodingType {
	case EncodingRaw, EncodingVarInt, EncodingVarIntID:
		// These encoding types don't use delta encoding
		encodedData = make([]T, len(data))
		copy(encodedData, data)
	case EncodingDeltaID, EncodingDeltaValue, EncodingDeltaBoth, EncodingVarIntValue, EncodingVarIntBoth:
		// These encoding types use delta encoding
		encodedData = deltaEncodeFunc(data)
	default:
		return nil, nil, 0, fmt.Errorf("unsupported encoding type: %d", encodingType)
	}

	// Then apply varint encoding if needed
	switch encodingType {
	case EncodingRaw, EncodingDeltaID, EncodingDeltaValue, EncodingDeltaBoth:
		// Fixed-width encoding
		sectionSize = uint32(len(encodedData) * 8)
	case EncodingVarInt, EncodingVarIntID, EncodingVarIntBoth, EncodingVarIntValue:
		// Variable-width encoding
		encodedDataBytes = make([][]byte, len(encodedData))
		sectionSize = 0
		for i, d := range encodedData {
			encodedDataBytes[i] = encodeVarIntFunc(d)
			dataSize := uint32(len(encodedDataBytes[i]))
			if dataSize == 0 {
				return nil, nil, 0, fmt.Errorf("encoded size of data at index %d is 0", i)
			}
			sectionSize += dataSize
		}
		if sectionSize == 0 && len(encodedData) > 0 {
			return nil, nil, 0, fmt.Errorf("calculated section size is 0 with %d items", len(encodedData))
		}
	}

	return encodedData, encodedDataBytes, sectionSize, nil
}
