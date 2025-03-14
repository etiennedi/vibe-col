package col

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func TestDeltaEncodeEmpty(t *testing.T) {
	// Test with empty slices
	emptyUint64 := []uint64{}
	emptyInt64 := []int64{}

	// Test encoding empty slices
	encodedUint64 := deltaEncode(emptyUint64)
	encodedInt64 := deltaEncodeInt64(emptyInt64)

	// Should return empty slices
	if len(encodedUint64) != 0 {
		t.Errorf("Expected empty slice for uint64, got %v", encodedUint64)
	}
	if len(encodedInt64) != 0 {
		t.Errorf("Expected empty slice for int64, got %v", encodedInt64)
	}

	// Test decoding empty slices
	decodedUint64 := deltaDecode(emptyUint64)
	decodedInt64 := deltaDecodeInt64(emptyInt64)

	// Should return empty slices
	if len(decodedUint64) != 0 {
		t.Errorf("Expected empty slice for uint64, got %v", decodedUint64)
	}
	if len(decodedInt64) != 0 {
		t.Errorf("Expected empty slice for int64, got %v", decodedInt64)
	}
}

func TestDeltaEncodeSingle(t *testing.T) {
	// Test with slices containing single value
	singleUint64 := []uint64{42}
	singleInt64 := []int64{-123}

	// Test encoding single-value slices
	encodedUint64 := deltaEncode(singleUint64)
	encodedInt64 := deltaEncodeInt64(singleInt64)

	// First value should be unchanged
	if len(encodedUint64) != 1 || encodedUint64[0] != 42 {
		t.Errorf("Expected [42] for uint64, got %v", encodedUint64)
	}
	if len(encodedInt64) != 1 || encodedInt64[0] != -123 {
		t.Errorf("Expected [-123] for int64, got %v", encodedInt64)
	}

	// Test decoding single-value slices
	decodedUint64 := deltaDecode(encodedUint64)
	decodedInt64 := deltaDecodeInt64(encodedInt64)

	// Should get original values back
	if len(decodedUint64) != 1 || decodedUint64[0] != 42 {
		t.Errorf("Expected decoded [42] for uint64, got %v", decodedUint64)
	}
	if len(decodedInt64) != 1 || decodedInt64[0] != -123 {
		t.Errorf("Expected decoded [-123] for int64, got %v", decodedInt64)
	}
}

func TestDeltaEncodeSequential(t *testing.T) {
	// Test with sequential values - common case for IDs
	sequentialUint64 := []uint64{1000, 1001, 1002, 1003, 1004}
	sequentialInt64 := []int64{500, 550, 600, 650, 700}

	// Expected deltas
	expectedUint64Deltas := []uint64{1000, 1, 1, 1, 1}
	expectedInt64Deltas := []int64{500, 50, 50, 50, 50}

	// Test encoding
	encodedUint64 := deltaEncode(sequentialUint64)
	encodedInt64 := deltaEncodeInt64(sequentialInt64)

	// Check encoded values match expected deltas
	if !reflect.DeepEqual(encodedUint64, expectedUint64Deltas) {
		t.Errorf("Expected deltas %v, got %v", expectedUint64Deltas, encodedUint64)
	}
	if !reflect.DeepEqual(encodedInt64, expectedInt64Deltas) {
		t.Errorf("Expected deltas %v, got %v", expectedInt64Deltas, encodedInt64)
	}

	// Test decoding
	decodedUint64 := deltaDecode(encodedUint64)
	decodedInt64 := deltaDecodeInt64(encodedInt64)

	// Check decoded values match original
	if !reflect.DeepEqual(decodedUint64, sequentialUint64) {
		t.Errorf("Roundtrip failed. Expected %v, got %v", sequentialUint64, decodedUint64)
	}
	if !reflect.DeepEqual(decodedInt64, sequentialInt64) {
		t.Errorf("Roundtrip failed. Expected %v, got %v", sequentialInt64, decodedInt64)
	}
}

func TestDeltaEncodeWithGaps(t *testing.T) {
	// Test with non-uniform gaps
	gappyUint64 := []uint64{1000, 1100, 1105, 1200, 1500}
	gappyInt64 := []int64{5000, 4000, 4500, 3000, 5000}

	// Expected deltas
	expectedUint64Deltas := []uint64{1000, 100, 5, 95, 300}
	expectedInt64Deltas := []int64{5000, -1000, 500, -1500, 2000}

	// Test encoding
	encodedUint64 := deltaEncode(gappyUint64)
	encodedInt64 := deltaEncodeInt64(gappyInt64)

	// Check encoded values match expected deltas
	if !reflect.DeepEqual(encodedUint64, expectedUint64Deltas) {
		t.Errorf("Expected deltas %v, got %v", expectedUint64Deltas, encodedUint64)
	}
	if !reflect.DeepEqual(encodedInt64, expectedInt64Deltas) {
		t.Errorf("Expected deltas %v, got %v", expectedInt64Deltas, encodedInt64)
	}

	// Test decoding
	decodedUint64 := deltaDecode(encodedUint64)
	decodedInt64 := deltaDecodeInt64(encodedInt64)

	// Check decoded values match original
	if !reflect.DeepEqual(decodedUint64, gappyUint64) {
		t.Errorf("Roundtrip failed. Expected %v, got %v", gappyUint64, decodedUint64)
	}
	if !reflect.DeepEqual(decodedInt64, gappyInt64) {
		t.Errorf("Roundtrip failed. Expected %v, got %v", gappyInt64, decodedInt64)
	}
}

func TestDeltaEncodeDecreasingValues(t *testing.T) {
	// Test with decreasing values - edge case for uint64
	decreasingUint64 := []uint64{1000, 990, 980, 970, 960}
	decreasingInt64 := []int64{500, 400, 300, 200, 100}

	// Expected deltas (note underflow for uint64)
	expectedUint64Deltas := []uint64{1000, 18446744073709551606, 18446744073709551606, 18446744073709551606, 18446744073709551606}
	expectedInt64Deltas := []int64{500, -100, -100, -100, -100}

	// Test encoding
	encodedUint64 := deltaEncode(decreasingUint64)
	encodedInt64 := deltaEncodeInt64(decreasingInt64)

	// Check encoded values match expected deltas
	if !reflect.DeepEqual(encodedUint64, expectedUint64Deltas) {
		t.Errorf("Expected deltas %v, got %v", expectedUint64Deltas, encodedUint64)
	}
	if !reflect.DeepEqual(encodedInt64, expectedInt64Deltas) {
		t.Errorf("Expected deltas %v, got %v", expectedInt64Deltas, encodedInt64)
	}

	// Test decoding
	decodedUint64 := deltaDecode(encodedUint64)
	decodedInt64 := deltaDecodeInt64(encodedInt64)

	// Check decoded values match original
	if !reflect.DeepEqual(decodedUint64, decreasingUint64) {
		t.Errorf("Roundtrip failed. Expected %v, got %v", decreasingUint64, decodedUint64)
	}
	if !reflect.DeepEqual(decodedInt64, decreasingInt64) {
		t.Errorf("Roundtrip failed. Expected %v, got %v", decreasingInt64, decodedInt64)
	}
}

func TestDeltaEncodeWithDuplicates(t *testing.T) {
	// Test with duplicate values
	dupUint64 := []uint64{1000, 1000, 1000, 1200, 1200}
	dupInt64 := []int64{500, 500, 500, 700, 700}

	// Expected deltas
	expectedUint64Deltas := []uint64{1000, 0, 0, 200, 0}
	expectedInt64Deltas := []int64{500, 0, 0, 200, 0}

	// Test encoding
	encodedUint64 := deltaEncode(dupUint64)
	encodedInt64 := deltaEncodeInt64(dupInt64)

	// Check encoded values match expected deltas
	if !reflect.DeepEqual(encodedUint64, expectedUint64Deltas) {
		t.Errorf("Expected deltas %v, got %v", expectedUint64Deltas, encodedUint64)
	}
	if !reflect.DeepEqual(encodedInt64, expectedInt64Deltas) {
		t.Errorf("Expected deltas %v, got %v", expectedInt64Deltas, encodedInt64)
	}

	// Test decoding
	decodedUint64 := deltaDecode(encodedUint64)
	decodedInt64 := deltaDecodeInt64(encodedInt64)

	// Check decoded values match original
	if !reflect.DeepEqual(decodedUint64, dupUint64) {
		t.Errorf("Roundtrip failed. Expected %v, got %v", dupUint64, decodedUint64)
	}
	if !reflect.DeepEqual(decodedInt64, dupInt64) {
		t.Errorf("Roundtrip failed. Expected %v, got %v", dupInt64, decodedInt64)
	}
}

func TestDeltaEncodeLarge(t *testing.T) {
	// Larger test with random values
	size := 1000

	// For reproducibility, use fixed seed
	r := rand.New(rand.NewSource(42))

	// Generate a mix of increasing/decreasing/plateaus
	inputUint64 := make([]uint64, size)
	inputInt64 := make([]int64, size)

	// Start with a base value
	inputUint64[0] = 10000
	inputInt64[0] = 5000

	// Generate values with various patterns
	for i := 1; i < size; i++ {
		// Uint64 - generally increasing with occasional decreases
		change := r.Intn(100)
		if r.Intn(10) < 1 { // 10% chance of decrease
			change = -change
		}

		if inputUint64[i-1] > uint64(change) || change >= 0 {
			inputUint64[i] = inputUint64[i-1] + uint64(change)
		} else {
			inputUint64[i] = inputUint64[i-1] // avoid underflow
		}

		// Int64 - can increase or decrease freely
		inputInt64[i] = inputInt64[i-1] + int64(r.Intn(200)-100)
	}

	// Encode
	encodedUint64 := deltaEncode(inputUint64)
	encodedInt64 := deltaEncodeInt64(inputInt64)

	// Decode
	decodedUint64 := deltaDecode(encodedUint64)
	decodedInt64 := deltaDecodeInt64(encodedInt64)

	// Verify roundtrip
	if !reflect.DeepEqual(decodedUint64, inputUint64) {
		t.Errorf("Uint64 roundtrip failed for large dataset")
		// Print first mismatch for debugging
		for i := 0; i < size; i++ {
			if decodedUint64[i] != inputUint64[i] {
				t.Errorf("First mismatch at index %d: expected %d, got %d",
					i, inputUint64[i], decodedUint64[i])
				break
			}
		}
	}

	if !reflect.DeepEqual(decodedInt64, inputInt64) {
		t.Errorf("Int64 roundtrip failed for large dataset")
		// Print first mismatch for debugging
		for i := 0; i < size; i++ {
			if decodedInt64[i] != inputInt64[i] {
				t.Errorf("First mismatch at index %d: expected %d, got %d",
					i, inputInt64[i], decodedInt64[i])
				break
			}
		}
	}
}

func TestVarIntEncoding(t *testing.T) {
	testCases := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0}},
		{1, []byte{1}},
		{127, []byte{127}},
		{128, []byte{128, 1}},
		{16383, []byte{255, 127}},
		{16384, []byte{128, 128, 1}},
		{2097151, []byte{255, 255, 127}},
		{2097152, []byte{128, 128, 128, 1}},
		{268435455, []byte{255, 255, 255, 127}},
		{268435456, []byte{128, 128, 128, 128, 1}},
		{0xFFFFFFFF, []byte{255, 255, 255, 255, 15}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Encode_%d", tc.value), func(t *testing.T) {
			result := encodeVarInt(tc.value)
			if !bytes.Equal(result, tc.expected) {
				t.Errorf("encodeVarInt(%d) = %v, expected %v", tc.value, result, tc.expected)
			}
		})

		t.Run(fmt.Sprintf("Decode_%d", tc.value), func(t *testing.T) {
			value, bytesRead := decodeVarInt(tc.expected)
			if value != tc.value {
				t.Errorf("decodeVarInt(%v) = %d, expected %d", tc.expected, value, tc.value)
			}
			if bytesRead != len(tc.expected) {
				t.Errorf("decodeVarInt(%v) read %d bytes, expected %d", tc.expected, bytesRead, len(tc.expected))
			}
		})
	}
}

func TestSignedVarIntEncoding(t *testing.T) {
	testCases := []struct {
		value int64
	}{
		{0},
		{1},
		{-1},
		{63},
		{-63},
		{64},
		{-64},
		{8191},
		{-8191},
		{8192},
		{-8192},
		{1048575},
		{-1048575},
		{1048576},
		{-1048576},
		{134217727},
		{-134217727},
		{134217728},
		{-134217728},
		{0x7FFFFFFF},
		{-0x7FFFFFFF},
		{0x7FFFFFFFFFFFFFFF},
		{-0x7FFFFFFFFFFFFFFF},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("SignedRoundTrip_%d", tc.value), func(t *testing.T) {
			encoded := encodeSignedVarInt(tc.value)
			decoded, bytesRead := decodeSignedVarInt(encoded)

			if decoded != tc.value {
				t.Errorf("Round trip failed for %d: got %d", tc.value, decoded)
			}

			if bytesRead != len(encoded) {
				t.Errorf("decodeSignedVarInt(%v) read %d bytes, expected %d", encoded, bytesRead, len(encoded))
			}
		})
	}
}

func TestVarintEncodingSize(t *testing.T) {
	// Test various value ranges to verify encoding sizes
	cases := []struct {
		value        uint64
		expectedSize int
	}{
		{0, 1},           // Minimum size is 1 byte
		{127, 1},         // Max value for 1 byte
		{128, 2},         // Min value for 2 bytes
		{16383, 2},       // Max value for 2 bytes
		{16384, 3},       // Min value for 3 bytes
		{2097151, 3},     // Max value for 3 bytes
		{2097152, 4},     // Min value for 4 bytes
		{268435455, 4},   // Max value for 4 bytes
		{1 << 35, 6},     // Larger value
		{1 << 56, 9},     // Even larger
		{^uint64(0), 10}, // Max uint64 value
	}

	for _, c := range cases {
		encoded := encodeVarInt(c.value)
		if len(encoded) != c.expectedSize {
			t.Errorf("Value %d: expected encoding size %d, got %d",
				c.value, c.expectedSize, len(encoded))
		}

		// Verify decoding
		decoded, bytesRead := decodeVarInt(encoded)
		if decoded != c.value {
			t.Errorf("Value %d: decoded as %d", c.value, decoded)
		}
		if bytesRead != c.expectedSize {
			t.Errorf("Value %d: bytes read %d, expected %d",
				c.value, bytesRead, c.expectedSize)
		}
	}
}

func BenchmarkVarIntEncoding(b *testing.B) {
	// Test values in different ranges
	testValues := []uint64{
		0,           // 1 byte
		42,          // 1 byte
		128,         // 2 bytes
		16384,       // 3 bytes
		2097152,     // 4 bytes
		268435456,   // 5 bytes
		34359738368, // 6 bytes
		^uint64(0),  // 10 bytes (max uint64)
	}

	for _, v := range testValues {
		b.Run(fmt.Sprintf("Encode_%d", v), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = encodeVarInt(v)
			}
		})
	}

	// Prepare encoded values for decoding benchmark
	encodedValues := make([][]byte, len(testValues))
	for i, v := range testValues {
		encodedValues[i] = encodeVarInt(v)
	}

	for i, encoded := range encodedValues {
		b.Run(fmt.Sprintf("Decode_%d", testValues[i]), func(b *testing.B) {
			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				_, _ = decodeVarInt(encoded)
			}
		})
	}
}

func BenchmarkSignedVarIntEncoding(b *testing.B) {
	// Test values in different ranges
	testValues := []int64{
		0,                   // 1 byte
		42,                  // 1 byte
		-42,                 // 1 byte
		1000,                // 2 bytes
		-1000,               // 2 bytes
		1000000,             // 3 bytes
		-1000000,            // 3 bytes
		1000000000,          // 5 bytes
		-1000000000,         // 5 bytes
		0x7FFFFFFFFFFFFFFF,  // 10 bytes (max int64)
		-0x8000000000000000, // 10 bytes (min int64)
	}

	for _, v := range testValues {
		b.Run(fmt.Sprintf("Encode_%d", v), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = encodeSignedVarInt(v)
			}
		})
	}

	// Prepare encoded values for decoding benchmark
	encodedValues := make([][]byte, len(testValues))
	for i, v := range testValues {
		encodedValues[i] = encodeSignedVarInt(v)
	}

	for i, encoded := range encodedValues {
		b.Run(fmt.Sprintf("Decode_%d", testValues[i]), func(b *testing.B) {
			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				_, _ = decodeSignedVarInt(encoded)
			}
		})
	}
}
