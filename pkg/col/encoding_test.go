package col

import (
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
		inputInt64[i] = inputInt64[i-1] + int64(r.Intn(200) - 100)
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