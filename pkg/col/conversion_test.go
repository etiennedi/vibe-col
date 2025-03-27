package col

import (
	"testing"
)

// TestInt64Uint64Conversion tests the int64ToUint64 and uint64ToInt64 conversion functions
func TestInt64Uint64Conversion(t *testing.T) {
	testValues := []int64{
		0,
		1,
		-1,
		42,
		-42,
		9223372036854775807,  // Max int64
		-9223372036854775808, // Min int64
		-923,                 // Value from failing test
	}

	for _, val := range testValues {
		converted := int64ToUint64(val)
		roundtrip := uint64ToInt64(converted)

		t.Logf("Original: %d, Converted: %d, Roundtrip: %d", val, converted, roundtrip)

		if roundtrip != val {
			t.Errorf("Conversion failed for %d: got %d after roundtrip", val, roundtrip)
		}
	}
}
