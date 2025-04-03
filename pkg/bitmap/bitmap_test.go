package bitmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitmap(t *testing.T) {
	// Test New
	b := New()
	assert.NotNil(t, b)
	assert.True(t, b.IsEmpty())
	assert.Equal(t, uint64(0), b.Count())

	// Test Add
	b.Add(1)
	b.Add(10)
	b.Add(100)
	assert.Equal(t, uint64(3), b.Count())
	assert.False(t, b.IsEmpty())

	// Test Contains
	assert.True(t, b.Contains(1))
	assert.True(t, b.Contains(10))
	assert.True(t, b.Contains(100))
	assert.False(t, b.Contains(2))
	assert.False(t, b.Contains(11))
	assert.False(t, b.Contains(101))

	// Test ToArray
	ids := b.ToArray()
	assert.Equal(t, 3, len(ids))
	assert.Contains(t, ids, uint64(1))
	assert.Contains(t, ids, uint64(10))
	assert.Contains(t, ids, uint64(100))

	// Test Clone
	b2 := b.Clone()
	assert.Equal(t, b.Count(), b2.Count())
	assert.True(t, b2.Contains(1))
	assert.True(t, b2.Contains(10))
	assert.True(t, b2.Contains(100))

	// Test operations
	c := New()
	c.Add(1)
	c.Add(2)
	c.Add(3)

	// Test And
	b2.And(c)
	assert.Equal(t, uint64(1), b2.Count())
	assert.True(t, b2.Contains(1))
	assert.False(t, b2.Contains(10))
	assert.False(t, b2.Contains(100))

	// Test Or
	d := New()
	d.Add(5)
	d.Add(6)
	c.Or(d)
	assert.Equal(t, uint64(5), c.Count())
	assert.True(t, c.Contains(1))
	assert.True(t, c.Contains(2))
	assert.True(t, c.Contains(3))
	assert.True(t, c.Contains(5))
	assert.True(t, c.Contains(6))

	// Test AndNot
	c.AndNot(d)
	assert.Equal(t, uint64(3), c.Count())
	assert.True(t, c.Contains(1))
	assert.True(t, c.Contains(2))
	assert.True(t, c.Contains(3))
	assert.False(t, c.Contains(5))
	assert.False(t, c.Contains(6))

	// Test Clear
	c.Clear()
	assert.Equal(t, uint64(0), c.Count())
	assert.True(t, c.IsEmpty())
}

func TestFromIDs(t *testing.T) {
	ids := []uint64{1, 5, 10, 50, 100, 500, 1000}
	b := FromIDs(ids)

	assert.Equal(t, uint64(len(ids)), b.Count())
	for _, id := range ids {
		assert.True(t, b.Contains(id))
	}

	// Check a few non-included IDs
	assert.False(t, b.Contains(2))
	assert.False(t, b.Contains(51))
	assert.False(t, b.Contains(101))
}

func TestSerialization(t *testing.T) {
	b := New()
	b.Add(1)
	b.Add(10)
	b.Add(100)
	b.Add(1000)
	b.Add(10000)

	// Serialize
	buffer := b.ToBuffer()
	assert.NotEmpty(t, buffer)

	// Deserialize
	b2 := FromBuffer(buffer)

	// Verify content
	assert.Equal(t, b.Count(), b2.Count())
	assert.True(t, b2.Contains(1))
	assert.True(t, b2.Contains(10))
	assert.True(t, b2.Contains(100))
	assert.True(t, b2.Contains(1000))
	assert.True(t, b2.Contains(10000))
}

func TestInternalFunctions(t *testing.T) {
	b := New()
	b.Add(1)
	b.Add(10)

	// GetSroarBitmap should return non-nil
	rb := b.GetSroarBitmap()
	assert.NotNil(t, rb)

	// FromSroarBitmap should create a valid bitmap
	b2 := FromSroarBitmap(rb)
	assert.NotNil(t, b2)
	assert.Equal(t, b.Count(), b2.Count())
	assert.True(t, b2.Contains(1))
	assert.True(t, b2.Contains(10))
}

func TestLargeIDs(t *testing.T) {
	// Test IDs beyond uint32
	b := New()

	// Add a mixture of small and large IDs
	smallID := uint64(42)
	largeID := uint64(1<<33 + 5) // Beyond uint32 range

	b.Add(smallID)
	b.Add(largeID)

	// Sroar supports full uint64 range
	assert.True(t, b.Contains(smallID))
	assert.True(t, b.Contains(largeID))
	assert.Equal(t, uint64(2), b.Count())
}
