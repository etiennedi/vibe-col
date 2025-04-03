// Package bitmap provides a wrapper around the roaring bitmap library
package bitmap

import (
	"github.com/RoaringBitmap/roaring"
)

// Bitmap is a wrapper around roaring bitmap
type Bitmap struct {
	bitmap *roaring.Bitmap
}

// New creates a new empty bitmap
func New() *Bitmap {
	return &Bitmap{
		bitmap: roaring.New(),
	}
}

// FromIDs creates a bitmap from a list of IDs
func FromIDs(ids []uint64) *Bitmap {
	b := New()
	for _, id := range ids {
		b.Add(id)
	}
	return b
}

// Add adds an ID to the bitmap
func (b *Bitmap) Add(id uint64) {
	if id <= roaring.MaxUint32 {
		b.bitmap.Add(uint32(id))
	}
}

// Contains checks if the bitmap contains the ID
func (b *Bitmap) Contains(id uint64) bool {
	if id <= roaring.MaxUint32 {
		return b.bitmap.Contains(uint32(id))
	}
	return false
}

// Count returns the number of IDs in the bitmap
func (b *Bitmap) Count() uint64 {
	return uint64(b.bitmap.GetCardinality())
}

// ToArray returns all IDs in the bitmap as a slice
func (b *Bitmap) ToArray() []uint64 {
	uint32Array := b.bitmap.ToArray()
	result := make([]uint64, len(uint32Array))
	for i, v := range uint32Array {
		result[i] = uint64(v)
	}
	return result
}

// Clone returns a copy of the bitmap
func (b *Bitmap) Clone() *Bitmap {
	return &Bitmap{
		bitmap: b.bitmap.Clone(),
	}
}

// And performs a bitwise AND operation with another bitmap
// modifies the bitmap in place
func (b *Bitmap) And(other *Bitmap) {
	b.bitmap.And(other.bitmap)
}

// Or performs a bitwise OR operation with another bitmap
// modifies the bitmap in place
func (b *Bitmap) Or(other *Bitmap) {
	b.bitmap.Or(other.bitmap)
}

// AndNot performs a bitwise AND NOT operation with another bitmap
// modifies the bitmap in place
func (b *Bitmap) AndNot(other *Bitmap) {
	b.bitmap.AndNot(other.bitmap)
}

// IsEmpty checks if the bitmap is empty
func (b *Bitmap) IsEmpty() bool {
	return b.bitmap.IsEmpty()
}

// Clear removes all IDs from the bitmap
func (b *Bitmap) Clear() {
	b.bitmap.Clear()
}

// ToBuffer returns the bitmap as a serialized byte slice
func (b *Bitmap) ToBuffer() ([]byte, error) {
	return b.bitmap.ToBytes()
}

// FromBuffer creates a bitmap from a serialized byte slice
func FromBuffer(buffer []byte) (*Bitmap, error) {
	rb := roaring.New()
	_, err := rb.FromBuffer(buffer)
	if err != nil {
		return nil, err
	}
	return &Bitmap{bitmap: rb}, nil
}

// GetSroarBitmap returns the underlying roaring bitmap
// This is for internal compatibility only
func (b *Bitmap) GetSroarBitmap() *roaring.Bitmap {
	return b.bitmap
}

// FromSroarBitmap creates a bitmap from a roaring bitmap
// This is for internal compatibility only
func FromSroarBitmap(rb *roaring.Bitmap) *Bitmap {
	return &Bitmap{
		bitmap: rb,
	}
}
