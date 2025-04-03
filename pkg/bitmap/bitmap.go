// Package bitmap provides a wrapper around the sroar bitmap library
package bitmap

import (
	"github.com/weaviate/sroar"
)

// Bitmap is a wrapper around sroar.Bitmap
type Bitmap struct {
	bitmap *sroar.Bitmap
}

// New creates a new empty bitmap
func New() *Bitmap {
	return &Bitmap{
		bitmap: sroar.NewBitmap(),
	}
}

// FromIDs creates a new bitmap from a list of IDs
func FromIDs(ids []uint64) *Bitmap {
	b := New()
	for _, id := range ids {
		b.Add(id)
	}
	return b
}

// Add adds an ID to the bitmap
func (b *Bitmap) Add(id uint64) {
	b.bitmap.Set(id)
}

// Contains checks if the bitmap contains the ID
func (b *Bitmap) Contains(id uint64) bool {
	return b.bitmap.Contains(id)
}

// Count returns the number of IDs in the bitmap
func (b *Bitmap) Count() uint64 {
	return uint64(b.bitmap.GetCardinality())
}

// ToArray returns all IDs in the bitmap as a slice
func (b *Bitmap) ToArray() []uint64 {
	return b.bitmap.ToArray()
}

// Clone returns a copy of the bitmap
func (b *Bitmap) Clone() *Bitmap {
	return &Bitmap{
		bitmap: b.bitmap.Clone(),
	}
}

// And performs the bitwise AND operation with another bitmap
func (b *Bitmap) And(other *Bitmap) {
	b.bitmap = sroar.And(b.bitmap, other.bitmap)
}

// Or performs the bitwise OR operation with another bitmap
func (b *Bitmap) Or(other *Bitmap) {
	b.bitmap = sroar.Or(b.bitmap, other.bitmap)
}

// AndNot performs the bitwise AND NOT operation with another bitmap
func (b *Bitmap) AndNot(other *Bitmap) {
	b.bitmap = sroar.AndNot(b.bitmap, other.bitmap)
}

// IsEmpty checks if the bitmap is empty
func (b *Bitmap) IsEmpty() bool {
	return b.bitmap.IsEmpty()
}

// Clear removes all IDs from the bitmap
func (b *Bitmap) Clear() {
	b.bitmap = sroar.NewBitmap()
}

// ToBuffer returns the bitmap as a serialized byte slice
func (b *Bitmap) ToBuffer() []byte {
	return b.bitmap.ToBuffer()
}

// FromBuffer creates a bitmap from a serialized byte slice
func FromBuffer(buf []byte) *Bitmap {
	return &Bitmap{
		bitmap: sroar.FromBuffer(buf),
	}
}

// GetSroarBitmap returns the underlying sroar bitmap
// This is needed for internal compatibility with the col package
func (b *Bitmap) GetSroarBitmap() *sroar.Bitmap {
	return b.bitmap
}

// FromSroarBitmap creates a bitmap from a sroar bitmap
// This is needed for internal compatibility with the col package
func FromSroarBitmap(bitmap *sroar.Bitmap) *Bitmap {
	return &Bitmap{
		bitmap: bitmap,
	}
}
