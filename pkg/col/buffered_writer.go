package col

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/weaviate/sroar"
)

// BufferedWriterOption defines a function type for configuring a BufferedWriter
type BufferedWriterOption func(*BufferedWriter)

// WithBufferedEncoding sets the encoding type for the BufferedWriter
func WithBufferedEncoding(encodingType uint32) BufferedWriterOption {
	return func(bw *BufferedWriter) {
		bw.encodingType = encodingType
	}
}

// WithBufferedBlockSize sets the block size target for the BufferedWriter
func WithBufferedBlockSize(blockSize uint32) BufferedWriterOption {
	return func(bw *BufferedWriter) {
		bw.blockSizeTarget = blockSize
	}
}

// WithBufferedLevel sets the compaction level for the BufferedWriter
func WithBufferedLevel(level uint16) BufferedWriterOption {
	return func(bw *BufferedWriter) {
		bw.level = level
	}
}

// BufferedWriter implements a writer for column files that buffers data in memory
// using BlockData structures and writes directly to disk.
type BufferedWriter struct {
	file            *os.File
	filename        string
	blockCount      uint64
	encodingType    uint32
	blockSizeTarget uint32
	level           uint16        // Compaction level (0 is base level)
	blockIndex      []FooterEntry // Detailed index of blocks
	globalIDs       *sroar.Bitmap
	deletedIDs      *sroar.Bitmap
	closed          bool

	// Position tracking to reduce Seek operations
	currentPosition int64

	// BlockData to buffer data before writing to disk
	pendingData *BlockData
	lastID      uint64 // for delta encoding calculation
	lastValue   int64  // for delta encoding calculation
}

// NewBufferedWriter creates a new BufferedWriter for the given filename
func NewBufferedWriter(filename string, options ...BufferedWriterOption) (*BufferedWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	writer := &BufferedWriter{
		file:            file,
		filename:        filename,
		blockCount:      0,
		encodingType:    EncodingRaw, // Default
		blockSizeTarget: defaultBlockSize,
		level:           0, // Default level is 0 (base level)
		blockIndex:      make([]FooterEntry, 0),
		globalIDs:       sroar.NewBitmap(),
		deletedIDs:      sroar.NewBitmap(),
		closed:          false,
		currentPosition: 0,
		pendingData:     nil,
		lastID:          0,
		lastValue:       0,
	}

	// Apply options
	for _, option := range options {
		option(writer)
	}

	// Write the file header
	if err := writer.writeHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return writer, nil
}

// Add adds a single ID-value pair
func (bw *BufferedWriter) Add(id uint64, value int64) error {
	if bw.closed {
		return fmt.Errorf("writer is already closed")
	}

	// A write is going to be at most 16 bytes (8 bytes for ID, 8 bytes for
	// value). It could technically be smaller if we're using varint encoding,
	// but that's close enough and simplfiies the logic considerably.
	if bw.CurrentBlockSize()+16 > bw.blockSizeTarget {
		// flush first
		if err := bw.Flush(); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
	}

	// Create a new BlockData for this single item if needed
	if bw.pendingData == nil {
		bw.pendingData = &BlockData{
			MinID:                  id,
			MaxID:                  id,
			MinValue:               value,
			MaxValue:               value,
			Sum:                    value,
			Count:                  1,
			SerializedIDSection:    make([]byte, 0, bw.blockSizeTarget/2), // Use half the block size target for IDs
			SerializedValueSection: make([]byte, 0, bw.blockSizeTarget/2), // Use half the block size target for values
		}

		// First ID  and value are	is not delta encoded, so we only need to pay
		// attention to varint or not
		var idEncoded []byte
		var valueEncoded []byte
		// if we're using varint encoding, we need to encode first to know the size
		if bw.encodingType == EncodingVarIntID || bw.encodingType == EncodingVarIntBoth {
			idEncoded = encodeVarInt(id)
		} else {
			idEncoded = make([]byte, 8)
			binary.LittleEndian.PutUint64(idEncoded, id)
		}

		if bw.encodingType == EncodingVarIntValue || bw.encodingType == EncodingVarIntBoth {
			valueEncoded = encodeSignedVarInt(value)
		} else {
			valueEncoded = make([]byte, 8)
			binary.LittleEndian.PutUint64(valueEncoded, int64ToUint64(value))
		}

		bw.pendingData.SerializedIDSection = bw.pendingData.SerializedIDSection[:len(idEncoded)] // always safe, we create a much larger buffer
		copy(bw.pendingData.SerializedIDSection, idEncoded)
		bw.pendingData.SerializedValueSection = bw.pendingData.SerializedValueSection[:len(valueEncoded)] // always safe, we create a much larger buffer
		copy(bw.pendingData.SerializedValueSection, valueEncoded)

	} else {
		// in case of delta encoding we need to override the id with the delta id
		idToWrite := id
		if bw.encodingType == EncodingDeltaID || bw.encodingType == EncodingDeltaBoth || bw.encodingType == EncodingVarIntID || bw.encodingType == EncodingVarIntBoth {
			idToWrite = id - bw.lastID
		}
		// in case of delta encoding we need to override the value with the delta value
		valueToWrite := value
		if bw.encodingType == EncodingDeltaValue || bw.encodingType == EncodingDeltaBoth || bw.encodingType == EncodingVarIntValue || bw.encodingType == EncodingVarIntBoth {
			valueToWrite = value - bw.lastValue
		}

		var idEncoded []byte
		var valueEncoded []byte
		// if we're using varint encoding, we need to encode first to know the size
		if bw.encodingType == EncodingVarIntID || bw.encodingType == EncodingVarIntBoth {
			idEncoded = encodeVarInt(idToWrite)
		} else {
			idEncoded = make([]byte, 8)
			binary.LittleEndian.PutUint64(idEncoded, idToWrite)
		}

		if bw.encodingType == EncodingVarIntValue || bw.encodingType == EncodingVarIntBoth {
			valueEncoded = encodeSignedVarInt(valueToWrite)
		} else {
			valueEncoded = make([]byte, 8)
			binary.LittleEndian.PutUint64(valueEncoded, int64ToUint64(valueToWrite))
		}

		if len(bw.pendingData.SerializedIDSection)+len(idEncoded) > cap(bw.pendingData.SerializedIDSection) {
			// we need to grow the slice
			newSlice := make([]byte, len(bw.pendingData.SerializedIDSection), cap(bw.pendingData.SerializedIDSection)*2)
			copy(newSlice, bw.pendingData.SerializedIDSection)
			bw.pendingData.SerializedIDSection = newSlice
		}

		// we know we have enough space
		bw.pendingData.SerializedIDSection = bw.pendingData.SerializedIDSection[:len(bw.pendingData.SerializedIDSection)+len(idEncoded)]
		copy(bw.pendingData.SerializedIDSection[len(bw.pendingData.SerializedIDSection)-len(idEncoded):], idEncoded)

		if len(bw.pendingData.SerializedValueSection)+len(valueEncoded) > cap(bw.pendingData.SerializedValueSection) {
			// we need to grow the slice
			newSlice := make([]byte, len(bw.pendingData.SerializedValueSection), cap(bw.pendingData.SerializedValueSection)*2)
			copy(newSlice, bw.pendingData.SerializedValueSection)
			bw.pendingData.SerializedValueSection = newSlice
		}

		// we know we have enough space
		bw.pendingData.SerializedValueSection = bw.pendingData.SerializedValueSection[:len(bw.pendingData.SerializedValueSection)+len(valueEncoded)]
		copy(bw.pendingData.SerializedValueSection[len(bw.pendingData.SerializedValueSection)-len(valueEncoded):], valueEncoded)

		// Update statistics
		if id < bw.pendingData.MinID {
			bw.pendingData.MinID = id
		}
		if id > bw.pendingData.MaxID {
			bw.pendingData.MaxID = id
		}
		if value < bw.pendingData.MinValue {
			bw.pendingData.MinValue = value
		}
		if value > bw.pendingData.MaxValue {
			bw.pendingData.MaxValue = value
		}
		bw.pendingData.Sum += value
		bw.pendingData.Count++
	}

	bw.globalIDs.Set(id)
	bw.lastID = id
	bw.lastValue = value

	return nil
}

// BatchAdd adds multiple ID-value pairs at once, which is much more efficient than
// calling Add multiple times. This is especially beneficial for large numbers of entries.
func (bw *BufferedWriter) BatchAdd(ids []uint64, values []int64) error {
	if bw.closed {
		return fmt.Errorf("writer is already closed")
	}

	if len(ids) != len(values) {
		return fmt.Errorf("ids and values must have the same length")
	}

	if len(ids) == 0 {
		return nil // Nothing to do
	}

	// If we have pending data and we'd exceed the block size target, flush first
	if bw.pendingData != nil {
		// Estimate a worst-case scenario: 16 bytes per entry (for both ID and value)
		worstCaseAddition := uint32(len(ids) * 16)
		if bw.CurrentBlockSize()+worstCaseAddition > bw.blockSizeTarget {
			if err := bw.Flush(); err != nil {
				return fmt.Errorf("failed to flush before batch add: %w", err)
			}
		}
	}

	// Process all entries
	for i := 0; i < len(ids); i++ {
		id := ids[i]
		value := values[i]

		// If this entry would make the block too large, flush first
		if bw.pendingData != nil && bw.CurrentBlockSize()+16 > bw.blockSizeTarget {
			if err := bw.Flush(); err != nil {
				return fmt.Errorf("failed to flush during batch add: %w", err)
			}
		}

		// If we don't have pending data yet, initialize it with the first entry
		if bw.pendingData == nil {
			bw.pendingData = &BlockData{
				MinID:                  id,
				MaxID:                  id,
				MinValue:               value,
				MaxValue:               value,
				Sum:                    value,
				Count:                  1,
				SerializedIDSection:    make([]byte, 0, bw.blockSizeTarget/2), // Use half the block size target for IDs
				SerializedValueSection: make([]byte, 0, bw.blockSizeTarget/2), // Use half the block size target for values
			}

			// First ID and value are not delta encoded, so we only need to pay
			// attention to varint or not
			var idEncoded []byte
			var valueEncoded []byte
			// if we're using varint encoding, we need to encode first to know the size
			if bw.encodingType == EncodingVarIntID || bw.encodingType == EncodingVarIntBoth {
				idEncoded = encodeVarInt(id)
			} else {
				idEncoded = make([]byte, 8)
				binary.LittleEndian.PutUint64(idEncoded, id)
			}

			if bw.encodingType == EncodingVarIntValue || bw.encodingType == EncodingVarIntBoth {
				valueEncoded = encodeSignedVarInt(value)
			} else {
				valueEncoded = make([]byte, 8)
				binary.LittleEndian.PutUint64(valueEncoded, int64ToUint64(value))
			}

			bw.pendingData.SerializedIDSection = bw.pendingData.SerializedIDSection[:len(idEncoded)] // always safe, we create a much larger buffer
			copy(bw.pendingData.SerializedIDSection, idEncoded)
			bw.pendingData.SerializedValueSection = bw.pendingData.SerializedValueSection[:len(valueEncoded)] // always safe, we create a much larger buffer
			copy(bw.pendingData.SerializedValueSection, valueEncoded)

		} else {
			// In case of delta encoding we need to override the id with the delta id
			idToWrite := id
			if bw.encodingType == EncodingDeltaID || bw.encodingType == EncodingDeltaBoth || bw.encodingType == EncodingVarIntID || bw.encodingType == EncodingVarIntBoth {
				idToWrite = id - bw.lastID
			}
			// In case of delta encoding we need to override the value with the delta value
			valueToWrite := value
			if bw.encodingType == EncodingDeltaValue || bw.encodingType == EncodingDeltaBoth || bw.encodingType == EncodingVarIntValue || bw.encodingType == EncodingVarIntBoth {
				valueToWrite = value - bw.lastValue
			}

			var idEncoded []byte
			var valueEncoded []byte
			// if we're using varint encoding, we need to encode first to know the size
			if bw.encodingType == EncodingVarIntID || bw.encodingType == EncodingVarIntBoth {
				idEncoded = encodeVarInt(idToWrite)
			} else {
				idEncoded = make([]byte, 8)
				binary.LittleEndian.PutUint64(idEncoded, idToWrite)
			}

			if bw.encodingType == EncodingVarIntValue || bw.encodingType == EncodingVarIntBoth {
				valueEncoded = encodeSignedVarInt(valueToWrite)
			} else {
				valueEncoded = make([]byte, 8)
				binary.LittleEndian.PutUint64(valueEncoded, int64ToUint64(valueToWrite))
			}

			if len(bw.pendingData.SerializedIDSection)+len(idEncoded) > cap(bw.pendingData.SerializedIDSection) {
				// we need to grow the slice
				newSlice := make([]byte, len(bw.pendingData.SerializedIDSection), cap(bw.pendingData.SerializedIDSection)*2)
				copy(newSlice, bw.pendingData.SerializedIDSection)
				bw.pendingData.SerializedIDSection = newSlice
			}

			// we know we have enough space
			bw.pendingData.SerializedIDSection = bw.pendingData.SerializedIDSection[:len(bw.pendingData.SerializedIDSection)+len(idEncoded)]
			copy(bw.pendingData.SerializedIDSection[len(bw.pendingData.SerializedIDSection)-len(idEncoded):], idEncoded)

			if len(bw.pendingData.SerializedValueSection)+len(valueEncoded) > cap(bw.pendingData.SerializedValueSection) {
				// we need to grow the slice
				newSlice := make([]byte, len(bw.pendingData.SerializedValueSection), cap(bw.pendingData.SerializedValueSection)*2)
				copy(newSlice, bw.pendingData.SerializedValueSection)
				bw.pendingData.SerializedValueSection = newSlice
			}

			// we know we have enough space
			bw.pendingData.SerializedValueSection = bw.pendingData.SerializedValueSection[:len(bw.pendingData.SerializedValueSection)+len(valueEncoded)]
			copy(bw.pendingData.SerializedValueSection[len(bw.pendingData.SerializedValueSection)-len(valueEncoded):], valueEncoded)

			// Update statistics
			if id < bw.pendingData.MinID {
				bw.pendingData.MinID = id
			}
			if id > bw.pendingData.MaxID {
				bw.pendingData.MaxID = id
			}
			if value < bw.pendingData.MinValue {
				bw.pendingData.MinValue = value
			}
			if value > bw.pendingData.MaxValue {
				bw.pendingData.MaxValue = value
			}
			bw.pendingData.Sum += value
			bw.pendingData.Count++
		}

		bw.globalIDs.Set(id)
		bw.lastID = id
		bw.lastValue = value
	}

	return nil
}

func (bw *BufferedWriter) CurrentBlockSize() uint32 {
	if bw.pendingData == nil {
		return 0
	}

	reservedSpace := 0
	if len(bw.blockIndex) == 0 {
		// the first block is smaller then the others because it's written after
		// the header and page-aligned together
		reservedSpace += headerSize
	}

	// The size of a block is the combination of the block header, the layout, and the serialized id and values data
	return uint32(reservedSpace + blockHeaderSize + blockLayoutSize + len(bw.pendingData.SerializedIDSection) + len(bw.pendingData.SerializedValueSection))
}

// writeAndTrack writes data to the file and updates the position tracker
// instead of using Seek(0, io.SeekCurrent) after each write
func (bw *BufferedWriter) writeAndTrack(data []byte) (int, error) {
	n, err := bw.file.Write(data)
	if err != nil {
		return n, err
	}
	bw.currentPosition += int64(n)
	return n, nil
}

// AddDeletedID adds a deleted ID to the writer
func (bw *BufferedWriter) AddDeletedID(id uint64) {
	bw.deletedIDs.Set(id)
}

// BatchAddDeletedIDs adds multiple deleted IDs to the writer
func (bw *BufferedWriter) BatchAddDeletedIDs(ids []uint64) {
	for _, id := range ids {
		bw.deletedIDs.Set(id)
	}
}

// AddDeletedIDBitmap adds all IDs from the provided bitmap to the deleted IDs bitmap
func (bw *BufferedWriter) AddDeletedIDBitmap(bitmap *sroar.Bitmap) {
	// Merge the provided bitmap with our deleted IDs bitmap
	bw.deletedIDs = bw.deletedIDs.Or(bitmap)
}

// Level returns the current compaction level
func (bw *BufferedWriter) Level() uint16 {
	return bw.level
}
