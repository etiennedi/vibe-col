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

// BufferedWriter implements a writer for column files that buffers data in memory
// using BlockData structures and writes directly to disk.
type BufferedWriter struct {
	file            *os.File
	filename        string
	blockCount      uint64
	encodingType    uint32
	blockSizeTarget uint32
	blockIndex      []FooterEntry // Detailed index of blocks
	globalIDs       *sroar.Bitmap
	closed          bool

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
		blockIndex:      make([]FooterEntry, 0),
		globalIDs:       sroar.NewBitmap(),
		closed:          false,
		pendingData:     nil,
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

	// Create a new BlockData for this single item if needed
	if bw.pendingData == nil {
		bw.pendingData = &BlockData{
			MinID:                  id,
			MaxID:                  id,
			MinValue:               value,
			MaxValue:               value,
			Sum:                    value,
			Count:                  1,
			SerializedIDSection:    make([]byte, 0, 4096),
			SerializedValueSection: make([]byte, 0, 4096),
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

// TotalBlocks returns the number of blocks written so far
func (bw *BufferedWriter) TotalBlocks() uint64 {
	return bw.blockCount
}
