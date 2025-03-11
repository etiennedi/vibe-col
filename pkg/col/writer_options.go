package col

// WriterOption defines a function type for configuring a Writer
type WriterOption func(*Writer)

// WithEncoding sets the encoding type for the Writer
func WithEncoding(encodingType uint32) WriterOption {
	return func(w *Writer) {
		w.encodingType = encodingType
	}
}

// WithBlockSize sets the block size for the Writer
func WithBlockSize(blockSize uint32) WriterOption {
	return func(w *Writer) {
		w.blockSizeTarget = blockSize
	}
}
