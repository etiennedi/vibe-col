// Package col implements a column-based storage format for id-value pairs.
package col

// This file provides a minimal re-export of the package API.
// The actual implementation is split into multiple files:
// - format.go: constants, types, and structures 
// - encoding.go: encoding/decoding functionality
// - writer.go: file writing functionality
// - reader.go: file reading functionality

// The package implements a column-based file format that supports:
// - Efficient disk-based storage of id-value pairs
// - Delta encoding for space efficiency
// - Aggregation capabilities (sum, count, min, max, avg)
// - Block-based organization with metadata
// - Fast filtering using value ranges

// Functions exported from this package:
//   - NewWriter: Creates a new file writer
//   - NewReader: Creates a new file reader
//   - WithEncoding, WithBlockSize: Options for writer configuration

// Types exported from this package:
//   - Writer: Writes column-based files
//   - Reader: Reads column-based files
//   - AggregateResult: Contains aggregation results