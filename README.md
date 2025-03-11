# Vibe-Col

Vibe-Col is a high-performance column-oriented storage engine designed for efficient data storage and retrieval.

## Features

### Storage Capabilities

- **Column-oriented storage**: Optimized for analytical workloads with efficient column-wise data access
- **Multi-block support**: Store large datasets across multiple blocks
- **Flexible encoding options**:
  - Raw encoding (fixed-width)
  - Delta encoding for IDs and values
  - Variable-length (VarInt) encoding for IDs and values
  - Combined Delta + VarInt encoding for maximum compression
- **Metadata caching**: Pre-calculated statistics for fast aggregation queries
- **Direct data access**: Option to bypass cached metadata for verification

### Data Types

- Support for 64-bit unsigned integers (uint64) for IDs
- Support for 64-bit signed integers (int64) for values

### Compression

- Significant space savings with variable-length encoding:
  - Up to 8x compression ratio for sequential data
  - 4-5x compression ratio for real-world data with gaps and variability
- Delta encoding for further compression of sequential or closely related values

### Query Capabilities

- Fast aggregation operations:
  - Count
  - Min
  - Max
  - Sum
  - Average
- Block-level data access for targeted queries
- Direct key-value pair retrieval

### Performance

- Efficient encoding and decoding of variable-length integers
- Optimized block layout for fast data access
- Metadata-based aggregation for near-instant results on large datasets
- Option to verify aggregation results by reading all values directly

### File Format

- Compact binary file format
- Header with file metadata
- Multiple data blocks
- Footer with block index for fast random access
- Checksum support for data integrity

### Tools

- Writer API for creating and populating column files
- Reader API for querying and analyzing data
- Command-line tools for data inspection

## Usage

The library provides simple APIs for writing and reading column files:

```go
// Writing data
writer, _ := col.NewWriter("data.col", col.WithEncoding(col.EncodingVarIntBoth))
writer.WriteBlock(ids, values)
writer.FinalizeAndClose()

// Reading data
reader, _ := col.NewReader("data.col")
ids, values, _ := reader.GetPairs(0)

// Fast aggregation
result := reader.Aggregate()
fmt.Printf("Count: %d, Min: %d, Max: %d, Sum: %d, Avg: %.2f\n",
    result.Count, result.Min, result.Max, result.Sum, result.Avg)

// Verification by reading all values
directResult := reader.AggregateWithOptions(col.AggregateOptions{SkipPreCalculated: true})
``` 