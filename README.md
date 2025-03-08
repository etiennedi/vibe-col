# Vibe-LSM

A custom Log-Structured Merge-Tree (LSM) storage engine implemented in Go.

## Overview

Vibe-LSM is a column-based storage engine built on the LSM-tree architecture, specifically designed for analytical workloads and efficient filtered aggregations.

## Current Implementation

- In-memory key-value store (MemTable)

## Architecture Details

### Data Model
- Keys are always uint64 IDs
- Values are initially int64 only
- Optimized for efficient range queries and filtered aggregations

### Storage Format
- Segments organized in blocks for efficient data skipping
- Each segment contains multiple blocks
- Each block contains a list of IDs and corresponding values
- Optimized for sparse ID lookups by allowing entire blocks to be skipped

### Query Patterns
- Support for dense and sparse ID access patterns
- Optimized for filtered aggregations where a whitelist of IDs is provided
- Block structure allows for efficient data skipping when processing sparse ID lists

## SSTable File Format

Each segment is stored on disk as a Sorted String Table (SSTable) file with the following structure:

```
+----------------+
| HEADER         |
+----------------+
| BLOCK INDEX    |
+----------------+
| BLOCK 1        |
+----------------+
| BLOCK 2        |
+----------------+
| ...            |
+----------------+
| BLOCK N        |
+----------------+
| FOOTER         |
+----------------+
```

### Header (64 bytes)
- Magic number (8 bytes): "VIBELSMX" to identify the file type
- Version (4 bytes): File format version
- Created timestamp (8 bytes): Unix timestamp
- Total entries (8 bytes): Total number of entries in the file
- Number of blocks (4 bytes): Number of blocks in the file
- Flags (4 bytes): Bit flags for file properties
- Reserved (28 bytes): Reserved for future use

### Block Index
- Array of block metadata entries, each containing:
  - Block offset (8 bytes): Byte offset of the block from start of file
  - Block size (4 bytes): Size of the block in bytes
  - Min ID (8 bytes): Minimum ID in the block
  - Max ID (8 bytes): Maximum ID in the block
  - Min value (8 bytes): Minimum value in the block
  - Max value (8 bytes): Maximum value in the block
  - Count (4 bytes): Number of entries in the block
  - Checksum (4 bytes): CRC32 checksum of the block

### Block Structure
Each block contains:

```
+-------------------+
| BLOCK HEADER      | (16 bytes)
+-------------------+
| ID METADATA       | (8 bytes)
+-------------------+
| VALUE METADATA    | (8 bytes)
+-------------------+
| ID ARRAY          | (variable size)
+-------------------+
| VALUE ARRAY       | (variable size)
+-------------------+
| BLOCK FOOTER      | (8 bytes)
+-------------------+
```

- Block Header:
  - Entry count (4 bytes): Number of entries in the block
  - ID array size (4 bytes): Size of the ID array in bytes
  - Value array size (4 bytes): Size of the value array in bytes
  - Flags (4 bytes): Encoding and compression flags

- ID Metadata:
  - Encoding type (1 byte): How IDs are encoded (raw, delta, etc.)
  - Compression type (1 byte): How IDs are compressed
  - Reserved (6 bytes): Reserved for future use

- Value Metadata:
  - Encoding type (1 byte): How values are encoded (raw, delta, FOR, etc.)
  - Compression type (1 byte): How values are compressed
  - Reserved (6 bytes): Reserved for future use

- ID Array: Contains the block's IDs, encoded and compressed according to metadata
- Value Array: Contains the block's values, encoded and compressed according to metadata
- Block Footer: CRC32 checksum of the entire block (4 bytes) + reserved (4 bytes)

### Footer (16 bytes)
- Block index offset (8 bytes): Byte offset of the block index from start of file
- Checksum (8 bytes): CRC32 checksum of the entire file excluding the footer

## Future Enhancements

### Compression
- Delta encoding for sequential IDs
- Value compression strategies
- Frame of reference encoding for value ranges with similar magnitudes
- Dictionary encoding for repeated values

### Persistence
- Immutable segment flushing
- Write-Ahead Log (WAL) for durability

### Optimization
- Block-level statistics for more efficient pruning
- Min/max metadata per block for range query optimization
- Bloom filters for membership testing
- SIMD acceleration for batch operations

### Maintenance
- Segment compaction strategies (initially not supported)
- Time-based segment merging policies
- Space amplification control via background merges

## Getting Started

```bash
# Build
go build -o vibe-lsm ./cmd

# Run
./vibe-lsm
```

## License

MIT