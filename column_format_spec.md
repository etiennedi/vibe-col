# Column-Based Storage Format Specification

## 1. Overview

This document defines a column-based storage format designed for efficient disk-based aggregations. The format stores id-value pairs where ids are uint64 and values can be of various types (initially supporting int64).

Key design goals include:
- Disk-based aggregations with minimal I/O
- Fast filtering using external ID lists (e.g., roaring bitmaps)
- Smart block skipping for sparse filters
- Support for zero-copy aggregations without loading full dataset
- Extensible encoding and compression schemes
- Parallel read capability optimized for SSDs
- Pre-computed block-level statistics for optimization

## 2. File Structure

```
+-----------------+
| File Header     |
+-----------------+
| Block 1         |
+-----------------+
| Block 2         |
+-----------------+
| ...             |
+-----------------+
| Block N         |
+-----------------+
| Footer          |
+-----------------+
```

## 3. File Header

The file header contains metadata about the entire file:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| Magic Number      | 8              | Identifies file format (VIBE_COL)|
| Version           | 4              | Format version number            |
| Column Type       | 4              | Data type of values (enum)       |
| Block Count       | 8              | Number of blocks                 |
| Block Size Target | 4              | Target size of blocks in bytes   |
| Compression Type  | 4              | Compression algorithm (enum)     |
| Encoding Type     | 4              | Encoding mechanism (enum)        |
| Creation Time     | 8              | Unix timestamp                   |
| Reserved          | 24             | Reserved for future use          |
+-------------------+----------------+----------------------------------+
```

Total header size: 64 bytes (fixed)

## 4. Block Structure

Each block is self-contained and contains:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| Block Header      | 64             | Block metadata                   |
| ID-Value Pairs    | Variable       | The actual data                  |
+-------------------+----------------+----------------------------------+
```

### 4.1 Block Header

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| Min ID            | 8              | Minimum ID in block              |
| Max ID            | 8              | Maximum ID in block              |
| Min Value         | 8              | Minimum value in block           |
| Max Value         | 8              | Maximum value in block           |
| Sum               | 8              | Sum of all values in block       |
| Count             | 4              | Number of ID-value pairs         |
| Encoding Type     | 4              | Block-specific encoding override |
| Compression Type  | 4              | Block-specific compression       |
| Uncompressed Size | 4              | Size before compression          |
| Compressed Size   | 4              | Size after compression           |
| Block Checksum    | 8              | CRC-64 of block data             |
| Reserved          | 4              | Reserved for future use          |
+-------------------+----------------+----------------------------------+
```

Total block header size: 64 bytes (fixed)

Note: For non-numeric types, the Sum field will be set to 0 or another appropriate sentinel value.

### 4.2 ID-Value Data Storage Layout

Each block has a common layout structure regardless of encoding:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| ID Section Offset | 4              | Offset to start of ID data       |
| ID Section Size   | 4              | Size of ID data section          |
| Value Section Off | 4              | Offset to start of value data    |
| Value Section Size| 4              | Size of value data section       |
| ID Data           | Variable       | Encoded ID data                  |
| Value Data        | Variable       | Encoded value data               |
+-------------------+----------------+----------------------------------+
```

This structure allows readers to quickly locate different sections without assumptions about encoding-specific sizes. The offsets are relative to the end of the block header.

#### 4.2.1 Raw Encoding

When using raw encoding (type = 0), the data sections contain:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| ID Data           | 8 * Count      | Array of uint64 IDs              |
| Value Data        | 8 * Count      | Array of int64 values            |
+-------------------+----------------+----------------------------------+
```

#### 4.2.2 Delta Encoding

When using delta encoding (type = 1), the data sections contain:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| ID Data           | Variable       | Contains:                        |
|                   |                | - Base ID (8 bytes)              |
|                   |                | - Delta encoding metadata        |
|                   |                | - Encoded delta values           |
+-------------------+----------------+----------------------------------+
| Value Data        | Variable       | Value encoding format depends on |
|                   |                | the compression method used      |
+-------------------+----------------+----------------------------------+
```

The delta encoding format includes metadata that specifies:
- Bit width used for delta values
- Whether zigzag encoding is used
- Any compression-specific parameters

## 5. Footer

The footer contains a lookup table for quickly finding blocks and aggregation metadata:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| Block Index Count | 4              | Number of blocks in index        |
| Block Index       | Variable       | Array of block index entries     |
| Footer Size       | 8              | Size of footer in bytes          |
| Checksum          | 8              | CRC-64 of entire file            |
| Magic Number      | 8              | Same as header (for validation)  |
+-------------------+----------------+----------------------------------+
```

### 5.1 Block Index Entry

Each block index entry contains:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| Block Offset      | 8              | Offset from file start to block  |
| Block Size        | 4              | Size of block in bytes           |
| Min ID            | 8              | Minimum ID in block (duplicate)  |
| Max ID            | 8              | Maximum ID in block (duplicate)  |
| Min Value         | 8              | Minimum value (duplicate)        |
| Max Value         | 8              | Maximum value (duplicate)        |
| Sum               | 8              | Sum of values (duplicate)        |
| Count             | 4              | Number of values (duplicate)     |
+-------------------+----------------+----------------------------------+
```

Total block index entry size: 48 bytes per block

By duplicating these statistics in the footer, readers can perform optimizations:
- Unfiltered aggregations (sum, count, min, max, avg) can be computed by reading only the footer
- Blocks can be filtered/skipped using min/max ID ranges without reading block data
- Cost-based query optimization can estimate I/O based on block statistics

## 6. Design Considerations

### 6.1 Block Size

The target block size should align with optimal I/O patterns:
- **Recommended**: 64KB - 1MB range
- **Page-aligned**: Multiples of 4KB (typical page size)
- **Parallelism**: Small enough for parallel processing
- **Metadata overhead**: Large enough to amortize header costs

For SSDs, blocks around 128KB-256KB balance read efficiency and parallelism.

### 6.2 ID Ordering

IDs within blocks should be stored in ascending order to:
- Enable binary search within blocks
- Allow efficient delta encoding
- Support fast merging of data with filter bitmaps

### 6.3 Skip Logic

The system can implement multiple levels of skipping:
1. **Block-level skipping**: Using min/max IDs and values
2. **Sub-block skipping**: For large blocks, additional internal skip indices

### 6.4 Future Extensions

#### 6.4.1 Encoding Types (reserved enum values)
- 0: Raw (unencoded)
- 1: Delta
- 2: Dictionary
- 3: Run-length
- 4: Bit-packed
- 5-15: Reserved for future encodings

#### 6.4.2 Compression Types (reserved enum values)
- 0: None
- 1: LZ4
- 2: Zstd
- 3: Snappy
- 4-15: Reserved for future compression algorithms

#### 6.4.3 Data Types (reserved enum values)
- 0: int64
- 1: int32
- 2: int16
- 3: int8
- 4: uint32
- 5: uint16
- 6: uint8
- 7: float64
- 8: float32
- 9: boolean
- 10: string
- 11-15: Reserved for future types

## 7. Implementation Recommendations

### 7.1 Reader Implementation

The reader should:
1. Read and validate file header
2. Read footer to obtain block index and block statistics
3. For aggregation queries:
   a. For unfiltered aggregations (sum, count, min, max, avg), compute directly from footer data
   b. For filtered aggregations:
      i. Determine required blocks by checking filter against min/max ID ranges
      ii. Read only necessary blocks in parallel
      iii. Apply filters to block data
      iv. Aggregate results

### 7.2 Writer Implementation

The writer should:
1. Buffer data to determine optimal block sizes
2. Sort data by ID within blocks
3. Compute block statistics (min, max, sum, count)
4. Apply appropriate encoding based on data characteristics
5. Compress blocks if enabled
6. Write blocks sequentially
7. Generate and write footer with block index including duplicated statistics

### 7.3 Optimization Techniques

#### 7.3.1 Fast Unfiltered Aggregations
For queries without filters, the reader can:
- Compute sum, count, min, max directly from footer
- Calculate average as sum/count
- Return results without reading any block data

#### 7.3.2 Partial Block Processing
For filtered queries:
1. Eliminate blocks where filter doesn't overlap with ID range
2. For remaining blocks, check if filter might affect aggregation result
   - If filter is guaranteed to include all IDs in a block, use pre-computed statistics
   - Otherwise, read and process the block data

#### 7.3.3 Parallel Processing
When multiple blocks need to be read:
1. Process blocks in parallel using multiple threads/cores
2. Use asynchronous I/O to overlap computation with disk reads
3. Prioritize blocks that are most likely to contribute significantly to the result