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
| Global ID Bitmap|
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
| Bitmap Offset     | 8              | Offset to global ID bitmap       |
| Bitmap Size       | 8              | Size of global ID bitmap in bytes|
| Reserved          | 8              | Reserved for future use          |
+-------------------+----------------+----------------------------------+
```

Total header size: 64 bytes (fixed)

## 3.1 Global ID Bitmap

The global ID bitmap is a roaring bitmap that contains all IDs stored in the file. This allows for efficient filtering operations without having to scan individual blocks.

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| Size              | 4              | Size of bitmap data in bytes     |
| Bitmap Data       | Variable       | Serialized roaring bitmap        |
+-------------------+----------------+----------------------------------+
```

The bitmap is serialized using the standard roaring bitmap serialization format. The size field allows readers to skip over the bitmap if it's not needed for the current operation.

Benefits of the global ID bitmap:
- Fast membership testing for IDs without scanning blocks
- Efficient set operations (union, intersection, difference) with query filters
- Quick cardinality estimation for the entire file
- Improved performance for sparse filtering operations

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
| Block Layout      | 16            | Contains:                        |
|                    |               | - ID Section Offset (4 bytes)    |
|                    |               | - ID Section Size (4 bytes)      |
|                    |               | - Value Section Offset (4 bytes) |
|                    |               | - Value Section Size (4 bytes)   |
+-------------------+----------------+----------------------------------+
| ID Data           | Variable       | Encoded ID data                  |
+-------------------+----------------+----------------------------------+
| Value Data        | Variable       | Encoded value data               |
+-------------------+----------------+----------------------------------+
```

This structure allows readers to quickly locate different sections without making assumptions about encoding-specific sizes. The header contains the exact size of each section, enabling precise navigation through the file.

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

When using delta encoding (types 1, 2, or 3), the data sections contain:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| ID Data           | Variable       | Contains:                        |
|                   |                | - First ID stored as-is          |
|                   |                | - Subsequent IDs as deltas from  |
|                   |                |   previous value                 |
+-------------------+----------------+----------------------------------+
| Value Data        | Variable       | Contains:                        |
|                   |                | - First value stored as-is       |
|                   |                | - Subsequent values as deltas    |
|                   |                |   from previous value            |
+-------------------+----------------+----------------------------------+
```

With EncodingDeltaID (type 1), only the IDs are delta-encoded, while values are stored as-is.
With EncodingDeltaValue (type 2), only the values are delta-encoded, while IDs are stored as-is.
With EncodingDeltaBoth (type 3), both IDs and values are delta-encoded.

#### 4.2.3 Variable-Length (VarInt) Encoding

When using variable-length encoding (types 4, 5, 6, or 7), the data sections contain:

```
+-------------------+----------------+----------------------------------+
| Field             | Size (bytes)   | Description                      |
+-------------------+----------------+----------------------------------+
| ID Data           | Variable       | Each ID encoded as a variable    |
|                   |                | number of bytes depending on     |
|                   |                | value magnitude                  |
+-------------------+----------------+----------------------------------+
| Value Data        | Variable       | Each value encoded using ZigZag  |
|                   |                | encoding followed by variable-   |
|                   |                | length encoding                  |
+-------------------+----------------+----------------------------------+
```

With EncodingVarInt (type 4), both IDs and values use variable-length encoding without delta.
With EncodingVarIntID (type 5), only IDs use variable-length encoding, and they are delta-encoded.
With EncodingVarIntValue (type 6), only values use variable-length encoding, and they are delta-encoded.
With EncodingVarIntBoth (type 7), both IDs and values use variable-length encoding with delta encoding applied.

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
- 1: Delta encoding for IDs only
- 2: Delta encoding for values only
- 3: Delta encoding for both IDs and values
- 4: Variable-length integer (VarInt) encoding
- 5: Variable-length encoding for IDs only
- 6: Variable-length encoding for values only
- 7: Variable-length encoding for both IDs and values
- 8-15: Reserved for future encodings

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
2. If needed, read the global ID bitmap:
   a. Use the bitmap offset and size from the header
   b. Deserialize the bitmap using the roaring bitmap library
   c. Use the bitmap for fast filtering operations
3. Read footer to obtain block index and block statistics
4. For block reading:
   a. Check the encoding type of the file and block
   b. For variable-length encoding (VarInt), use specialized decoding routines:
      - Use `decodeUVarInts` for ID sections
      - Use `decodeSignedVarInt` for value sections
   c. Apply delta decoding if the encoding type includes delta encoding:
      - For EncodingDeltaID, EncodingDeltaValue, EncodingDeltaBoth
      - For EncodingVarIntID, EncodingVarIntValue, EncodingVarIntBoth
5. For aggregation queries:
   a. For unfiltered aggregations (sum, count, min, max, avg), compute directly from footer data
   b. For filtered aggregations:
      i. Use the global ID bitmap to quickly determine if the filter has any matches
      ii. If using an allow filter, perform an intersection with the global bitmap to optimize the filter
      iii. If using a deny filter, perform a difference operation with the global bitmap
      iv. Determine required blocks by checking filter against min/max ID ranges
      v. Read only necessary blocks in parallel
      vi. Apply filters to block data
      vii. Aggregate results

### 7.2 Writer Implementation

The writer should:
1. Buffer data to determine optimal block sizes
2. Sort data by ID within blocks
3. Compute block statistics (min, max, sum, count)
4. Build the global ID bitmap as blocks are written:
   a. Create a new roaring bitmap
   b. Add all IDs from each block to the bitmap
   c. After all blocks are processed, serialize the bitmap
5. Apply appropriate encoding based on data characteristics
6. Compress blocks if enabled
7. Write the file header with the bitmap offset and size
8. Write the serialized global ID bitmap
9. Write blocks sequentially
10. Generate and write footer with block index including duplicated statistics

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

#### 7.3.4 Global ID Bitmap Optimizations

The global ID bitmap enables several performance optimizations:

1. **Fast Filter Evaluation**:
   - Before reading any blocks, check if the filter has any potential matches by intersecting with the global bitmap
   - If the intersection is empty, return an empty result immediately without reading any blocks
   - For deny filters, perform a difference operation to quickly determine the effective filter

2. **Filter Optimization**:
   - Optimize allow filters by intersecting them with the global bitmap to reduce their size
   - This can significantly reduce the memory footprint of large filters
   - The optimized filter can be used for more efficient block filtering

3. **Cardinality Estimation**:
   - Use the bitmap's cardinality methods to quickly estimate result sizes
   - This can inform query planning and resource allocation decisions
   - Particularly useful for cost-based optimization in query engines

4. **Set Operations**:
   - Perform set operations (union, intersection, difference) directly on bitmaps
   - These operations are highly optimized in roaring bitmap implementations
   - Results can be used to create new filters or to directly determine which IDs to process

5. **ID Existence Checking**:
   - Quickly check if a specific ID exists in the file without scanning blocks
   - Useful for point lookups and existence queries
   - Can be combined with block-level checks for more precise filtering

The global ID bitmap adds a small storage overhead but can provide substantial performance benefits, especially for:
- Files with a large number of blocks
- Queries with complex filtering conditions
- Workloads with many sparse filters
- Applications that need fast cardinality estimation

## 8. Encoding Details

### 8.1 Variable-Length Integer (VarInt) Encoding

The VarInt encoding uses a variable number of bytes to represent integers, which is more space-efficient for smaller values:

- Numbers between 0-127 are encoded in a single byte
- Larger numbers use multiple bytes with 7 bits per byte for the value
- The most significant bit (MSB) of each byte is used as a continuation flag (1 = more bytes follow, 0 = final byte)

This encoding is particularly efficient when:
- Most values are small (fitting in 1-2 bytes)
- Values have high variance, making delta encoding less effective
- The data is sparse

### 8.2 Signed VarInt Encoding

For signed integers (int64 values), we use a ZigZag encoding to map signed values to unsigned values before applying VarInt encoding:

- ZigZag encoding maps small negative and positive numbers to small unsigned numbers
- The mapping follows: (value << 1) ^ (value >> 63)
  - 0 → 0
  - -1 → 1
  - 1 → 2
  - -2 → 3
  - ...and so on

This approach ensures that small values (both positive and negative) use fewer bytes.

### 8.3 Delta Encoding

Delta encoding stores the differences between consecutive values instead of the values themselves:
- The first value is stored as-is
- For each subsequent value, we store the difference from the previous value
- This is particularly effective when values increase by small, consistent amounts

When combined with VarInt encoding (EncodingVarIntBoth, etc.), the delta values are encoded using variable-length encoding for maximum space efficiency.