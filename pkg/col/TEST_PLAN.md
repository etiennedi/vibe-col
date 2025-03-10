# Test Enhancement Plan for Column-Based Storage Format

## 1. Format Tests (`format_test.go`)

### a. Header Format Tests
- **Test**: Verify `NewFileHeader` creates correctly initialized headers with all fields properly set.
- **Value**: Ensures the header creator function correctly formats all fields, especially type conversions for timestamps and magic numbers.

### b. Block Size Calculation Tests
- **Test**: Validate `CalculateBlockSize` returns correct values for various input sizes.
- **Value**: Critical for ensuring proper space allocation and preventing buffer overflows.

### c. Int64/Uint64 Conversion Tests
- **Test**: Test boundary values (min/max int64, zero, negative values) for int64 to uint64 conversions and back.
- **Value**: These conversions are crucial for binary format correctness; bugs here lead to data corruption.

## 2. Encoding Tests (`encoding_test.go`)

### a. Delta Encoding Property Tests
- **Test**: Test delta encoding with various patterns (ascending, descending, random, plateaus).
- **Value**: Validates encoding works correctly with all data distributions.

### b. Delta Encoding Roundtrip Tests
- **Test**: Verify encode+decode roundtrip produces identical data for various input patterns.
- **Value**: Ensures lossless encoding/decoding under all conditions.

### c. Empty/Single Value Edge Cases
- **Test**: Test empty arrays and single-value arrays with the delta encoding functions.
- **Value**: Boundary conditions often cause bugs and crashes.

## 3. Writer Tests (`writer_test.go`)

### a. Error Handling Tests
- **Test**: Test writer behavior with invalid inputs (mismatched array lengths, write permissions issues, etc.)
- **Value**: Ensures graceful error handling instead of crashes or corruption.

### b. Multiple Block Writing Tests
- **Test**: Test writing files with varying numbers of blocks (0, 1, 2, many).
- **Value**: Validates correct behavior with complex multi-block files.

### c. Block Statistics Tests
- **Test**: Verify statistics (min, max, sum) are correctly calculated and stored.
- **Value**: These values are used for optimizations and incorrect values lead to wrong results.

## 4. Reader Tests (`reader_test.go`)

### a. Corrupt File Tests
- **Test**: Test reader behavior with corrupted files (invalid magic numbers, truncated files, etc.)
- **Value**: Ensures robustness in production environments with potentially damaged files.

### b. Aggregation Tests
- **Test**: Verify aggregation results are correct for various file patterns.
- **Value**: Validates one of the core features that drives performance optimizations.

### c. Filtering Tests
- **Test**: Add tests for filtering by ID ranges.
- **Value**: Tests a core feature for speeding up queries where not all data is needed.

## 5. Integration Tests (Remaining in `col_test.go`)

### a. Large File Tests
- **Test**: Test with realistically sized files (100K+ entries)
- **Value**: Validates performance and correctness at scale.

### b. Encoding Performance Tests
- **Test**: Benchmark various encoding options to measure space efficiency and speed.
- **Value**: Provides data to make informed encoding choices based on data characteristics.

### c. Concurrent Access Tests
- **Test**: Test multiple readers accessing the same file concurrently.
- **Value**: Ensures thread safety for server environments.

## 6. Fuzzing Tests

### a. Format Fuzzing
- **Test**: Apply fuzzing to file format to find edge cases and vulnerabilities.
- **Value**: Identifies unexpected format issues that manual testing might miss.

### b. Data Fuzzing
- **Test**: Test with randomized/extreme data values to ensure robustness.
- **Value**: Provides confidence in handling unexpected real-world data.

## Implementation Priority

1. Start with format and encoding tests as they validate the foundations
2. Add writer tests next as they're upstream of the reader
3. Add reader tests, especially focusing on error handling
4. Add integration tests to validate the components working together
5. Add fuzzing tests as a final validation layer

## Performance Testing for Large Datasets

For the 100k entries performance test, we'll want to measure:

1. **Write Performance**:
   - Time to write 100k entries in a single block
   - Time to write 100k entries split across multiple blocks (e.g., 10 blocks of 10k entries each)
   - Comparison of different encoding options (raw vs. delta)

2. **Read Performance**:
   - Time to read the entire file sequentially
   - Time to read specific blocks
   - Time to aggregate values without reading all data (using footer metadata)
   - Effect of different encodings on read performance

3. **Space Efficiency**:
   - File size comparison between raw and delta encoding
   - Effect of different block sizes on overall file size
   - Overhead percentage (metadata vs. actual data)

4. **Memory Usage**:
   - Peak memory usage during writing
   - Peak memory usage during reading

These benchmarks will help us understand the performance characteristics and make informed decisions about optimal configurations for different use cases.