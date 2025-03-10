# Plan: Implement Variable-Length Encoding for Better Space Efficiency

## Current State
- Current implementation uses fixed 8 bytes for each ID and value
- Wastes space when values are small (e.g., small integers can be stored in fewer bytes)
- Delta encoding helps with sequential IDs but not with arbitrary values

## Implementation Strategy

### 1. Design Variable-Length Encoding Format
- Use varint encoding (similar to Protocol Buffers or LEB128)
  - Small numbers (1-127) stored in single byte
  - Medium numbers (<16K) in 2 bytes
  - Larger numbers use progressively more bytes
- Add header flag to indicate variable-length encoding is used
- Update block layout to track actual byte sizes rather than entry counts

### 2. Writer Changes
- Add encoding mode parameter to `WriteBlock`
- Implement varint encoding logic:
  - Calculate minimal bytes needed for each value
  - Encode the length followed by the actual value
  - Maintain block layout with actual byte sizes
- Add optimization heuristics to choose encoding automatically based on data patterns
- Update block header to include encoding type information

### 3. Reader Changes
- Modify `readBlock` to handle variable-length values
- Add decoder for varint format
- Update position calculations for sequential reading
- Maintain cache-friendly memory layout post-decoding

### 4. Block Format Updates
- Update block header to include:
  - Encoding type (fixed, delta, varint, or combined)
  - Additional metadata for decoding
- Modify layout section to track actual byte ranges

### 5. Performance Considerations
- Benchmark encoding/decoding overhead vs. space savings
- Implement chunking for efficient CPU cache usage
- Consider SIMD optimization for parallel decoding
- Add adaptive encoding selection based on data characteristics

### 6. Testing Strategy
- Compare file sizes across encoding methods
- Test with real-world data patterns
- Verify correctness with roundtrip tests
- Benchmark read/write performance impact
- Test with extreme cases (very small/large values)

### 7. Implementation Phases
1. Basic varint implementation for values only
2. Extend to IDs (more complex due to lookup requirements)
3. Add hybrid modes (delta + varint)
4. Implement auto-selection heuristics

### 8. Documentation
- Update column format specification
- Document performance trade-offs
- Provide guidance on optimal encoding for different scenarios