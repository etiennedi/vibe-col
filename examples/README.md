# Vibe-LSM Examples

This directory contains example applications demonstrating how to use the Vibe-LSM library.

## Filtered Aggregation Example

The `filtered_aggregation_example.go` file demonstrates how to use the allow and deny filters for aggregation operations in the column file format.

### Key Concepts

#### Allow Filters

Allow filters specify which IDs should be included in the aggregation. When an allow filter is provided, only values associated with IDs in the filter will be included in the aggregation results.

```go
// Create an allow filter
allowFilter := sroar.NewBitmap()
// Add IDs to the filter
allowFilter.Set(10)
allowFilter.Set(20)
allowFilter.Set(30)

// Use the filter in aggregation
opts := col.AggregateOptions{
    Filter: allowFilter,
}
result := reader.AggregateWithOptions(opts)
```

#### Deny Filters

Deny filters specify which IDs should be excluded from the aggregation. When a deny filter is provided, values associated with IDs in the filter will be excluded from the aggregation results.

```go
// Create a deny filter
denyFilter := sroar.NewBitmap()
// Add IDs to exclude
denyFilter.Set(1)
denyFilter.Set(2)
denyFilter.Set(3)

// Use the filter in aggregation
opts := col.AggregateOptions{
    DenyFilter: denyFilter,
}
result := reader.AggregateWithOptions(opts)
```

#### Combined Filters

You can use both allow and deny filters together for more complex filtering scenarios. When both filters are provided, an ID must be in the allow filter AND NOT in the deny filter to be included in the aggregation.

```go
// Create both filters
allowFilter := sroar.NewBitmap()
for i := uint64(1); i <= 100; i++ {
    allowFilter.Set(i)
}

denyFilter := sroar.NewBitmap()
for i := uint64(1); i <= 100; i += 2 {
    denyFilter.Set(i)  // Exclude odd numbers
}

// Use both filters in aggregation
opts := col.AggregateOptions{
    Filter:     allowFilter,
    DenyFilter: denyFilter,
}
result := reader.AggregateWithOptions(opts)
```

### Running the Example

To run the example:

```bash
go run examples/filtered_aggregation_example.go
```

The example demonstrates:
1. Aggregation without any filters (baseline)
2. Aggregation with only an allow filter
3. Aggregation with only a deny filter
4. Aggregation with both allow and deny filters
5. A complex filtering scenario with specific ranges and exclusions

### Performance Considerations

- Using filters can significantly reduce the amount of data that needs to be processed, improving performance for large datasets.
- The implementation efficiently skips blocks that don't contain any IDs in the allow filter.
- When using only a deny filter, all blocks still need to be checked since any block could contain IDs that aren't denied.
- For optimal performance with large datasets, consider using both allow and deny filters to narrow down the search space as much as possible. 