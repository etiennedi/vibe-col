package main

import (
	"fmt"
	"log"
	"os"

	"vibe-lsm/pkg/col"

	"github.com/weaviate/sroar"
)

func main() {
	// Create a temporary file for our example
	tmpFile, err := os.CreateTemp("", "filter-example-*.col")
	if err != nil {
		log.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	filename := tmpFile.Name()
	defer os.Remove(filename)

	fmt.Printf("Creating column file at: %s\n", filename)

	// Create sample data with 3 blocks
	if err := createSampleData(filename); err != nil {
		log.Fatalf("Failed to create sample data: %v", err)
	}

	// Open the file for reading
	reader, err := col.NewReader(filename)
	if err != nil {
		log.Fatalf("Failed to open column file: %v", err)
	}
	defer reader.Close()

	// Example 1: No filters (baseline)
	fmt.Println("\n=== Example 1: No filters ===")
	result := reader.Aggregate()
	printResult("No filters", result)

	// Example 2: Only allow filter
	fmt.Println("\n=== Example 2: Only allow filter ===")
	allowFilter := sroar.NewBitmap()
	// Allow IDs 10, 20, 30, 110, 120, 130, 210, 220, 230
	for _, id := range []uint64{10, 20, 30, 110, 120, 130, 210, 220, 230} {
		allowFilter.Set(id)
	}

	allowOpts := col.AggregateOptions{
		Filter: allowFilter,
	}
	allowResult := reader.AggregateWithOptions(allowOpts)
	printResult("Allow specific IDs", allowResult)

	// Example 3: Only deny filter
	fmt.Println("\n=== Example 3: Only deny filter ===")
	denyFilter := sroar.NewBitmap()
	// Deny all IDs in the first block (1-100)
	for i := uint64(1); i <= 100; i++ {
		denyFilter.Set(i)
	}

	denyOpts := col.AggregateOptions{
		DenyFilter: denyFilter,
	}
	denyResult := reader.AggregateWithOptions(denyOpts)
	printResult("Deny first block", denyResult)

	// Example 4: Both allow and deny filters
	fmt.Println("\n=== Example 4: Both allow and deny filters ===")
	combinedAllowFilter := sroar.NewBitmap()
	// Allow IDs from 1 to 200
	for i := uint64(1); i <= 200; i++ {
		combinedAllowFilter.Set(i)
	}

	combinedDenyFilter := sroar.NewBitmap()
	// Deny all even IDs
	for i := uint64(2); i <= 200; i += 2 {
		combinedDenyFilter.Set(i)
	}

	combinedOpts := col.AggregateOptions{
		Filter:     combinedAllowFilter,
		DenyFilter: combinedDenyFilter,
	}
	combinedResult := reader.AggregateWithOptions(combinedOpts)
	printResult("Allow IDs 1-200, deny even IDs", combinedResult)

	// Example 5: Complex filtering scenario
	fmt.Println("\n=== Example 5: Complex filtering scenario ===")
	// Allow IDs in specific ranges
	rangeFilter := sroar.NewBitmap()
	// Allow IDs 50-75, 150-175, 250-275
	for i := uint64(50); i <= 75; i++ {
		rangeFilter.Set(i)
	}
	for i := uint64(150); i <= 175; i++ {
		rangeFilter.Set(i)
	}
	for i := uint64(250); i <= 275; i++ {
		rangeFilter.Set(i)
	}

	// Deny IDs divisible by 5
	rangeDenyFilter := sroar.NewBitmap()
	for i := uint64(5); i <= 300; i += 5 {
		rangeDenyFilter.Set(i)
	}

	rangeOpts := col.AggregateOptions{
		Filter:     rangeFilter,
		DenyFilter: rangeDenyFilter,
		// Force reading all blocks instead of using pre-calculated values
		SkipPreCalculated: true,
	}
	rangeResult := reader.AggregateWithOptions(rangeOpts)
	printResult("Allow specific ranges, deny IDs divisible by 5", rangeResult)
}

// Helper function to create sample data
func createSampleData(filename string) error {
	writer, err := col.NewWriter(filename)
	if err != nil {
		return err
	}

	// Block 1: IDs 1-100, values = id*10
	ids1 := make([]uint64, 100)
	values1 := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids1[i] = uint64(i + 1)
		values1[i] = int64((i + 1) * 10)
	}
	if err := writer.WriteBlock(ids1, values1); err != nil {
		return err
	}

	// Block 2: IDs 101-200, values = id*5
	ids2 := make([]uint64, 100)
	values2 := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids2[i] = uint64(i + 101)
		values2[i] = int64((i + 101) * 5)
	}
	if err := writer.WriteBlock(ids2, values2); err != nil {
		return err
	}

	// Block 3: IDs 201-300, values = id*2
	ids3 := make([]uint64, 100)
	values3 := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids3[i] = uint64(i + 201)
		values3[i] = int64((i + 201) * 2)
	}
	if err := writer.WriteBlock(ids3, values3); err != nil {
		return err
	}

	return writer.FinalizeAndClose()
}

// Helper function to print aggregation results
func printResult(description string, result col.AggregateResult) {
	fmt.Printf("%s:\n", description)
	fmt.Printf("  Count: %d\n", result.Count)
	if result.Count > 0 {
		fmt.Printf("  Min: %d\n", result.Min)
		fmt.Printf("  Max: %d\n", result.Max)
		fmt.Printf("  Sum: %d\n", result.Sum)
		fmt.Printf("  Avg: %.2f\n", result.Avg)
	}
}
