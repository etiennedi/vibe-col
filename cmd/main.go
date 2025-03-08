package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/etiennedi/vibe-lsm/pkg/store"
)

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())
	
	// Create a new ColumnStore with a flush trigger of 500 entries
	cs := store.NewColumnStore(500)
	fmt.Println("Created a new ColumnStore with flush trigger of 500 entries")

	// Insert 1000 random values
	fmt.Println("Inserting 1000 random values...")
	for i := 0; i < 1000; i++ {
		// Random value between -1000 and 1000
		val := rand.Int63n(2001) - 1000
		// Insert with sequential IDs
		if err := cs.Put(uint64(i), val); err != nil {
			fmt.Printf("Error inserting value: %v\n", err)
		}
	}
	fmt.Println("Finished inserting values")
	
	// Flush any remaining values in memtable
	fmt.Println("Flushing any remaining values in memtable...")
	if err := cs.Flush(); err != nil {
		fmt.Printf("Error flushing: %v\n", err)
	}
	
	// Get aggregations
	fmt.Println("\nRunning aggregations:")
	
	// Min
	min, err := cs.Aggregate(store.Min)
	if err != nil {
		fmt.Printf("Error getting minimum: %v\n", err)
	} else {
		fmt.Printf("Minimum value: %.0f\n", min)
	}
	
	// Max
	max, err := cs.Aggregate(store.Max)
	if err != nil {
		fmt.Printf("Error getting maximum: %v\n", err)
	} else {
		fmt.Printf("Maximum value: %.0f\n", max)
	}
	
	// Mean
	mean, err := cs.Aggregate(store.Mean)
	if err != nil {
		fmt.Printf("Error getting mean: %v\n", err)
	} else {
		fmt.Printf("Mean value: %.2f\n", mean)
	}
	
	// Count
	count, err := cs.Aggregate(store.Count)
	if err != nil {
		fmt.Printf("Error getting count: %v\n", err)
	} else {
		fmt.Printf("Count: %.0f\n", count)
	}
	
	// Sum
	sum, err := cs.Aggregate(store.Sum)
	if err != nil {
		fmt.Printf("Error getting sum: %v\n", err)
	} else {
		fmt.Printf("Sum: %.0f\n", sum)
	}
	
	// Median
	median, err := cs.Aggregate(store.Median)
	if err != nil {
		fmt.Printf("Error getting median: %v\n", err)
	} else {
		fmt.Printf("Median: %.2f\n", median)
	}
}