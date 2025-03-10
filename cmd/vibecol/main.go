package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"vibe-lsm/pkg/col"
)

func main() {
	// Define subcommands
	writeCmd := flag.NewFlagSet("write", flag.ExitOnError)
	readCmd := flag.NewFlagSet("read", flag.ExitOnError)
	
	// Write command flags
	writeOutputFile := writeCmd.String("o", "example.col", "Output file name")
	writeIDs := writeCmd.String("ids", "", "Comma-separated list of IDs (uint64)")
	writeValues := writeCmd.String("values", "", "Comma-separated list of values (int64)")
	
	// Read command flags
	readInputFile := readCmd.String("f", "example.col", "Input file name")
	dumpKV := readCmd.Bool("dump", false, "Dump all key-value pairs")
	aggregate := readCmd.Bool("agg", false, "Show aggregations (count, min, max, sum, avg)")
	
	// Check for subcommand
	if len(os.Args) < 2 {
		fmt.Println("Expected 'write' or 'read' subcommand")
		fmt.Println("Usage:")
		fmt.Println("  vibecol write -o output.col -ids \"1,2,3\" -values \"100,200,300\"")
		fmt.Println("  vibecol read -f input.col --dump --agg")
		os.Exit(1)
	}

	// Handle subcommands
	switch os.Args[1] {
	case "write":
		writeCmd.Parse(os.Args[2:])
		if *writeIDs == "" || *writeValues == "" {
			fmt.Println("Error: both --ids and --values must be provided")
			writeCmd.PrintDefaults()
			os.Exit(1)
		}
		runWrite(*writeOutputFile, *writeIDs, *writeValues)
	case "read":
		readCmd.Parse(os.Args[2:])
		runRead(*readInputFile, *dumpKV, *aggregate)
	default:
		fmt.Printf("%q is not a valid command.\n", os.Args[1])
		fmt.Println("Valid commands: 'write' or 'read'")
		os.Exit(1)
	}
}

func runWrite(outputFile, idsStr, valuesStr string) {
	// Parse IDs and values
	idsStrArr := strings.Split(idsStr, ",")
	valuesStrArr := strings.Split(valuesStr, ",")
	
	if len(idsStrArr) != len(valuesStrArr) {
		fmt.Printf("Error: number of IDs (%d) doesn't match number of values (%d)\n", 
			len(idsStrArr), len(valuesStrArr))
		os.Exit(1)
	}
	
	// Convert strings to appropriate types
	ids := make([]uint64, len(idsStrArr))
	values := make([]int64, len(valuesStrArr))
	
	for i, idStr := range idsStrArr {
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			fmt.Printf("Error parsing ID %q: %v\n", idStr, err)
			os.Exit(1)
		}
		ids[i] = id
	}
	
	for i, valueStr := range valuesStrArr {
		value, err := strconv.ParseInt(strings.TrimSpace(valueStr), 10, 64)
		if err != nil {
			fmt.Printf("Error parsing value %q: %v\n", valueStr, err)
			os.Exit(1)
		}
		values[i] = value
	}
	
	// Create writer
	writer, err := col.NewWriter(outputFile)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		os.Exit(1)
	}
	
	// Write block
	if err := writer.WriteBlock(ids, values); err != nil {
		fmt.Printf("Error writing block: %v\n", err)
		writer.Close()
		os.Exit(1)
	}
	
	// Finalize and close
	if err := writer.FinalizeAndClose(); err != nil {
		fmt.Printf("Error finalizing file: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("Wrote file with %d entries to %s\n", len(ids), outputFile)
}

func runRead(inputFile string, dumpKV, aggregate bool) {
	// Open the reader
	reader, err := col.NewReader(inputFile)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	// Print file information
	fmt.Printf("File: %s\n", inputFile)
	fmt.Printf("Version: %d\n", reader.Version())
	fmt.Printf("Blocks: %d\n\n", reader.BlockCount())

	// Execute requested operations
	if dumpKV {
		fmt.Println("ID\tValue")
		fmt.Println("--\t-----")
		
		// For each block
		for i := uint32(0); i < uint32(reader.BlockCount()); i++ {
			ids, values, err := reader.GetPairs(i)
			if err != nil {
				fmt.Printf("Error reading pairs from block %d: %v\n", i, err)
				os.Exit(1)
			}
			
			// Print pairs
			for j := 0; j < len(ids); j++ {
				fmt.Printf("%d\t%d\n", ids[j], values[j])
			}
		}
		fmt.Println()
	}

	if aggregate {
		result := reader.Aggregate()
		fmt.Println("Aggregate Statistics (from metadata only):")
		fmt.Printf("Count: %d\n", result.Count)
		fmt.Printf("Min: %d\n", result.Min)
		fmt.Printf("Max: %d\n", result.Max)
		fmt.Printf("Sum: %d\n", result.Sum)
		fmt.Printf("Average: %.2f\n", result.Avg)
	}

	// If no operation was specified, show help
	if !dumpKV && !aggregate {
		fmt.Println("No operation specified. Use --dump to show key-value pairs or --agg to show aggregations.")
		readCmd.PrintDefaults()
	}
}