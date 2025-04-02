#!/bin/bash

# Run the large buffered write test to see performance metrics
echo "Running TestLargeBufferedWrite for performance metrics..."
go test -v ./pkg/col -run TestLargeBufferedWrite

# Run benchmarks to compare BufferedWriter vs StandardWriter
echo "Running benchmark comparison between BufferedWriter and StandardWriter..."
go test -v ./pkg/col -run=NONE -bench=BenchmarkBufferedWriterVsStandard

# Run benchmarks for different block and batch sizes with our new BatchAdd implementation
echo "Running benchmark with different batch sizes..."
go test -v ./pkg/col -run=NONE -bench=BenchmarkBufferedWriter/BlockSize_16KB -benchtime=3x 