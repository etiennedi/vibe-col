#!/bin/bash

# Create output directory for profiles
mkdir -p profiles

# Function to run tests with profiling
run_profile() {
  local type=$1
  local test_name=$2
  local output_file="profiles/${type}_${test_name}.out"
  local profile_file="profiles/${type}_${test_name}.pdf"
  
  echo "Running $type profiling for $test_name..."
  
  # Run the test with profiling
  if [ "$type" == "cpu" ]; then
    go test -run="$test_name" ./pkg/col -cpuprofile="$output_file" -v
  elif [ "$type" == "mem" ]; then
    go test -run="$test_name" ./pkg/col -memprofile="$output_file" -v
  elif [ "$type" == "block" ]; then
    go test -run="$test_name" ./pkg/col -blockprofile="$output_file" -v
  fi
  
  # Generate PDF from the profile
  if [ -f "$output_file" ]; then
    go tool pprof -pdf "$output_file" > "$profile_file"
    echo "Profile generated at $profile_file"
  else
    echo "Profile file not created. Make sure the test ran successfully."
  fi
}

# Function to run benchmarks with profiling
run_benchmark_profile() {
  local type=$1
  local bench_name=$2
  local output_file="profiles/${type}_${bench_name}.out"
  local profile_file="profiles/${type}_${bench_name}.pdf"
  
  echo "Running $type profiling for benchmark $bench_name..."
  
  # Run the benchmark with profiling
  if [ "$type" == "cpu" ]; then
    go test -run=NONE -bench="$bench_name" ./pkg/col -cpuprofile="$output_file"
  elif [ "$type" == "mem" ]; then
    go test -run=NONE -bench="$bench_name" ./pkg/col -memprofile="$output_file"
  elif [ "$type" == "block" ]; then
    go test -run=NONE -bench="$bench_name" ./pkg/col -blockprofile="$output_file"
  fi
  
  # Generate PDF from the profile
  if [ -f "$output_file" ]; then
    go tool pprof -pdf "$output_file" > "$profile_file"
    echo "Profile generated at $profile_file"
  else
    echo "Profile file not created. Make sure the benchmark ran successfully."
  fi
}

# Run CPU profiling for TestLargeBufferedWrite
run_profile "cpu" "TestLargeBufferedWrite"

# Run Memory profiling for TestLargeBufferedWrite
run_profile "mem" "TestLargeBufferedWrite"

# Run CPU profiling for benchmarks
run_benchmark_profile "cpu" "BenchmarkBufferedWriter"
run_benchmark_profile "cpu" "BenchmarkBufferedWriterVsStandard"

# Run Memory profiling for benchmarks
run_benchmark_profile "mem" "BenchmarkBufferedWriter"
run_benchmark_profile "mem" "BenchmarkBufferedWriterVsStandard"

echo "All profiling completed. Check the profiles directory for results." 