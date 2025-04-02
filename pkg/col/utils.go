package col

// calculateMinMaxUint64 calculates the minimum and maximum values in a uint64 slice
func calculateMinMaxUint64(values []uint64) (min, max uint64) {
	if len(values) == 0 {
		return 0, 0
	}

	min = values[0]
	max = values[0]

	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max
}

// calculateMinMaxInt64 calculates the minimum and maximum values in an int64 slice
func calculateMinMaxInt64(values []int64) (min, max int64) {
	if len(values) == 0 {
		return 0, 0
	}

	min = values[0]
	max = values[0]

	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max
}

// calculateSumInt64 calculates the sum of an int64 slice
func calculateSumInt64(values []int64) int64 {
	sum := int64(0)
	for _, v := range values {
		sum += v
	}
	return sum
}

// minUint64 returns the minimum of two uint64 values
func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// maxUint64 returns the maximum of two uint64 values
func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// minInt64 returns the minimum of two int64 values
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// maxInt64 returns the maximum of two int64 values
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
