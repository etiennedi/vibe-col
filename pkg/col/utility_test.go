package col

// isSorted returns true if the given slice of uint64 is sorted in ascending order
func isSorted(ids []uint64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			return false
		}
	}
	return true
}

// sortByID sorts the given IDs and values arrays by the IDs.
// The values array is reordered to correspond with the sorted IDs.
func sortByID(ids []uint64, values []int64) {
	if len(ids) != len(values) {
		panic("ids and values must have the same length")
	}

	// Use bubble sort for simplicity (inefficient for large arrays but okay for tests)
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[i] > ids[j] {
				// Swap IDs
				ids[i], ids[j] = ids[j], ids[i]
				// Swap values accordingly
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}
