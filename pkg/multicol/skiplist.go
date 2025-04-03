package multicol

// randomLevel generates a random level for a skip list node
// Uses the probabilistic skip list algorithm where each level has a 1/4 chance
// of being included in the node
func (sl *ConcurrentSkipList) randomLevel() int {
	level := 1
	for level < sl.maxHeight && sl.rng.Float64() < DefaultProbability {
		level++
	}
	return level
}

// Insert adds a key-value pair to the skip list
func (sl *ConcurrentSkipList) Insert(key uint64, value int64) {
	// Lock the head to synchronize height updates
	sl.head.mu.Lock()
	defer sl.head.mu.Unlock()

	// Create an array to track update points at each level
	update := make([]*skipNode, sl.maxHeight)
	current := sl.head

	// Find position to insert
	for level := sl.height - 1; level >= 0; level-- {
		for current.next[level] != nil && current.next[level].key < key {
			current = current.next[level]
		}
		update[level] = current
	}

	// Check if key already exists
	current = current.next[0]
	if current != nil && current.key == key {
		// Update value with lock
		current.mu.Lock()
		current.value = value
		// If it was previously deleted, mark it as active
		if current.deleted.Load() {
			current.deleted.Store(false)
		}
		current.mu.Unlock()
		return
	}

	// Generate random height for new node
	newLevel := sl.randomLevel()
	if newLevel > sl.height {
		// Extend height if needed
		for level := sl.height; level < newLevel; level++ {
			update[level] = sl.head
		}
		sl.height = newLevel
	}

	// Create new node
	newNode := &skipNode{
		key:   key,
		value: value,
		next:  make([]*skipNode, newLevel),
	}

	// Insert node at all levels
	for level := 0; level < newLevel; level++ {
		newNode.next[level] = update[level].next[level]
		update[level].next[level] = newNode
	}

	// Update size
	sl.size.Add(1)
}

// Get searches for a key in the skip list and returns its value
func (sl *ConcurrentSkipList) Get(key uint64) (int64, bool) {
	// No locks needed for reading
	current := sl.head

	// Traverse from top to bottom level
	for level := sl.height - 1; level >= 0; level-- {
		for current.next[level] != nil && current.next[level].key < key {
			current = current.next[level]
		}
	}

	// Check if node exists at bottom level
	current = current.next[0]
	if current != nil && current.key == key {
		// Check if the node is marked as deleted
		if current.deleted.Load() {
			// Node exists but is logically deleted
			return 0, false
		}
		return current.value, true
	}

	return 0, false
}

// Size returns the number of elements in the skip list
func (sl *ConcurrentSkipList) Size() int64 {
	return sl.size.Load()
}

// SkipListIterator is an iterator for traversing a skip list
type SkipListIterator struct {
	list    *ConcurrentSkipList
	current *skipNode
}

// Next advances the iterator to the next node
// Returns false if there are no more nodes
func (it *SkipListIterator) Next() bool {
	// Advance to next node
	if it.current == nil {
		it.current = it.list.head.next[0]
	} else {
		it.current = it.current.next[0]
	}

	return it.current != nil
}

// Key returns the key of the current node
func (it *SkipListIterator) Key() uint64 {
	return it.current.key
}

// Value returns the value of the current node
func (it *SkipListIterator) Value() int64 {
	return it.current.value
}

// IsDeleted returns true if the current node is marked as deleted
func (it *SkipListIterator) IsDeleted() bool {
	return it.current.deleted.Load()
}

// Iterator returns a new iterator for the skip list
func (sl *ConcurrentSkipList) Iterator() *SkipListIterator {
	return &SkipListIterator{
		list:    sl,
		current: nil,
	}
}

// SkipListRangeIterator is an iterator for traversing a range of nodes in a skip list
type SkipListRangeIterator struct {
	iterator *SkipListIterator
	endID    uint64
}

// Next advances the iterator to the next node in the range
// Returns false if there are no more nodes in the range
func (it *SkipListRangeIterator) Next() bool {
	if !it.iterator.Next() {
		return false
	}

	if it.iterator.Key() > it.endID {
		return false
	}

	return true
}

// Key returns the key of the current node
func (it *SkipListRangeIterator) Key() uint64 {
	return it.iterator.Key()
}

// Value returns the value of the current node
func (it *SkipListRangeIterator) Value() int64 {
	return it.iterator.Value()
}

// IsDeleted returns true if the current node is marked as deleted
func (it *SkipListRangeIterator) IsDeleted() bool {
	return it.iterator.IsDeleted()
}

// RangeIterator returns a new iterator for traversing a range of nodes in the skip list
func (sl *ConcurrentSkipList) RangeIterator(startID, endID uint64) *SkipListRangeIterator {
	// Find starting position
	current := sl.head
	for level := sl.height - 1; level >= 0; level-- {
		for current.next[level] != nil && current.next[level].key < startID {
			current = current.next[level]
		}
	}

	iterator := &SkipListIterator{
		list:    sl,
		current: current, // Start before the first node >= startID
	}

	return &SkipListRangeIterator{
		iterator: iterator,
		endID:    endID,
	}
}
