package gohalt

type latheap []uint64

// Len is the number of elements in the collection.
func (lh latheap) Len() int {
	return len(lh)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (lh latheap) Less(i int, j int) bool {
	return lh[i] < lh[j]
}

// Swap swaps the elements with indexes i and j.
func (lh latheap) Swap(i int, j int) {
	lh[i], lh[j] = lh[j], lh[i]
}

// Push add x as element Len().
func (lh *latheap) Push(x interface{}) {
	if lat, ok := x.(uint64); ok {
		*lh = append(*lh, lat)
	}
}

// Pop remove and return element Len() - 1.
func (lh *latheap) Pop() interface{} {
	h := *lh
	l := len(h)
	x := h[l-1]
	*lh = h[:l-1]
	return x
}

// At returns element at position.
func (lh *latheap) At(pos int) uint64 {
	return (*lh)[pos]
}
