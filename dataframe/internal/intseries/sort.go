package intseries

type SortIndex struct {
	index   []uint32
	data    []int
	reverse bool
}

func (si SortIndex) Less(i, j int) bool {
	if si.reverse {
		return si.data[si.index[i]] > si.data[si.index[j]]
	}

	return si.data[si.index[i]] < si.data[si.index[j]]
}

func (si SortIndex) Swap(i, j int) {
	si.index[i], si.index[j] = si.index[j], si.index[i]
}

func (si SortIndex) Len() int {
	return len(si.index)
}

type ReverseSortIndex struct {
	SortIndex
}

func (rsi ReverseSortIndex) Less(i, j int) bool {
	return rsi.data[rsi.index[i]] > rsi.data[rsi.index[j]]
}
