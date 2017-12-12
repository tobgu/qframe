package intseries

type indexedData struct {
	pos  uint32
	data int
}

type sortIndex struct {
	indexedData   []indexedData
	reverse bool
}

func newSortIndex(index []uint32, data []int, reverse bool) sortIndex {
	id := make([]indexedData, 0, len(index))
	for _, ix := range index {
		id = append(id, indexedData{pos: ix, data: data[ix]})
	}
	return sortIndex{indexedData: id, reverse: reverse}
}

func (si sortIndex) fillIndex(index []uint32) {
	for i, id := range si.indexedData {
		index[i] = id.pos
	}
}