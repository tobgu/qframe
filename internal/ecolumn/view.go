package ecolumn

import "github.com/tobgu/qframe/internal/index"

type View struct {
	column Column
	index  index.Int
}

func (v View) ItemAt(i int) *string {
	return v.column.stringPtrAt(v.index[i])
}

func (v View) Len() int {
	return len(v.index)
}

func (v View) Slice() []*string {
	result := make([]*string, v.Len())
	for i := range v.index {
		result[i] = v.ItemAt(i)
	}

	return result
}
