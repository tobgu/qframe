package scolumn

import "github.com/tobgu/qframe/internal/index"

type View struct {
	column Column
	index  index.Int
}

func (v View) ItemAt(i int) *string {
	return stringToPtr(v.column.stringAt(v.index[i]))
}

func (v View) Len() int {
	return len(v.index)
}

func (v View) Slice() []*string {
	result := make([]*string, v.Len())
	for i, j := range v.index {
		result[i] = stringToPtr(v.column.stringCopyAt(j))
	}

	return result
}
