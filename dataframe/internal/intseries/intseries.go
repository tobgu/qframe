package intseries

import (
	"fmt"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/series"
)

type IntSeries struct {
	data []int
}

func New(d []int) IntSeries {
	return IntSeries{data: d}
}

var intFilterFuncs = map[filter.Comparator]func([]uint32, []int, interface{}, []bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
}

func (s IntSeries) Filter(index []uint32, c filter.Comparator, comparatee interface{}, bIndex []bool) error {
	// TODO: Also make it possible to compare to values in other column
	intComp, ok := comparatee.(int)
	if !ok {
		return fmt.Errorf("invalid type for integer comparison")
	}

	compFunc, ok := intFilterFuncs[c]
	if !ok {
		return fmt.Errorf("invalid comparison operator for int, %s", c)
	}

	compFunc(index, s.data, intComp, bIndex)
	return nil
}

func (s IntSeries) Equals(index []uint32, other series.Series, otherIndex []uint32) bool {
	otherI, ok := other.(IntSeries)
	if !ok {
		return false
	}

	for ix, x := range index {
		if s.data[x] != otherI.data[otherIndex[ix]] {
			return false
		}
	}

	return true
}

func (s IntSeries) Subset(index []uint32) series.Series {
	data := make([]int, 0, len(index))
	for _, ix := range index {
		data = append(data, s.data[ix])
	}

	return IntSeries{data: data}
}

func (s IntSeries) Sort(index []uint32, reverse bool, stable bool) {
	si := SortIndex{data: s.data, index: index, reverse: reverse}

	// Specific stdlib
	if stable {
		Stable(si)
	} else {
		Sort(si)
	}

}

// TODO: Some kind of code generation for all the below functions for all supported types

func gt(index []uint32, column []int, comparatee interface{}, bIndex []bool) error {
	comp, ok := comparatee.(int)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] > comp
	}

	return nil
}

func lt(index []uint32, column []int, comparatee interface{}, bIndex []bool) error {
	comp, ok := comparatee.(int)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] < comp
	}

	return nil
}
