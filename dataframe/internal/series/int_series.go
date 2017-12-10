package series

import (
	"fmt"
	"github.com/tobgu/go-qcache/dataframe/filter"
)

type IntSeries struct {
	data []int
}

func NewIntSeries(d []int) IntSeries {
	return IntSeries{data: d}
}

var intFilterFuncs = map[filter.Comparator]func([]uint32, []int, interface{}, []bool) error{
	filter.Gt: intGt,
	filter.Lt: intLt,
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

func (s IntSeries) Equals(index []uint32, other Series, otherIndex []uint32) bool {
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

func (s IntSeries) Subset(index []uint32) Series {
	data := make([]int, 0, len(index))
	for _, ix := range index {
		data = append(data, s.data[ix])
	}

	return IntSeries{data: data}
}

// TODO: Some kind of code generation for all the below functions for all supported types

func intGt(index []uint32, column []int, comparatee interface{}, bIndex []bool) error {
	comp, ok := comparatee.(int)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] > comp
	}

	return nil
}

func intLt(index []uint32, column []int, comparatee interface{}, bIndex []bool) error {
	comp, ok := comparatee.(int)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] < comp
	}

	return nil
}
