package icolumn

import (
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/hash"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/qerrors"
	"github.com/tobgu/qframe/types"
	"reflect"
	"strconv"
	"unsafe"
)

func (c Column) DataType() types.DataType {
	return types.Int
}

func (c Column) StringAt(i uint32, _ string) string {
	return strconv.FormatInt(int64(c.data[i]), 10)
}

func (c Column) AppendByteStringAt(buf []byte, i uint32) []byte {
	return strconv.AppendInt(buf, int64(c.data[i]), 10)
}

func (c Column) ByteSize() int {
	// Slice header + data
	return 2*8 + 8*len(c.data)
}

func (c Column) Equals(index index.Int, other column.Column, otherIndex index.Int) bool {
	otherI, ok := other.(Column)
	if !ok {
		return false
	}

	for ix, x := range index {
		if c.data[x] != otherI.data[otherIndex[ix]] {
			return false
		}
	}

	return true
}

func (c Comparable) Compare(i, j uint32) column.CompareResult {
	x, y := c.data[i], c.data[j]
	if x < y {
		return c.ltValue
	}

	if x > y {
		return c.gtValue
	}

	return column.Equal
}

func (c Comparable) Hash(i uint32, seed uint64) uint64 {
	x := &c.data[i]
	b := (*[8]byte)(unsafe.Pointer(x))[:]
	return hash.HashBytes(b, seed)
}

func intComp(comparatee interface{}) (int, bool) {
	comp, ok := comparatee.(int)
	if !ok {
		// Accept floats by truncating them
		compFloat, ok := comparatee.(float64)
		if !ok {
			return 0, false
		}
		comp = int(compFloat)
	}

	return comp, true
}

type intSet map[int]struct{}

func interfaceSliceToIntSlice(ss []interface{}) ([]int, bool) {
	result := make([]int, len(ss))
	for i, s := range ss {
		switch t := s.(type) {
		case int:
			result[i] = t
		case float64:
			result[i] = int(t)
		default:
			return nil, false
		}
	}
	return result, true
}

func newIntSet(input interface{}) (intSet, bool) {
	var result intSet
	var ok bool
	switch t := input.(type) {
	case []int:
		result, ok = make(intSet, len(t)), true
		for _, v := range t {
			result[v] = struct{}{}
		}
	case []float64:
		result, ok = make(intSet, len(t)), true
		for _, v := range t {
			result[int(v)] = struct{}{}
		}
	case []interface{}:
		if intSlice, innerOk := interfaceSliceToIntSlice(t); innerOk {
			result, ok = newIntSet(intSlice)
		}
	}

	return result, ok
}

func (is intSet) Contains(x int) bool {
	_, ok := is[x]
	return ok
}

func (c Column) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	if intC, ok := intComp(comparatee); ok {
		filterFn, ok := filterFuncs[comparator]
		if !ok {
			return qerrors.New("filter int", "unknown filter operator %v", comparator)
		}
		filterFn(index, c.data, intC, bIndex)
	} else if set, ok := newIntSet(comparatee); ok {
		filterFn, ok := multiInputFilterFuncs[comparator]
		if !ok {
			return qerrors.New("filter int", "unknown filter operator %v", comparator)
		}
		filterFn(index, c.data, set, bIndex)
	} else if columnC, ok := comparatee.(Column); ok {
		filterFn, ok := filterFuncs2[comparator]
		if !ok {
			return qerrors.New("filter int", "unknown filter operator %v", comparator)
		}
		filterFn(index, c.data, columnC.data, bIndex)
	} else {
		return qerrors.New("filter int", "invalid comparison value type %v", reflect.TypeOf(comparatee))
	}

	return nil
}

func (c Column) filterCustom1(index index.Int, fn func(int) bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(c.data[index[i]])
		}
	}
}

func (c Column) filterCustom2(index index.Int, fn func(int, int) bool, comparatee interface{}, bIndex index.Bool) error {
	otherC, ok := comparatee.(Column)
	if !ok {
		return qerrors.New("filter int", "expected comparatee to be int column, was %v", reflect.TypeOf(comparatee))
	}

	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(c.data[index[i]], otherC.data[index[i]])
		}
	}

	return nil
}

func (c Column) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	var err error
	switch t := comparator.(type) {
	case string:
		err = c.filterBuiltIn(index, t, comparatee, bIndex)
	case func(int) bool:
		c.filterCustom1(index, t, bIndex)
	case func(int, int) bool:
		err = c.filterCustom2(index, t, comparatee, bIndex)
	default:
		err = qerrors.New("filter int", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}

func (c Column) FunctionType() types.FunctionType {
	return types.FunctionTypeInt
}
