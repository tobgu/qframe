package iseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"reflect"
	"strconv"
)

func (s Series) StringAt(i uint32, _ string) string {
	return strconv.FormatInt(int64(s.data[i]), 10)
}

func (s Series) AppendByteStringAt(buf []byte, i uint32) []byte {
	return strconv.AppendInt(buf, int64(s.data[i]), 10)
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonInt(s.subset(index).data)
}

func (s Series) ByteSize() int {
	// Slice header + data
	return 2*8 + 8*len(s.data)
}

func (s Series) Equals(index index.Int, other series.Series, otherIndex index.Int) bool {
	otherI, ok := other.(Series)
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

func (c Comparable) Compare(i, j uint32) series.CompareResult {
	x, y := c.data[i], c.data[j]
	if x < y {
		return c.ltValue
	}

	if x > y {
		return c.gtValue
	}

	return series.Equal
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
	}
	return result, ok
}

func (is intSet) Contains(x int) bool {
	_, ok := is[x]
	return ok
}

func (s Series) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	if intC, ok := intComp(comparatee); ok {
		filterFn, ok := filterFuncs[comparator]
		if !ok {
			return errors.New("filter int", "unknown filter operator %v", comparatee)
		}
		filterFn(index, s.data, intC, bIndex)
	} else if set, ok := newIntSet(comparatee); ok {
		filterFn, ok := multiInputFilterFuncs[comparator]
		if !ok {
			return errors.New("filter int", "unknown filter operator %v", comparatee)
		}
		filterFn(index, s.data, set, bIndex)
	} else if seriesC, ok := comparatee.(Series); ok {
		filterFn, ok := filterFuncs2[comparator]
		if !ok {
			return errors.New("filter int", "unknown filter operator %v", comparatee)
		}
		filterFn(index, s.data, seriesC.data, bIndex)
	} else {
		return errors.New("filter int", "invalid comparison value type %v", reflect.TypeOf(comparatee))
	}

	return nil
}

func (s Series) filterCustom1(index index.Int, fn func(int) bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(s.data[index[i]])
		}
	}
}

func (s Series) filterCustom2(index index.Int, fn func(int, int) bool, comparatee interface{}, bIndex index.Bool) error {
	otherS, ok := comparatee.(Series)
	if !ok {
		return errors.New("filter int", "expected comparatee to be int series, was %v", reflect.TypeOf(comparatee))
	}

	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(s.data[index[i]], otherS.data[index[i]])
		}
	}

	return nil
}

func (s Series) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	var err error
	switch t := comparator.(type) {
	case string:
		err = s.filterBuiltIn(index, t, comparatee, bIndex)
	case func(int) bool:
		s.filterCustom1(index, t, bIndex)
	case func(int, int) bool:
		err = s.filterCustom2(index, t, comparatee, bIndex)
	default:
		err = errors.New("filter int", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}
