package iseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"reflect"
	"strconv"
)

func sum(values []int) int {
	result := 0
	for _, v := range values {
		result += v
	}
	return result
}

var aggregations = map[string]func([]int) int{
	"sum": sum,
}

// Series - constant
var filterFuncs = map[string]func(index.Int, []int, int, index.Bool){
	filter.Gt:  gt,
	filter.Gte: gte,
	filter.Lt:  lt,
	filter.Eq:  eq,
}

// Series - Series
var filterFuncs2 = map[string]func(index.Int, []int, []int, index.Bool){
	filter.Gt:  gt2,
	filter.Gte: gte2,
	filter.Lt:  lt2,
	filter.Eq:  eq2,
}

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

func (s Series) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	intC, ok := intComp(comparatee)
	if ok {
		filterFn, ok := filterFuncs[comparator]
		if !ok {
			return errors.New("filter int", "unknown filter operator %v", comparatee)
		}
		filterFn(index, s.data, intC, bIndex)
	} else {
		seriesC, ok := comparatee.(Series)
		if !ok {
			return errors.New("filter int", "invalid comparison value type %v", reflect.TypeOf(comparatee))
		}

		filterFn, ok := filterFuncs2[comparator]
		if !ok {
			return errors.New("filter int", "unknown filter operator %v", comparatee)
		}
		filterFn(index, s.data, seriesC.data, bIndex)
	}

	return nil
}

func (s Series) filterCustom1(index index.Int, bIndex index.Bool, fn func(int) bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(s.data[index[i]])
		}
	}
}

func (s Series) filterCustom2(index index.Int, bIndex index.Bool, comparatee interface{}, fn func(int, int) bool) error {
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
	switch t := comparator.(type) {
	case string:
		return s.filterBuiltIn(index, t, comparatee, bIndex)
	case func(int) bool:
		s.filterCustom1(index, bIndex, t)
		return nil
	case func(int, int) bool:
		return s.filterCustom2(index, bIndex, comparatee, t)
	default:
		return errors.New("filter int", "invalid filter type %v", reflect.TypeOf(comparator))
	}
}

func gt(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] > comp
		}
	}
}

func gte(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] >= comp
		}
	}
}

func lt(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] < comp
		}
	}
}

func eq(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] == comp
		}
	}
}

func gt2(index index.Int, column []int, compCol []int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] > compCol[pos]
		}
	}
}

func gte2(index index.Int, column []int, compCol []int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] >= compCol[pos]
		}
	}
}

func lt2(index index.Int, column []int, compCol []int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] < compCol[pos]
		}
	}
}

func eq2(index index.Int, column []int, compCol []int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] == compCol[pos]
		}
	}
}
