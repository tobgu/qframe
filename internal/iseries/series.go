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

var filterFuncs = map[string]func(index.Int, []int, int, index.Bool){
	filter.Gt:  gt,
	filter.Gte: gte,
	filter.Lt:  lt,
	filter.Eq:  eq,
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

func (s Series) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Also make it possible to compare to values in other column
	switch t := comparator.(type) {
	case string:
		comp, ok := comparatee.(int)
		if !ok {
			// Accept floats by truncating them
			compFloat, ok := comparatee.(float64)
			if !ok {
				return errors.New("filter int", "invalid comparison value type %v", reflect.TypeOf(comparatee))
			}
			comp = int(compFloat)
		}

		compFunc, ok := filterFuncs[t]
		if !ok {
			return errors.New("filter int", "unknown filter operator %v", comparatee)
		}
		compFunc(index, s.data, comp, bIndex)
		return nil
	case func(int) bool:
		for i, x := range bIndex {
			if !x {
				bIndex[i] = t(s.data[index[i]])
			}
		}
		return nil
	default:
		return errors.New("filter int", "invalid filter type %v", comparator)
	}
}

// TODO: Some kind of code generation for all the below functions for all supported types

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
