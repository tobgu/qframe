package fseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"math"
	"reflect"
	"strconv"
)

func sum(values []float64) float64 {
	result := 0.0
	for _, v := range values {
		result += v
	}
	return result
}

var aggregations = map[string]func([]float64) float64{
	"sum": sum,
}

var filterFuncs = map[string]func(index.Int, []float64, float64, index.Bool){
	filter.Gt: gt,
	filter.Lt: lt,
}

func (s Series) StringAt(i uint32, naRep string) string {
	value := s.data[i]
	if math.IsNaN(value) {
		return naRep
	}
	return strconv.FormatFloat(s.data[i], 'f', -1, 64)
}

func (s Series) AppendByteStringAt(buf []byte, i uint32) []byte {
	return strconv.AppendFloat(buf, s.data[i], 'f', -1, 64)
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonFloat64(s.subset(index).data)
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
		v1, v2 := s.data[x], otherI.data[otherIndex[ix]]
		if v1 != v2 {
			// NaN != NaN but for our purposes they are the same
			if !(math.IsNaN(v1) && math.IsNaN(v2)) {
				return false
			}
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

	if math.IsNaN(x) || math.IsNaN(y) {
		if math.IsNaN(x) {
			return c.ltValue
		}

		if math.IsNaN(y) {
			return c.gtValue
		}

		// Consider NaN == NaN, this means that we can group
		// by null values for example (this differs from Pandas)
	}

	return series.Equal
}

func (s Series) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Also make it possible to compare to values in other column
	switch t := comparator.(type) {
	case string:
		comp, ok := comparatee.(float64)
		if !ok {
			return errors.New("filter float", "invalid comparison value type %v", reflect.TypeOf(comparatee))
		}

		compFunc, ok := filterFuncs[t]
		if !ok {
			return errors.New("filter float", "invalid comparison operator for float64, %v", comparator)
		}
		compFunc(index, s.data, comp, bIndex)
		return nil
	case func(float64) bool:
		for i, x := range bIndex {
			if !x {
				bIndex[i] = t(s.data[index[i]])
			}
		}
		return nil
	default:
		return errors.New("filter float", "invalid filter type %v", comparator)
	}
}

// TODO: Handle NaN in comparisons, etc.
func gt(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] > comp
		}
	}
}

func lt(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] < comp
		}
	}
}
