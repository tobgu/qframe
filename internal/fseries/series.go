package fseries

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"math"
	"strconv"
)

func sum(values []float64) float64 {
	result := 0.0
	for _, v := range values {
		result += v
	}
	return result
}

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]float64) float64{
	"sum": sum,
}

var filterFuncs = map[filter.Comparator]func(index.Int, []float64, interface{}, index.Bool) error{
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

func (s Series) Filter(index index.Int, c filter.Comparator, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Also make it possible to compare to values in other column
	compFunc, ok := filterFuncs[c]
	if !ok {
		return fmt.Errorf("invalid comparison operator for float64, %v", c)
	}

	return compFunc(index, s.data, comparatee, bIndex)
}

// TODO: Some kind of code generation for all the below functions for all supported types

func gt(index index.Int, column []float64, comparatee interface{}, bIndex index.Bool) error {
	comp, ok := comparatee.(float64)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] > comp
	}

	return nil
}

func lt(index index.Int, column []float64, comparatee interface{}, bIndex index.Bool) error {
	comp, ok := comparatee.(float64)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] < comp
	}

	return nil
}

// TODO: Handle NaN in comparisons, etc.
