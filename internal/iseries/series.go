package iseries

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"strconv"
)

func sum(values []int) int {
	result := 0
	for _, v := range values {
		result += v
	}
	return result
}

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]int) int{
	"sum": sum,
}

var filterFuncs = map[filter.Comparator]func(index.Int, []int, interface{}, index.Bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
}

func (s Series) StringAt(i int, _ string) string {
	return strconv.FormatInt(int64(s.data[i]), 10)
}

func (s Series) AppendByteStringAt(buf []byte, i int) []byte {
	return strconv.AppendInt(buf, int64(s.data[i]), 10)
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonInt(s.subset(index).data)
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

func (s Series) Filter(index index.Int, c filter.Comparator, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Also make it possible to compare to values in other column
	compFunc, ok := filterFuncs[c]
	if !ok {
		return fmt.Errorf("invalid comparison operator for int, %v", c)
	}

	return compFunc(index, s.data, comparatee, bIndex)
}

// TODO: Some kind of code generation for all the below functions for all supported types

func gt(index index.Int, column []int, comparatee interface{}, bIndex index.Bool) error {
	comp, ok := comparatee.(int)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] > comp
	}

	return nil
}

func lt(index index.Int, column []int, comparatee interface{}, bIndex index.Bool) error {
	comp, ok := comparatee.(int)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] < comp
	}

	return nil
}
