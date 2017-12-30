package bseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"strconv"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]bool) bool{}

var filterFuncs = map[filter.Comparator]func(index.Int, []bool, interface{}, index.Bool) error{}

func (c Comparable) Compare(i, j uint32) series.CompareResult {
	x, y := c.data[i], c.data[j]
	if x == y {
		return series.Equal
	}

	if x {
		return c.gtValue
	}

	return c.ltValue
}

func (s Series) StringAt(i int, _ string) string {
	return strconv.FormatBool(s.data[i])
}

func (s Series) AppendByteStringAt(buf []byte, i int) []byte {
	return strconv.AppendBool(buf, s.data[i])
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonBool(s.subset(index).data)
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
