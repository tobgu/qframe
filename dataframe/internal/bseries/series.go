package bseries

import (
	"encoding/json"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
	"github.com/tobgu/go-qcache/dataframe/internal/io"
	"github.com/tobgu/go-qcache/dataframe/internal/series"
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

func (s Series) StringAt(i int) string {
	return strconv.FormatBool(s.data[i])
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonBool(s.subset(index).data)
}
