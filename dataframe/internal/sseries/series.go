package sseries

import (
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]string) string{}

var filterFuncs = map[filter.Comparator]func(index.Int, []string, interface{}, index.Bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
}

func (s Series) StringAt(i int) string {
	return s.data[i]
}
