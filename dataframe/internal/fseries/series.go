package fseries

import (
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]float64) float64{}

var filterFuncs = map[filter.Comparator]func(index.Int, []float64, interface{}, index.Bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
}
