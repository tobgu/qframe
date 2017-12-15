package iseries

import (
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
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
