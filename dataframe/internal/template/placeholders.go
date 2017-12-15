package template

import (
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
)

// This file contains definitions for data and functions that need to be added
// manually for each data type.

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]dataType) dataType{}

var filterFuncs = map[filter.Comparator]func(index.Int, []dataType, interface{}, index.Bool) error{}
