package bcolumn

import (
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
)

var filterFuncs = map[string]func(index.Int, []bool, bool, index.Bool){
	filter.Eq:  eq,
	filter.Neq: neq,
}

var filterFuncs2 = map[string]func(index.Int, []bool, []bool, index.Bool){
	filter.Eq:  eq2,
	filter.Neq: neq2,
}
