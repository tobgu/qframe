package fcolumn

import (
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
)

var filterFuncs = map[string]func(index.Int, []float64, float64, index.Bool){
	filter.Gt:  gt,
	filter.Gte: gte,
	filter.Lt:  lt,
	filter.Lte: lte,
	filter.Eq:  eq,
	filter.Neq: neq,
}

var filterFuncs2 = map[string]func(index.Int, []float64, []float64, index.Bool){
	filter.Gt:  gt2,
	filter.Gte: gte2,
	filter.Lt:  lt2,
	filter.Lte: lte2,
	filter.Eq:  eq2,
	filter.Neq: neq2,
}
