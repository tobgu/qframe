package fcolumn

import (
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
)

var filterFuncs = map[string]func(index.Int, []float64, float64, index.Bool){
	filter.Gt: gt,
	filter.Lt: lt,
}

var filterFuncs2 = map[string]func(index.Int, []float64, []float64, index.Bool){
	filter.Gt: gt2,
	filter.Lt: lt2,
}
