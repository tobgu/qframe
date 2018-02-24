package fseries

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

// TODO: Handle NaN in comparisons, etc.
func gt(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] > comp
		}
	}
}

func lt(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] < comp
		}
	}
}

func gt2(index index.Int, column []float64, comp []float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] > comp[index[i]]
		}
	}
}

func lt2(index index.Int, column []float64, comp []float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] < comp[index[i]]
		}
	}
}
