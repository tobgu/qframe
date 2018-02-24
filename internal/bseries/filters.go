package bseries

import (
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
)

var filterFuncs = map[string]func(index.Int, []bool, bool, index.Bool){
	filter.Eq: eq,
}

func eq(index index.Int, column []bool, comp bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] == comp
		}
	}
}

var filterFuncs2 = map[string]func(index.Int, []bool, []bool, index.Bool){
	filter.Eq: eq2,
}

func eq2(index index.Int, column []bool, comp []bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] == comp[index[i]]
		}
	}
}
