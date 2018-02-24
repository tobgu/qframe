package iseries

import (
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
)

// Series - constant
var filterFuncs = map[string]func(index.Int, []int, int, index.Bool){
	filter.Gt:  gt,
	filter.Gte: gte,
	filter.Lt:  lt,
	filter.Eq:  eq,
}

// Series - Series
var filterFuncs2 = map[string]func(index.Int, []int, []int, index.Bool){
	filter.Gt:  gt2,
	filter.Gte: gte2,
	filter.Lt:  lt2,
	filter.Eq:  eq2,
}

func gt(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] > comp
		}
	}
}

func gte(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] >= comp
		}
	}
}

func lt(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] < comp
		}
	}
}

func eq(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] == comp
		}
	}
}

func gt2(index index.Int, column []int, compCol []int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] > compCol[pos]
		}
	}
}

func gte2(index index.Int, column []int, compCol []int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] >= compCol[pos]
		}
	}
}

func lt2(index index.Int, column []int, compCol []int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] < compCol[pos]
		}
	}
}

func eq2(index index.Int, column []int, compCol []int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] == compCol[pos]
		}
	}
}
