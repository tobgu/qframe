package icolumn

import (
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
)

// Column - constant
var filterFuncs = map[string]func(index.Int, []int, int, index.Bool){
	filter.Gt:  gt,
	filter.Gte: gte,
	filter.Lt:  lt,
	filter.Lte: lte,
	filter.Eq:  eq,
	filter.Neq: neq,
	"any_bits": anyBits,
	"all_bits": allBits,
}

// Comparisons against multiple values
var multiInputFilterFuncs = map[string]func(index.Int, []int, intSet, index.Bool){
	filter.In: in,
}

// Column - Column
var filterFuncs2 = map[string]func(index.Int, []int, []int, index.Bool){
	filter.Gt:  gt2,
	filter.Gte: gte2,
	filter.Lt:  lt2,
	filter.Lte: lte2,
	filter.Eq:  eq2,
	filter.Neq: neq2,
}

// Column only
var filterFuncs0 = map[string]func(index.Int, []int, index.Bool){
	filter.IsNull:    isNull,
	filter.IsNotNull: isNotNull,
}

func isNull(_ index.Int, _ []int, bIndex index.Bool) {
	// Int columns are never null, this function is provided for convenience to avoid
	// clients from having to keep track of if a column is of type int or float for
	// common operations.
	for i := range bIndex {
		bIndex[i] = false
	}
}

func isNotNull(_ index.Int, _ []int, bIndex index.Bool) {
	// Int columns are never null, this function is provided for convenience to avoid
	// clients from having to keep track of if a column is of type int or float for
	// common operations.
	for i := range bIndex {
		bIndex[i] = true
	}
}

func in(index index.Int, column []int, comp intSet, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = comp.Contains(column[index[i]])
		}
	}
}

func anyBits(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]]&comp > 0
		}
	}
}

func allBits(index index.Int, column []int, comp int, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]]&comp == comp
		}
	}
}
