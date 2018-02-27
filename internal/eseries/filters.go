package eseries

import (
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	qfstrings "github.com/tobgu/qframe/internal/strings"
)

var filterFuncs = map[string]func(index.Int, []enumVal, enumVal, index.Bool){
	filter.Gt: gt,
	filter.Lt: lt,
}

var filterFuncs2 = map[string]func(index.Int, []enumVal, []enumVal, index.Bool){
	filter.Gt: gt2,
	filter.Lt: lt2,
}

var multiFilterFuncs = map[string]func(comparatee string, values []string) (*bitset, error){
	"like":  like,
	"ilike": ilike,
}

func gt(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() > comparatee.compVal()
		}
	}
}

func lt(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() < comparatee.compVal()
		}
	}
}

func like(comp string, values []string) (*bitset, error) {
	return filterLike(comp, values, true)
}

func ilike(comp string, values []string) (*bitset, error) {
	return filterLike(comp, values, false)
}

func filterLike(comp string, values []string, caseSensitive bool) (*bitset, error) {
	matcher, err := qfstrings.NewMatcher(comp, caseSensitive)
	if err != nil {
		return nil, errors.Propagate("enum like", err)
	}

	bset := &bitset{}
	for i, v := range values {
		if matcher.Matches(v) {
			bset.set(enumVal(i))
		}
	}

	return bset, nil
}

func gt2(index index.Int, col, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() > col2[index[i]].compVal()
		}
	}
}

func lt2(index index.Int, col []enumVal, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() < col2[index[i]].compVal()
		}
	}
}
