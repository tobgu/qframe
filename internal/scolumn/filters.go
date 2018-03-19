package scolumn

import (
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	qfstrings "github.com/tobgu/qframe/internal/strings"
)

var filterFuncs0 = map[string]func(index.Int, Column, index.Bool) error{
	filter.IsNull:    isNull,
	filter.IsNotNull: isNotNull,
}

var filterFuncs1 = map[string]func(index.Int, Column, string, index.Bool) error{
	filter.Gt:  gt,
	filter.Gte: gte,
	filter.Lt:  lt,
	filter.Lte: lte,
	filter.Eq:  eq,
	filter.Neq: neq,
	"like":     like,
	"ilike":    ilike,
}

var multiInputFilterFuncs = map[string]func(index.Int, Column, qfstrings.StringSet, index.Bool) error{
	filter.In: in,
}

var filterFuncs2 = map[string]func(index.Int, Column, Column, index.Bool) error{
	filter.Gt:  gt2,
	filter.Gte: gte2,
	filter.Lt:  lt2,
	filter.Lte: lte2,
	filter.Eq:  eq2,
	filter.Neq: neq2,
}

func neq(index index.Int, s Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			bIndex[i] = isNull || s != comparatee
		}
	}

	return nil
}

func like(index index.Int, s Column, comparatee string, bIndex index.Bool) error {
	return regexFilter(index, s, comparatee, bIndex, true)
}

func ilike(index index.Int, s Column, comparatee string, bIndex index.Bool) error {
	return regexFilter(index, s, comparatee, bIndex, false)
}

func in(index index.Int, s Column, comparatee qfstrings.StringSet, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			if !isNull {
				bIndex[i] = comparatee.Contains(s)
			}
		}
	}

	return nil
}

func regexFilter(index index.Int, s Column, comparatee string, bIndex index.Bool, caseSensitive bool) error {
	matcher, err := qfstrings.NewMatcher(comparatee, caseSensitive)
	if err != nil {
		return errors.Propagate("Regex filter", err)
	}

	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			if !isNull {
				bIndex[i] = matcher.Matches(s)
			}
		}
	}

	return nil
}

func neq2(index index.Int, col, col2 Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := col.stringAt(index[i])
			s2, isNull2 := col2.stringAt(index[i])
			bIndex[i] = isNull || isNull2 || s != s2
		}
	}
	return nil
}

func isNull(index index.Int, col Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			_, isNull := col.stringAt(index[i])
			bIndex[i] = isNull
		}
	}
	return nil
}

func isNotNull(index index.Int, col Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			_, isNull := col.stringAt(index[i])
			bIndex[i] = !isNull
		}
	}
	return nil
}
