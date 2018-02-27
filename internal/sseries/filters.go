package sseries

import (
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	qfstrings "github.com/tobgu/qframe/internal/strings"
)

var filterFuncs = map[string]func(index.Int, Series, string, index.Bool) error{
	filter.Gt:  gt,
	filter.Lt:  lt,
	filter.Eq:  eq,
	filter.Neq: neq,
	"like":     like,
	"ilike":    ilike,
}

var filterFuncs2 = map[string]func(index.Int, Series, Series, index.Bool) error{
	filter.Gt: gt2,
	filter.Lt: lt2,
}

func gt(index index.Int, s Series, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			if !isNull {
				bIndex[i] = s > comparatee
			}
		}
	}

	return nil
}

func lt(index index.Int, s Series, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			str, isNull := s.stringAt(index[i])
			bIndex[i] = isNull || str < comparatee
		}
	}

	return nil
}

func eq(index index.Int, s Series, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			if !isNull {
				bIndex[i] = s == comparatee
			}
		}
	}

	return nil
}

func neq(index index.Int, s Series, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			if !isNull {
				bIndex[i] = s != comparatee
			}
		}
	}

	return nil
}

func like(index index.Int, s Series, comparatee string, bIndex index.Bool) error {
	return regexFilter(index, s, comparatee, bIndex, true)
}

func ilike(index index.Int, s Series, comparatee string, bIndex index.Bool) error {
	return regexFilter(index, s, comparatee, bIndex, false)
}

func regexFilter(index index.Int, s Series, comparatee string, bIndex index.Bool, caseSensitive bool) error {
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

func gt2(index index.Int, s, s2 Series, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			str, isNull := s.stringAt(index[i])
			str2, isNull2 := s2.stringAt(index[i])
			if !isNull && !isNull2 {
				bIndex[i] = str > str2
			} else {
				bIndex[i] = !isNull && isNull2
			}
		}
	}

	return nil
}

func lt2(index index.Int, s, s2 Series, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			str, isNull := s.stringAt(index[i])
			str2, isNull2 := s2.stringAt(index[i])
			if !isNull && !isNull2 {
				bIndex[i] = str < str2
			} else {
				bIndex[i] = isNull && !isNull2
			}
		}
	}

	return nil
}
