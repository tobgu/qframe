package sseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	qfstrings "github.com/tobgu/qframe/internal/strings"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]*string) *string{}

var stringFilterFuncs = map[filter.Comparator]func(index.Int, []*string, string, index.Bool) error{
	filter.Gt:  gt,
	filter.Lt:  lt,
	filter.Eq:  eq,
	filter.Neq: neq,
	"like":     like,
	"ilike":    ilike,
}

func (s Series) StringAt(i uint32, naRep string) string {
	p := s.data[i]
	if p == nil {
		return naRep
	}

	return *p
}

func (s Series) AppendByteStringAt(buf []byte, i int) []byte {
	if s.data[i] == nil {
		return append(buf, "null"...)
	}

	return qfstrings.AppendQuotedString(buf, *s.data[i])
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonString(s.subset(index).data)
}

func (s Series) ByteSize() int {
	// TODO: This is probably not how we want to do it in the end
	//       since it's both inefficient and potentially wrong when
	//       string sharing is significant.
	// Slice header + pointers
	totalSize := 2*8 + 8*len(s.data)
	for _, s := range s.data {
		totalSize += len(*s)
	}

	return totalSize
}

func (s Series) Equals(index index.Int, other series.Series, otherIndex index.Int) bool {
	otherI, ok := other.(Series)
	if !ok {
		return false
	}

	for ix, x := range index {
		sPtr := s.data[x]
		osPtr := otherI.data[otherIndex[ix]]
		if sPtr == nil || osPtr == nil {
			if sPtr == osPtr {
				continue
			}

			return false
		}

		if *sPtr != *osPtr {
			return false
		}
	}

	return true
}

func (c Comparable) Compare(i, j uint32) series.CompareResult {
	x, y := c.data[i], c.data[j]
	if x == nil || y == nil {
		if x != nil {
			return c.gtValue
		}

		if y != nil {
			return c.ltValue
		}

		// Consider nil == nil, this means that we can group
		// by null values for example (this differs from Pandas)
		return series.Equal
	}

	if *x < *y {
		return c.ltValue
	}

	if *x > *y {
		return c.gtValue
	}

	return series.Equal
}

func (s Series) Filter(index index.Int, c filter.Comparator, comparatee interface{}, bIndex index.Bool) error {
	if compFunc, ok := stringFilterFuncs[c]; ok {
		sComp, ok := comparatee.(string)
		if !ok {
			return errors.New("filter string column", "invalid filter type, expected string")
		}

		return compFunc(index, s.data, sComp, bIndex)
	}

	return errors.New("filter string column", "Unknown filter %s", c)
}

func gt(index index.Int, column []*string, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			sp := column[index[i]]
			if sp != nil {
				bIndex[i] = *sp > comparatee
			}
		}
	}

	return nil
}

func lt(index index.Int, column []*string, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			sp := column[index[i]]
			bIndex[i] = sp == nil || *sp < comparatee
		}
	}

	return nil
}

func eq(index index.Int, column []*string, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			sp := column[index[i]]
			if sp != nil {
				bIndex[i] = *sp == comparatee
			}
		}
	}

	return nil
}

func neq(index index.Int, column []*string, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			sp := column[index[i]]
			if sp != nil {
				bIndex[i] = *sp != comparatee
			}
		}
	}

	return nil
}

func like(index index.Int, column []*string, comparatee string, bIndex index.Bool) error {
	return regexFilter(index, column, comparatee, bIndex, true)
}

func ilike(index index.Int, column []*string, comparatee string, bIndex index.Bool) error {
	return regexFilter(index, column, comparatee, bIndex, false)
}

func regexFilter(index index.Int, column []*string, comparatee string, bIndex index.Bool, caseSensitive bool) error {
	matcher, err := qfstrings.NewMatcher(comparatee, caseSensitive)
	if err != nil {
		return errors.Propagate("Regex filter", err)
	}

	for i, x := range bIndex {
		if !x {
			sp := column[index[i]]
			if sp != nil {
				bIndex[i] = matcher.Matches(sp)
			}
		}
	}

	return nil
}
