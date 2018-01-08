package sseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/serialize"
	"github.com/tobgu/qframe/internal/series"
	"regexp"
	"strings"
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

func (s Series) StringAt(i int, naRep string) string {
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

	return serialize.AppendQuotedString(buf, *s.data[i])
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonString(s.subset(index).data)
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

type Matcher interface {
	Matches(s *string) bool
}

type CIStringMatcher struct {
	matchString string
	buf         []byte
}

type CIPrefixMatcher CIStringMatcher

func (m *CIPrefixMatcher) Matches(s *string) bool {
	return strings.HasPrefix(serialize.ToUpper(&m.buf, s), m.matchString)
}

type CISuffixMatcher CIStringMatcher

func (m *CISuffixMatcher) Matches(s *string) bool {
	return strings.HasSuffix(serialize.ToUpper(&m.buf, s), m.matchString)
}

type CIContainsMatcher CIStringMatcher

func (m *CIContainsMatcher) Matches(s *string) bool {
	return strings.Contains(serialize.ToUpper(&m.buf, s), m.matchString)
}

type CIExactMatcher CIStringMatcher

func (m *CIExactMatcher) Matches(s *string) bool {
	return serialize.ToUpper(&m.buf, s) == m.matchString
}

type StringMatcher struct {
	matchString string
}

type PrefixMatcher StringMatcher

func (m *PrefixMatcher) Matches(s *string) bool {
	return strings.HasPrefix(*s, m.matchString)
}

type SuffixMatcher StringMatcher

func (m *SuffixMatcher) Matches(s *string) bool {
	return strings.HasSuffix(*s, m.matchString)
}

type ContainsMatcher StringMatcher

func (m *ContainsMatcher) Matches(s *string) bool {
	println("Contains matcher", *s)
	return strings.Contains(*s, m.matchString)
}

type ExactMatcher StringMatcher

func (m *ExactMatcher) Matches(s *string) bool {
	return *s == m.matchString
}

type RegexpMatcher struct {
	r *regexp.Regexp
}

func (m *RegexpMatcher) Matches(s *string) bool {
	return m.r.MatchString(*s)
}

func stripPercent(s string) string {
	if strings.HasPrefix(s, "%") {
		s = s[1:]
	}

	if strings.HasSuffix(s, "%") {
		s = s[:len(s)-1]
	}

	return s
}

func NewMatcher(comparatee string, caseSensitive bool) (Matcher, error) {
	fuzzyStart := strings.HasPrefix(comparatee, "%")
	fuzzyEnd := strings.HasSuffix(comparatee, "%")
	if regexp.QuoteMeta(comparatee) != comparatee {
		// There are regex characters in the match string
		if !fuzzyStart {
			comparatee = "^" + comparatee
		} else {
			comparatee = comparatee[1:]
		}

		if !fuzzyEnd {
			comparatee = comparatee + "$"
		} else {
			comparatee = comparatee[:len(comparatee)-1]
		}

		if !caseSensitive {
			comparatee = "(?i)" + comparatee
		}

		r, err := regexp.Compile(comparatee)
		if err != nil {
			return nil, errors.Propagate("string like", err)
		}

		return &RegexpMatcher{r: r}, nil
	}

	if !caseSensitive {
		comparatee = strings.ToUpper(comparatee)

		// Initial size, this will grow if needed
		buf := make([]byte, 10)
		if fuzzyStart && fuzzyEnd {
			return &CIContainsMatcher{matchString: stripPercent(comparatee), buf: buf}, nil
		}

		if fuzzyStart {
			return &CISuffixMatcher{matchString: stripPercent(comparatee), buf: buf}, nil
		}

		if fuzzyEnd {
			return &CIPrefixMatcher{matchString: stripPercent(comparatee), buf: buf}, nil
		}

		return &CIExactMatcher{matchString: comparatee, buf: buf}, nil
	}

	if fuzzyStart && fuzzyEnd {
		return &ContainsMatcher{matchString: stripPercent(comparatee)}, nil
	}

	if fuzzyStart {
		return &SuffixMatcher{matchString: stripPercent(comparatee)}, nil
	}

	if fuzzyEnd {
		return &PrefixMatcher{matchString: stripPercent(comparatee)}, nil
	}

	return &ExactMatcher{matchString: comparatee}, nil
}

func regexFilter(index index.Int, column []*string, comparatee string, bIndex index.Bool, caseSensitive bool) error {
	matcher, err := NewMatcher(comparatee, caseSensitive)
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
