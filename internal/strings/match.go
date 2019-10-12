package strings

import (
	"regexp"
	"strings"

	"github.com/tobgu/qframe/errors"
)

type Matcher interface {
	Matches(s string) bool
}

type CIStringMatcher struct {
	matchString string
	buf         []byte
}

type CIPrefixMatcher CIStringMatcher

func (m *CIPrefixMatcher) Matches(s string) bool {
	return strings.HasPrefix(ToUpper(&m.buf, s), m.matchString)
}

type CISuffixMatcher CIStringMatcher

func (m *CISuffixMatcher) Matches(s string) bool {
	return strings.HasSuffix(ToUpper(&m.buf, s), m.matchString)
}

type CIContainsMatcher CIStringMatcher

func (m *CIContainsMatcher) Matches(s string) bool {
	return strings.Contains(ToUpper(&m.buf, s), m.matchString)
}

type CIExactMatcher CIStringMatcher

func (m *CIExactMatcher) Matches(s string) bool {
	return ToUpper(&m.buf, s) == m.matchString
}

type StringMatcher struct {
	matchString string
}

type PrefixMatcher StringMatcher

func (m *PrefixMatcher) Matches(s string) bool {
	return strings.HasPrefix(s, m.matchString)
}

type SuffixMatcher StringMatcher

func (m *SuffixMatcher) Matches(s string) bool {
	return strings.HasSuffix(s, m.matchString)
}

type ContainsMatcher StringMatcher

func (m *ContainsMatcher) Matches(s string) bool {
	return strings.Contains(s, m.matchString)
}

type ExactMatcher StringMatcher

func (m *ExactMatcher) Matches(s string) bool {
	return s == m.matchString
}

type RegexpMatcher struct {
	r *regexp.Regexp
}

func (m *RegexpMatcher) Matches(s string) bool {
	return m.r.MatchString(s)
}

func trimPercent(s string) string {
	s = strings.TrimPrefix(s, "%")
	s = strings.TrimSuffix(s, "%")
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
			return &CIContainsMatcher{matchString: trimPercent(comparatee), buf: buf}, nil
		}

		if fuzzyStart {
			return &CISuffixMatcher{matchString: trimPercent(comparatee), buf: buf}, nil
		}

		if fuzzyEnd {
			return &CIPrefixMatcher{matchString: trimPercent(comparatee), buf: buf}, nil
		}

		return &CIExactMatcher{matchString: comparatee, buf: buf}, nil
	}

	if fuzzyStart && fuzzyEnd {
		return &ContainsMatcher{matchString: trimPercent(comparatee)}, nil
	}

	if fuzzyStart {
		return &SuffixMatcher{matchString: trimPercent(comparatee)}, nil
	}

	if fuzzyEnd {
		return &PrefixMatcher{matchString: trimPercent(comparatee)}, nil
	}

	return &ExactMatcher{matchString: comparatee}, nil
}
