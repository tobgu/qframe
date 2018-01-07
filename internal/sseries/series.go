package sseries

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/serialize"
	"github.com/tobgu/qframe/internal/series"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]*string) *string{}

var filterFuncs = map[filter.Comparator]func(index.Int, []*string, interface{}, index.Bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
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

// TODO: Some kind of code generation for all the below functions for all supported types

func gt(index index.Int, column []*string, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Handle nil values
	comp, ok := comparatee.(string)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || *column[index[i]] > comp
	}

	return nil
}

func lt(index index.Int, column []*string, comparatee interface{}, bIndex index.Bool) error {
	comp, ok := comparatee.(string)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || *column[index[i]] < comp
	}

	return nil
}
