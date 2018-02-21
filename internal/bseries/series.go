package bseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"strconv"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]bool) bool{}

var filterFuncs = map[string]func(index.Int, []bool, interface{}, index.Bool) error{}

func (c Comparable) Compare(i, j uint32) series.CompareResult {
	x, y := c.data[i], c.data[j]
	if x == y {
		return series.Equal
	}

	if x {
		return c.gtValue
	}

	return c.ltValue
}

func (s Series) StringAt(i uint32, _ string) string {
	return strconv.FormatBool(s.data[i])
}

func (s Series) AppendByteStringAt(buf []byte, i uint32) []byte {
	return strconv.AppendBool(buf, s.data[i])
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonBool(s.subset(index).data)
}

func (s Series) ByteSize() int {
	// Slice header + data
	return 2*8 + len(s.data)
}

func (s Series) Equals(index index.Int, other series.Series, otherIndex index.Int) bool {
	otherI, ok := other.(Series)
	if !ok {
		return false
	}

	for ix, x := range index {
		if s.data[x] != otherI.data[otherIndex[ix]] {
			return false
		}
	}

	return true
}

func (s Series) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Also make it possible to compare to values in other column
	switch t := comparator.(type) {
	case string:
		compFunc, ok := filterFuncs[t]
		if !ok {
			return errors.New("filter bool", "invalid comparison operator for bool, %v", comparator)
		}
		compFunc(index, s.data, comparatee, bIndex)
		return nil
	case func(bool) bool:
		for i, x := range bIndex {
			if !x {
				bIndex[i] = t(s.data[index[i]])
			}
		}
		return nil
	default:
		return errors.New("filter bool", "invalid filter type %v", comparator)
	}
}
