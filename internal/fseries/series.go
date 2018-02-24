package fseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"math"
	"reflect"
	"strconv"
)

func (s Series) StringAt(i uint32, naRep string) string {
	value := s.data[i]
	if math.IsNaN(value) {
		return naRep
	}
	return strconv.FormatFloat(s.data[i], 'f', -1, 64)
}

func (s Series) AppendByteStringAt(buf []byte, i uint32) []byte {
	return strconv.AppendFloat(buf, s.data[i], 'f', -1, 64)
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonFloat64(s.subset(index).data)
}

func (s Series) ByteSize() int {
	// Slice header + data
	return 2*8 + 8*len(s.data)
}

func (s Series) Equals(index index.Int, other series.Series, otherIndex index.Int) bool {
	otherI, ok := other.(Series)
	if !ok {
		return false
	}

	for ix, x := range index {
		v1, v2 := s.data[x], otherI.data[otherIndex[ix]]
		if v1 != v2 {
			// NaN != NaN but for our purposes they are the same
			if !(math.IsNaN(v1) && math.IsNaN(v2)) {
				return false
			}
		}
	}

	return true
}

func (c Comparable) Compare(i, j uint32) series.CompareResult {
	x, y := c.data[i], c.data[j]
	if x < y {
		return c.ltValue
	}

	if x > y {
		return c.gtValue
	}

	if math.IsNaN(x) || math.IsNaN(y) {
		if math.IsNaN(x) {
			return c.ltValue
		}

		if math.IsNaN(y) {
			return c.gtValue
		}

		// Consider NaN == NaN, this means that we can group
		// by null values for example (this differs from Pandas)
	}

	return series.Equal
}

func (s Series) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	switch t := comparatee.(type) {
	case float64:
		compFunc, ok := filterFuncs[comparator]
		if !ok {
			return errors.New("filter float", "invalid comparison operator, %v", comparator)
		}
		compFunc(index, s.data, t, bIndex)
	case Series:
		compFunc, ok := filterFuncs2[comparator]
		if !ok {
			return errors.New("filter float", "invalid comparison operator, %v", comparator)
		}
		compFunc(index, s.data, t.data, bIndex)
	default:
		return errors.New("filter float", "invalid comparison value type %v", reflect.TypeOf(comparatee))
	}
	return nil
}

func (s Series) filterCustom1(index index.Int, fn func(float64) bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(s.data[index[i]])
		}
	}
}

func (s Series) filterCustom2(index index.Int, fn func(float64, float64) bool, comparatee interface{}, bIndex index.Bool) error {
	otherS, ok := comparatee.(Series)
	if !ok {
		return errors.New("filter float", "expected comparatee to be float series, was %v", reflect.TypeOf(comparatee))
	}

	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(s.data[index[i]], otherS.data[index[i]])
		}
	}

	return nil
}

func (s Series) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	var err error
	switch t := comparator.(type) {
	case string:
		err = s.filterBuiltIn(index, t, comparatee, bIndex)
	case func(float64) bool:
		s.filterCustom1(index, t, bIndex)
	case func(float64, float64) bool:
		err = s.filterCustom2(index, t, comparatee, bIndex)
	default:
		err = errors.New("filter float", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}
