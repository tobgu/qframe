package bseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"reflect"
	"strconv"
)

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

func (s Series) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	switch t := comparatee.(type) {
	case bool:
		compFunc, ok := filterFuncs[comparator]
		if !ok {
			return errors.New("filter bool", "invalid comparison operator for bool, %v", comparator)
		}
		compFunc(index, s.data, t, bIndex)
	case Series:
		compFunc, ok := filterFuncs2[comparator]
		if !ok {
			return errors.New("filter bool", "invalid comparison operator for bool, %v", comparator)
		}
		compFunc(index, s.data, t.data, bIndex)
	default:
		return errors.New("filter bool", "invalid comparison value type %v", reflect.TypeOf(comparatee))
	}
	return nil
}

func (s Series) filterCustom1(index index.Int, fn func(bool) bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(s.data[index[i]])
		}
	}
}

func (s Series) filterCustom2(index index.Int, fn func(bool, bool) bool, comparatee interface{}, bIndex index.Bool) error {
	otherS, ok := comparatee.(Series)
	if !ok {
		return errors.New("filter bool", "expected comparatee to be bool series, was %v", reflect.TypeOf(comparatee))
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
	case func(bool) bool:
		s.filterCustom1(index, t, bIndex)
	case func(bool, bool) bool:
		err = s.filterCustom2(index, t, comparatee, bIndex)
	default:
		err = errors.New("filter bool", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}
