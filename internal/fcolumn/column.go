package fcolumn

import (
	"encoding/json"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"math"
	"reflect"
	"strconv"
)

func (c Column) StringAt(i uint32, naRep string) string {
	value := c.data[i]
	if math.IsNaN(value) {
		return naRep
	}
	return strconv.FormatFloat(c.data[i], 'f', -1, 64)
}

func (c Column) AppendByteStringAt(buf []byte, i uint32) []byte {
	return strconv.AppendFloat(buf, c.data[i], 'f', -1, 64)
}

func (c Column) Marshaler(index index.Int) json.Marshaler {
	return io.JsonFloat64(c.subset(index).data)
}

func (c Column) ByteSize() int {
	// Slice header + data
	return 2*8 + 8*len(c.data)
}

func (c Column) Equals(index index.Int, other column.Column, otherIndex index.Int) bool {
	otherI, ok := other.(Column)
	if !ok {
		return false
	}

	for ix, x := range index {
		v1, v2 := c.data[x], otherI.data[otherIndex[ix]]
		if v1 != v2 {
			// NaN != NaN but for our purposes they are the same
			if !(math.IsNaN(v1) && math.IsNaN(v2)) {
				return false
			}
		}
	}

	return true
}

func (c Comparable) Compare(i, j uint32) column.CompareResult {
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

	return column.Equal
}

func (c Column) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	switch t := comparatee.(type) {
	case float64:
		compFunc, ok := filterFuncs[comparator]
		if !ok {
			return errors.New("filter float", "invalid comparison operator, %v", comparator)
		}
		compFunc(index, c.data, t, bIndex)
	case Column:
		compFunc, ok := filterFuncs2[comparator]
		if !ok {
			return errors.New("filter float", "invalid comparison operator, %v", comparator)
		}
		compFunc(index, c.data, t.data, bIndex)
	default:
		return errors.New("filter float", "invalid comparison value type %v", reflect.TypeOf(comparatee))
	}
	return nil
}

func (c Column) filterCustom1(index index.Int, fn func(float64) bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(c.data[index[i]])
		}
	}
}

func (c Column) filterCustom2(index index.Int, fn func(float64, float64) bool, comparatee interface{}, bIndex index.Bool) error {
	otherC, ok := comparatee.(Column)
	if !ok {
		return errors.New("filter float", "expected comparatee to be float column, was %v", reflect.TypeOf(comparatee))
	}

	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(c.data[index[i]], otherC.data[index[i]])
		}
	}

	return nil
}

func (c Column) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	var err error
	switch t := comparator.(type) {
	case string:
		err = c.filterBuiltIn(index, t, comparatee, bIndex)
	case func(float64) bool:
		c.filterCustom1(index, t, bIndex)
	case func(float64, float64) bool:
		err = c.filterCustom2(index, t, comparatee, bIndex)
	default:
		err = errors.New("filter float", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}
