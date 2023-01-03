package bcolumn

import (
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/hash"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/qerrors"
	"github.com/tobgu/qframe/types"
	"reflect"
	"strconv"
)

func (c Comparable) Compare(i, j uint32) column.CompareResult {
	x, y := c.data[i], c.data[j]
	if x == y {
		return column.Equal
	}

	if x {
		return c.gtValue
	}

	return c.ltValue
}

func (c Comparable) Hash(i uint32, seed uint64) uint64 {
	if c.data[i] {
		b := [1]byte{1}
		return hash.HashBytes(b[:], seed)
	}

	b := [1]byte{0}
	return hash.HashBytes(b[:], seed)
}

func (c Column) DataType() types.DataType {
	return types.Bool
}

func (c Column) StringAt(i uint32, _ string) string {
	return strconv.FormatBool(c.data[i])
}

func (c Column) AppendByteStringAt(buf []byte, i uint32) []byte {
	return strconv.AppendBool(buf, c.data[i])
}

func (c Column) ByteSize() int {
	// Slice header + data
	return 2*8 + cap(c.data)
}

func (c Column) Equals(index index.Int, other column.Column, otherIndex index.Int) bool {
	otherI, ok := other.(Column)
	if !ok {
		return false
	}

	for ix, x := range index {
		if c.data[x] != otherI.data[otherIndex[ix]] {
			return false
		}
	}

	return true
}

func (c Column) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	switch t := comparatee.(type) {
	case bool:
		compFunc, ok := filterFuncs[comparator]
		if !ok {
			return qerrors.New("filter bool", "invalid comparison operator for bool, %v", comparator)
		}
		compFunc(index, c.data, t, bIndex)
	case Column:
		compFunc, ok := filterFuncs2[comparator]
		if !ok {
			return qerrors.New("filter bool", "invalid comparison operator for bool, %v", comparator)
		}
		compFunc(index, c.data, t.data, bIndex)
	default:
		return qerrors.New("filter bool", "invalid comparison value type %v", reflect.TypeOf(comparatee))
	}
	return nil
}

func (c Column) filterCustom1(index index.Int, fn func(bool) bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(c.data[index[i]])
		}
	}
}

func (c Column) filterCustom2(index index.Int, fn func(bool, bool) bool, comparatee interface{}, bIndex index.Bool) error {
	otherC, ok := comparatee.(Column)
	if !ok {
		return qerrors.New("filter bool", "expected comparatee to be bool column, was %v", reflect.TypeOf(comparatee))
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
	case func(bool) bool:
		c.filterCustom1(index, t, bIndex)
	case func(bool, bool) bool:
		err = c.filterCustom2(index, t, comparatee, bIndex)
	default:
		err = qerrors.New("filter bool", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}

func (c Column) FunctionType() types.FunctionType {
	return types.FunctionTypeBool
}

func (c Column) Append(cols ...column.Column) (column.Column, error) {
	// TODO Append
	return nil, qerrors.New("Append", "Not implemented yet")
}
