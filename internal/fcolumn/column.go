package fcolumn

import (
	"fmt"
	qfbinary "github.com/tobgu/qframe/internal/binary"
	"github.com/tobgu/qframe/internal/ryu"
	"io"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/hash"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/qerrors"
	"github.com/tobgu/qframe/types"
)

func (c Column) DataType() types.DataType {
	return types.Float
}

func (c Column) StringAt(i uint32, naRep string) string {
	value := c.data[i]
	if math.IsNaN(value) {
		return naRep
	}
	return strconv.FormatFloat(c.data[i], 'f', -1, 64)
}

func (c Column) AppendByteStringAt(buf []byte, i uint32) []byte {
	value := c.data[i]
	if math.IsNaN(value) {
		return append(buf, "null"...)
	}

	return ryu.AppendFloat64f(buf, value)
}

func (c Column) ByteSize() int {
	// Slice header + data
	return 2*8 + 8*cap(c.data)
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
		if !math.IsNaN(x) {
			return c.nullGtValue
		}

		if !math.IsNaN(y) {
			return c.nullLtValue
		}

		return c.equalNullValue
	}

	return column.Equal
}

func (c Comparable) Hash(i uint32, seed uint64) uint64 {
	f := c.data[i]
	if math.IsNaN(f) && c.equalNullValue == column.NotEqual {
		// Use a random value here to avoid hash collisions when
		// we don't consider null to equal null.
		return rand.Uint64()
	}

	bits := math.Float64bits(c.data[i])
	b := (*[8]byte)(unsafe.Pointer(&bits))[:]
	return hash.HashBytes(b, seed)
}

func (c Column) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	switch t := comparatee.(type) {
	case float64:
		if math.IsNaN(t) {
			return qerrors.New("filter float", "NaN not allowed as filter argument")
		}

		compFunc, ok := filterFuncs1[comparator]
		if !ok {
			return qerrors.New("filter float", "invalid comparison operator to single argument filter, %v", comparator)
		}
		compFunc(index, c.data, t, bIndex)
	case Column:
		compFunc, ok := filterFuncs2[comparator]
		if !ok {
			return qerrors.New("filter float", "invalid comparison operator to column - column filter, %v", comparator)
		}
		compFunc(index, c.data, t.data, bIndex)
	case nil:
		compFunc, ok := filterFuncs0[comparator]
		if !ok {
			return qerrors.New("filter float", "invalid comparison operator to zero argument filter, %v", comparator)
		}
		compFunc(index, c.data, bIndex)
	default:
		return qerrors.New("filter float", "invalid comparison value type %v", reflect.TypeOf(comparatee))
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
		return qerrors.New("filter float", "expected comparatee to be float column, was %v", reflect.TypeOf(comparatee))
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
		err = qerrors.New("filter float", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}

func (c Column) FunctionType() types.FunctionType {
	return types.FunctionTypeFloat
}

func (c Column) Append(cols ...column.Column) (column.Column, error) {
	// TODO Append
	return nil, qerrors.New("Append", "Not implemented yet")
}

func (c Column) ToQBin(w io.Writer) error {
	err := qfbinary.Write[uint64](w, uint64(len(c.data)))
	if err != nil {
		return fmt.Errorf("error writing float column length: %w", err)
	}

	_, err = w.Write(qfbinary.UnsafeByteSlice(c.data))
	if err != nil {
		return fmt.Errorf("error writing float column: %w", err)
	}

	return nil
}

func ReadQBin(r io.Reader) (Column, error) {
	colLen, err := qfbinary.Read[uint64](r)
	if err != nil {
		return Column{}, fmt.Errorf("error reading float column length: %w", err)
	}

	data := make([]float64, colLen)
	_, err = io.ReadFull(r, qfbinary.UnsafeByteSlice(data))
	if err != nil {
		return Column{}, fmt.Errorf("error reading float column data: %w", err)
	}

	return New(data), nil
}
