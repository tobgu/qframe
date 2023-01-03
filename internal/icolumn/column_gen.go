// Code generated by genny. DO NOT EDIT.
// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package icolumn

// Code generated from template/column.go DO NOT EDIT

import (
	"fmt"
	"io"

	"github.com/tobgu/qframe/config/rolling"
	qfbinary "github.com/tobgu/qframe/internal/binary"

	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/qerrors"
)

type Column struct {
	data []int
}

func New(d []int) Column {
	return Column{data: d}
}

func NewConst(val int, count int) Column {
	var nullVal int
	data := make([]int, count)
	if val != nullVal {
		for i := range data {
			data[i] = val
		}
	}

	return Column{data: data}
}

func (c Column) fnName(name string) string {
	return fmt.Sprintf("%s.%s", c.DataType(), name)
}

// Apply1 applies a single argument function. The result may be a column
// of a different type than the current column.
func (c Column) Apply1(fn interface{}, ix index.Int) (interface{}, error) {
	switch t := fn.(type) {
	case func(int) int:
		result := make([]int, len(c.data))
		for _, i := range ix {
			result[i] = t(c.data[i])
		}
		return result, nil
	case func(int) float64:
		result := make([]float64, len(c.data))
		for _, i := range ix {
			result[i] = t(c.data[i])
		}
		return result, nil
	case func(int) bool:
		result := make([]bool, len(c.data))
		for _, i := range ix {
			result[i] = t(c.data[i])
		}
		return result, nil
	case func(int) *string:
		result := make([]*string, len(c.data))
		for _, i := range ix {
			result[i] = t(c.data[i])
		}
		return result, nil
	default:
		return nil, qerrors.New(c.fnName("Apply1"), "cannot apply type %#v to column", fn)
	}
}

// Apply2 applies a double argument function to two columns. Both columns must have the
// same type. The resulting column will have the same type as this column.
func (c Column) Apply2(fn interface{}, s2 column.Column, ix index.Int) (column.Column, error) {
	ss2, ok := s2.(Column)
	if !ok {
		return Column{}, qerrors.New(c.fnName("Apply2"), "invalid column type: %s", s2.DataType())
	}

	t, ok := fn.(func(int, int) int)
	if !ok {
		return Column{}, qerrors.New("Apply2", "invalid function type: %#v", fn)
	}

	result := make([]int, len(c.data))
	for _, i := range ix {
		result[i] = t(c.data[i], ss2.data[i])
	}

	return New(result), nil
}

func (c Column) subset(index index.Int) Column {
	data := make([]int, len(index))
	for i, ix := range index {
		data[i] = c.data[ix]
	}

	return Column{data: data}
}

func (c Column) Subset(index index.Int) column.Column {
	return c.subset(index)
}

func (c Column) Comparable(reverse, equalNull, nullLast bool) column.Comparable {
	result := Comparable{data: c.data, ltValue: column.LessThan, gtValue: column.GreaterThan, nullLtValue: column.LessThan, nullGtValue: column.GreaterThan, equalNullValue: column.NotEqual}
	if reverse {
		result.ltValue, result.nullLtValue, result.gtValue, result.nullGtValue =
			result.gtValue, result.nullGtValue, result.ltValue, result.nullLtValue
	}

	if nullLast {
		result.nullLtValue, result.nullGtValue = result.nullGtValue, result.nullLtValue
	}

	if equalNull {
		result.equalNullValue = column.Equal
	}

	return result
}

func (c Column) String() string {
	return fmt.Sprintf("%v", c.data)
}

func (c Column) Len() int {
	return len(c.data)
}

func (c Column) Aggregate(indices []index.Int, fn interface{}) (column.Column, error) {
	var actualFn func([]int) int
	var ok bool

	switch t := fn.(type) {
	case string:
		actualFn, ok = aggregations[t]
		if !ok {
			return nil, qerrors.New(c.fnName("Aggregate"), "aggregation function %c is not defined for column", fn)
		}
	case func([]int) int:
		actualFn = t
	default:
		return nil, qerrors.New(c.fnName("Aggregate"), "invalid aggregation function type: %v", t)
	}

	data := make([]int, 0, len(indices))
	var buf []int
	for _, ix := range indices {
		subS := c.subsetWithBuf(ix, &buf)
		data = append(data, actualFn(subS.data))
	}

	return Column{data: data}, nil
}

func (c Column) subsetWithBuf(index index.Int, buf *[]int) Column {
	if cap(*buf) < len(index) {
		*buf = make([]int, 0, len(index))
	}

	data := (*buf)[:0]
	for _, ix := range index {
		data = append(data, c.data[ix])
	}

	return Column{data: data}
}

func (c Column) View(ix index.Int) View {
	return View{data: c.data, index: ix}
}

func (c Column) Rolling(fn interface{}, ix index.Int, config rolling.Config) (column.Column, error) {
	return c, nil
}

func (c Column) ToQBin(w io.Writer) error {
	err := qfbinary.Write[uint64](w, uint64(len(c.data)))
	if err != nil {
		return fmt.Errorf("error writing %s column length: %w", c.DataType(), err)
	}

	_, err = w.Write(qfbinary.UnsafeByteSlice(c.data))
	if err != nil {
		return fmt.Errorf("error writing %s column: %w", c.DataType(), err)
	}

	return nil
}

func ReadQBin(r io.Reader) (Column, error) {
	colLen, err := qfbinary.Read[uint64](r)
	if err != nil {
		return Column{}, fmt.Errorf("error reading %s column length: %w", Column{}.DataType(), err)
	}

	data := make([]int, colLen)
	_, err = io.ReadFull(r, qfbinary.UnsafeByteSlice(data))
	if err != nil {
		return Column{}, fmt.Errorf("error reading %s column data: %w", Column{}.DataType(), err)
	}

	return New(data), nil
}

type Comparable struct {
	data           []int
	ltValue        column.CompareResult
	nullLtValue    column.CompareResult
	gtValue        column.CompareResult
	nullGtValue    column.CompareResult
	equalNullValue column.CompareResult
}

// View is a view into a column that allows access to individual elements by index.
type View struct {
	data  []int
	index index.Int
}

// ItemAt returns the value at position i.
func (v View) ItemAt(i int) int {
	return v.data[v.index[i]]
}

// Len returns the column length.
func (v View) Len() int {
	return len(v.index)
}

// Slice returns a slice containing a copy of the column data.
func (v View) Slice() []int {
	// TODO: This forces an alloc, as an alternative a slice could be taken
	//       as input that can be (re)used by the client. Are there use cases
	//       where this would actually make sense?
	result := make([]int, v.Len())
	for i, j := range v.index {
		result[i] = v.data[j]
	}
	return result
}
