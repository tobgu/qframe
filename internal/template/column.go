package template

// Code generated from template/column.go DO NOT EDIT

import (
	"fmt"
	"reflect"

	"github.com/cheekybits/genny/generic"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
)

type dataType generic.Number

//go:generate genny -in=$GOFILE -out=../icolumn/column_gen.go -pkg=icolumn gen "dataType=int"
//go:generate genny -in=$GOFILE -out=../fcolumn/column_gen.go -pkg=fcolumn gen "dataType=float64"
//go:generate genny -in=$GOFILE -out=../bcolumn/column_gen.go -pkg=bcolumn gen "dataType=bool"

type Column struct {
	data []dataType
}

func New(d []dataType) Column {
	return Column{data: d}
}

func NewConst(val dataType, count int) Column {
	var nullVal dataType
	data := make([]dataType, count)
	if val != nullVal {
		for i := range data {
			data[i] = val
		}
	}

	return Column{data: data}
}

// Apply single argument function. The result may be a column
// of a different type than the current column.
func (c Column) Apply1(fn interface{}, ix index.Int) (interface{}, error) {
	switch t := fn.(type) {
	case func(dataType) int:
		result := make([]int, len(c.data))
		for _, i := range ix {
			result[i] = t(c.data[i])
		}
		return result, nil
	case func(dataType) float64:
		result := make([]float64, len(c.data))
		for _, i := range ix {
			result[i] = t(c.data[i])
		}
		return result, nil
	case func(dataType) bool:
		result := make([]bool, len(c.data))
		for _, i := range ix {
			result[i] = t(c.data[i])
		}
		return result, nil
	case func(dataType) *string:
		result := make([]*string, len(c.data))
		for _, i := range ix {
			result[i] = t(c.data[i])
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%s.Apply1: cannot apply type %#v to column", c.DataType(), fn)
	}
}

// Apply double argument function to two columns. Both columns must have the
// same type. The resulting column will have the same type as this column.
func (c Column) Apply2(fn interface{}, s2 column.Column, ix index.Int) (column.Column, error) {
	ss2, ok := s2.(Column)
	if !ok {
		return Column{}, fmt.Errorf("%s.Apply2: invalid column type: %s", c.DataType(), s2.DataType())
	}

	t, ok := fn.(func(dataType, dataType) dataType)
	if !ok {
		return Column{}, fmt.Errorf("%s.Apply2: invalid function type: %#v", c.DataType(), fn)
	}

	result := make([]dataType, len(c.data))
	for _, i := range ix {
		result[i] = t(c.data[i], ss2.data[i])
	}

	return New(result), nil
}

func (c Column) subset(index index.Int) Column {
	data := make([]dataType, len(index))
	for i, ix := range index {
		data[i] = c.data[ix]
	}

	return Column{data: data}
}

func (c Column) Subset(index index.Int) column.Column {
	return c.subset(index)
}

func (c Column) Comparable(reverse, equalNull bool) column.Comparable {
	result := Comparable{data: c.data, ltValue: column.LessThan, gtValue: column.GreaterThan, equalNullValue: column.NotEqual}
	if reverse {
		result.ltValue, result.gtValue = result.gtValue, result.ltValue
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
	var actualFn func([]dataType) dataType
	var ok bool

	switch t := fn.(type) {
	case string:
		actualFn, ok = aggregations[t]
		if !ok {
			return nil, fmt.Errorf("aggregation function %c is not defined for column", fn)
		}
	case func([]dataType) dataType:
		actualFn = t
	default:
		// TODO: Genny is buggy and won't let you use your own errors package.
		//       We use a standard error here for now.
		return nil, fmt.Errorf("invalid aggregation function type: %v", t)
	}

	data := make([]dataType, 0, len(indices))
	var buf []dataType
	for _, ix := range indices {
		subS := c.subsetWithBuf(ix, &buf)
		data = append(data, actualFn(subS.data))
	}

	return Column{data: data}, nil
}

func (c Column) subsetWithBuf(index index.Int, buf *[]dataType) Column {
	if cap(*buf) < len(index) {
		*buf = make([]dataType, 0, len(index))
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

func (c Column) DataType() string {
	var x dataType
	return fmt.Sprintf("%v", reflect.TypeOf(x))
}

type Comparable struct {
	data           []dataType
	ltValue        column.CompareResult
	gtValue        column.CompareResult
	equalNullValue column.CompareResult
}

// View is a view into a column that allows access to individual elements by index.
type View struct {
	data  []dataType
	index index.Int
}

// ItemAt returns the value at position i.
func (v View) ItemAt(i int) dataType {
	return v.data[v.index[i]]
}

// Len returns the column length.
func (v View) Len() int {
	return len(v.index)
}

// Slice returns a slice containing a copy of the column data.
func (v View) Slice() []dataType {
	// TODO: This forces an alloc, as an alternative a slice could be taken
	//       as input that can be (re)used by the client. Are there use cases
	//       where this would actually make sense?
	result := make([]dataType, v.Len())
	for i, j := range v.index {
		result[i] = v.data[j]
	}
	return result
}
