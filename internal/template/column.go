package template

// Code generated from template/column.go DO NOT EDIT

import (
	"fmt"
	"github.com/tobgu/genny/generic"
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
	var err error
	switch t := fn.(type) {
	case func(dataType) (int, error):
		result := make([]int, len(c.data))
		for _, i := range ix {
			if result[i], err = t(c.data[i]); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(dataType) (float64, error):
		result := make([]float64, len(c.data))
		for _, i := range ix {
			if result[i], err = t(c.data[i]); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(dataType) (bool, error):
		result := make([]bool, len(c.data))
		for _, i := range ix {
			if result[i], err = t(c.data[i]); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(dataType) (*string, error):
		result := make([]*string, len(c.data))
		for _, i := range ix {
			if result[i], err = t(c.data[i]); err != nil {
				return nil, err
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("cannot apply type %#v to column", fn)
	}
}

// Apply double argument function to two columns. Both columns must have the
// same type. The resulting column will have the same type as this column.
func (c Column) Apply2(fn interface{}, s2 column.Column, ix index.Int) (column.Column, error) {
	ss2, ok := s2.(Column)
	if !ok {
		return Column{}, fmt.Errorf("apply2: invalid column type: %#v", s2)
	}

	t, ok := fn.(func(dataType, dataType) (dataType, error))
	if !ok {
		return Column{}, fmt.Errorf("apply2: invalid function type: %#v", fn)
	}

	result := make([]dataType, len(c.data))
	var err error
	for _, i := range ix {
		if result[i], err = t(c.data[i], ss2.data[i]); err != nil {
			return Column{}, err
		}
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

func (c Column) Comparable(reverse bool) column.Comparable {
	if reverse {
		return Comparable{data: c.data, ltValue: column.GreaterThan, gtValue: column.LessThan}
	}

	return Comparable{data: c.data, ltValue: column.LessThan, gtValue: column.GreaterThan}
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
	for _, ix := range indices {
		subS := c.subset(ix)
		data = append(data, actualFn(subS.data))
	}

	return Column{data: data}, nil
}

type Comparable struct {
	data    []dataType
	ltValue column.CompareResult
	gtValue column.CompareResult
}
