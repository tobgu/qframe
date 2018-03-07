package scolumn

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
	qfstrings "github.com/tobgu/qframe/internal/strings"
	"reflect"
)

//go:generate easyjson $GOFILE

//easyjson:json
type JsonString []*string

var stringApplyFuncs = map[string]func(index.Int, Column) (interface{}, error){
	"ToUpper": toUpper,
}

// This is an example of how a more efficient built in function
// could be implemented that makes use of the underlying representation
// to make the operation faster than what could be done using the
// generic function based API.
// This function is roughly 3 - 4 times faster than applying the corresponding
// general function (depending on the input size, etc. of course).
func toUpper(ix index.Int, source Column) (interface{}, error) {
	if len(source.pointers) == 0 {
		return source, nil
	}

	pointers := make([]qfstrings.Pointer, len(source.pointers))
	sizeEstimate := int(float64(len(source.data)) * (float64(len(ix)) / float64(len(source.pointers))))
	data := make([]byte, 0, sizeEstimate)
	strBuf := make([]byte, 1024)
	for _, i := range ix {
		str, isNull := source.stringAt(i)
		pointers[i] = qfstrings.NewPointer(len(data), len(str), isNull)
		data = append(data, qfstrings.ToUpper(&strBuf, str)...)
	}

	return NewBytes(pointers, data), nil
}

func (c Column) StringAt(i uint32, naRep string) string {
	if s, isNull := c.stringAt(i); !isNull {
		return s
	}

	return naRep
}

func (c Column) stringSlice(index index.Int) []*string {
	result := make([]*string, len(index))
	for i, ix := range index {
		s, isNull := c.stringAt(ix)
		if isNull {
			result[i] = nil
		} else {
			result[i] = &s
		}
	}

	return result
}

func (c Column) AppendByteStringAt(buf []byte, i uint32) []byte {
	p := c.pointers[i]
	if p.IsNull() {
		return append(buf, "null"...)
	}
	str := qfstrings.UnsafeBytesToString(c.data[p.Offset() : p.Offset()+p.Len()])
	return qfstrings.AppendQuotedString(buf, str)
}

func (c Column) Marshaler(index index.Int) json.Marshaler {
	// TODO: This is a very inefficient way of marshalling to JSON
	return JsonString(c.stringSlice(index))
}

func (c Column) ByteSize() int {
	return 8*len(c.pointers) + cap(c.data)
}

func (c Column) Len() int {
	return len(c.pointers)
}

func (c Column) Equals(index index.Int, other column.Column, otherIndex index.Int) bool {
	otherC, ok := other.(Column)
	if !ok {
		return false
	}

	for ix, x := range index {
		s, sNull := c.stringAt(x)
		os, osNull := otherC.stringAt(otherIndex[ix])
		if sNull || osNull {
			if sNull && osNull {
				continue
			}

			return false
		}

		if s != os {
			return false
		}
	}

	return true
}

func (c Comparable) Compare(i, j uint32) column.CompareResult {
	x, xNull := c.column.stringAt(i)
	y, yNull := c.column.stringAt(j)
	if xNull || yNull {
		if !xNull {
			return c.gtValue
		}

		if !yNull {
			return c.ltValue
		}

		// Consider nil == nil, this means that we can group
		// by null values for example (this differs from Pandas)
		return column.Equal
	}

	if x < y {
		return c.ltValue
	}

	if x > y {
		return c.gtValue
	}

	return column.Equal
}

func (c Column) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	switch t := comparatee.(type) {
	case string:
		filterFn, ok := filterFuncs[comparator]
		if !ok {
			return errors.New("filter string", "unknown filter operator %v", comparator)
		}
		filterFn(index, c, t, bIndex)
	case []string:
		filterFn, ok := multiInputFilterFuncs[comparator]
		if !ok {
			return errors.New("filter string", "unknown filter operator %v", comparator)
		}

		filterFn(index, c, qfstrings.NewStringSet(t), bIndex)
	case Column:
		filterFn, ok := filterFuncs2[comparator]
		if !ok {
			return errors.New("filter string", "unknown filter operator %v", comparator)
		}
		filterFn(index, c, t, bIndex)
	default:
		return errors.New("filter string", "invalid comparison value type %v", reflect.TypeOf(comparatee))
	}

	return nil
}

func (c Column) filterCustom1(index index.Int, fn func(*string) bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(stringToPtr(c.stringAt(index[i])))
		}
	}
}

func (c Column) filterCustom2(index index.Int, fn func(*string, *string) bool, comparatee interface{}, bIndex index.Bool) error {
	otherC, ok := comparatee.(Column)
	if !ok {
		return errors.New("filter string", "expected comparatee to be string column, was %v", reflect.TypeOf(comparatee))
	}

	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(stringToPtr(c.stringAt(index[i])), stringToPtr(otherC.stringAt(index[i])))
		}
	}

	return nil
}

func (c Column) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	var err error
	switch t := comparator.(type) {
	case string:
		err = c.filterBuiltIn(index, t, comparatee, bIndex)
	case func(*string) bool:
		c.filterCustom1(index, t, bIndex)
	case func(*string, *string) bool:
		err = c.filterCustom2(index, t, comparatee, bIndex)
	default:
		err = errors.New("filter string", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}

type Column struct {
	pointers []qfstrings.Pointer
	data     []byte
}

func NewBytes(pointers []qfstrings.Pointer, bytes []byte) Column {
	return Column{pointers: pointers, data: bytes}
}

func NewStrings(strings []string) Column {
	data := make([]byte, 0, len(strings))
	pointers := make([]qfstrings.Pointer, len(strings))
	offset := 0
	for i, s := range strings {
		pointers[i] = qfstrings.NewPointer(offset, len(s), false)
		offset += len(s)
		data = append(data, s...)
	}

	return NewBytes(pointers, data)
}

func New(strings []*string) Column {
	data := make([]byte, 0, len(strings))
	pointers := make([]qfstrings.Pointer, len(strings))
	offset := 0
	for i, s := range strings {
		if s == nil {
			pointers[i] = qfstrings.NewPointer(offset, 0, true)
		} else {
			sLen := len(*s)
			pointers[i] = qfstrings.NewPointer(offset, sLen, false)
			offset += sLen
			data = append(data, *s...)
		}
	}

	return NewBytes(pointers, data)
}

func NewConst(val *string, count int) Column {
	var data []byte
	pointers := make([]qfstrings.Pointer, count)
	if val == nil {
		data = make([]byte, 0)
		for i := range pointers {
			pointers[i] = qfstrings.NewPointer(0, 0, true)
		}
	} else {
		sLen := len(*val)
		data = make([]byte, 0, count*sLen)
		for i := range pointers {
			pointers[i] = qfstrings.NewPointer(i*sLen, sLen, false)
			data = append(data, *val...)
		}
	}

	return NewBytes(pointers, data)
}

func (c Column) stringAt(i uint32) (string, bool) {
	p := c.pointers[i]
	if p.IsNull() {
		return "", true
	}
	return qfstrings.UnsafeBytesToString(c.data[p.Offset() : p.Offset()+p.Len()]), false
}

func (c Column) stringCopyAt(i uint32) (string, bool) {
	// Similar to stringAt but will allocate a new string and copy the content into it.
	p := c.pointers[i]
	if p.IsNull() {
		return "", true
	}
	return string(c.data[p.Offset() : p.Offset()+p.Len()]), false
}

func (c Column) subset(index index.Int) Column {
	data := make([]byte, 0, len(index))
	pointers := make([]qfstrings.Pointer, len(index))
	offset := 0
	for i, ix := range index {
		p := c.pointers[ix]
		pointers[i] = qfstrings.NewPointer(offset, p.Len(), p.IsNull())
		if !p.IsNull() {
			data = append(data, c.data[p.Offset():p.Offset()+p.Len()]...)
			offset += p.Len()
		}
	}

	return Column{data: data, pointers: pointers}
}

func (c Column) Subset(index index.Int) column.Column {
	return c.subset(index)
}

func (c Column) Comparable(reverse bool) column.Comparable {
	if reverse {
		return Comparable{column: c, ltValue: column.GreaterThan, gtValue: column.LessThan}
	}

	return Comparable{column: c, ltValue: column.LessThan, gtValue: column.GreaterThan}
}

func (c Column) String() string {
	return fmt.Sprintf("%v", c.data)
}

func (c Column) Aggregate(indices []index.Int, fn interface{}) (column.Column, error) {
	switch t := fn.(type) {
	case string:
		// There are currently no build in aggregations for strings
		return nil, errors.New("enum aggregate", "aggregation function %c is not defined for string column", fn)
	case func([]*string) *string:
		data := make([]*string, 0, len(indices))
		for _, ix := range indices {
			data = append(data, t(c.stringSlice(ix)))
		}
		return New(data), nil
	default:
		return nil, errors.New("string aggregate", "invalid aggregation function type: %v", t)
	}
}

func stringToPtr(s string, isNull bool) *string {
	if isNull {
		return nil
	}
	return &s
}

func (c Column) Apply1(fn interface{}, ix index.Int) (interface{}, error) {
	var err error
	switch t := fn.(type) {
	case func(*string) (int, error):
		result := make([]int, len(c.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(c.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(*string) (float64, error):
		result := make([]float64, len(c.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(c.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(*string) (bool, error):
		result := make([]bool, len(c.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(c.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(*string) (*string, error):
		result := make([]*string, len(c.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(c.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return result, nil
	case string:
		if f, ok := stringApplyFuncs[t]; ok {
			return f(ix, c)
		}
		return nil, errors.New("string.apply1", "unknown built in function %c", t)
	default:
		return nil, errors.New("string.apply1", "cannot apply type %#v to column", fn)
	}
}

func (c Column) Apply2(fn interface{}, s2 column.Column, ix index.Int) (column.Column, error) {
	s2S, ok := s2.(Column)
	if !ok {
		return nil, errors.New("string.apply2", "invalid column type %v", reflect.TypeOf(s2))
	}

	switch t := fn.(type) {
	case func(*string, *string) (*string, error):
		var err error
		result := make([]*string, len(c.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(c.stringAt(i)), stringToPtr(s2S.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return New(result), nil
	case string:
		// No built in functions for strings at this stage
		return nil, errors.New("string.apply2", "unknown built in function %c", t)
	default:
		return nil, errors.New("string.apply2", "cannot apply type %#v to column", fn)
	}
}

func (c Column) View(ix index.Int) View {
	return View{column: c, index: ix}
}

type Comparable struct {
	column  Column
	ltValue column.CompareResult
	gtValue column.CompareResult
}
