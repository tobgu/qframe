package sseries

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/series"
	qfstrings "github.com/tobgu/qframe/internal/strings"
	"reflect"
)

//go:generate easyjson $GOFILE

//easyjson:json
type JsonString []*string

var stringApplyFuncs = map[string]func(index.Int, Series) (interface{}, error){
	"ToUpper": toUpper,
}

// This is an example of how a more efficient built in function
// could be implemented that makes use of the underlying representation
// to make the operation faster than what could be done using the
// generic function based API.
// This function is roughly 3 - 4 times faster than applying the corresponding
// general function (depending on the input size, etc. of course).
func toUpper(ix index.Int, source Series) (interface{}, error) {
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

func (s Series) StringAt(i uint32, naRep string) string {
	if s, isNull := s.stringAt(i); !isNull {
		return s
	}

	return naRep
}

func (s Series) stringSlice(index index.Int) []*string {
	result := make([]*string, len(index))
	for i, ix := range index {
		s, isNull := s.stringAt(ix)
		if isNull {
			result[i] = nil
		} else {
			result[i] = &s
		}
	}

	return result
}

func (s Series) AppendByteStringAt(buf []byte, i uint32) []byte {
	p := s.pointers[i]
	if p.IsNull() {
		return append(buf, "null"...)
	}
	str := qfstrings.UnsafeBytesToString(s.data[p.Offset() : p.Offset()+p.Len()])
	return qfstrings.AppendQuotedString(buf, str)
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	// TODO: This is a very inefficient way of marshalling to JSON
	return JsonString(s.stringSlice(index))
}

func (s Series) ByteSize() int {
	return 8*len(s.pointers) + cap(s.data)
}

func (s Series) Len() int {
	return len(s.pointers)
}

func (s Series) Equals(index index.Int, other series.Series, otherIndex index.Int) bool {
	otherS, ok := other.(Series)
	if !ok {
		return false
	}

	for ix, x := range index {
		s, sNull := s.stringAt(x)
		os, osNull := otherS.stringAt(otherIndex[ix])
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

func (c Comparable) Compare(i, j uint32) series.CompareResult {
	x, xNull := c.series.stringAt(i)
	y, yNull := c.series.stringAt(j)
	if xNull || yNull {
		if !xNull {
			return c.gtValue
		}

		if !yNull {
			return c.ltValue
		}

		// Consider nil == nil, this means that we can group
		// by null values for example (this differs from Pandas)
		return series.Equal
	}

	if x < y {
		return c.ltValue
	}

	if x > y {
		return c.gtValue
	}

	return series.Equal
}

func (s Series) filterBuiltIn(index index.Int, comparator string, comparatee interface{}, bIndex index.Bool) error {
	switch t := comparatee.(type) {
	case string:
		filterFn, ok := filterFuncs[comparator]
		if !ok {
			return errors.New("filter string", "unknown filter operator %v", comparator)
		}
		filterFn(index, s, t, bIndex)
	case []string:
		filterFn, ok := multiInputFilterFuncs[comparator]
		if !ok {
			return errors.New("filter string", "unknown filter operator %v", comparator)
		}

		filterFn(index, s, qfstrings.NewStringSet(t), bIndex)
	case Series:
		filterFn, ok := filterFuncs2[comparator]
		if !ok {
			return errors.New("filter string", "unknown filter operator %v", comparator)
		}
		filterFn(index, s, t, bIndex)
	default:
		return errors.New("filter string", "invalid comparison value type %v", reflect.TypeOf(comparatee))
	}

	return nil
}

func (s Series) filterCustom1(index index.Int, fn func(*string) bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(stringToPtr(s.stringAt(index[i])))
		}
	}
}

func (s Series) filterCustom2(index index.Int, fn func(*string, *string) bool, comparatee interface{}, bIndex index.Bool) error {
	otherS, ok := comparatee.(Series)
	if !ok {
		return errors.New("filter string", "expected comparatee to be string series, was %v", reflect.TypeOf(comparatee))
	}

	for i, x := range bIndex {
		if !x {
			bIndex[i] = fn(stringToPtr(s.stringAt(index[i])), stringToPtr(otherS.stringAt(index[i])))
		}
	}

	return nil
}

func (s Series) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	var err error
	switch t := comparator.(type) {
	case string:
		err = s.filterBuiltIn(index, t, comparatee, bIndex)
	case func(*string) bool:
		s.filterCustom1(index, t, bIndex)
	case func(*string, *string) bool:
		err = s.filterCustom2(index, t, comparatee, bIndex)
	default:
		err = errors.New("filter string", "invalid filter type %v", reflect.TypeOf(comparator))
	}
	return err
}

type Series struct {
	pointers []qfstrings.Pointer
	data     []byte
}

func NewBytes(pointers []qfstrings.Pointer, bytes []byte) Series {
	return Series{pointers: pointers, data: bytes}
}

func NewStrings(strings []string) Series {
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

func New(strings []*string) Series {
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

func NewConst(val *string, count int) Series {
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

func (s Series) stringAt(i uint32) (string, bool) {
	p := s.pointers[i]
	if p.IsNull() {
		return "", true
	}
	return qfstrings.UnsafeBytesToString(s.data[p.Offset() : p.Offset()+p.Len()]), false
}

func (s Series) subset(index index.Int) Series {
	data := make([]byte, 0, len(index))
	pointers := make([]qfstrings.Pointer, len(index))
	offset := 0
	for i, ix := range index {
		p := s.pointers[ix]
		pointers[i] = qfstrings.NewPointer(offset, p.Len(), p.IsNull())
		if !p.IsNull() {
			data = append(data, s.data[p.Offset():p.Offset()+p.Len()]...)
			offset += p.Len()
		}
	}

	return Series{data: data, pointers: pointers}
}

func (s Series) Subset(index index.Int) series.Series {
	return s.subset(index)
}

func (s Series) Comparable(reverse bool) series.Comparable {
	if reverse {
		return Comparable{series: s, ltValue: series.GreaterThan, gtValue: series.LessThan}
	}

	return Comparable{series: s, ltValue: series.LessThan, gtValue: series.GreaterThan}
}

func (s Series) String() string {
	return fmt.Sprintf("%v", s.data)
}

func (s Series) Aggregate(indices []index.Int, fn interface{}) (series.Series, error) {
	switch t := fn.(type) {
	case string:
		// There are currently no build in aggregations for strings
		return nil, errors.New("enum aggregate", "aggregation function %s is not defined for string series", fn)
	case func([]*string) *string:
		data := make([]*string, 0, len(indices))
		for _, ix := range indices {
			data = append(data, t(s.stringSlice(ix)))
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

func (s Series) Apply1(fn interface{}, ix index.Int) (interface{}, error) {
	var err error
	switch t := fn.(type) {
	case func(*string) (int, error):
		result := make([]int, len(s.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(s.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(*string) (float64, error):
		result := make([]float64, len(s.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(s.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(*string) (bool, error):
		result := make([]bool, len(s.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(s.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return result, nil
	case func(*string) (*string, error):
		result := make([]*string, len(s.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(s.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return result, nil
	case string:
		if f, ok := stringApplyFuncs[t]; ok {
			return f(ix, s)
		}
		return nil, errors.New("string.apply1", "unknown built in function %s", t)
	default:
		return nil, errors.New("string.apply1", "cannot apply type %#v to column", fn)
	}
}

func (s Series) Apply2(fn interface{}, s2 series.Series, ix index.Int) (series.Series, error) {
	s2S, ok := s2.(Series)
	if !ok {
		return nil, errors.New("string.apply2", "invalid column type %v", reflect.TypeOf(s2))
	}

	switch t := fn.(type) {
	case func(*string, *string) (*string, error):
		var err error
		result := make([]*string, len(s.pointers))
		for _, i := range ix {
			if result[i], err = t(stringToPtr(s.stringAt(i)), stringToPtr(s2S.stringAt(i))); err != nil {
				return nil, err
			}
		}
		return New(result), nil
	case string:
		// No built in functions for strings at this stage
		return nil, errors.New("string.apply2", "unknown built in function %s", t)
	default:
		return nil, errors.New("string.apply2", "cannot apply type %#v to column", fn)
	}
}

type Comparable struct {
	series  Series
	ltValue series.CompareResult
	gtValue series.CompareResult
}
