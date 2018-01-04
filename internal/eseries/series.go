package eseries

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/series"
)

type enumVal uint8

const maxCardinality = 255
const nullValue = maxCardinality

func (v enumVal) isNull() bool {
	return v == nullValue
}

type Series struct {
	data   []enumVal
	values []string
}

// Factory is a helper used during construction of the enum series
type Factory struct {
	s         Series
	valToEnum map[string]enumVal
	strict    bool
}

func New(data []*string, values []string) (Series, error) {
	f, err := NewFactory(values, len(data))
	if err != nil {
		return Series{}, err
	}

	for _, d := range data {
		if d != nil {
			if err := f.AppendString(*d); err != nil {
				return Series{}, err
			}
		} else {
			f.AppendNil()
		}
	}

	return f.ToSeries(), nil
}

func NewFactory(values []string, sizeHint int) (*Factory, error) {
	if len(values) > maxCardinality {
		return nil, errors.New("New enum", "too many unique values, max cardinality is %d", maxCardinality)
	}

	if values == nil {
		values = make([]string, 0)
	}

	valToEnum := make(map[string]enumVal, len(values))
	for i, v := range values {
		valToEnum[v] = enumVal(i)
	}

	return &Factory{s: Series{data: make([]enumVal, 0, sizeHint), values: values},
		valToEnum: valToEnum,
		strict:    len(values) > 0}, nil
}

func (f *Factory) AppendNil() {
	f.s.data = append(f.s.data, nullValue)
}

func (f *Factory) AppendByteString(str []byte) error {
	if e, ok := f.valToEnum[string(str)]; ok {
		f.s.data = append(f.s.data, e)
		return nil
	}

	v := string(str)
	return f.appendString(v)
}

func (f *Factory) AppendString(str string) error {
	if e, ok := f.valToEnum[str]; ok {
		f.s.data = append(f.s.data, e)
		return nil
	}

	return f.appendString(str)
}

func (f *Factory) appendString(str string) error {
	if f.strict {
		return errors.New("append enum val", `unknown enum value "%f" using strict enum`, str)
	}

	if len(f.s.values) >= maxCardinality {
		return errors.New("append enum val", `enum max cardinality (%d) exceeded`, maxCardinality)
	}

	f.s.values = append(f.s.values, str)
	ev := enumVal(len(f.s.values) - 1)
	f.s.data = append(f.s.data, ev)
	f.valToEnum[str] = ev
	return nil
}

func (f *Factory) ToSeries() Series {
	// Using the factory after this method has been called and the series exposed
	// is not recommended.
	return f.s
}

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func(Series) enumVal{}

var filterFuncs = map[filter.Comparator]func(index.Int, []enumVal, interface{}, index.Bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
}

func (s Series) Len() int {
	return len(s.data)
}

func (s Series) StringAt(i int, naRep string) string {
	v := s.data[i]
	if v.isNull() {
		return naRep
	}

	return s.values[v]
}

func (s Series) AppendByteStringAt(buf []byte, i int) []byte {
	// TODO: Share with string series
	return buf
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	// TODO
	return nil
}

func (s Series) Equals(index index.Int, other series.Series, otherIndex index.Int) bool {
	otherE, ok := other.(Series)
	if !ok {
		return false
	}

	for ix, x := range index {
		enumVal := s.data[x]
		oEnumVal := otherE.data[otherIndex[ix]]
		if enumVal.isNull() || oEnumVal.isNull() {
			if enumVal == oEnumVal {
				continue
			}

			return false
		}

		if s.values[enumVal] != otherE.values[oEnumVal] {
			return false
		}
	}

	return true
}

func (c Comparable) Compare(i, j uint32) series.CompareResult {
	// TODO
	return series.Equal
}

func (s Series) Filter(index index.Int, c filter.Comparator, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Also make it possible to compare to values in other column
	compFunc, ok := filterFuncs[c]
	if !ok {
		return fmt.Errorf("invalid comparison operator for *string, %v", c)
	}

	return compFunc(index, s.data, comparatee, bIndex)
}

func (s Series) subset(index index.Int) Series {
	data := make([]enumVal, 0, len(index))
	for _, ix := range index {
		data = append(data, s.data[ix])
	}

	return Series{data: data, values: s.values}
}

func (s Series) Subset(index index.Int) series.Series {
	return s.subset(index)
}

func (s Series) stringSlice(index index.Int) []*string {
	result := make([]*string, 0, len(index))
	for _, ix := range index {
		v := s.data[ix]
		if v.isNull() {
			result = append(result, nil)
		} else {
			result = append(result, &s.values[v])
		}
	}
	return result
}

func (s Series) Comparable(reverse bool) series.Comparable {
	if reverse {
		return Comparable{s: s, ltValue: series.GreaterThan, gtValue: series.LessThan}
	}

	return Comparable{s: s, ltValue: series.LessThan, gtValue: series.GreaterThan}
}

func (s Series) String() string {
	strs := make([]string, len(s.data))
	for i, v := range s.data {
		if v.isNull() {
			// For now
			strs[i] = "null"
		} else {
			strs[i] = s.values[v]
		}
	}

	return fmt.Sprintf("%v", strs)
}

func (s Series) Aggregate(indices []index.Int, fnName string) (series.Series, error) {
	fn, ok := aggregations[fnName]
	if !ok {
		return nil, fmt.Errorf("aggregation function %s is not defined for in series", fnName)
	}

	data := make([]enumVal, 0, len(indices))
	for _, ix := range indices {
		subS := s.subset(ix)
		data = append(data, fn(subS))
	}

	return Series{data: data}, nil
}

func (s Series) FillRecords(records []map[string]interface{}, index index.Int, colName string) {
	for i, ix := range index {
		records[i][colName] = s.data[ix]
	}
}

type Comparable struct {
	s       Series
	ltValue series.CompareResult
	gtValue series.CompareResult
}

func gt(index index.Int, column []enumVal, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Handle nil values
	// TODO: Error if not locked type
	comp, ok := comparatee.(enumVal)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] > comp
	}

	return nil
}

func lt(index index.Int, column []enumVal, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Handle nil values
	// TODO: Error if not locked type
	comp, ok := comparatee.(enumVal)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] < comp
	}

	return nil
}
