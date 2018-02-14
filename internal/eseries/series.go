package eseries

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/series"
	"github.com/tobgu/qframe/internal/sseries"
	qfstrings "github.com/tobgu/qframe/internal/strings"
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
		return errors.New("append enum val", `unknown enum value "%s" using strict enum`, str)
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

var filterFuncs = map[filter.Comparator]func(index.Int, []enumVal, enumVal, index.Bool){
	filter.Gt: gt,
	filter.Lt: lt,
}

var multiFilterFuncs = map[filter.Comparator]func(comparatee interface{}, values []string) (*bitset, error){
	"like":  like,
	"ilike": ilike,
}

func (s Series) Len() int {
	return len(s.data)
}

func (s Series) StringAt(i uint32, naRep string) string {
	v := s.data[i]
	if v.isNull() {
		return naRep
	}

	return s.values[v]
}

func (s Series) AppendByteStringAt(buf []byte, i uint32) []byte {
	enum := s.data[i]
	if enum.isNull() {
		return append(buf, "null"...)
	}

	return qfstrings.AppendQuotedString(buf, s.values[enum])
}

type marshaler struct {
	Series
	index index.Int
}

func (m marshaler) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 0, len(m.index))
	buf = append(buf, '[')
	for i, ix := range m.index {
		if i > 0 {
			buf = append(buf, ',')
		}

		enum := m.data[ix]
		if enum.isNull() {
			buf = append(buf, "null"...)
		} else {
			buf = qfstrings.AppendQuotedString(buf, m.values[enum])
		}
	}

	buf = append(buf, ']')
	return buf, nil
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return marshaler{Series: s, index: index}
}

func (s Series) ByteSize() int {
	totalSize := 2 * 2 * 8 // Slice headers
	for _, s := range s.values {
		totalSize += len(s)
	}
	totalSize += len(s.data)
	return totalSize
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
	x, y := c.s.data[i], c.s.data[j]
	if x.isNull() || y.isNull() {
		if !x.isNull() {
			return c.gtValue
		}

		if !y.isNull() {
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

func (s Series) Filter(index index.Int, c filter.Comparator, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Also make it possible to compare to values in other column
	compFunc, ok := filterFuncs[c]
	if ok {
		comp, ok := comparatee.(string)
		if !ok {
			return errors.New("Filter enum", "invalid comparison type, %s, expected string", comp)
		}

		for i, value := range s.values {
			if value == comp {
				compFunc(index, s.data, enumVal(i), bIndex)
				return nil
			}
		}

		return errors.New("Filter enum", "Unknown enum value in filter argument: %s", comp)
	}

	multiFunc, ok := multiFilterFuncs[c]
	if ok {
		bset, err := multiFunc(comparatee, s.values)
		if err != nil {
			return errors.Propagate("Filter enum", err)
		}

		for i, x := range bIndex {
			if !x {
				enum := s.data[index[i]]
				bIndex[i] = bset.isSet(enum)
			}
		}

		return nil
	}

	return errors.New("Filter enum", "unknown comparison operator, %v", c)
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

func (s Series) Aggregate(indices []index.Int, fn interface{}) (series.Series, error) {
	// NB! The result of aggregating over an enum series is a string series
	switch t := fn.(type) {
	case string:
		// There are currently no build in aggregations for enums
		return nil, errors.New("enum aggregate", "aggregation function %s is not defined for enum series", fn)
	case func([]*string) *string:
		data := make([]*string, 0, len(indices))
		for _, ix := range indices {
			data = append(data, t(s.stringSlice(ix)))
		}
		return sseries.New(data), nil
	default:
		return nil, errors.New("enum aggregate", "invalid aggregation function type: %v", t)
	}
}

type Comparable struct {
	s       Series
	ltValue series.CompareResult
	gtValue series.CompareResult
}

func gt(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			if !enum.isNull() {
				bIndex[i] = enum > comparatee
			}
		}
	}
}

func lt(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.isNull() || enum < comparatee
		}
	}
}

func like(comparatee interface{}, values []string) (*bitset, error) {
	return filterLike(comparatee, values, true)
}

func ilike(comparatee interface{}, values []string) (*bitset, error) {
	return filterLike(comparatee, values, false)
}

func filterLike(comparatee interface{}, values []string, caseSensitive bool) (*bitset, error) {
	comp, ok := comparatee.(string)
	if !ok {
		return nil, errors.New("enum like", "invalid comparator type %v", comparatee)
	}

	matcher, err := qfstrings.NewMatcher(comp, caseSensitive)
	if err != nil {
		return nil, errors.Propagate("enum like", err)
	}

	bset := &bitset{}
	for i, v := range values {
		if matcher.Matches(v) {
			bset.set(enumVal(i))
		}
	}

	return bset, nil
}

// Helper type for multi value filtering
type bitset [4]uint64

func (s *bitset) set(val enumVal) {
	s[val>>6] |= 1 << (val & 0x3F)
}

func (s *bitset) isSet(val enumVal) bool {
	return s[val>>6]&(1<<(val&0x3F)) > 0
}

func (s *bitset) String() string {
	return fmt.Sprintf("%X %X %X %X", s[3], s[2], s[1], s[0])
}
