package qframe

import (
	"encoding/csv"
	"fmt"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/bseries"
	"github.com/tobgu/qframe/internal/eseries"
	"github.com/tobgu/qframe/internal/fseries"
	"github.com/tobgu/qframe/internal/index"
	dfio "github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/iseries"
	"github.com/tobgu/qframe/internal/series"
	"github.com/tobgu/qframe/internal/sseries"
	"github.com/tobgu/qframe/types"
	"io"
	"sort"
	"strings"
)

type namedSeries struct {
	series.Series
	name string
	pos  int
}

type QFrame struct {
	series       []namedSeries
	seriesByName map[string]namedSeries
	index        index.Int
	Err          error
}

type Config struct {
	columnOrder []string
}

type ConfigFunc func(c *Config)

func ColumnOrder(columns ...string) ConfigFunc {
	return func(c *Config) {
		c.columnOrder = make([]string, len(columns))
		copy(c.columnOrder, columns)
	}
}

func (qf QFrame) withErr(err error) QFrame {
	return QFrame{Err: err, series: qf.series, seriesByName: qf.seriesByName, index: qf.index}
}

func (qf QFrame) withIndex(ix index.Int) QFrame {
	return QFrame{Err: qf.Err, series: qf.series, seriesByName: qf.seriesByName, index: ix}
}

func New(data map[string]interface{}, fns ...ConfigFunc) QFrame {
	c := &Config{}
	for _, fn := range fns {
		fn(c)
	}

	if len(c.columnOrder) == 0 {
		c.columnOrder = make([]string, 0, len(data))
		for name := range data {
			c.columnOrder = append(c.columnOrder, name)
			sort.Strings(c.columnOrder)
		}
	}

	if len(c.columnOrder) != len(data) {
		return QFrame{Err: errors.New("New", "columns and column order length do not match")}
	}

	for _, name := range c.columnOrder {
		if _, ok := data[name]; !ok {
			return QFrame{Err: errors.New("New", `key "%s" does not exist in supplied data`, name)}
		}
	}

	s := make([]namedSeries, len(data))
	sByName := make(map[string]namedSeries, len(data))
	firstLen, currentLen := 0, 0
	for i, name := range c.columnOrder {
		var localS series.Series
		column := data[name]
		switch c := column.(type) {
		case []int:
			localS = iseries.New(c)
			currentLen = len(c)
		case []float64:
			localS = fseries.New(c)
			currentLen = len(c)
		case []string:
			// Convenience conversion
			sp := make([]*string, len(c))
			for i := range c {
				sp[i] = &c[i]
			}
			localS = sseries.New(sp)
			currentLen = len(c)
		case []*string:
			localS = sseries.New(c)
			currentLen = len(c)
		case []bool:
			localS = bseries.New(c)
			currentLen = len(c)
		case eseries.Series:
			localS = c
			currentLen = c.Len()
		default:
			return QFrame{Err: errors.New("New", "unknown column format of: %v", c)}
		}

		s[i] = namedSeries{name: name, Series: localS, pos: i}
		sByName[name] = s[i]

		if firstLen == 0 {
			firstLen = currentLen
		}

		if firstLen != currentLen {
			return QFrame{Err: errors.New("New", "different lengths on columns not allowed")}
		}
	}

	return QFrame{series: s, seriesByName: sByName, index: index.NewAscending(uint32(currentLen)), Err: nil}
}

func (qf QFrame) Filter(filters ...filter.Filter) QFrame {
	if qf.Err != nil {
		return qf
	}

	bIndex := index.NewBool(qf.index.Len())
	for _, f := range filters {
		s, ok := qf.seriesByName[f.Column]
		if !ok {
			return qf.withErr(errors.New("Filter", `column does not exist, "%s"`, f.Column))
		}
		s.Filter(qf.index, f.Comparator, f.Arg, bIndex)
	}

	return qf.withIndex(qf.index.Filter(bIndex))
}

func (qf QFrame) Equals(other QFrame) (equal bool, reason string) {
	if len(qf.index) != len(other.index) {
		return false, "Different length"
	}

	if len(qf.series) != len(other.series) {
		return false, "Different number of columns"
	}

	for i, s := range qf.series {
		otherS := other.series[i]
		if s.name != otherS.name {
			return false, fmt.Sprintf("Column name difference at %d, %s != %s", i, s.name, otherS.name)
		}

		if !s.Equals(qf.index, otherS.Series, other.index) {
			return false, fmt.Sprintf("Content of columns %s differ", s.name)
		}
	}

	return true, ""
}

func (qf QFrame) Len() int {
	if qf.Err != nil {
		return -1
	}

	return qf.index.Len()
}

type Order struct {
	Column  string
	Reverse bool
}

func (qf QFrame) Sort(orders ...Order) QFrame {
	if qf.Err != nil {
		return qf
	}

	comparables := make([]series.Comparable, 0, len(orders))
	for _, o := range orders {
		s, ok := qf.seriesByName[o.Column]
		if !ok {
			return qf.withErr(errors.New("Sort", "unknown column: %s", o.Column))
		}

		comparables = append(comparables, s.Comparable(o.Reverse))
	}

	newDf := qf.withIndex(qf.index.Copy())
	sorter := Sorter{index: newDf.index, series: comparables}
	sortDf(sorter)

	return newDf
}

func (qf QFrame) columnNames() []string {
	result := make([]string, len(qf.series))
	for i, s := range qf.series {
		result[i] = s.name
	}

	return result
}

func (qf QFrame) columnsOrAll(columns []string) []string {
	if len(columns) == 0 {
		return qf.columnNames()
	}

	return columns
}

func (qf QFrame) orders(columns []string) []Order {
	orders := make([]Order, len(columns))
	for i, column := range columns {
		orders[i] = Order{Column: column}
	}

	return orders
}

func (qf QFrame) reverseComparables(columns []string, orders []Order) []series.Comparable {
	// Compare the columns in reverse order compared to the sort order
	// since it's likely to produce differences with fewer comparisons.
	comparables := make([]series.Comparable, 0, len(columns))
	for i := len(columns) - 1; i >= 0; i-- {
		comparables = append(comparables, qf.seriesByName[orders[i].Column].Comparable(false))
	}
	return comparables
}

func (qf QFrame) Distinct(columns ...string) QFrame {
	if qf.Err != nil {
		return qf
	}

	if qf.Len() == 0 {
		return qf
	}

	for _, c := range columns {
		if _, ok := qf.seriesByName[c]; !ok {
			return qf.withErr(errors.New("Distinct", `unknown column "%s"`, c))
		}
	}

	columns = qf.columnsOrAll(columns)
	orders := qf.orders(columns)
	comparables := qf.reverseComparables(columns, orders)

	// Sort dataframe on the columns that should be distinct. Loop over all rows
	// comparing the specified columns of each row with the previous rows. If there
	// is a difference the new row will be added to the new index.
	sortedDf := qf.Sort(orders...)
	prevPos, currPos := uint32(0), sortedDf.index[0]
	newIx := make(index.Int, 0)
	newIx = append(newIx, currPos)
	for i := 1; i < sortedDf.Len(); i++ {
		prevPos, currPos = currPos, sortedDf.index[i]
		for _, c := range comparables {
			if c.Compare(prevPos, currPos) != series.Equal {
				newIx = append(newIx, currPos)
				break
			}
		}
	}

	return qf.withIndex(newIx)
}

func (qf QFrame) checkColumns(operation string, columns []string) error {
	for _, c := range columns {
		if _, ok := qf.seriesByName[c]; !ok {
			return errors.New("operation", `unknown column "%s"`, c)
		}
	}

	return nil
}

func (qf QFrame) Select(columns ...string) QFrame {
	if qf.Err != nil {
		return qf
	}

	if err := qf.checkColumns("Select", columns); err != nil {
		return qf.withErr(err)
	}

	if len(columns) == 0 {
		return QFrame{}
	}

	newSeriesByName := make(map[string]namedSeries, len(columns))
	newSeries := make([]namedSeries, len(columns))
	for i, c := range columns {
		s := qf.seriesByName[c]
		s.pos = i
		newSeriesByName[c] = s
		newSeries[i] = s
	}

	return QFrame{series: newSeries, seriesByName: newSeriesByName, index: qf.index}
}

type Grouper struct {
	indices        []index.Int
	groupedColumns []string
	series         []namedSeries
	seriesByName   map[string]namedSeries
	Err            error
}

// Leaving out columns will make one large group over which aggregations can be done
func (qf QFrame) GroupBy(columns ...string) Grouper {
	if err := qf.checkColumns("Select", columns); err != nil {
		return Grouper{Err: err}
	}

	grouper := Grouper{series: qf.series, seriesByName: qf.seriesByName, groupedColumns: columns}
	if qf.Len() == 0 {
		return grouper
	}

	orders := qf.orders(columns)
	comparables := qf.reverseComparables(columns, orders)

	// Sort dataframe on the columns that should be grouped. Loop over all rows
	// comparing the specified columns of each row with the first in the current group.
	// If there is a difference create a new group.
	sortedDf := qf.Sort(orders...)
	groupStart, groupStartPos := 0, sortedDf.index[0]
	indices := make([]index.Int, 0)
	for i := 1; i < sortedDf.Len(); i++ {
		currPos := sortedDf.index[i]
		for _, c := range comparables {
			if c.Compare(groupStartPos, currPos) != series.Equal {
				indices = append(indices, sortedDf.index[groupStart:i])
				groupStart, groupStartPos = i, sortedDf.index[i]
				break
			}
		}
	}

	grouper.indices = append(indices, sortedDf.index[groupStart:])
	return grouper
}

// TODO: Type "Aggregation"?

// fnsAndCols is a list of alternating function names and column names
func (g Grouper) Aggregate(fnsAndCols ...string) QFrame {
	if g.Err != nil {
		return QFrame{Err: g.Err}
	}

	if len(fnsAndCols)%2 != 0 || len(fnsAndCols) == 0 {
		return QFrame{Err: errors.New("Aggregate", "aggregation expects even number of arguments, col1, fn1, col2, fn2")}
	}

	// TODO: Check that columns exist but are not part of groupedColumns
	firstElementIx := make(index.Int, len(g.indices))
	for i, ix := range g.indices {
		firstElementIx[i] = ix[0]
	}

	newSeriesByName := make(map[string]namedSeries, len(g.groupedColumns)+len(fnsAndCols)/2)
	newSeries := make([]namedSeries, 0, len(g.groupedColumns)+len(fnsAndCols)/2)
	for i, col := range g.groupedColumns {
		s := g.seriesByName[col]
		s.pos = i
		s.Series = s.Subset(firstElementIx)
		newSeriesByName[col] = s
		newSeries = append(newSeries, s)
	}

	var err error
	for i := 0; i < len(fnsAndCols); i += 2 {
		fn := fnsAndCols[i]
		col := fnsAndCols[i+1]
		s := g.seriesByName[col]
		s.Series, err = s.Aggregate(g.indices, fn)
		if err != nil {
			return QFrame{Err: errors.Propagate("Aggregate", err)}
		}

		newSeriesByName[col] = s
		newSeries = append(newSeries, s)
	}

	return QFrame{series: newSeries, seriesByName: newSeriesByName, index: index.NewAscending(uint32(len(g.indices)))}
}

func (qf QFrame) String() string {
	// TODO: Fix
	if qf.Err != nil {
		return qf.Err.Error()
	}

	result := ""
	s := make([]string, 0, len(qf.index))
	for name, values := range qf.seriesByName {
		s = s[:0]
		for _, ix := range qf.index {
			s = append(s, values.StringAt(int(ix), "NaN"))
		}

		result += fmt.Sprintf("%s: [%s] ", name, strings.Join(s, ", "))
	}

	return result
}

func (qf QFrame) Slice(start, end int) QFrame {
	if qf.Err != nil {
		return qf
	}

	if start < 0 {
		return qf.withErr(errors.New("Slice", "start must be non negative"))
	}

	if start > end {
		return qf.withErr(errors.New("Slice", "start must not be greater than end"))
	}

	if end > qf.Len() {
		return qf.withErr(errors.New("Slice", "end must not be greater than dataframe length"))
	}

	return qf.withIndex(qf.index[start:end])
}

type LoadConfig struct {
	emptyNull bool
	types     map[string]types.DataType
}

type LoadConfigFunc func(*LoadConfig)

func EmptyNull(emptyNull bool) LoadConfigFunc {
	return func(c *LoadConfig) {
		c.emptyNull = emptyNull
	}
}

func Types(typs map[string]string) LoadConfigFunc {
	return func(c *LoadConfig) {
		c.types = make(map[string]types.DataType, len(typs))
		for k, v := range typs {
			c.types[k] = types.DataType(v)
		}
	}
}

func ReadCsv(reader io.Reader, confFuncs ...LoadConfigFunc) QFrame {
	conf := &LoadConfig{}
	for _, f := range confFuncs {
		f(conf)
	}

	data, columns, err := dfio.ReadCsv(reader, conf.emptyNull, conf.types)
	if err != nil {
		return QFrame{Err: err}
	}

	return New(data, ColumnOrder(columns...))
}

func ReadJson(reader io.Reader) QFrame {
	data, err := dfio.UnmarshalJson(reader)
	if err != nil {
		return QFrame{Err: err}
	}

	return New(data)
}

// This is currently fairly slow. Could probably be a lot speedier with
// a custom written CSV writer that handles quoting etc. differently.
func (qf QFrame) ToCsv(writer io.Writer) error {
	if qf.Err != nil {
		return errors.Propagate("ToCsv", qf.Err)
	}

	row := make([]string, 0, len(qf.series))
	for _, s := range qf.series {
		row = append(row, s.name)
	}

	columns := make([]series.Series, 0, len(qf.series))
	for _, name := range row {
		columns = append(columns, qf.seriesByName[name])
	}

	w := csv.NewWriter(writer)
	err := w.Write(row)
	if err != nil {
		return err
	}

	for i := 0; i < qf.Len(); i++ {
		row = row[:0]
		for _, c := range columns {
			row = append(row, c.StringAt(int(qf.index[i]), ""))
		}
		w.Write(row)
	}

	w.Flush()
	return nil
}

func (qf QFrame) ToJson(writer io.Writer, orient string) error {
	if qf.Err != nil {
		return errors.Propagate("ToJson", qf.Err)
	}

	colByteNames := make([][]byte, 0, len(qf.series))
	columns := make([]series.Series, 0, len(qf.series))
	for name, column := range qf.seriesByName {
		columns = append(columns, column)
		colByteNames = append(colByteNames, dfio.QuotedBytes(name))
	}

	if orient == "records" {
		// Custom JSON generator for records due to performance reasons
		jsonBuf := []byte{'['}
		_, err := writer.Write(jsonBuf)
		if err != nil {
			return err
		}

		for i, ix := range qf.index {
			jsonBuf = jsonBuf[:0]
			if i > 0 {
				jsonBuf = append(jsonBuf, byte(','))
			}

			jsonBuf = append(jsonBuf, byte('{'))

			for j, c := range columns {
				jsonBuf = append(jsonBuf, colByteNames[j]...)
				jsonBuf = append(jsonBuf, byte(':'))
				jsonBuf = c.AppendByteStringAt(jsonBuf, int(ix))
				jsonBuf = append(jsonBuf, byte(','))
			}

			if jsonBuf[len(jsonBuf)-1] == ',' {
				jsonBuf = jsonBuf[:len(jsonBuf)-1]
			}

			jsonBuf = append(jsonBuf, byte('}'))

			_, err = writer.Write(jsonBuf)
			if err != nil {
				return err
			}
		}

		_, err = writer.Write([]byte{']'})
		return err
	}

	// Series/column orientation
	jsonBuf := []byte{'{'}
	_, err := writer.Write(jsonBuf)
	if err != nil {
		return err
	}

	for i, column := range columns {
		jsonBuf = jsonBuf[:0]
		if i > 0 {
			jsonBuf = append(jsonBuf, ',')
		}

		jsonBuf = append(jsonBuf, colByteNames[i]...)
		jsonBuf = append(jsonBuf, ':')
		_, err = writer.Write(jsonBuf)
		if err != nil {
			return err
		}

		m := column.Marshaler(qf.index)
		b, err := m.MarshalJSON()
		if err != nil {
			return err
		}
		_, err = writer.Write(b)
		if err != nil {
			return err
		}
	}

	_, err = writer.Write([]byte{'}'})
	return err
}

// TODO enums:
// - Split series into series and factory
// - Strict mode with defined values (including order)
// - Tests
// - Filtering, sorting, etc
// - Support for other import types than CSV

// TODO:
// - Perhaps it would be nicer to output null for float NaNs than NaN. It would also be nice if
//   null could be interpreted as NaN. Should not be impossible using the generated easyjson code
//   as starting point for column based format and by refining type detection for the record based
//   read. That would also allow proper parsing of integers for record format rather than making them
//   floats.
// - Optional typing when reading CSV
// - Nice table printing in String function
// - Support access by x, y (to support GoNum matrix interface)
// - Implement query language.
// - Implement de Morgan transformations to handle "not".
// - Common filter functions
// - Bitwise filters for int
// - Regex filters for strings
// - More general structure for aggregation functions that allows []int->float []float->int, []bool->bool
// - Handle string nil in filtering
// - Handle float NaN in filtering
// - AppendBytesString support to add columns to DF (in addition to project). Should produce a new df, no mutation!
//   To be used with standin columns.
// - Possibility to run operations on two or more columns that result in a new column (addition for example).
//   Lower priority.
// - Benchmarks comparing performance with Pandas
// - Documentation
// - Use https://goreportcard.com
