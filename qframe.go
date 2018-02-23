package qframe

import (
	"encoding/csv"
	"fmt"
	"github.com/tobgu/qframe/aggregation"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/bseries"
	"github.com/tobgu/qframe/internal/eseries"
	"github.com/tobgu/qframe/internal/fseries"
	"github.com/tobgu/qframe/internal/index"
	qfio "github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/iseries"
	"github.com/tobgu/qframe/internal/series"
	"github.com/tobgu/qframe/internal/sseries"
	qfstrings "github.com/tobgu/qframe/internal/strings"
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

func (ns namedSeries) ByteSize() int {
	return ns.Series.ByteSize() + 2*8 + 8 + len(ns.name)
}

type QFrame struct {
	series       []namedSeries
	seriesByName map[string]namedSeries
	index        index.Int
	Err          error
}

type Config struct {
	columnOrder []string
	enumColumns map[string][]string
}

type ConfigFunc func(c *Config)

func ColumnOrder(columns ...string) ConfigFunc {
	return func(c *Config) {
		c.columnOrder = make([]string, len(columns))
		copy(c.columnOrder, columns)
	}
}

// If columns should be considered enums. The map key specifies the
// column name, the value if there is a fixed set of values and their
// internal ordering. If value is nil or empty list the values will be
// derived from the column content and the ordering unspecified.
func Enums(columns map[string][]string) ConfigFunc {
	return func(c *Config) {
		c.enumColumns = make(map[string][]string)
		for k, v := range columns {
			c.enumColumns[k] = v
		}
	}
}

func (qf QFrame) withErr(err error) QFrame {
	return QFrame{Err: err, series: qf.series, seriesByName: qf.seriesByName, index: qf.index}
}

func (qf QFrame) withIndex(ix index.Int) QFrame {
	return QFrame{Err: qf.Err, series: qf.series, seriesByName: qf.seriesByName, index: ix}
}

func New(data map[string]interface{}, fns ...ConfigFunc) QFrame {
	config := &Config{}
	for _, fn := range fns {
		fn(config)
	}

	if len(config.columnOrder) == 0 {
		config.columnOrder = make([]string, 0, len(data))
		for name := range data {
			config.columnOrder = append(config.columnOrder, name)
			sort.Strings(config.columnOrder)
		}
	}

	if len(config.columnOrder) != len(data) {
		return QFrame{Err: errors.New("New", "columns and column order length do not match")}
	}

	for _, name := range config.columnOrder {
		if _, ok := data[name]; !ok {
			return QFrame{Err: errors.New("New", `key "%s" does not exist in supplied data`, name)}
		}
	}

	s := make([]namedSeries, len(data))
	sByName := make(map[string]namedSeries, len(data))
	firstLen, currentLen := 0, 0
	for i, name := range config.columnOrder {
		var localS series.Series
		column := data[name]

		// TODO: Change this case to use strings directly for strings and enums
		if sc, ok := column.([]string); ok {
			// Convenience conversion to support string slices in addition
			// to string pointer slices.
			sp := make([]*string, len(sc))
			for i := range sc {
				sp[i] = &sc[i]
			}
			column = sp
		}

		var err error
		switch c := column.(type) {
		case []int:
			localS = iseries.New(c)
			currentLen = len(c)
		case []float64:
			localS = fseries.New(c)
			currentLen = len(c)
		case []*string:
			if values, ok := config.enumColumns[name]; ok {
				localS, err = eseries.New(c, values)
				if err != nil {
					return QFrame{Err: errors.Propagate(fmt.Sprintf("New column %s", name), err)}
				}
				// Book keeping
				delete(config.enumColumns, name)
			} else {
				localS = sseries.New(c)
			}
			currentLen = len(c)
		case []bool:
			localS = bseries.New(c)
			currentLen = len(c)
		case eseries.Series:
			localS = c
			currentLen = c.Len()
		case qfstrings.StringBlob:
			localS = sseries.NewBytes(c.Pointers, c.Data)
			currentLen = len(c.Pointers)
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

	if len(config.enumColumns) > 0 {
		colNames := make([]string, 0)
		for k := range config.enumColumns {
			colNames = append(colNames, k)
		}

		return QFrame{Err: errors.New("New", "unknown enum columns: %v", colNames)}
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

		var err error
		if f.Inverse {
			// This is a small optimization, if the inverse operation is implemented
			// as built in on the series use that directly to avoid building an inverse boolean
			// index further below.
			done := false
			if sComp, ok := f.Comparator.(string); ok {
				if inverse, ok := filter.Inverse[sComp]; ok {
					err = s.Filter(qf.index, inverse, f.Arg, bIndex)

					// Assume inverse not implemented in case of error here
					if err == nil {
						done = true
					}
				}
			}

			if !done {
				// TODO: This branch needs proper testing
				invBIndex := index.NewBool(bIndex.Len())
				err = s.Filter(qf.index, f.Comparator, f.Arg, invBIndex)
				if err == nil {
					for i, x := range bIndex {
						if !x {
							bIndex[i] = !invBIndex[i]
						}
					}
				}
			}
		} else {
			err = s.Filter(qf.index, f.Comparator, f.Arg, bIndex)
		}

		if err != nil {
			return qf.withErr(errors.Propagate("Filter", err))
		}
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

	if len(orders) == 0 {
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
	if err := qf.checkColumns("GroupBy", columns); err != nil {
		return Grouper{Err: err}
	}

	grouper := Grouper{series: qf.series, seriesByName: qf.seriesByName, groupedColumns: columns}
	if qf.Len() == 0 {
		return grouper
	}

	if len(columns) == 0 {
		grouper.indices = []index.Int{qf.index}
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

// fnsAndCols is a list of alternating function names and column names
func (g Grouper) Aggregate(aggs ...aggregation.Aggregation) QFrame {
	if g.Err != nil {
		return QFrame{Err: g.Err}
	}

	// TODO: Check that columns exist but are not part of groupedColumns

	// Loop over all groups and pick the first row in each of the groups.
	// This index will be used to populate the grouped by columns below.
	firstElementIx := make(index.Int, len(g.indices))
	for i, ix := range g.indices {
		firstElementIx[i] = ix[0]
	}

	newSeriesByName := make(map[string]namedSeries, len(g.groupedColumns)+len(aggs))
	newSeries := make([]namedSeries, 0, len(g.groupedColumns)+len(aggs))
	for i, col := range g.groupedColumns {
		s := g.seriesByName[col]
		s.pos = i
		s.Series = s.Subset(firstElementIx)
		newSeriesByName[col] = s
		newSeries = append(newSeries, s)
	}

	var err error
	for _, agg := range aggs {
		s := g.seriesByName[agg.Column]
		s.Series, err = s.Aggregate(g.indices, agg.Fn)
		if err != nil {
			return QFrame{Err: errors.Propagate("Aggregate", err)}
		}

		newSeriesByName[agg.Column] = s
		newSeries = append(newSeries, s)
	}

	return QFrame{series: newSeries, seriesByName: newSeriesByName, index: index.NewAscending(uint32(len(g.indices)))}
}

func fixLengthString(s string, pad string, desiredLen int) string {
	// NB: Assumes desiredLen to be >= 3
	if len(s) > desiredLen {
		return s[:desiredLen-3] + "..."
	}

	padCount := desiredLen - len(s)
	if padCount > 0 {
		return strings.Repeat(pad, padCount) + s
	}

	return s
}

// Simple string representation of the table
func (qf QFrame) String() string {
	// There are a lot of potential improvements to this function at the moment:
	// - Limit output, both columns and rows
	// - Configurable output widths, potentially per columns
	// - Configurable alignment
	if qf.Err != nil {
		return qf.Err.Error()
	}

	result := make([]string, 0, len(qf.index))
	row := make([]string, len(qf.series))
	colWidths := make([]int, len(qf.series))
	minColWidth := 5
	for i, s := range qf.series {
		colWidths[i] = intMax(len(s.name), minColWidth)
		row[i] = fixLengthString(s.name, " ", colWidths[i])
	}
	result = append(result, strings.Join(row, " "))

	for i := range qf.series {
		row[i] = fixLengthString("", "-", colWidths[i])
	}
	result = append(result, strings.Join(row, " "))

	for i := 0; i < qf.Len(); i++ {
		for j, s := range qf.series {
			row[j] = fixLengthString(s.StringAt(qf.index[i], "NaN"), " ", colWidths[j])
		}
		result = append(result, strings.Join(row, " "))
	}

	return strings.Join(result, "\n")
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
		return qf.withErr(errors.New("Slice", "end must not be greater than qframe length"))
	}

	return qf.withIndex(qf.index[start:end])
}

func (qf QFrame) setSeries(name string, s series.Series) QFrame {
	newF := qf.withIndex(qf.index)
	existingS, overwrite := qf.seriesByName[name]
	newColCount := len(qf.series)
	pos := newColCount
	if overwrite {
		pos = existingS.pos
	} else {
		newColCount++
	}

	newF.series = make([]namedSeries, newColCount)
	newF.seriesByName = make(map[string]namedSeries, newColCount)
	copy(newF.series, qf.series)
	for k, v := range qf.seriesByName {
		newF.seriesByName[k] = v
	}

	newS := namedSeries{Series: s, name: name, pos: pos}
	newF.seriesByName[name] = newS
	newF.series[pos] = newS
	return newF
}

func (qf QFrame) Copy(dstCol, srcCol string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedSeries, ok := qf.seriesByName[srcCol]
	if !ok {
		return qf.withErr(errors.New("Apply", "no such column: %s", srcCol))
	}

	return qf.setSeries(dstCol, namedSeries.Series)
}

func (qf QFrame) Apply1(fn interface{}, dstCol, srcCol string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedSeries, ok := qf.seriesByName[srcCol]
	if !ok {
		return qf.withErr(errors.New("Apply1", "no such column: %s", srcCol))
	}

	srcSeries := namedSeries.Series

	sliceResult, err := srcSeries.Apply1(fn, qf.index)
	if err != nil {
		return qf.withErr(errors.Propagate("Apply1", err))
	}

	var resultSeries series.Series
	switch t := sliceResult.(type) {
	case []int:
		resultSeries = iseries.New(t)
	case []float64:
		resultSeries = fseries.New(t)
	case []bool:
		resultSeries = bseries.New(t)
	case []*string:
		resultSeries = sseries.New(t)
	case series.Series:
		resultSeries = t
	default:
		return qf.withErr(errors.New("Apply1", "unexpected type of new series %#v", t))
	}

	return qf.setSeries(dstCol, resultSeries)
}

func (qf QFrame) Apply2(fn interface{}, dstCol, srcCol1, srcCol2 string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedSrcSeries1, ok := qf.seriesByName[srcCol1]
	if !ok {
		return qf.withErr(errors.New("Apply2", "no such column: %s", srcCol1))
	}
	srcSeries1 := namedSrcSeries1.Series

	namedSrcSeries2, ok := qf.seriesByName[srcCol2]
	if !ok {
		return qf.withErr(errors.New("Apply2", "no such column: %s", srcCol2))
	}
	srcSeries2 := namedSrcSeries2.Series

	resultSeries, err := srcSeries1.Apply2(fn, srcSeries2, qf.index)
	if err != nil {
		return qf.withErr(errors.Propagate("Apply2", err))
	}

	return qf.setSeries(dstCol, resultSeries)
}

////////////
//// IO ////
////////////

type CsvConfig qfio.CsvConfig

type CsvConfigFunc func(*CsvConfig)

func EmptyNull(emptyNull bool) CsvConfigFunc {
	return func(c *CsvConfig) {
		c.EmptyNull = emptyNull
	}
}

func Types(typs map[string]string) CsvConfigFunc {
	return func(c *CsvConfig) {
		c.Types = make(map[string]types.DataType, len(typs))
		for k, v := range typs {
			c.Types[k] = types.DataType(v)
		}
	}
}

func EnumValues(values map[string][]string) CsvConfigFunc {
	return func(c *CsvConfig) {
		c.EnumVals = make(map[string][]string)
		for k, v := range values {
			c.EnumVals[k] = v
		}
	}
}

func ReadCsv(reader io.Reader, confFuncs ...CsvConfigFunc) QFrame {
	conf := &CsvConfig{}
	for _, f := range confFuncs {
		f(conf)
	}

	data, columns, err := qfio.ReadCsv(reader, qfio.CsvConfig(*conf))
	if err != nil {
		return QFrame{Err: err}
	}

	return New(data, ColumnOrder(columns...))
}

func ReadJson(reader io.Reader, fns ...ConfigFunc) QFrame {
	data, err := qfio.UnmarshalJson(reader)
	if err != nil {
		return QFrame{Err: err}
	}

	return New(data, fns...)
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
			row = append(row, c.StringAt(qf.index[i], ""))
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
		colByteNames = append(colByteNames, qfstrings.QuotedBytes(name))
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
				jsonBuf = c.AppendByteStringAt(jsonBuf, ix)
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

// Return a best effort guess of the current size occupied by the frame.
// This does not factor for cases where multiple, different, frames reference
// the underlying data.
func (qf QFrame) ByteSize() int {
	totalSize := 0
	for k, v := range qf.seriesByName {
		totalSize += len(k)
		totalSize += 40 // Estimate of map entry overhead
		totalSize += 16 // String header map key

		// Series both in map and slice, hence 2 x, but don't double count the space
		// occupied by the series itself.
		totalSize += 2*v.ByteSize() - v.Series.ByteSize()
	}

	totalSize += qf.index.ByteSize()
	totalSize += 16 // Error interface
	return totalSize
}

// TODO filter
// - Complete basic filtering for all types
// - Implement "in"
// - Bitwise filters for int (and their inverse/not?), or can we handle not in a more general
//   way where no complementary functions are implemented by adding an extra step involving
//   an additional, new, boolean slice that is kept in isolation and inverted before being
//   merged with the current slice? Also consider "(not (or ....))".
// - Change == to = for equality
// - Also allow custom filtering by allowing functions "fn(type) bool" to be passed to filter.
// - Check out https://github.com/glenn-brown/golang-pkg-pcre for regex filtering. Could be performing better
//   than the stdlib version.
// - Filtering by comparing to values in other columns

// TODO:
// - Make it possible to implement custom Series and use as input to QFrame constructor (this could probably
//   be extended to allow custom series to be created from JSON, CSV, etc. as well, this is not in scope at the
//   moment though).
// - Perhaps it would be nicer to output null for float NaNs than NaN. It would also be nice if
//   null could be interpreted as NaN. Should not be impossible using the generated easyjson code
//   as starting point for column based format and by refining type detection for the record based
//   read. That would also allow proper parsing of integers for record format rather than making them
//   floats.
// - Support access by x, y (to support GoNum matrix interface), or support returning a datatype that supports that
//   interface.
// - Handle float NaN in filtering
// - Support to add columns to DF (in addition to project). Should produce a new df, no mutation!
//   To be used with standin columns.
// - Possibility to run operations on two or more columns that result in a new column (addition for example).
// - Benchmarks comparing performance with Pandas
// - Documentation
// - Use https://goreportcard.com
// - More serialization and deserialization tests
// - Perhaps make a special case for distinct with only one column involved that simply calls distinct on
//   a series for that specific column. Should be quite a bit faster than current sort based implementation.
// - Improve error handling further. Make it possible to classify errors. Fix errors conflict in Genny.
// - Apply2 for strings and enums
// - Split series files into different files (aggregations, filters, apply funcs, etc.)
// - Start documenting public functions
// - Switch to using vgo for dependencies?
// - Apply2 enum + string, convert enum to string automatically to allow it or are we fine with explicit casts?
