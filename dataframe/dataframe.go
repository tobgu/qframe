package dataframe

import (
	"encoding/csv"
	"fmt"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/bseries"
	"github.com/tobgu/go-qcache/dataframe/internal/fseries"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
	dfio "github.com/tobgu/go-qcache/dataframe/internal/io"
	"github.com/tobgu/go-qcache/dataframe/internal/iseries"
	"github.com/tobgu/go-qcache/dataframe/internal/series"
	"github.com/tobgu/go-qcache/dataframe/internal/sseries"
	"io"
	"sort"
)

type DataFrame struct {
	series map[string]series.Series
	index  index.Int
	Err    error
}

func New(d map[string]interface{}) DataFrame {
	df := DataFrame{series: make(map[string]series.Series, len(d))}
	firstLen, currentLen := 0, 0
	for name, column := range d {
		switch c := column.(type) {
		case []int:
			df.series[name] = iseries.New(c)
			currentLen = len(c)
		case []float64:
			df.series[name] = fseries.New(c)
			currentLen = len(c)
		case []string:
			df.series[name] = sseries.New(c)
			currentLen = len(c)
		case []bool:
			df.series[name] = bseries.New(c)
			currentLen = len(c)
		default:
			df.Err = fmt.Errorf("unknown column format of: %v", c)
			return df
		}

		if firstLen == 0 {
			firstLen = currentLen
		}

		if firstLen != currentLen {
			df.Err = fmt.Errorf("different lengths on columns not allowed")
			return df
		}
	}

	df.index = index.NewAscending(firstLen)
	return df
}

func (df DataFrame) Filter(filters ...filter.Filter) DataFrame {
	bIndex := index.NewBool(df.index.Len())
	for _, f := range filters {
		// TODO: Check that Column exists
		s := df.series[f.Column]
		s.Filter(df.index, f.Comparator, f.Arg, bIndex)
	}

	return DataFrame{series: df.series, index: df.index.Filter(bIndex)}
}

func (df DataFrame) Equals(other DataFrame) (equal bool, reason string) {
	if len(df.index) != len(other.index) {
		return false, "Different length"
	}

	if len(df.series) != len(other.series) {
		return false, "Different number of columns"
	}

	for col, s := range df.series {
		otherS, ok := other.series[col]
		if !ok {
			return false, fmt.Sprintf("Column %s missing in other datframe", col)
		}

		if !s.Equals(df.index, otherS, other.index) {
			return false, fmt.Sprintf("Content of column %s differs", col)
		}
	}

	return true, ""
}

func (df DataFrame) Len() int {
	return df.index.Len()
}

type Order struct {
	Column  string
	Reverse bool
}

func (df DataFrame) Sort(orders ...Order) DataFrame {
	// Only copy on sort now, may provide in place later
	newDf := DataFrame{series: df.series, index: df.index.Copy()}

	s := make([]series.Comparable, 0, len(orders))
	for _, o := range orders {
		s = append(s, df.series[o.Column].Comparable(o.Reverse))
	}

	sorter := Sorter{index: newDf.index, series: s}
	sortDf(sorter)

	return newDf
}

func (df DataFrame) columnsOrAll(columns []string) []string {
	// TODO: Check that columns exist
	if len(columns) == 0 {
		columns = make([]string, 0, len(df.series))
		for column := range df.series {
			columns = append(columns, column)
		}
	}

	return columns
}

func (df DataFrame) orders(columns []string) []Order {
	orders := make([]Order, len(columns))
	for i, column := range columns {
		orders[i] = Order{Column: column}
	}

	return orders
}

func (df DataFrame) reverseComparables(columns []string, orders []Order) []series.Comparable {
	// Compare the columns in reverse order compared to the sort order
	// since it's likely to produce differences with fewer comparisons.
	comparables := make([]series.Comparable, 0, len(columns))
	for i := len(columns) - 1; i >= 0; i-- {
		comparables = append(comparables, df.series[orders[i].Column].Comparable(false))
	}
	return comparables
}

func (df DataFrame) Distinct(columns ...string) DataFrame {
	if df.Len() == 0 {
		return df
	}

	columns = df.columnsOrAll(columns)
	orders := df.orders(columns)
	comparables := df.reverseComparables(columns, orders)

	// Sort dataframe on the columns that should be distinct. Loop over all rows
	// comparing the specified columns of each row with the previous rows. If there
	// is a difference the new row will be added to the new index.
	sortedDf := df.Sort(orders...)
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

	return DataFrame{series: df.series, index: newIx}
}

func (df DataFrame) Select(columns ...string) DataFrame {
	if len(columns) == 0 {
		return DataFrame{}
	}

	newSeries := make(map[string]series.Series, len(columns))
	newDf := DataFrame{series: newSeries, index: df.index}
	for _, c := range columns {
		s, ok := df.series[c]
		if !ok {
			newDf.Err = fmt.Errorf("column %s does not exist", c)
			return newDf
		}

		newSeries[c] = s
	}

	return newDf
}

type Grouper struct {
	indices        []index.Int
	groupedColumns []string
	series         map[string]series.Series
}

// Leaving out columns will group by all columns in the frame.
func (df DataFrame) GroupBy(columns ...string) Grouper {
	columns = df.columnsOrAll(columns)
	grouper := Grouper{series: df.series, groupedColumns: columns}
	if df.Len() == 0 {
		return grouper
	}

	orders := df.orders(columns)
	comparables := df.reverseComparables(columns, orders)

	// Sort dataframe on the columns that should be grouped. Loop over all rows
	// comparing the specified columns of each row with the first in the current group.
	// If there is a difference create a new group.
	sortedDf := df.Sort(orders...)
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
func (g Grouper) Aggregate(fnsAndCols ...string) DataFrame {
	if len(fnsAndCols)%2 != 0 || len(fnsAndCols) == 0 {
		return DataFrame{Err: fmt.Errorf("aggregation expects even number of arguments, col1, fn1, col2, fn2")}
	}

	// TODO: Check that columns exist but are not part of groupedColumns
	firstElementIx := make(index.Int, len(g.indices))
	for i, ix := range g.indices {
		firstElementIx[i] = ix[0]
	}

	s := make(map[string]series.Series, len(g.groupedColumns))
	for _, col := range g.groupedColumns {
		s[col] = g.series[col].Subset(firstElementIx)
	}

	var err error
	for i := 0; i < len(fnsAndCols); i += 2 {
		fn := fnsAndCols[i]
		col := fnsAndCols[i+1]
		s[col], err = g.series[col].Aggregate(g.indices, fn)
		if err != nil {
			// TODO: Wrap up error
			return DataFrame{Err: err}
		}
	}

	return DataFrame{series: s, index: index.NewAscending(len(g.indices))}
}

func (df DataFrame) String() string {
	// TODO: Fix
	if df.Err != nil {
		return df.Err.Error()
	}

	result := ""
	for name, values := range df.series {
		result += fmt.Sprintf("%s: %v", name, values)
	}

	return result
}

func (df DataFrame) Slice(start, end int) DataFrame {
	if start < 0 {
		return DataFrame{Err: fmt.Errorf("start must be non negative")}
	}

	if start > end {
		return DataFrame{Err: fmt.Errorf("start must not be greater than end")}
	}

	if end > df.Len() {
		return DataFrame{Err: fmt.Errorf("end must not be greater than dataframe length")}
	}

	return DataFrame{series: df.series, index: df.index[start:end]}
}

func FromCsv(reader io.Reader) DataFrame {
	data, err := dfio.FromCsv(reader)
	if err != nil {
		return DataFrame{Err: err}
	}

	return New(data)
}

func FromJson(reader io.Reader) DataFrame {
	data, err := dfio.UnmarshalJson(reader)
	if err != nil {
		return DataFrame{Err: err}
	}

	return New(data)
}

// This is currently fairly slow. Could probably be a lot speedier with
// a custom written CSV writer that handles quoting etc. differently.
func (df DataFrame) ToCsv(writer io.Writer) error {
	// TODO: Column index
	row := make([]string, 0, len(df.series))
	for name := range df.series {
		row = append(row, name)
	}
	sort.Strings(row)

	columns := make([]series.Series, 0, len(df.series))
	for _, name := range row {
		columns = append(columns, df.series[name])
	}

	w := csv.NewWriter(writer)
	err := w.Write(row)
	if err != nil {
		return err
	}

	for i := 0; i < df.Len(); i++ {
		row = row[:0]
		for _, c := range columns {
			row = append(row, c.StringAt(int(df.index[i])))
		}
		w.Write(row)
	}

	w.Flush()
	return nil
}

func (df DataFrame) ToJson(writer io.Writer, orient string) error {
	colByteNames := make([][]byte, 0, len(df.series))
	columns := make([]series.Series, 0, len(df.series))
	for name, column := range df.series {
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

		for i, ix := range df.index {
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

		m := column.Marshaler(df.index)
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

// TODO:
// - Split repo, QCache and QFrame?

// TODO QFrame:
// - Error checks and general improvements to error structures
// - More and better tests, use sub tests for table driven tests!
// - Optional typing when reading CSV
// - Nice table printing in String function
// - Column index to support access by x, y (to support GoNum matrix interface)
// - Implement query language.
// - Implement de Morgan transformations to handle "not".
// - Common filter functions
// - Bitwise filters for int
// - Regex filters for strings
// - More general structure for aggregation functions that allows []int->float []float->int, []bool->bool
// - Handle null values for strings
// - Handle NaN for floats (sorting, filtering, etc.
// - Add support to add columns to DF (in addition to project). Should produce a new df, no mutation!
//   To be used with standin columns.
// - Possibility to run operations on two or more columns that result in a new column (addition for example).
//   Lower priority.
// - Benchmarks comparing performance with NumPy

// TODO QCache:
// - HTTP(S) interface
// - Caching