package qframe

import (
	"database/sql"
	stdcsv "encoding/csv"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/tobgu/qframe/config/rolling"

	"github.com/tobgu/qframe/config/csv"
	"github.com/tobgu/qframe/config/eval"
	"github.com/tobgu/qframe/config/groupby"
	"github.com/tobgu/qframe/config/newqf"
	qsql "github.com/tobgu/qframe/config/sql"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/bcolumn"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/ecolumn"
	"github.com/tobgu/qframe/internal/fcolumn"
	"github.com/tobgu/qframe/internal/grouper"
	"github.com/tobgu/qframe/internal/icolumn"
	"github.com/tobgu/qframe/internal/index"
	qfio "github.com/tobgu/qframe/internal/io"
	qfsqlio "github.com/tobgu/qframe/internal/io/sql"
	"github.com/tobgu/qframe/internal/math/integer"
	"github.com/tobgu/qframe/internal/scolumn"
	qfsort "github.com/tobgu/qframe/internal/sort"
	qfstrings "github.com/tobgu/qframe/internal/strings"
	"github.com/tobgu/qframe/qerrors"
	"github.com/tobgu/qframe/types"

	// This dependency has been been added just to make sure that "go get" installs it.
	_ "github.com/mauricelam/genny/generic"
)

type namedColumn struct {
	column.Column
	name string
	pos  int
}

func (ns namedColumn) ByteSize() int {
	return ns.Column.ByteSize() + 2*8 + 8 + len(ns.name)
}

// QFrame holds a number of columns together and offers methods for filtering,
// group+aggregate and data manipulation.
type QFrame struct {
	columns       []namedColumn
	columnsByName map[string]namedColumn
	index         index.Int

	// Err indicates that an error has occurred while running an operation.
	// If Err is set it will prevent any further operations from being executed
	// on the QFrame.
	Err error
}

func (qf QFrame) withErr(err error) QFrame {
	return QFrame{Err: err, columns: qf.columns, columnsByName: qf.columnsByName, index: qf.index}
}

func (qf QFrame) withIndex(ix index.Int) QFrame {
	return QFrame{Err: qf.Err, columns: qf.columns, columnsByName: qf.columnsByName, index: ix}
}

// ConstString describes a string column with only one value. It can be used
// during during construction of new QFrames.
type ConstString struct {
	Val   *string
	Count int
}

// ConstInt describes a string column with only one value. It can be used
// during during construction of new QFrames.
type ConstInt struct {
	Val   int
	Count int
}

// ConstFloat describes a string column with only one value. It can be used
// during during construction of new QFrames.
type ConstFloat struct {
	Val   float64
	Count int
}

// ConstBool describes a string column with only one value. It can be used
// during during construction of new QFrames.
type ConstBool struct {
	Val   bool
	Count int
}

func createColumn(name string, data interface{}, config *newqf.Config) (column.Column, error) {
	var localS column.Column

	if sc, ok := data.([]string); ok {
		// Convenience conversion to support string slices in addition
		// to string pointer slices.
		sp := make([]*string, len(sc))
		for i := range sc {
			sp[i] = &sc[i]
		}
		data = sp
	}

	var err error
	switch t := data.(type) {
	case []int:
		localS = icolumn.New(t)
	case ConstInt:
		localS = icolumn.NewConst(t.Val, t.Count)
	case []float64:
		localS = fcolumn.New(t)
	case ConstFloat:
		localS = fcolumn.NewConst(t.Val, t.Count)
	case []*string:
		if values, ok := config.EnumColumns[name]; ok {
			localS, err = ecolumn.New(t, values)
			if err != nil {
				return nil, qerrors.Propagate(fmt.Sprintf("New columns %s", name), err)
			}
			// Book keeping
			delete(config.EnumColumns, name)
		} else {
			localS = scolumn.New(t)
		}
	case ConstString:
		if values, ok := config.EnumColumns[name]; ok {
			localS, err = ecolumn.NewConst(t.Val, t.Count, values)
			if err != nil {
				return nil, qerrors.Propagate(fmt.Sprintf("New columns %s", name), err)
			}
			// Book keeping
			delete(config.EnumColumns, name)
		} else {
			localS = scolumn.NewConst(t.Val, t.Count)
		}

	case []bool:
		localS = bcolumn.New(t)
	case ConstBool:
		localS = bcolumn.NewConst(t.Val, t.Count)
	case ecolumn.Column:
		localS = t
	case qfstrings.StringBlob:
		localS = scolumn.NewBytes(t.Pointers, t.Data)
	case column.Column:
		localS = t
	default:
		return nil, qerrors.New("createColumn", `unknown column data type "%s" for column "%s"`, reflect.TypeOf(t), name)
	}
	return localS, nil
}

// New creates a new QFrame with column content from data.
//
// Time complexity O(m * n) where m = number of columns, n = number of rows.
func New(data map[string]types.DataSlice, fns ...newqf.ConfigFunc) QFrame {
	config := newqf.NewConfig(fns)

	for colName := range data {
		if err := qfstrings.CheckName(colName); err != nil {
			return QFrame{Err: qerrors.Propagate("New", err)}
		}
	}

	if len(config.ColumnOrder) == 0 {
		config.ColumnOrder = make([]string, 0, len(data))
		for name := range data {
			config.ColumnOrder = append(config.ColumnOrder, name)
			sort.Strings(config.ColumnOrder)
		}
	}

	if len(config.ColumnOrder) != len(data) {
		return QFrame{Err: qerrors.New("New", "number of columns and columns order length do not match, %d, %d", len(config.ColumnOrder), len(data))}
	}

	for _, name := range config.ColumnOrder {
		if _, ok := data[name]; !ok {
			return QFrame{Err: qerrors.New("New", `column "%s" in column order does not exist`, name)}
		}
	}

	columns := make([]namedColumn, len(data))
	colByName := make(map[string]namedColumn, len(data))
	firstLen, currentLen := 0, 0
	for i, name := range config.ColumnOrder {
		col := data[name]
		localCol2, err := createColumn(name, col, config)
		if err != nil {
			return QFrame{Err: err}
		}

		columns[i] = namedColumn{name: name, Column: localCol2, pos: i}
		colByName[name] = columns[i]
		currentLen = localCol2.Len()
		if firstLen == 0 {
			firstLen = currentLen
		}

		if firstLen != currentLen {
			return QFrame{Err: qerrors.New("New", "different lengths on columns not allowed")}
		}
	}

	if len(config.EnumColumns) > 0 {
		colNames := make([]string, 0)
		for k := range config.EnumColumns {
			colNames = append(colNames, k)
		}

		return QFrame{Err: qerrors.New("New", "unknown enum columns: %v", colNames)}
	}

	return QFrame{columns: columns, columnsByName: colByName, index: index.NewAscending(uint32(currentLen)), Err: nil}
}

// Contains reports if a columns with colName is present in the frame.
//
// Time complexity is O(1).
func (qf QFrame) Contains(colName string) bool {
	_, ok := qf.columnsByName[colName]
	return ok
}

// Filter filters the frame according to the filters in clause.
//
// Filters are applied via depth first traversal of the provided filter clause from left
// to right. Use the following rules of thumb for best performance when constructing filters:
//
//  1. Cheap filters (eg. integer comparisons, ...) should go to the left of more
//     expensive ones (eg. string regex, ...).
//  2. High impact filters (eg. filters that you expect will drop a lot of data) should go to
//     the left of low impact filters.
//
// Time complexity O(m * n) where m = number of columns to filter by, n = number of rows.
func (qf QFrame) Filter(clause FilterClause) QFrame {
	if qf.Err != nil {
		return qf
	}

	return clause.filter(qf)
}

func unknownCol(c string) string {
	return fmt.Sprintf(`unknown column: "%s"`, c)
}

func (qf QFrame) filter(filters ...filter.Filter) QFrame {
	if qf.Err != nil {
		return qf
	}

	bIndex := index.NewBool(qf.index.Len())
	for _, f := range filters {
		s, ok := qf.columnsByName[f.Column]
		if !ok {
			return qf.withErr(qerrors.New("Filter", unknownCol(f.Column)))
		}

		if name, ok := f.Arg.(types.ColumnName); ok {
			argC, ok := qf.columnsByName[string(name)]
			if !ok {
				return qf.withErr(qerrors.New("Filter", `unknown argument column: "%s"`, name))
			}

			// Allow comparison of int and float columns by temporarily promoting int column to float.
			// This is expensive compared to a comparison between columns of the same type and should be avoided
			// if performance is critical.
			if ic, ok := s.Column.(icolumn.Column); ok {
				if _, ok := argC.Column.(fcolumn.Column); ok {
					s.Column = fcolumn.New(ic.FloatSlice())
				}
			} else if _, ok := s.Column.(fcolumn.Column); ok {
				if ic, ok := argC.Column.(icolumn.Column); ok {
					argC.Column = fcolumn.New(ic.FloatSlice())
				}
			} // else: No conversions for other combinations

			f.Arg = argC.Column
		}

		var err error
		if f.Inverse {
			// This is a small optimization, if the inverse operation is implemented
			// as built in on the columns use that directly to avoid building an inverse boolean
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
			return qf.withErr(qerrors.Propagate(fmt.Sprintf("Filter column '%s'", f.Column), err))
		}
	}

	return qf.withIndex(qf.index.Filter(bIndex))
}

// Equals compares this QFrame to another QFrame.
// If the QFrames are equal (true, "") will be returned else (false, <string describing why>) will be returned.
//
// Time complexity O(m * n) where m = number of columns to group by, n = number of rows.
func (qf QFrame) Equals(other QFrame) (equal bool, reason string) {
	if len(qf.index) != len(other.index) {
		return false, "Different length"
	}

	if len(qf.columns) != len(other.columns) {
		return false, "Different number of columns"
	}

	for i, s := range qf.columns {
		otherCol := other.columns[i]
		if s.name != otherCol.name {
			return false, fmt.Sprintf("Column name difference at %d, %s != %s", i, s.name, otherCol.name)
		}

		if !s.Equals(qf.index, otherCol.Column, other.index) {
			return false, fmt.Sprintf("Content of columns %s differ", s.name)
		}
	}

	return true, ""
}

// Len returns the number of rows in the QFrame.
//
// Time complexity O(1).
func (qf QFrame) Len() int {
	if qf.Err != nil {
		return -1
	}

	return qf.index.Len()
}

// Order is used to specify how sorting should be performed.
type Order struct {
	// Column is the name of the column to sort by.
	Column string

	// Reverse specifies if sorting should be performed ascending (false, default) or descending (true)
	Reverse bool

	// NullLast specifies if null values should go last (true) or first (false, default) for columns that support null.
	NullLast bool
}

// Sort returns a new QFrame sorted according to the orders specified.
//
// Time complexity O(m * n * log(n)) where m = number of columns to sort by, n = number of rows in QFrame.
func (qf QFrame) Sort(orders ...Order) QFrame {
	if qf.Err != nil {
		return qf
	}

	if len(orders) == 0 {
		return qf
	}

	comparables := make([]column.Comparable, 0, len(orders))
	for _, o := range orders {
		s, ok := qf.columnsByName[o.Column]
		if !ok {
			return qf.withErr(qerrors.New("Sort", unknownCol(o.Column)))
		}

		comparables = append(comparables, s.Comparable(o.Reverse, false, o.NullLast))
	}

	newDf := qf.withIndex(qf.index.Copy())
	sorter := qfsort.New(newDf.index, comparables)
	sorter.Sort()
	return newDf
}

// ColumnNames returns the names of all columns in the QFrame.
//
// Time complexity O(n) where n = number of columns.
func (qf QFrame) ColumnNames() []string {
	result := make([]string, len(qf.columns))
	for i, s := range qf.columns {
		result[i] = s.name
	}

	return result
}

// ColumnTypes returns all underlying column types.DataType
//
// Time complexity O(n) where n = number of columns.
func (qf QFrame) ColumnTypes() []types.DataType {
	types := make([]types.DataType, len(qf.columns))
	for i, col := range qf.columns {
		types[i] = col.DataType()
	}
	return types
}

// ColumnTypeMap returns a map of each underlying column with
// the column name as a key and it's types.DataType as a value.
//
// Time complexity O(n) where n = number of columns.
func (qf QFrame) ColumnTypeMap() map[string]types.DataType {
	types := map[string]types.DataType{}
	for name, col := range qf.columnsByName {
		types[name] = col.DataType()
	}
	return types
}

func (qf QFrame) columnsOrAll(columns []string) []string {
	if len(columns) == 0 {
		return qf.ColumnNames()
	}

	return columns
}

func (qf QFrame) orders(columns []string) []Order {
	orders := make([]Order, len(columns))
	for i, col := range columns {
		orders[i] = Order{Column: col}
	}

	return orders
}

func (qf QFrame) comparables(columns []string, orders []Order, groupByNull bool) []column.Comparable {
	result := make([]column.Comparable, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		result = append(result, qf.columnsByName[orders[i].Column].Comparable(false, groupByNull, false))
	}

	return result
}

// Distinct returns a new QFrame that only contains unique rows with respect to the specified columns.
// If no columns are given Distinct will return rows where allow columns are unique.
//
// The order of the returned rows in undefined.
//
// Time complexity O(m * n) where m = number of columns to compare for distinctness, n = number of rows.
func (qf QFrame) Distinct(configFns ...groupby.ConfigFunc) QFrame {
	if qf.Err != nil {
		return qf
	}

	if qf.Len() == 0 {
		return qf
	}

	config := groupby.NewConfig(configFns)

	for _, col := range config.Columns {
		if _, ok := qf.columnsByName[col]; !ok {
			return qf.withErr(qerrors.New("Distinct", unknownCol(col)))
		}
	}

	columns := qf.columnsOrAll(config.Columns)
	orders := qf.orders(columns)
	comparables := qf.comparables(columns, orders, config.GroupByNull)
	newIx := grouper.Distinct(qf.index, comparables)
	return qf.withIndex(newIx)
}

func (qf QFrame) checkColumns(operation string, columns []string) error {
	for _, col := range columns {
		if _, ok := qf.columnsByName[col]; !ok {
			return qerrors.New(operation, unknownCol(col))
		}
	}

	return nil
}

// Drop creates a new projection of te QFrame without the specified columns.
//
// Time complexity O(1).
func (qf QFrame) Drop(columns ...string) QFrame {
	if qf.Err != nil || len(columns) == 0 {
		return qf
	}

	sSet := qfstrings.NewStringSet(columns)
	selectColumns := make([]string, 0)
	for _, c := range qf.columns {
		if !sSet.Contains(c.name) {
			selectColumns = append(selectColumns, c.name)
		}
	}

	return qf.Select(selectColumns...)
}

// Select creates a new projection of the QFrame containing only the specified columns.
//
// Time complexity O(1).
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

	newColumnsByName := make(map[string]namedColumn, len(columns))
	newColumns := make([]namedColumn, len(columns))
	for i, col := range columns {
		s := qf.columnsByName[col]
		s.pos = i
		newColumnsByName[col] = s
		newColumns[i] = s
	}

	return QFrame{columns: newColumns, columnsByName: newColumnsByName, index: qf.index}
}

// GroupBy groups rows together for which the values of specified columns are the same.
// Aggregations on the groups can be executed on the returned Grouper object.
// Leaving out columns to group by will make one large group over which aggregations can be done.
//
// The order of the rows in the Grouper is undefined.
//
// Time complexity O(m * n) where m = number of columns to group by, n = number of rows.
func (qf QFrame) GroupBy(configFns ...groupby.ConfigFunc) Grouper {
	if qf.Err != nil {
		return Grouper{Err: qf.Err}
	}

	config := groupby.NewConfig(configFns)

	if err := qf.checkColumns("Columns", config.Columns); err != nil {
		return Grouper{Err: err}
	}

	g := Grouper{columns: qf.columns, columnsByName: qf.columnsByName, groupedColumns: config.Columns}
	if qf.Len() == 0 {
		return g
	}

	if len(config.Columns) == 0 {
		g.indices = []index.Int{qf.index}
		return g
	}

	orders := qf.orders(config.Columns)
	comparables := qf.comparables(config.Columns, orders, config.GroupByNull)
	indices, stats := grouper.GroupBy(qf.index, comparables)
	g.indices = indices
	g.Stats = GroupStats(stats)
	return g
}

func (qf QFrame) Rolling(fn types.SliceFuncOrBuiltInId, dstCol, srcCol string, configFns ...rolling.ConfigFunc) QFrame {
	if qf.Err != nil {
		return qf
	}

	conf, err := rolling.NewConfig(configFns)
	if err != nil {
		return qf.withErr(err)
	}

	namedColumn, ok := qf.columnsByName[srcCol]
	if !ok {
		return qf.withErr(qerrors.New("Rolling", unknownCol(srcCol)))
	}

	srcColumn := namedColumn.Column
	resultColumn, err := srcColumn.Rolling(fn, qf.index, conf)
	if err != nil {
		return qf.withErr(qerrors.Propagate("Rolling", err))
	}

	return qf.setColumn(dstCol, resultColumn)
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

// String returns a simple string representation of the table.
// Column type is indicated in parenthesis following the column name. The initial
// letter in the type name is used for this.
// Output is currently capped to 50 rows. Use Slice followed by String if you want
// to print rows that are not among the first 50.
func (qf QFrame) String() string {
	// There are a lot of potential improvements to this function at the moment:
	// - Limit output, both columns and rows
	// - Configurable output widths, potentially per columns
	// - Configurable alignment
	if qf.Err != nil {
		return qf.Err.Error()
	}

	result := make([]string, 0, len(qf.index))
	row := make([]string, len(qf.columns))
	colWidths := make([]int, len(qf.columns))
	minColWidth := 5
	for i, s := range qf.columns {
		colHeader := s.name + "(" + string(s.DataType())[:1] + ")"
		colWidths[i] = integer.Max(len(colHeader), minColWidth)
		row[i] = fixLengthString(colHeader, " ", colWidths[i])
	}
	result = append(result, strings.Join(row, " "))

	for i := range qf.columns {
		row[i] = fixLengthString("", "-", colWidths[i])
	}
	result = append(result, strings.Join(row, " "))

	maxRowCount := 50
	for i := 0; i < integer.Min(qf.Len(), maxRowCount); i++ {
		for j, s := range qf.columns {
			row[j] = fixLengthString(s.StringAt(qf.index[i], "null"), " ", colWidths[j])
		}
		result = append(result, strings.Join(row, " "))
	}

	if qf.Len() > maxRowCount {
		result = append(result, "... printout truncated ...")
	}

	result = append(result, fmt.Sprintf("\nDims = %d x %d", len(qf.columns), qf.Len()))

	return strings.Join(result, "\n")
}

// Slice returns a new QFrame consisting of rows [start, end[.
// Note that the underlying storage is kept. Slicing a frame will not release memory used to store the columns.
//
// Time complexity O(1).
func (qf QFrame) Slice(start, end int) QFrame {
	if qf.Err != nil {
		return qf
	}

	if start < 0 {
		return qf.withErr(qerrors.New("Slice", "start must be non negative"))
	}

	if start > end {
		return qf.withErr(qerrors.New("Slice", "start must not be greater than end"))
	}

	if end > qf.Len() {
		return qf.withErr(qerrors.New("Slice", "end must not be greater than qframe length"))
	}

	return qf.withIndex(qf.index[start:end])
}

func (qf QFrame) setColumn(name string, c column.Column) QFrame {
	if err := qfstrings.CheckName(name); err != nil {
		return qf.withErr(qerrors.Propagate("setColumn", err))
	}

	newF := qf.withIndex(qf.index)
	existingCol, overwrite := qf.columnsByName[name]
	newColCount := len(qf.columns)
	pos := newColCount
	if overwrite {
		pos = existingCol.pos
	} else {
		newColCount++
	}

	newF.columns = make([]namedColumn, newColCount)
	newF.columnsByName = make(map[string]namedColumn, newColCount)
	copy(newF.columns, qf.columns)
	for k, v := range qf.columnsByName {
		newF.columnsByName[k] = v
	}

	newS := namedColumn{Column: c, name: name, pos: pos}
	newF.columnsByName[name] = newS
	newF.columns[pos] = newS
	return newF
}

// Copy copies the content of dstCol into srcCol.
//
// dstCol - Name of the column to copy to.
// srcCol - Name of the column to copy from.
//
// Time complexity O(1). Under the hood no actual copy takes place. The columns
// will share the underlying data. Since the frame is immutable this is safe.
func (qf QFrame) Copy(dstCol, srcCol string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedColumn, ok := qf.columnsByName[srcCol]
	if !ok {
		return qf.withErr(qerrors.New("Copy", unknownCol(srcCol)))
	}

	if dstCol == srcCol {
		// NOP
		return qf
	}

	return qf.setColumn(dstCol, namedColumn.Column)
}

// apply0 is a helper function for zero argument applies.
func (qf QFrame) apply0(fn types.DataFuncOrBuiltInId, dstCol string) QFrame {
	if qf.Err != nil {
		return qf
	}

	colLen := 0
	if len(qf.columns) > 0 {
		colLen = qf.columns[0].Len()
	}

	var data interface{}
	switch t := fn.(type) {
	case func() int:
		lData := make([]int, colLen)
		for _, i := range qf.index {
			lData[i] = t()
		}
		data = lData
	case int:
		data = ConstInt{Val: t, Count: colLen}
	case func() float64:
		lData := make([]float64, colLen)
		for _, i := range qf.index {
			lData[i] = t()
		}
		data = lData
	case float64:
		data = ConstFloat{Val: t, Count: colLen}
	case func() bool:
		lData := make([]bool, colLen)
		for _, i := range qf.index {
			lData[i] = t()
		}
		data = lData
	case bool:
		data = ConstBool{Val: t, Count: colLen}
	case func() *string:
		lData := make([]*string, colLen)
		for _, i := range qf.index {
			lData[i] = t()
		}
		data = lData
	case *string:
		data = ConstString{Val: t, Count: colLen}
	case string:
		data = ConstString{Val: &t, Count: colLen}
	case types.ColumnName:
		return qf.Copy(dstCol, string(t))
	default:
		return qf.withErr(qerrors.New("apply0", "unknown apply type: %v", reflect.TypeOf(fn)))
	}

	c, err := createColumn(dstCol, data, newqf.NewConfig(nil))
	if err != nil {
		return qf.withErr(err)
	}

	return qf.setColumn(dstCol, c)
}

// apply1 is a helper function for single argument applies.
func (qf QFrame) apply1(fn types.DataFuncOrBuiltInId, dstCol, srcCol string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedColumn, ok := qf.columnsByName[srcCol]
	if !ok {
		return qf.withErr(qerrors.New("apply1", unknownCol(srcCol)))
	}

	srcColumn := namedColumn.Column

	sliceResult, err := srcColumn.Apply1(fn, qf.index)
	if err != nil {
		return qf.withErr(qerrors.Propagate("apply1", err))
	}

	var resultColumn column.Column
	switch t := sliceResult.(type) {
	case []int:
		resultColumn = icolumn.New(t)
	case []float64:
		resultColumn = fcolumn.New(t)
	case []bool:
		resultColumn = bcolumn.New(t)
	case []*string:
		resultColumn = scolumn.New(t)
	case column.Column:
		resultColumn = t
	default:
		return qf.withErr(qerrors.New("apply1", "unexpected type of new columns %#v", t))
	}

	return qf.setColumn(dstCol, resultColumn)
}

// apply2 is a helper function for zero argument applies.
func (qf QFrame) apply2(fn types.DataFuncOrBuiltInId, dstCol, srcCol1, srcCol2 string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedSrcColumn1, ok := qf.columnsByName[srcCol1]
	if !ok {
		return qf.withErr(qerrors.New("apply2", unknownCol(srcCol1)))
	}
	srcColumn1 := namedSrcColumn1.Column

	namedSrcColumn2, ok := qf.columnsByName[srcCol2]
	if !ok {
		return qf.withErr(qerrors.New("apply2", unknownCol(srcCol2)))
	}
	srcColumn2 := namedSrcColumn2.Column

	resultColumn, err := srcColumn1.Apply2(fn, srcColumn2, qf.index)
	if err != nil {
		return qf.withErr(qerrors.Propagate("apply2", err))
	}

	return qf.setColumn(dstCol, resultColumn)
}

// Instruction describes an operation that will be applied to a row in the QFrame.
type Instruction struct {
	// Fn is the function to apply.
	//
	// IMPORTANT: For pointer and reference types you must not assume that the data passed argument
	// to this function is valid after the function returns. If you plan to keep it around you need
	// to take a copy of the data.
	Fn types.DataFuncOrBuiltInId

	// DstCol is the name of the column that the result of applying Fn should be stored in.
	DstCol string

	// SrcCol1 is the first column to take arguments to Fn from.
	// This field is optional and must only be set if Fn takes one or more arguments.
	SrcCol1 string

	// SrcCol2 is the second column to take arguments to Fn from.
	// This field is optional and must only be set if Fn takes two arguments.
	SrcCol2 string
}

// Apply applies instructions to each row in the QFrame.
//
// Time complexity O(m * n), where m = number of instructions, n = number of rows.
func (qf QFrame) Apply(instructions ...Instruction) QFrame {
	result := qf
	for _, a := range instructions {
		if a.SrcCol1 == "" {
			result = result.apply0(a.Fn, a.DstCol)
		} else if a.SrcCol2 == "" {
			result = result.apply1(a.Fn, a.DstCol, a.SrcCol1)
		} else {
			result = result.apply2(a.Fn, a.DstCol, a.SrcCol1, a.SrcCol2)
		}
	}

	return result
}

// WithRowNums returns a new QFrame with a new column added which
// contains the row numbers. Row numbers start at 0.
//
// Time complexity O(n), where n = number of rows.
func (qf QFrame) WithRowNums(colName string) QFrame {
	i := -1
	return qf.Apply(Instruction{
		DstCol: colName,
		Fn: func() int {
			i++
			return i
		},
	})
}

// FilteredApply works like Apply but allows adding a filter which limits the
// rows to which the instructions are applied to. Any rows not matching the filter
// will be assigned the zero value of the column type.
//
// Time complexity O(m * n), where m = number of instructions, n = number of rows.
func (qf QFrame) FilteredApply(clause FilterClause, instructions ...Instruction) QFrame {
	filteredQf := qf.Filter(clause)
	if filteredQf.Err != nil {
		return filteredQf
	}

	// Use the filtered index when applying instructions then restore it to the original index.
	newQf := qf
	newQf.index = filteredQf.index
	newQf = newQf.Apply(instructions...)
	newQf.index = qf.index
	return newQf
}

// Eval evaluates an expression assigning the result to dstCol.
//
// Eval can be considered an abstraction over Apply. For example it handles management
// of intermediate/temporary columns that are needed as part of evaluating more complex
// expressions.
//
// Time complexity O(m*n) where m = number of clauses in the expression, n = number of rows.
func (qf QFrame) Eval(dstCol string, expr Expression, ff ...eval.ConfigFunc) QFrame {
	if qf.Err != nil {
		return qf
	}

	conf := eval.NewConfig(ff)
	result, col := expr.execute(qf, conf.Ctx)
	colName := string(col)

	// colName is often just a temporary name of a column created as a result of
	// executing the expression. We want to rename this column to the requested
	// destination columns name. Remove colName from the result if not present in
	// the original frame to avoid polluting the frame with intermediate results.
	result = result.Copy(dstCol, colName)
	if !qf.Contains(colName) {
		result = result.Drop(colName)
	}

	return result
}

func (qf QFrame) functionType(name string) (types.FunctionType, error) {
	namedColumn, ok := qf.columnsByName[name]
	if !ok {
		return types.FunctionTypeUndefined, qerrors.New("functionType", unknownCol(name))
	}

	return namedColumn.FunctionType(), nil
}

// Append appends all supplied QFrames, in order, to the current one and returns
// a new QFrame with the result.
// Column count, names and types must be the same for all involved QFrames.
//
// NB! This functionality is very much work in progress and should not be used yet.
//
//	A lot of the implementation is still missing and what is currently there will be rewritten.
//
// Time complexity: ???
func (qf QFrame) Append(qff ...QFrame) QFrame {
	// TODO: Check error status on all involved QFrames
	// TODO: Check that all columns have the same length? This should always be true.
	result := qf
	appendCols := make([]column.Column, 0, len(qff))
	for _, col := range qf.columns {
		for _, otherQf := range qff {
			// TODO: Verify that column exists
			appendCols = append(appendCols, otherQf.columnsByName[col.name].Column)
		}

		newCol, err := col.Append(appendCols...)
		if err != nil {
			return result.withErr(err)
		}

		// TODO: Could potentially be optimized with a "setColumns" function that sets all colums provided
		//       to avoid excessive allocations per column.
		result = result.setColumn(col.name, newCol)
	}

	// Construct new index
	newIxLen := qf.index.Len()
	for _, otherQf := range qff {
		newIxLen += otherQf.Len()
	}

	newIx := make(index.Int, newIxLen)
	start := copy(newIx, qf.index)
	rowOffset := uint32(qf.columns[0].Len())
	for _, otherQf := range qff {
		for i := 0; i < otherQf.Len(); i++ {
			newIx[start+i] = otherQf.index[i] + rowOffset
		}
		start += otherQf.Len()
		rowOffset += uint32(otherQf.columns[0].Len())
	}

	return result.withIndex(newIx)
}

////////////
//// IO ////
////////////

// ReadCSV returns a QFrame with data, in CSV format, taken from reader.
// Column data types are auto detected if not explicitly specified.
//
// Time complexity O(m * n) where m = number of columns, n = number of rows.
func ReadCSV(reader io.Reader, confFuncs ...csv.ConfigFunc) QFrame {
	conf := csv.NewConfig(confFuncs)
	data, columns, err := qfio.ReadCSV(reader, qfio.CSVConfig(conf))
	if err != nil {
		return QFrame{Err: err}
	}

	return New(data, newqf.ColumnOrder(columns...))
}

// ReadJSON returns a QFrame with data, in JSON format, taken from reader.
//
// Time complexity O(m * n) where m = number of columns, n = number of rows.
func ReadJSON(reader io.Reader, confFuncs ...newqf.ConfigFunc) QFrame {
	data, err := qfio.UnmarshalJSON(reader)
	if err != nil {
		return QFrame{Err: err}
	}

	return New(data, confFuncs...)
}

// ReadSQL returns a QFrame by reading the results of a SQL query.
func ReadSQL(tx *sql.Tx, confFuncs ...qsql.ConfigFunc) QFrame {
	return ReadSQLWithArgs(tx, []interface{}{}, confFuncs...)
}

// ReadSQLWithArgs returns a QFrame by reading the results of a SQL query with arguments
func ReadSQLWithArgs(tx *sql.Tx, queryArgs []interface{}, confFuncs ...qsql.ConfigFunc) QFrame {
	conf := qsql.NewConfig(confFuncs)
	// The MySQL can only use prepared
	// statements to return "native" types, otherwise
	// everything is returned as text.
	// see https://github.com/go-sql-driver/mysql/issues/407
	stmt, err := tx.Prepare(conf.Query)
	if err != nil {
		return QFrame{Err: err}
	}
	defer stmt.Close()
	rows, err := stmt.Query(queryArgs...)
	if err != nil {
		return QFrame{Err: err}
	}
	data, columns, err := qfsqlio.ReadSQL(rows, qfsqlio.SQLConfig(conf))
	if err != nil {
		return QFrame{Err: err}
	}
	return New(data, newqf.ColumnOrder(columns...))
}

// ToCSV writes the data in the QFrame, in CSV format, to writer.
//
// Time complexity O(m * n) where m = number of rows, n = number of columns.
//
// This is function is currently unoptimized. It could probably be a lot speedier with
// a custom written CSV writer that handles quoting etc. differently.
func (qf QFrame) ToCSV(writer io.Writer, confFuncs ...csv.ToConfigFunc) error {
	conf := csv.NewToConfig(confFuncs)
	if qf.Err != nil {
		return qerrors.Propagate("ToCSV", qf.Err)
	}

	var iterCols []namedColumn
	if conf.Columns != nil {
		if len(conf.Columns) != len(qf.columns) {
			return qerrors.New("ToCSV", fmt.Sprintf("wrong number of columns: expected: %d", len(qf.columns)))
		}
		iterCols = make([]namedColumn, 0, len(qf.columns))
		for i := 0; i < len(conf.Columns); i++ {
			cname := conf.Columns[i]
			if col, ok := qf.columnsByName[cname]; !ok {
				return qerrors.New("ToCSV", fmt.Sprintf("%s: column does not exist in QFrame", cname))
			} else {
				iterCols = append(iterCols, col)
			}
		}
	} else {
		iterCols = qf.columns
	}

	row := make([]string, 0, len(iterCols))
	for _, s := range iterCols {
		row = append(row, s.name)
	}
	columns := make([]column.Column, 0, len(qf.columns))
	for _, name := range row {
		columns = append(columns, qf.columnsByName[name])
	}

	w := stdcsv.NewWriter(writer)

	if conf.Header {
		err := w.Write(row)
		if err != nil {
			return err
		}
	}

	for i := 0; i < qf.Len(); i++ {
		row = row[:0]
		for _, col := range columns {
			row = append(row, col.StringAt(qf.index[i], ""))
		}
		err := w.Write(row)
		if err != nil {
			return err
		}
	}

	w.Flush()
	return nil
}

// ToJSON writes the data in the QFrame, in JSON format one record per row, to writer.
//
// Time complexity O(m * n) where m = number of rows, n = number of columns.
func (qf QFrame) ToJSON(writer io.Writer) error {
	if qf.Err != nil {
		return qerrors.Propagate("ToJSON", qf.Err)
	}

	colByteNames := make([][]byte, len(qf.columns))
	for i, col := range qf.columns {
		colByteNames[i] = qfstrings.QuotedBytes(col.name)
	}

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

		for j, col := range qf.columns {
			jsonBuf = append(jsonBuf, colByteNames[j]...)
			jsonBuf = append(jsonBuf, byte(':'))
			jsonBuf = col.AppendByteStringAt(jsonBuf, ix)
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

// ToSQL writes a QFrame into a SQL database.
func (qf QFrame) ToSQL(tx *sql.Tx, confFuncs ...qsql.ConfigFunc) error {
	if qf.Err != nil {
		return qerrors.Propagate("ToSQL", qf.Err)
	}
	builders := make([]qfsqlio.ArgBuilder, len(qf.columns))
	var err error
	for i, column := range qf.columns {
		builders[i], err = qfsqlio.NewArgBuilder(column.Column)
		if err != nil {
			return qerrors.New("ToSQL", err.Error())
		}
	}
	for i := range qf.index {
		args := make([]interface{}, len(qf.columns))
		for j, b := range builders {
			args[j] = b(qf.index, i)
		}
		_, err = tx.Exec(qfsqlio.Insert(qf.ColumnNames(), qfsqlio.SQLConfig(qsql.NewConfig(confFuncs))), args...)
		if err != nil {
			return qerrors.New("ToSQL", err.Error())
		}
	}
	return nil
}

// ByteSize returns a best effort estimate of the current size occupied by the QFrame.
//
// This does not factor for cases where multiple, different, frames reference
// the same underlying data.
//
// Time complexity O(m) where m is the number of columns in the QFrame.
func (qf QFrame) ByteSize() int {
	totalSize := 0
	for k, v := range qf.columnsByName {
		totalSize += len(k)
		totalSize += 40 // Estimate of map entry overhead
		totalSize += 16 // String header map key

		// Column both in map and slice, hence 2 x, but don't double count the space
		// occupied by the columns itself.
		totalSize += 2*v.ByteSize() - v.Column.ByteSize()
	}

	totalSize += qf.index.ByteSize()
	totalSize += 16 // Error interface
	return totalSize
}

// Doc returns a generated documentation string that states which built in filters,
// aggregations and transformations that exist for each column type.
func Doc() string {
	result := fmt.Sprintf("Default context\n===============\n%s\n", eval.NewDefaultCtx())
	result += "\nColumns\n=======\n\n"
	for typeName, docString := range map[types.DataType]string{
		types.Bool:   bcolumn.Doc(),
		types.Enum:   ecolumn.Doc(),
		types.Float:  fcolumn.Doc(),
		types.Int:    icolumn.Doc(),
		types.String: scolumn.Doc()} {
		result += fmt.Sprintf("%s\n%s\n%s\n", string(typeName), strings.Repeat("-", len(typeName)), docString)
	}

	return result
}

// TODO?
// - It would also be nice if null could be interpreted as NaN for floats when reading JSON. Should not be impossible
//   using the generated easyjson code as starting point for columns based format and by refining type
//   detection for the record based read. That would also allow proper parsing of integers for record
//   format rather than making them floats.
// - Support access by x, y (to support GoNum matrix interface), or support returning a data type that supports that
//   interface.
// - More serialization and deserialization tests
// - Improve error handling further. Make it possible to classify errors.
// - ApplyN?
// - Are special cases in aggregations that do not rely on index order worth the extra code for the increase in
//   performance allowed by avoiding use of the index?
// - Optional specification of destination column for aggregations, to be able to do 50perc, 90perc, 99perc in one
//   aggregation for example.
// - Equals should support an option to ignore column orders in the QFrame.

// TODO performance?
// - Check out https://github.com/glenn-brown/golang-pkg-pcre for regex filtering. Could be performing better
//   than the stdlib version.
