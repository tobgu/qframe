package qframe

import (
	"encoding/csv"
	"fmt"
	"github.com/tobgu/qframe/aggregation"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/bcolumn"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/ecolumn"
	"github.com/tobgu/qframe/internal/fcolumn"
	"github.com/tobgu/qframe/internal/hashgrouper"
	"github.com/tobgu/qframe/internal/icolumn"
	"github.com/tobgu/qframe/internal/index"
	qfio "github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/scolumn"
	qfstrings "github.com/tobgu/qframe/internal/strings"
	"github.com/tobgu/qframe/types"
	"io"
	"reflect"
	"sort"
	"strings"
)

type namedColumn struct {
	column.Column
	name string
	pos  int
}

func (ns namedColumn) ByteSize() int {
	return ns.Column.ByteSize() + 2*8 + 8 + len(ns.name)
}

type QFrame struct {
	columns       []namedColumn
	columnsByName map[string]namedColumn
	index         index.Int
	Err           error
}

type Config struct {
	columnOrder []string
	enumColumns map[string][]string
}

type ConfigFunc func(c *Config)

func newConfig(fns []ConfigFunc) *Config {
	config := &Config{}
	for _, fn := range fns {
		fn(config)
	}
	return config
}

func ColumnOrder(columns ...string) ConfigFunc {
	return func(c *Config) {
		c.columnOrder = make([]string, len(columns))
		copy(c.columnOrder, columns)
	}
}

// If columns should be considered enums. The map key specifies the
// columns name, the value if there is a fixed set of values and their
// internal ordering. If value is nil or empty list the values will be
// derived from the columns content and the ordering unspecified.
func Enums(columns map[string][]string) ConfigFunc {
	return func(c *Config) {
		c.enumColumns = make(map[string][]string)
		for k, v := range columns {
			c.enumColumns[k] = v
		}
	}
}

func (qf QFrame) withErr(err error) QFrame {
	return QFrame{Err: err, columns: qf.columns, columnsByName: qf.columnsByName, index: qf.index}
}

func (qf QFrame) withIndex(ix index.Int) QFrame {
	return QFrame{Err: qf.Err, columns: qf.columns, columnsByName: qf.columnsByName, index: ix}
}

type ConstString struct {
	Val   *string
	Count int
}

type ConstInt struct {
	Val   int
	Count int
}

type ConstFloat struct {
	Val   float64
	Count int
}

type ConstBool struct {
	Val   bool
	Count int
}

func createColumn(name string, data interface{}, config *Config) (column.Column, error) {
	var localS column.Column

	// TODO: Change this case to use strings directly for strings and enums
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
		if values, ok := config.enumColumns[name]; ok {
			localS, err = ecolumn.New(t, values)
			if err != nil {
				return nil, errors.Propagate(fmt.Sprintf("New columns %s", name), err)
			}
			// Book keeping
			delete(config.enumColumns, name)
		} else {
			localS = scolumn.New(t)
		}
	case ConstString:
		if values, ok := config.enumColumns[name]; ok {
			localS, err = ecolumn.NewConst(t.Val, t.Count, values)
			if err != nil {
				return nil, errors.Propagate(fmt.Sprintf("New columns %s", name), err)
			}
			// Book keeping
			delete(config.enumColumns, name)
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
	default:
		return nil, errors.New("New", "unknown columns format of: %v", t)
	}
	return localS, nil
}

func New(data map[string]interface{}, fns ...ConfigFunc) QFrame {
	config := newConfig(fns)
	if len(config.columnOrder) == 0 {
		config.columnOrder = make([]string, 0, len(data))
		for name := range data {
			config.columnOrder = append(config.columnOrder, name)
			sort.Strings(config.columnOrder)
		}
	}

	if len(config.columnOrder) != len(data) {
		return QFrame{Err: errors.New("New", "columns and columns order length do not match")}
	}

	for _, name := range config.columnOrder {
		if _, ok := data[name]; !ok {
			return QFrame{Err: errors.New("New", `key "%s" does not exist in supplied data`, name)}
		}
	}

	s := make([]namedColumn, len(data))
	sByName := make(map[string]namedColumn, len(data))
	firstLen, currentLen := 0, 0
	for i, name := range config.columnOrder {
		col := data[name]
		localS, err := createColumn(name, col, config)
		if err != nil {
			return QFrame{Err: err}
		}

		s[i] = namedColumn{name: name, Column: localS, pos: i}
		sByName[name] = s[i]
		currentLen = localS.Len()
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

	return QFrame{columns: s, columnsByName: sByName, index: index.NewAscending(uint32(currentLen)), Err: nil}
}

func (qf QFrame) Contains(colName string) bool {
	_, ok := qf.columnsByName[colName]
	return ok
}

func (qf QFrame) Filter(clause FilterClause) QFrame {
	if qf.Err != nil {
		return qf
	}

	return clause.filter(qf)
}

func (qf QFrame) filter(filters ...filter.Filter) QFrame {
	if qf.Err != nil {
		return qf
	}

	bIndex := index.NewBool(qf.index.Len())
	for _, f := range filters {
		s, ok := qf.columnsByName[f.Column]
		if !ok {
			return qf.withErr(errors.New("Filter", `column does not exist, "%s"`, f.Column))
		}

		if name, ok := f.Arg.(filter.ColumnName); ok {
			argC, ok := qf.columnsByName[string(name)]
			if !ok {
				return qf.withErr(errors.New("Filter", `argument column does not exist, "%s"`, name))
			}
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
			return qf.withErr(errors.Propagate("Filter", err))
		}
	}

	return qf.withIndex(qf.index.Filter(bIndex))
}

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

	comparables := make([]column.Comparable, 0, len(orders))
	for _, o := range orders {
		s, ok := qf.columnsByName[o.Column]
		if !ok {
			return qf.withErr(errors.New("Sort", "unknown columns: %s", o.Column))
		}

		comparables = append(comparables, s.Comparable(o.Reverse, false))
	}

	newDf := qf.withIndex(qf.index.Copy())
	sorter := Sorter{index: newDf.index, columns: comparables}
	sortDf(sorter)

	return newDf
}

func (qf QFrame) columnNames() []string {
	result := make([]string, len(qf.columns))
	for i, s := range qf.columns {
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
	for i, col := range columns {
		orders[i] = Order{Column: col}
	}

	return orders
}

func (qf QFrame) comparables(columns []string, orders []Order, groupByNull bool) []column.Comparable {
	result := make([]column.Comparable, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		result = append(result, qf.columnsByName[orders[i].Column].Comparable(false, groupByNull))
	}

	return result
}

func (qf QFrame) Distinct(configFns ...GroupByConfigFn) QFrame {
	if qf.Err != nil {
		return qf
	}

	if qf.Len() == 0 {
		return qf
	}

	config := newGroupByConfig(configFns)

	for _, col := range config.columns {
		if _, ok := qf.columnsByName[col]; !ok {
			return qf.withErr(errors.New("Distinct", `unknown columns "%s"`, col))
		}
	}

	columns := qf.columnsOrAll(config.columns)
	orders := qf.orders(columns)
	comparables := qf.comparables(columns, orders, config.groupByNull)
	newIx := hashgrouper.Distinct(qf.index, comparables)
	return qf.withIndex(newIx)
}

func (qf QFrame) checkColumns(operation string, columns []string) error {
	for _, col := range columns {
		if _, ok := qf.columnsByName[col]; !ok {
			return errors.New("operation", `unknown columns "%s"`, col)
		}
	}

	return nil
}

func (qf QFrame) Drop(columns ...string) QFrame {
	if qf.Err != nil || len(columns) == 0 {
		return qf
	}

	dropColumns := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		dropColumns[c] = struct{}{}
	}

	selectColumns := make([]string, 0)
	for _, c := range qf.columns {
		if _, ok := dropColumns[c.name]; !ok {
			selectColumns = append(selectColumns, c.name)
		}
	}

	return qf.Select(selectColumns...)
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

// Internal statistics for grouping. Clients should not depend on this for any
// type of decision making. It is strictly "for info". The layout may change
// if the underlying grouping mechanisms change.
type GroupStats hashgrouper.GroupStats

type Grouper struct {
	indices        []index.Int
	groupedColumns []string
	columns        []namedColumn
	columnsByName  map[string]namedColumn
	Err            error
	Stats          GroupStats
}

type GroupByConfig struct {
	columns     []string
	groupByNull bool
	// dropNulls?
}

type GroupByConfigFn func(c *GroupByConfig)

func GroupBy(columns ...string) GroupByConfigFn {
	return func(c *GroupByConfig) {
		c.columns = columns
	}
}

// Setting this to "true" means that nil/NaN values are grouped
// together. Default "false".
func GroupByNull(b bool) GroupByConfigFn {
	return func(c *GroupByConfig) {
		c.groupByNull = b
	}
}

func newGroupByConfig(configFns []GroupByConfigFn) GroupByConfig {
	var config GroupByConfig
	for _, f := range configFns {
		f(&config)
	}

	return config
}

// Leaving out columns will make one large group over which aggregations can be done
func (qf QFrame) GroupBy(configFns ...GroupByConfigFn) Grouper {
	if qf.Err != nil {
		return Grouper{Err: qf.Err}
	}

	config := newGroupByConfig(configFns)

	if err := qf.checkColumns("GroupBy", config.columns); err != nil {
		return Grouper{Err: err}
	}

	g := Grouper{columns: qf.columns, columnsByName: qf.columnsByName, groupedColumns: config.columns}
	if qf.Len() == 0 {
		return g
	}

	if len(config.columns) == 0 {
		g.indices = []index.Int{qf.index}
		return g
	}

	orders := qf.orders(config.columns)
	comparables := qf.comparables(config.columns, orders, config.groupByNull)
	indices, stats := hashgrouper.GroupBy(qf.index, comparables)
	g.indices = indices
	g.Stats = GroupStats(stats)
	return g
}

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

	newColumnsByName := make(map[string]namedColumn, len(g.groupedColumns)+len(aggs))
	newColumns := make([]namedColumn, 0, len(g.groupedColumns)+len(aggs))
	for i, colName := range g.groupedColumns {
		col := g.columnsByName[colName]
		col.pos = i
		col.Column = col.Subset(firstElementIx)
		newColumnsByName[colName] = col
		newColumns = append(newColumns, col)
	}

	var err error
	for _, agg := range aggs {
		col := g.columnsByName[agg.Column]
		col.Column, err = col.Aggregate(g.indices, agg.Fn)
		if err != nil {
			return QFrame{Err: errors.Propagate("Aggregate", err)}
		}

		newColumnsByName[agg.Column] = col
		newColumns = append(newColumns, col)
	}

	return QFrame{columns: newColumns, columnsByName: newColumnsByName, index: index.NewAscending(uint32(len(g.indices)))}
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
	row := make([]string, len(qf.columns))
	colWidths := make([]int, len(qf.columns))
	minColWidth := 5
	for i, s := range qf.columns {
		colHeader := s.name + "(" + s.DataType()[:1] + ")"
		colWidths[i] = intMax(len(colHeader), minColWidth)
		row[i] = fixLengthString(colHeader, " ", colWidths[i])
	}
	result = append(result, strings.Join(row, " "))

	for i := range qf.columns {
		row[i] = fixLengthString("", "-", colWidths[i])
	}
	result = append(result, strings.Join(row, " "))

	maxRowCount := 50
	for i := 0; i < intMin(qf.Len(), maxRowCount); i++ {
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

func (qf QFrame) setColumn(name string, c column.Column) QFrame {
	newF := qf.withIndex(qf.index)
	existingS, overwrite := qf.columnsByName[name]
	newColCount := len(qf.columns)
	pos := newColCount
	if overwrite {
		pos = existingS.pos
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

func (qf QFrame) Copy(dstCol, srcCol string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedColumn, ok := qf.columnsByName[srcCol]
	if !ok {
		return qf.withErr(errors.New("Instruction", "no such columns: %s", srcCol))
	}

	if dstCol == srcCol {
		// NOP
		return qf
	}

	return qf.setColumn(dstCol, namedColumn.Column)
}

func (qf QFrame) apply0(fn interface{}, dstCol string) QFrame {
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
	case filter.ColumnName:
		return qf.Copy(dstCol, string(t))
	default:
		return qf.withErr(errors.New("apply0", "unknown apply type: %v", reflect.TypeOf(fn)))
	}

	c, err := createColumn(dstCol, data, newConfig(nil))
	if err != nil {
		return qf.withErr(err)
	}

	return qf.setColumn(dstCol, c)
}

func (qf QFrame) apply1(fn interface{}, dstCol, srcCol string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedColumn, ok := qf.columnsByName[srcCol]
	if !ok {
		return qf.withErr(errors.New("apply1", "no such columns: %s", srcCol))
	}

	srcColumn := namedColumn.Column

	sliceResult, err := srcColumn.Apply1(fn, qf.index)
	if err != nil {
		return qf.withErr(errors.Propagate("apply1", err))
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
		return qf.withErr(errors.New("apply1", "unexpected type of new columns %#v", t))
	}

	return qf.setColumn(dstCol, resultColumn)
}

func (qf QFrame) apply2(fn interface{}, dstCol, srcCol1, srcCol2 string) QFrame {
	if qf.Err != nil {
		return qf
	}

	namedSrcColumn1, ok := qf.columnsByName[srcCol1]
	if !ok {
		return qf.withErr(errors.New("apply2", "no such columns: %s", srcCol1))
	}
	srcColumn1 := namedSrcColumn1.Column

	namedSrcColumn2, ok := qf.columnsByName[srcCol2]
	if !ok {
		return qf.withErr(errors.New("apply2", "no such columns: %s", srcCol2))
	}
	srcColumn2 := namedSrcColumn2.Column

	resultColumn, err := srcColumn1.Apply2(fn, srcColumn2, qf.index)
	if err != nil {
		return qf.withErr(errors.Propagate("apply2", err))
	}

	return qf.setColumn(dstCol, resultColumn)
}

type Instruction struct {
	Fn     interface{}
	DstCol string

	// Optional fields
	SrcCol1 string
	SrcCol2 string
}

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

func (qf QFrame) Eval(destCol string, expr Expression, ctx *ExprCtx) QFrame {
	if qf.Err != nil {
		return qf
	}

	if ctx == nil {
		ctx = NewDefaultExprCtx()
	}

	result, colName := expr.execute(qf, ctx)

	// colName is often just a temporary name of a column created as a result of
	// executing the expression. We want to rename this column to the requested
	// destination columns name. Remove colName from the result if not present in
	// the original frame to avoid polluting the frame with intermediate results.
	result = result.Copy(destCol, colName)
	if !qf.Contains(colName) {
		result = result.Drop(colName)
	}

	return result
}

type FloatView struct {
	fcolumn.View
}

func (qf QFrame) FloatView(name string) (FloatView, error) {
	namedColumn, ok := qf.columnsByName[name]
	if !ok {
		return FloatView{}, errors.New("FloatView", "no such column: %s", name)
	}

	fCol, ok := namedColumn.Column.(fcolumn.Column)
	if !ok {
		return FloatView{}, errors.New(
			"FloatView",
			"invalid column type, expected float, was: %s", namedColumn.DataType())
	}

	return FloatView{fCol.View(qf.index)}, nil
}

type IntView struct {
	icolumn.View
}

func (qf QFrame) IntView(name string) (IntView, error) {
	namedColumn, ok := qf.columnsByName[name]
	if !ok {
		return IntView{}, errors.New("IntView", "no such column: %s", name)
	}

	iCol, ok := namedColumn.Column.(icolumn.Column)
	if !ok {
		return IntView{}, errors.New(
			"IntView",
			"invalid column type, expected int, was: %s", namedColumn.DataType())
	}

	return IntView{iCol.View(qf.index)}, nil
}

type BoolView struct {
	bcolumn.View
}

func (qf QFrame) BoolView(name string) (BoolView, error) {
	namedColumn, ok := qf.columnsByName[name]
	if !ok {
		return BoolView{}, errors.New("BoolView", "no such column: %s", name)
	}

	bCol, ok := namedColumn.Column.(bcolumn.Column)
	if !ok {
		return BoolView{}, errors.New(
			"BoolView",
			"invalid column type, expected bool, was: %s", namedColumn.DataType())
	}

	return BoolView{bCol.View(qf.index)}, nil
}

type StringView struct {
	scolumn.View
}

func (qf QFrame) StringView(name string) (StringView, error) {
	namedColumn, ok := qf.columnsByName[name]
	if !ok {
		return StringView{}, errors.New("StringView", "no such column: %sCol", name)
	}

	sCol, ok := namedColumn.Column.(scolumn.Column)
	if !ok {
		return StringView{}, errors.New(
			"StringView",
			"invalid column type, expected string, was: %s", namedColumn.DataType())
	}

	return StringView{sCol.View(qf.index)}, nil
}

type EnumView struct {
	ecolumn.View
}

func (qf QFrame) EnumView(name string) (EnumView, error) {
	namedColumn, ok := qf.columnsByName[name]
	if !ok {
		return EnumView{}, errors.New("EnumView", "no such column: %s", name)
	}

	eCol, ok := namedColumn.Column.(ecolumn.Column)
	if !ok {
		return EnumView{}, errors.New(
			"EnumView",
			"invalid column type, expected enum, was: %s", namedColumn.DataType(),
			reflect.TypeOf(namedColumn.Column))
	}

	return EnumView{View: eCol.View(qf.index)}, nil
}

func (qf QFrame) functionType(name string) (types.FunctionType, error) {
	namedColumn, ok := qf.columnsByName[name]
	if !ok {
		return types.FunctionTypeUndefined, errors.New("functionType", "no such column: %s", name)
	}

	return namedColumn.FunctionType(), nil
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

	row := make([]string, 0, len(qf.columns))
	for _, s := range qf.columns {
		row = append(row, s.name)
	}

	columns := make([]column.Column, 0, len(qf.columns))
	for _, name := range row {
		columns = append(columns, qf.columnsByName[name])
	}

	w := csv.NewWriter(writer)
	err := w.Write(row)
	if err != nil {
		return err
	}

	for i := 0; i < qf.Len(); i++ {
		row = row[:0]
		for _, col := range columns {
			row = append(row, col.StringAt(qf.index[i], ""))
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

	colByteNames := make([][]byte, 0, len(qf.columns))
	columns := make([]column.Column, 0, len(qf.columns))
	for name, col := range qf.columnsByName {
		columns = append(columns, col)
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

			for j, col := range columns {
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

	// Column/columns orientation
	jsonBuf := []byte{'{'}
	_, err := writer.Write(jsonBuf)
	if err != nil {
		return err
	}

	for i, col := range columns {
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

		m := col.Marshaler(qf.index)
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

// TODO filter
// - Check out https://github.com/glenn-brown/golang-pkg-pcre for regex filtering. Could be performing better
//   than the stdlib version.

// TODO:
// - It would also be nice if null could be interpreted as NaN for floats. Should not be impossible
//   using the generated easyjson code as starting point for columns based format and by refining type
//   detection for the record based read. That would also allow proper parsing of integers for record
//   format rather than making them floats.
// - Support access by x, y (to support GoNum matrix interface), or support returning a datatype that supports that
//   interface.
// - Benchmarks comparing performance with Pandas
// - Documentation
// - Use https://goreportcard.com
// - More serialization and deserialization tests
// - Improve error handling further. Make it possible to classify errors. Fix errors conflict in Genny.
// - Document public functions
// - Switch to using vgo for dependencies?
// - ApplyN?
// - Add option to drop NaN/Null before grouping?
// - Consider changing most API functions to take variadic "config functions" for better future proofing.
// - Make Filter and Eval APIs more similar
// - During aggregation, would it make sense (performance wise) to reuse buffers used as targets for subsetting?
// - Are special cases in aggregations that do not rely on index order worth the extra code for the increase in
//   performance allowed by avoiding use of the index?
// - Tune group by further. Hashing without copying? Hash table of size 2^X to get rid of modulo calculation?
// - Move benchmarks to own repo to get rid of dependencies only needed for the benchmarks. Make a benchmark script
//   comparing old and new implementations. Find suitable dataset to compare with Pandas and Gota.
// - Make package filter internal?
