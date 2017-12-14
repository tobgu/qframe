package dataframe

import (
	"fmt"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
	"github.com/tobgu/go-qcache/dataframe/internal/intseries"
	"github.com/tobgu/go-qcache/dataframe/internal/series"
)

type DataFrame struct {
	series map[string]series.Series
	index  index.Int
	Err    error
}

func New(d map[string]interface{}) DataFrame {
	df := DataFrame{series: make(map[string]series.Series, len(d))}
	firstLen := -1
	currentLen := 0
	for name, column := range d {
		switch column.(type) {
		case []int:
			c := column.([]int)
			df.series[name] = intseries.New(c)
			currentLen = len(c)
		}

		if firstLen == -1 {
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
	Sort(sorter)

	return newDf
}

func (df DataFrame) Distinct(columns ...string) DataFrame {
	if df.Len() == 0 {
		return df
	}

	if len(columns) == 0 {
		columns = make([]string, 0, len(df.series))
		for column := range df.series {
			columns = append(columns, column)
		}
	}

	// TODO: Check that columns exist
	orders := make([]Order, len(columns))
	for i, column := range columns {
		orders[i] = Order{Column: column}
	}

	// Compare the columns in reverse order compared to the sort order
	// since it's likely to produce differences with fewer comparisons.
	comparables := make([]series.Comparable, 0, len(columns))
	for i := len(columns) - 1; i >= 0; i-- {
		comparables = append(comparables, df.series[orders[i].Column].Comparable(false))
	}

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

func (df DataFrame) String() string {
	// TODO: Fix
	result := ""
	for name, values := range df.series {
		result += fmt.Sprintf("%s: %v", name, values)
	}

	return result
}

// TODO dataframe:
// - Error checks and general improvements to error structures
// - Select/projection
// - Code generation to support all common operations for all data types
// - Custom filtering for different types (bitwise, regex, etc)
// - Read and write CSV and JSON
// - Grouping
// - Aggregation functions
// - Select distinct
