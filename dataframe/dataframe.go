package dataframe

import (
	"fmt"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/series"
	"github.com/tobgu/go-qcache/dataframe/internal/intseries"
)

type DataFrame struct {
	series map[string]series.Series
	index  []uint32
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

	df.index = make([]uint32, firstLen)
	for i := range df.index {
		df.index[i] = uint32(i)
	}

	return df
}

func applyBoolIndex(ints []uint32, bools []bool) []uint32 {
	result := make([]uint32, 0)
	for ix, b := range bools {
		if b {
			result = append(result, ints[ix])
		}
	}
	return result
}

func (df DataFrame) Filter(filters ...filter.Filter) DataFrame {
	bIndex := make([]bool, len(df.index))
	for _, f := range filters {
		// TODO: Check that Column exists
		s := df.series[f.Column]
		s.Filter(df.index, f.Comparator, f.Arg, bIndex)
	}

	newIndex := applyBoolIndex(df.index, bIndex)
	return DataFrame{series: df.series, index: newIndex}
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
	return len(df.index)
}

type Order struct {
	Column  string
	Reverse bool
}

func (df DataFrame) Sort(orders... Order) DataFrame {
	// Only copy on sort now, may provide in place later
	newIndex := make([]uint32, len(df.index))
	copy(newIndex, df.index)
	newDf := DataFrame{series: df.series, index: newIndex}

	for i := len(orders) - 1; i >= 0; i-- {
		order := orders[i]
		if s, ok := df.series[order.Column]; ok {
			s.Sort(newIndex, order.Reverse)
		} else {
			newDf.Err = fmt.Errorf("unknown column: %s", order.Column)
			break
		}
	}

	return newDf
}

// TODO dataframe:
// - Error checks and general improvements to error structures
// - Sorting based on one or multiple columns, ascending and descending
// - Select/projection
// - Code generation to support all common operations for all data types
// - Custom filtering for different types (bitwise, regex, etc)
// - Read and write CSV and JSON
// - Grouping
// - Aggregation functions
