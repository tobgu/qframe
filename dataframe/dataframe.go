package dataframe

import (
	"fmt"
	"log"
)

type comparator string

type Series interface {
	Filter(c comparator, comparatee interface{}, index []bool)
	Subset(index []bool, sizeHint int) Series
	Equals(other Series) bool
}

type IntSeries struct {
	data []int
}

func NewIntSeries(d []int) IntSeries {
	return IntSeries{data: d}
}

func (s IntSeries) largerThan(comparatee int, index []bool) {
	for i, x := range s.data {
		// TODO: Loop over index instead of data?
		// TODO: Only compare when needed (depending on false/true)?
		index[i] = index[i] || x > comparatee
	}
}

func (s IntSeries) smallerThan(comparatee int, index []bool) {
	for i, x := range s.data {
		// TODO: Loop over index instead of data?
		// TODO: Only compare when needed (depending on false/true)?
		index[i] = index[i] || x < comparatee
	}
}

func (s IntSeries) Filter(c comparator, comparatee interface{}, index []bool) {
	// TODO: Proper error handling
	intComp := comparatee.(int)
	switch c {
	case ">":
		s.largerThan(intComp, index)
	case "<":
		s.smallerThan(intComp, index)
	default:
		// TODO: Proper error handling
		log.Fatalf("Unknown integer comparison %s", c)
	}
}

func (s IntSeries) Equals(other Series) bool {
	otherI, ok := other.(IntSeries)
	if !ok {
		return false
	}

	for ix, x := range s.data {
		if otherI.data[ix] != x {
			return false
		}
	}

	return true
}

// TODO: Additional size hint to avoid having to grow slice?
func (s IntSeries) Subset(index []bool, sizeHint int) Series {
	data := make([]int, 0, sizeHint)
	for i, include := range index {
		if include {
			data = append(data, s.data[i])
		}
	}

	return IntSeries{data: data}
}

type DataFrame struct {
	series map[string]Series
	length int
}

func New(d map[string]interface{}) DataFrame {
	df := DataFrame{series: make(map[string]Series, len(d))}

	// TODO: Assert that all columns have the same length
	for name, column := range d {
		switch column.(type) {
		case []int:
			c := column.([]int)
			df.series[name] = NewIntSeries(c)
			df.length = len(c)
		}
	}

	return df
}

// TODO
type TargetFilter struct {
	operator string        // Comparisons, and, or, etc
	args     []interface{} // Columns, constants, or nested filters
}

// For now...
type SimpleFilter struct {
	Comparator comparator
	Column     string
	Arg        interface{}
}

func countTrue(bools []bool) int {
	result := 0
	for _, b := range bools {
		if b {
			result += 1
		}
	}
	return result
}

// TODO: Bool index should perhaps be changed to a unint64 bitmap slice instead
//       for tighter representation

func (df DataFrame) Filter(filters ...SimpleFilter) DataFrame {
	index := make([]bool, df.length)
	for _, f := range filters {
		// TODO: Check that Column exists
		s := df.series[f.Column]
		s.Filter(f.Comparator, f.Arg, index)
	}

	newSeries := make(map[string]Series, len(df.series))
	sizeHint := countTrue(index)
	for column, series := range df.series {
		newSeries[column] = series.Subset(index, sizeHint)
	}

	return DataFrame{series: newSeries, length: sizeHint}
}

func (df DataFrame) Equals(other DataFrame) (equal bool, reason string) {
	if df.length != other.length {
		return false, "Different length"
	}

	if len(df.series) != len(other.series) {
		return false, "Different number of columns"
	}

	for col, series := range df.series {
		otherSeries, ok := other.series[col]
		if !ok {
			return false, fmt.Sprintf("Column %s missing in other datframe", col)
		}

		if !series.Equals(otherSeries) {
			return false, fmt.Sprintf("Content of column %s differs", col)
		}
	}

	return true, ""
}

func (df DataFrame) Len() int {
	return df.length
}
