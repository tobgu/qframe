package template

// Code generated from template/series.go DO NOT EDIT

import (
	"fmt"
	"github.com/tobgu/genny/generic"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
	"github.com/tobgu/go-qcache/dataframe/internal/series"
)

type dataType generic.Number

//go:generate genny -in=$GOFILE -out=../iseries/series_gen.go -pkg=iseries gen "dataType=int"
//go:generate genny -in=$GOFILE -out=../fseries/series_gen.go -pkg=fseries gen "dataType=float64"
//go:generate genny -in=$GOFILE -out=../bseries/series_gen.go -pkg=bseries gen "dataType=bool"
//go:generate genny -in=$GOFILE -out=../sseries/series_gen.go -pkg=sseries gen "dataType=string"

type Series struct {
	data []dataType
}

func New(d []dataType) Series {
	return Series{data: d}
}

func (s Series) Filter(index index.Int, c filter.Comparator, comparatee interface{}, bIndex index.Bool) error {
	// TODO: Also make it possible to compare to values in other column
	compFunc, ok := filterFuncs[c]
	if !ok {
		return fmt.Errorf("invalid comparison operator for dataType, %v", c)
	}

	return compFunc(index, s.data, comparatee, bIndex)
}

func (s Series) Equals(index index.Int, other series.Series, otherIndex index.Int) bool {
	otherI, ok := other.(Series)
	if !ok {
		return false
	}

	for ix, x := range index {
		if s.data[x] != otherI.data[otherIndex[ix]] {
			return false
		}
	}

	return true
}

func (s Series) subset(index index.Int) Series {
	data := make([]dataType, 0, len(index))
	for _, ix := range index {
		data = append(data, s.data[ix])
	}

	return Series{data: data}
}

func (s Series) Subset(index index.Int) series.Series {
	return s.subset(index)
}

func (s Series) Comparable(reverse bool) series.Comparable {
	if reverse {
		return Comparable{data: s.data, ltValue: series.GreaterThan, gtValue: series.LessThan}
	}

	return Comparable{data: s.data, ltValue: series.LessThan, gtValue: series.GreaterThan}
}

func (s Series) String() string {
	return fmt.Sprintf("%v", s.data)
}

func (s Series) Aggregate(indices []index.Int, fnName string) (series.Series, error) {
	fn, ok := aggregations[fnName]
	if !ok {
		return nil, fmt.Errorf("aggregation function %s is not defined for in series", fnName)
	}

	data := make([]dataType, 0, len(indices))
	for _, ix := range indices {
		subS := s.subset(ix)
		data = append(data, fn(subS.data))
	}

	return Series{data: data}, nil
}

func (s Series) FillRecords(records []map[string]interface{}, index index.Int, colName string) {
	for i, ix := range index {
		records[i][colName] = s.data[ix]
	}
}

type Comparable struct {
	data    []dataType
	ltValue series.CompareResult
	gtValue series.CompareResult
}
