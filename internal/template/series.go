package template

// Code generated from template/series.go DO NOT EDIT

import (
	"fmt"
	"github.com/tobgu/genny/generic"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/series"
)

type dataType generic.Number

//go:generate genny -in=$GOFILE -out=../iseries/series_gen.go -pkg=iseries gen "dataType=int"
//go:generate genny -in=$GOFILE -out=../fseries/series_gen.go -pkg=fseries gen "dataType=float64"
//go:generate genny -in=$GOFILE -out=../bseries/series_gen.go -pkg=bseries gen "dataType=bool"

type Series struct {
	data []dataType
}

func New(d []dataType) Series {
	return Series{data: d}
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

func (s Series) Aggregate(indices []index.Int, fn interface{}) (series.Series, error) {
	var actualFn func([]dataType) dataType
	var ok bool

	switch t := fn.(type) {
	case string:
		actualFn, ok = aggregations[t]
		if !ok {
			return nil, fmt.Errorf("aggregation function %s is not defined for series", fn)
		}
	case func([]dataType) dataType:
		actualFn = t
	default:
		// TODO: Genny is buggy and won't let you use your own errors package.
		//       We use a standard error here for now.
		return nil, fmt.Errorf("invalid aggregation function type: %v", t)
	}

	data := make([]dataType, 0, len(indices))
	for _, ix := range indices {
		subS := s.subset(ix)
		data = append(data, actualFn(subS.data))
	}

	return Series{data: data}, nil
}

type Comparable struct {
	data    []dataType
	ltValue series.CompareResult
	gtValue series.CompareResult
}
