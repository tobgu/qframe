package template

import (
	"fmt"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
	"github.com/tobgu/go-qcache/dataframe/internal/series"
)

// Code generated from template/comparable.go DO NOT EDIT

//go:generate genny -in=$GOFILE -out=../iseries/comparable_gen.go -pkg=iseries gen "dataType=int"
//go:generate genny -in=$GOFILE -out=../fseries/comparable_gen.go -pkg=fseries gen "dataType=float64"
//go:generate genny -in=$GOFILE -out=../sseries/comparable_gen.go -pkg=sseries gen "dataType=string"

func (c Comparable) Compare(i, j uint32) series.CompareResult {
	x, y := c.data[i], c.data[j]
	if x < y {
		return c.ltValue
	}

	if x > y {
		return c.gtValue
	}

	return series.Equal
}

// TODO: Some kind of code generation for all the below functions for all supported types

func gt(index index.Int, column []dataType, comparatee interface{}, bIndex index.Bool) error {
	comp, ok := comparatee.(dataType)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] > comp
	}

	return nil
}

func lt(index index.Int, column []dataType, comparatee interface{}, bIndex index.Bool) error {
	comp, ok := comparatee.(dataType)
	if !ok {
		return fmt.Errorf("invalid comparison type")
	}

	for i, x := range bIndex {
		bIndex[i] = x || column[index[i]] < comp
	}

	return nil
}
