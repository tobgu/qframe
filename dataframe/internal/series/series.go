package series

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
)

type Series interface {
	fmt.Stringer
	Filter(index index.Int, c filter.Comparator, comparatee interface{}, bIndex index.Bool) error
	Subset(index index.Int) Series
	Equals(index index.Int, other Series, otherIndex index.Int) bool
	Comparable(reverse bool) Comparable
	Aggregate(indices []index.Int, fnName string) (Series, error)
	StringAt(i int) string
	Marshaler(index index.Int) json.Marshaler
	FillRecords(records []map[string]interface{}, index index.Int, colName string)
}

type CompareResult int

const (
	LessThan CompareResult = iota
	Equal
	GreaterThan
)

type Comparable interface {
	Compare(i, j uint32) CompareResult
}
