package series

import "github.com/tobgu/go-qcache/dataframe/filter"

type Series interface {
	Filter(index []uint32, c filter.Comparator, comparatee interface{}, bIndex []bool) error
	Subset(index []uint32) Series
	Equals(index []uint32, other Series, otherIndex []uint32) bool
	Comparable(reverse bool) Comparable
}

type CompareResult int

const (
	LessThan    CompareResult = iota
	Equal
	GreaterThan
)

type Comparable interface {
	Compare(i, j uint32) CompareResult
}