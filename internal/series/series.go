package series

import (
	"encoding/json"
	"fmt"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
)

type Series interface {
	fmt.Stringer
	Filter(index index.Int, c filter.Comparator, comparatee interface{}, bIndex index.Bool) error
	Subset(index index.Int) Series
	Equals(index index.Int, other Series, otherIndex index.Int) bool
	Comparable(reverse bool) Comparable
	Aggregate(indices []index.Int, fn interface{}) (Series, error)
	StringAt(i uint32, naRep string) string
	AppendByteStringAt(buf []byte, i uint32) []byte
	Marshaler(index index.Int) json.Marshaler
	ByteSize() int
}

// TODO: Change to byte
type CompareResult int

const (
	LessThan CompareResult = iota
	Equal
	GreaterThan
)

type Comparable interface {
	Compare(i, j uint32) CompareResult
}
