package series

import (
	"encoding/json"
	"fmt"
	// TODO: Make index a public package?
	"github.com/tobgu/qframe/internal/index"
)

type Series interface {
	fmt.Stringer
	Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error
	Subset(index index.Int) Series
	Equals(index index.Int, other Series, otherIndex index.Int) bool
	Comparable(reverse bool) Comparable
	Aggregate(indices []index.Int, fn interface{}) (Series, error)
	StringAt(i uint32, naRep string) string
	AppendByteStringAt(buf []byte, i uint32) []byte
	Marshaler(index index.Int) json.Marshaler
	ByteSize() int

	Apply1(fn interface{}, ix index.Int) (interface{}, error)
	Apply2(fn interface{}, s2 Series, ix index.Int) (Series, error)
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
