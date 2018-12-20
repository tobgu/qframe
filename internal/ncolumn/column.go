package ncolumn

/*
Package ncolumn contains a "null implementation" of the Column interface. It is typeless and of size 0.

It is for example used when reading zero row CSVs without type hints.
*/

import (
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/hash"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/types"
)

type Column struct{}

func (c Column) String() string {
	return "[]"
}

func (c Column) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	return nil
}

func (c Column) Subset(index index.Int) column.Column {
	return c
}

func (c Column) Equals(index index.Int, other column.Column, otherIndex index.Int) bool {
	return false
}

func (c Column) Comparable(reverse, equalNull bool) column.Comparable {
	return Comparable{}
}

func (c Column) Aggregate(indices []index.Int, fn interface{}) (column.Column, error) {
	return c, nil
}

func (c Column) StringAt(i uint32, naRep string) string {
	return naRep
}

func (c Column) AppendByteStringAt(buf []byte, i uint32) []byte {
	return buf
}

func (c Column) ByteSize() int {
	return 0
}

func (c Column) Len() int {
	return 0
}

func (c Column) Apply1(fn interface{}, ix index.Int) (interface{}, error) {
	return c, nil
}

func (c Column) Apply2(fn interface{}, s2 column.Column, ix index.Int) (column.Column, error) {
	return c, nil
}

func (c Column) FunctionType() types.FunctionType {
	return types.FunctionTypeUndefined
}

func (c Column) DataType() types.DataType {
	return types.None
}

type Comparable struct{}

func (c Comparable) Compare(i, j uint32) column.CompareResult {
	return column.NotEqual
}

func (c Comparable) HashBytes(i uint32, buf *hash.Murm32) {
}
