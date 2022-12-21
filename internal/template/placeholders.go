package template

import (
	"encoding/json"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/types"
	"io"
)

// This file contains definitions for data and functions that need to be added
// manually for each data type.

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]genericDataType) genericDataType{}

func (c Column) DataType() types.DataType {
	panic("Not implemented")
}

// Functions not generated but needed to fulfill interface

func (c Column) AppendByteStringAt(buf []byte, i uint32) []byte {
	panic("Not implemented")
}

func (c Column) ByteSize() int {
	panic("Not implemented")
}

func (c Column) Equals(index index.Int, other column.Column, otherIndex index.Int) bool {
	panic("Not implemented")
}

func (c Column) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	panic("Not implemented")
}

func (c Column) FunctionType() types.FunctionType {
	panic("Not implemented")
}

func (c Column) Marshaler(index index.Int) json.Marshaler {
	panic("Not implemented")
}

func (c Column) StringAt(i uint32, naRep string) string {
	panic("Not implemented")
}

func (c Column) Append(cols ...column.Column) (column.Column, error) {
	panic("Not implemented")
}

func (c Column) ToQBin(w io.Writer) error {
	panic("Not implemented")
}

func (c Comparable) Compare(i, j uint32) column.CompareResult {
	panic("Not implemented")
}

func (c Comparable) Hash(i uint32, seed uint64) uint64 {
	panic("Not implemented")
}
