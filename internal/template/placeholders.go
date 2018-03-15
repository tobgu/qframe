package template

import (
	"encoding/json"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/types"
)

// This file contains definitions for data and functions that need to be added
// manually for each data type.

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]dataType) dataType{}

var filterFuncs = map[string]func(index.Int, []dataType, interface{}, index.Bool) error{}

// Functions not generated but needed to fulfill interface
func (c Column) AppendByteStringAt(buf []byte, i uint32) []byte {
	return nil
}

func (c Column) ByteSize() int {
	return 0
}

func (c Column) Equals(index index.Int, other column.Column, otherIndex index.Int) bool {
	return false
}

func (c Column) Filter(index index.Int, comparator interface{}, comparatee interface{}, bIndex index.Bool) error {
	return nil
}

func (c Column) FunctionType() types.FunctionType {
	return types.FunctionTypeBool
}

func (c Column) Marshaler(index index.Int) json.Marshaler {
	return nil
}

func (c Column) StringAt(i uint32, naRep string) string {
	return ""
}

func (c Comparable) Compare(i, j uint32) column.CompareResult {
	return column.Equal
}
