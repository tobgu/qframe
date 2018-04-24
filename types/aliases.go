package types

/*
DataSlice can be a slice of any of the supported data types.

The following types are currently supported:
	[]bool
	[]float64
	[]int
	[]string
	[]*string
*/
type DataSlice = interface{}

/*
SliceFuncOrBuiltInId can be a function taking a slice of type T and returning a value of type T.

For example:
	func(x []float64) float64
	func(x []int) int
	func(x []*string) *string
	func(x []bool) bool
*/
type SliceFuncOrBuiltInId = interface{}
