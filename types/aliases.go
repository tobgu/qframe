package types

/*
This slightly unconventional use of type aliasing is meant to provide a hook for documentation
of the different uses of interface{} that exists in QFrame. Since there is nothing like a union
or a sum type in Go, QFrame settles for the use of interface{} for some input.

Hopefully this construct says a bit more than nothing about the empty interfaces used.
*/

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

Or it can be a string identifying a built in function.

For example:
    "sum"

IMPORTANT: Reference arguments (eg. slices) must never be assumed to be valid after that the passed function returns.
Under the hood reuse and other performance enhancements may trigger unexpected behaviour if this is ever done.
If, for some reason, you want to retain the data a copy must be made.
*/
type SliceFuncOrBuiltInId = interface{}

/*
DataFuncOrBuiltInId can be a function taking one argument of type T and returning a value of type U.

For example:
	func(x float64) float64
	func(x float64) int

Or it can be a function taking zero arguments returning a value of type T.

For example:
	func() float64
	func() int

Or it can be a function taking two arguments of type T and returning a value of type T. Note that arguments
and return values must all have the same type in this case.

For example:
	func(x, y float64) float64
	func(x, y int) int

Or it can be a string identifying a built in function.

For example:
    "abs"

IMPORTANT: Pointer arguments (eg. *string) must never be assumed to be valid after that the passed function returns.
Under the hood reuse and other performance enhancements may trigger unexpected behaviour if this is ever done.
If, for some reason, you want to retain the data a copy must be made.
*/
type DataFuncOrBuiltInId = interface{}
