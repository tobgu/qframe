package types

type DataType string

const (
	None   DataType = ""
	Int             = "int"
	String          = "string"
	Float           = "float"
	Bool            = "bool"
	Enum            = "enum"
)

//go:generate stringer -type=FunctionType

// The different types of input that functions operating on columns can take
type FunctionType byte

const (
	FunctionTypeUndefined FunctionType = iota
	FunctionTypeInt
	FunctionTypeFloat
	FunctionTypeBool
	FunctionTypeString
)
