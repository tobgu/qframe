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

// The different types of input that functions operating on columns can take
type FunctionType byte

const (
	FunctionTypeUndefined FunctionType = iota
	FunctionTypeInt
	FunctionTypeFloat
	FunctionTypeBool
	FunctionTypeString
)

func (t FunctionType) String() string {
	switch t {
	case FunctionTypeInt:
		return "Int function"
	case FunctionTypeBool:
		return "Bool function"
	case FunctionTypeString:
		return "String function"
	case FunctionTypeFloat:
		return "Float function"
	default:
		return "Unknown function"
	}
}
