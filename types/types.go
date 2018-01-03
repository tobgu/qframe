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
