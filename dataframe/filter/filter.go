package filter

type Comparator string

const (
	Gt  Comparator = ">"
	Gte Comparator = ">="
	Eq  Comparator = "=="
	Neq Comparator = "!="
	Lt  Comparator = "<"
	Lte Comparator = "<="
	In  Comparator = "in"
	Nin Comparator = "not in"
)

type Filter struct {
	Comparator Comparator
	Column     string
	Arg        interface{}
}

// TODO: Map to inverse filter to support "not" through de Morgan transformation of expressions
