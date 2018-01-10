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

var Inverse = map[Comparator]Comparator{
	Gt:  Lte,
	Gte: Lt,
	Eq:  Neq,
	Lt:  Gte,
	Lte: Gt,
	In:  Nin,
	Nin: In,
}

type Filter struct {
	Comparator Comparator
	Column     string
	Arg        interface{}
	Inverse    bool
}

// TODO: Map to inverse filter to support "not" through de Morgan transformation of expressions
