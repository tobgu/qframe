package filter

// TODO: Perhaps this should not be exposed in an externally accessible
//       package but rather be moved into an internal folder and be wrapped
//       by a couple of config functions for the QFrame.

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
