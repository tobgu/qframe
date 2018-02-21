package filter

import "fmt"

// TODO: Perhaps this should not be exposed in an externally accessible
//       package but rather be moved into an internal folder and be wrapped
//       by a couple of config functions for the QFrame.

const (
	Gt  = ">"
	Gte = ">="
	Eq  = "=="
	Neq = "!="
	Lt  = "<"
	Lte = "<="
	In  = "in"
	Nin = "not in"
)

var Inverse = map[string]string{
	Gt:  Lte,
	Gte: Lt,
	Eq:  Neq,
	Lt:  Gte,
	Lte: Gt,
	In:  Nin,
	Nin: In,
}

type Filter struct {
	// Comparator may be a string referring to a built in or a function returning bool
	Comparator interface{}
	Column     string
	Arg        interface{}
	Inverse    bool
}

func (f Filter) String() string {
	arg := f.Arg
	if s, ok := f.Arg.(string); ok {
		arg = fmt.Sprintf(`"%s"`, s)
	}

	s := fmt.Sprintf(`["%v", "%s", %v]`, f.Comparator, f.Column, arg)
	if f.Inverse {
		return fmt.Sprintf(`["not", %s]`, s)
	}
	return s
}
