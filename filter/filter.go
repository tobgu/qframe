package filter

import "fmt"

// TODO: Perhaps this should not be exposed in an externally accessible
//       package but rather be moved into an internal folder and be wrapped
//       by a couple of config functions for the QFrame.

const (
	Gt        = ">"
	Gte       = ">="
	Eq        = "="
	Neq       = "!="
	Lt        = "<"
	Lte       = "<="
	In        = "in"
	Nin       = "not in"
	IsNull    = "isnull"
	IsNotNull = "isnotnull"
)

var Inverse = map[string]string{
	Gt:        Lte,
	Gte:       Lt,
	Eq:        Neq,
	Lt:        Gte,
	Lte:       Gt,
	In:        Nin,
	Nin:       In,
	IsNotNull: IsNull,
	IsNull:    IsNotNull,
}

// Marker type to use in Filter.Arg to denote that the argument is
// another column in the QFrame as opposed to a fixed value.
type ColumnName string

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
		return fmt.Sprintf(`["!", %s]`, s)
	}
	return s
}
