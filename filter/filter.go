package filter

import "fmt"

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

// Filter represents a filter to apply to a QFrame.
//
// Example using a built in comparator on a float column:
//   Filter{Comparator: ">", Column: "COL1", Arg: 1.2}
//
// Same example as above but with a custom function:
//   Filter{Comparator: func(f float64) bool { return f > 1.2 }, Column: "COL1"}
type Filter struct {
	// Comparator may be a string referring to a built in or a function taking an argument matching the
	// column type and returning a bool bool.
	//
	// IMPORTANT: For pointer and reference types you must not assume that the data passed argument
	// to this function is valid after the function returns. If you plan to keep it around you need
	// to take a copy of the data.
	Comparator interface{}

	// Column is the name to filter by
	Column string

	// Arg is passed as argument to built in functions.
	Arg interface{}

	// Inverse can be set to true to negate the filter.
	Inverse bool
}

// String returns a string representation of the filter.
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
