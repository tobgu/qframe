package filter

import "github.com/tobgu/qframe/errors"

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

type ClauseType byte

const (
	ClauseTypeAnd ClauseType = iota
	ClauseTypeOr
	ClauseTypeLeaf
)

type Clause interface {
	Type() ClauseType

	// true if "not"
	IsInverse() bool

	// Only valid for clause type And and Or
	SubClauses() []Clause

	// Only valid for clause type Leaf
	Filter() *Filter
}

type Filter struct {
	Comparator Comparator
	Column     string
	Arg        interface{}
	Inverse    bool
}

func (f Filter) Type() ClauseType {
	return ClauseTypeLeaf
}

func (f Filter) IsInverse() bool {
	return f.Inverse
}

func (f Filter) SubClauses() []Clause {
	return nil
}

func (f Filter) Filter() *Filter {
	return nil
}

type comboClause struct {
	subClauses []Clause
	typ        ClauseType
	inverse    bool
}

func newComboClause(typ ClauseType, clauses []Clause, inverse bool) (Clause, error) {
	if len(clauses) == 0 {
		return comboClause{}, errors.New("New clause", "zero subclauses not allowed")
	}

	if len(clauses) == 1 {
		// We can just propagate this clause up the tree
		return clauses[0], nil
	}

	return comboClause{typ: typ, subClauses: clauses, inverse: inverse}, nil
}

func And(clauses []Clause, inverse bool) (Clause, error) {
	return newComboClause(ClauseTypeAnd, clauses, inverse)
}

func Or(clauses []Clause, inverse bool) (Clause, error) {
	return newComboClause(ClauseTypeOr, clauses, inverse)
}

func (cc comboClause) Type() ClauseType {
	return cc.typ
}

func (cc comboClause) IsInverse() bool {
	return cc.inverse
}

func (cc comboClause) SubClauses() []Clause {
	return cc.subClauses
}

func (cc comboClause) Filter() *Filter {
	return nil
}
