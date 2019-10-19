package qframe

import (
	"fmt"
	"strings"

	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/math/integer"
	"github.com/tobgu/qframe/qerrors"
)

// FilterClause is an internal interface representing a filter of some kind that can be applied on a QFrame.
type FilterClause interface {
	fmt.Stringer
	filter(qf QFrame) QFrame
	Err() error
}

// Filter is the lowest level in a filter clause.
// See the docs for filter.Filter for an in depth description of the fields.
type Filter filter.Filter

type comboClause struct {
	err        error          //nolint:structcheck
	subClauses []FilterClause //nolint:structcheck
}

// AndClause represents the logical conjunction of multiple clauses.
type AndClause comboClause

// OrClause represents the logical disjunction of multiple clauses.
type OrClause comboClause

// NotClause represents the logical inverse of of a filter clause.
type NotClause struct {
	subClause FilterClause
}

// NullClause is a convenience type to simplify clients when no filtering is to be done.
type NullClause struct{}

func anyFilterErr(clauses []FilterClause) error {
	for _, c := range clauses {
		if c.Err() != nil {
			return c.Err()
		}
	}
	return nil
}

// And returns a new AndClause that represents the conjunction of the passed filter clauses.
func And(clauses ...FilterClause) AndClause {
	if len(clauses) == 0 {
		return AndClause{err: qerrors.New("new AND clause", "zero subclauses not allowed")}
	}

	return AndClause{subClauses: clauses, err: anyFilterErr(clauses)}
}

func clauseString(clauses []FilterClause) string {
	reps := make([]string, 0, len(clauses))
	for _, c := range clauses {
		reps = append(reps, c.String())
	}

	return strings.Join(reps, ", ")
}

// String returns a textual description of the filter.
func (c AndClause) String() string {
	if c.Err() != nil {
		return c.Err().Error()
	}
	return fmt.Sprintf(`["and", %s]`, clauseString(c.subClauses))
}

func (c AndClause) filter(qf QFrame) QFrame {
	if qf.Err != nil {
		return qf
	}

	if c.Err() != nil {
		return qf.withErr(c.Err())
	}

	filteredQf := &qf
	for _, c := range c.subClauses {
		newQf := c.filter(*filteredQf)
		filteredQf = &newQf
	}

	return *filteredQf
}

// Err returns any error that may have occurred during creation of the filter
func (c AndClause) Err() error {
	return c.err
}

// Or returns a new OrClause that represents the disjunction of the passed filter clauses.
func Or(clauses ...FilterClause) OrClause {
	if len(clauses) == 0 {
		return OrClause{err: qerrors.New("new OR clause", "zero subclauses not allowed")}
	}

	return OrClause{subClauses: clauses, err: anyFilterErr(clauses)}
}

// String returns a textual description of the filter.
func (c OrClause) String() string {
	if c.Err() != nil {
		return c.Err().Error()
	}

	return fmt.Sprintf(`["or", %s]`, clauseString(c.subClauses))
}

func orFrames(original, lhs, rhs *QFrame) *QFrame {
	if lhs == nil {
		return rhs
	}

	if lhs.Err != nil {
		return lhs
	}

	if rhs.Err != nil {
		return rhs
	}

	resultIx := make(index.Int, 0, integer.Max(len(lhs.index), len(rhs.index)))
	lhsI, rhsI := 0, 0
	for _, ix := range original.index {
		found := false
		if lhsI < len(lhs.index) && lhs.index[lhsI] == ix {
			found = true
			lhsI++
		}

		if rhsI < len(rhs.index) && rhs.index[rhsI] == ix {
			found = true
			rhsI++
		}

		if found {
			resultIx = append(resultIx, ix)
		}

		// Perhaps optimized special cases here for when one or both of
		// the sides are exhausted?
	}

	newFrame := original.withIndex(resultIx)
	return &newFrame
}

func (c OrClause) filter(qf QFrame) QFrame {
	if qf.Err != nil {
		return qf
	}

	if c.Err() != nil {
		return qf.withErr(c.Err())
	}

	filters := make([]filter.Filter, 0)
	var filteredQf *QFrame

	for _, c := range c.subClauses {
		if f, ok := c.(Filter); ok {
			filters = append(filters, filter.Filter(f))
		} else {
			if len(filters) > 0 {
				newQf := qf.filter(filters...)
				filteredQf = orFrames(&qf, filteredQf, &newQf)
				filters = filters[:0]
			}

			newQf := c.filter(qf)
			filteredQf = orFrames(&qf, filteredQf, &newQf)
		}
	}

	if len(filters) > 0 {
		newQf := qf.filter(filters...)
		filteredQf = orFrames(&qf, filteredQf, &newQf)
	}

	return *filteredQf
}

// Err returns any error that may have occurred during creation of the filter
func (c OrClause) Err() error {
	return c.err
}

// String returns a textual description of the filter.
func (c Filter) String() string {
	if c.Err() != nil {
		return c.Err().Error()
	}

	return filter.Filter(c).String()
}

func (c Filter) filter(qf QFrame) QFrame {
	return qf.filter(filter.Filter(c))
}

// Err returns any error that may have occurred during creation of the filter
func (c Filter) Err() error {
	return nil
}

// Not creates a new NotClause that represents the inverse of the passed filter clause.
func Not(c FilterClause) NotClause {
	return NotClause{subClause: c}
}

// String returns a textual description of the filter clause.
func (c NotClause) String() string {
	if c.Err() != nil {
		return c.Err().Error()
	}

	return fmt.Sprintf(`["!", %s]`, c.subClause.String())
}

func (c NotClause) filter(qf QFrame) QFrame {
	if qf.Err != nil {
		return qf
	}

	if c.Err() != nil {
		return qf.withErr(c.Err())
	}

	if fc, ok := c.subClause.(Filter); ok {
		f := filter.Filter(fc)
		f.Inverse = !f.Inverse
		return qf.filter(f)
	}

	newQf := c.subClause.filter(qf)
	if newQf.Err != nil {
		return newQf
	}

	newIx := make(index.Int, 0, qf.index.Len()-newQf.index.Len())
	newQfI := 0
	for _, ix := range qf.index {
		if newQfI < newQf.index.Len() && newQf.index[newQfI] == ix {
			newQfI++
		} else {
			newIx = append(newIx, ix)
		}
	}

	return qf.withIndex(newIx)
}

// Err returns any error that may have occurred during creation of the filter
func (c NotClause) Err() error {
	return c.subClause.Err()
}

// Null returns a new NullClause
func Null() NullClause {
	return NullClause{}
}

// Err for NullClause always returns an empty string.
func (c NullClause) String() string {
	return ""
}

func (c NullClause) filter(qf QFrame) QFrame {
	return qf
}

// Err for NullClause always returns nil.
func (c NullClause) Err() error {
	return nil
}
