package qframe

import (
	"fmt"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"strings"
)

type Clause interface {
	fmt.Stringer
	Filter(qf QFrame) QFrame
	Err() error
}

type FilterClause filter.Filter

type comboClause struct {
	err        error
	subClauses []Clause
}

type AndClause comboClause

type OrClause comboClause

type NotClause struct {
	subClause Clause
}

// Convinience type to simplify clients when no filtering is to be done.
type NullClause struct{}

func anyFilterErr(clauses []Clause) error {
	for _, c := range clauses {
		if c.Err() != nil {
			return c.Err()
		}
	}
	return nil
}

func And(clauses ...Clause) AndClause {
	if len(clauses) == 0 {
		return AndClause{err: errors.New("new and clause", "zero subclauses not allowed")}
	}

	return AndClause{subClauses: clauses, err: anyFilterErr(clauses)}
}

func clauseString(clauses []Clause) string {
	reps := make([]string, 0, len(clauses))
	for _, c := range clauses {
		reps = append(reps, c.String())
	}

	return strings.Join(reps, ", ")
}

func (c AndClause) String() string {
	if c.Err() != nil {
		return c.Err().Error()
	}
	return fmt.Sprintf(`["and", %s]`, clauseString(c.subClauses))
}

func (c AndClause) Filter(qf QFrame) QFrame {
	if c.Err() != nil {
		return qf.withErr(c.Err())
	}

	filteredQf := &qf
	for _, c := range c.subClauses {
		newQf := c.Filter(*filteredQf)
		filteredQf = &newQf
	}

	return *filteredQf
}

func (c AndClause) Err() error {
	return c.err
}

func Or(clauses ...Clause) OrClause {
	if len(clauses) == 0 {
		return OrClause{err: errors.New("new or clause", "zero subclauses not allowed")}
	}

	return OrClause{subClauses: clauses, err: anyFilterErr(clauses)}
}

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

	resultIx := make(index.Int, 0, intMax(len(lhs.index), len(rhs.index)))
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

func (c OrClause) Filter(qf QFrame) QFrame {
	if c.Err() != nil {
		return qf.withErr(c.Err())
	}

	filters := make([]filter.Filter, 0)
	var filteredQf *QFrame

	for _, c := range c.subClauses {
		if f, ok := c.(FilterClause); ok {
			filters = append(filters, filter.Filter(f))
		} else {
			if len(filters) > 0 {
				newQf := qf.Filter(filters...)
				filteredQf = orFrames(&qf, filteredQf, &newQf)
				filters = filters[:0]
			}

			newQf := c.Filter(qf)
			filteredQf = orFrames(&qf, filteredQf, &newQf)
		}
	}

	if len(filters) > 0 {
		newQf := qf.Filter(filters...)
		filteredQf = orFrames(&qf, filteredQf, &newQf)
	}

	return *filteredQf
}

func (c OrClause) Err() error {
	return c.err
}

func Filter(f filter.Filter) FilterClause {
	return FilterClause(f)
}

func (c FilterClause) String() string {
	if c.Err() != nil {
		return c.Err().Error()
	}

	return filter.Filter(c).String()
}

func (c FilterClause) Filter(qf QFrame) QFrame {
	return qf.Filter(filter.Filter(c))
}

func (c FilterClause) Err() error {
	return nil
}

func Not(c Clause) NotClause {
	return NotClause{subClause: c}
}

func (c NotClause) String() string {
	if c.Err() != nil {
		return c.Err().Error()
	}

	return fmt.Sprintf(`["not", %s]`, c.subClause.String())
}

func (c NotClause) Filter(qf QFrame) QFrame {
	if c.Err() != nil {
		return qf.withErr(c.Err())
	}

	if fc, ok := c.subClause.(FilterClause); ok {
		f := filter.Filter(fc)
		f.Inverse = !f.Inverse
		return qf.Filter(f)
	}

	newQf := c.subClause.Filter(qf)
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

func (c NotClause) Err() error {
	return c.subClause.Err()
}

func Null() NullClause {
	return NullClause{}
}

func (c NullClause) String() string {
	return ""
}

func (c NullClause) Filter(qf QFrame) QFrame {
	return qf
}

func (c NullClause) Err() error {
	return nil
}
