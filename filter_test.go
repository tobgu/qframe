package qframe_test

import (
	"fmt"
	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/filter"
	"testing"
)

func f(column string, comparator filter.Comparator, arg interface{}) qframe.FilterClause {
	return qframe.Filter(filter.Filter{Column: column, Comparator: comparator, Arg: arg})
}

func and(clauses ...qframe.Clause) qframe.AndClause {
	return qframe.And(false, clauses...)
}

func or(clauses ...qframe.Clause) qframe.OrClause {
	return qframe.Or(false, clauses...)
}

func TestFilter_Success(t *testing.T) {
	a := qframe.New(map[string]interface{}{
		"COL1": []int{1, 2, 3, 4, 5},
	})

	eq := func(x int) qframe.FilterClause {
		return f("COL1", "==", x)
	}

	table := []struct {
		name     string
		clause   qframe.Clause
		expected []int
	}{
		{
			"Single filter",
			f("COL1", ">", 3),
			[]int{4, 5},
		},
		{
			"Simple or",
			or(f("COL1", ">", 3), f("COL1", "<", 2)),
			[]int{1, 4, 5},
		},
		{
			"Simple and",
			and(f("COL1", "<", 3), f("COL1", ">", 1)),
			[]int{2},
		},
		{
			"Or with nested and",
			or(
				and(f("COL1", "<", 3), f("COL1", ">", 1)),
				eq(5)),
			[]int{2, 5},
		},
		{
			"Or with nested and, reverse clause",
			or(eq(5),
				and(f("COL1", "<", 3), f("COL1", ">", 1))),
			[]int{2, 5},
		},
		{
			"Or with mixed nested or and filters",
			or(eq(1), or(eq(3), eq(4)), eq(5)),
			[]int{1, 3, 4, 5},
		},
		{
			"Nested single clause",
			or(and(eq(4))),
			[]int{4},
		},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("Filter %s", tc.name), func(t *testing.T) {
			assertNotErr(t, tc.clause.Err())
			b := tc.clause.Filter(a)
			assertNotErr(t, b.Err)
			assertEquals(t, qframe.New(map[string]interface{}{"COL1": tc.expected}), b)
		})
	}
}

func TestFilter_ErrorColumnDoesNotExist(t *testing.T) {
	a := qframe.New(map[string]interface{}{
		"COL1": []int{1, 2, 3, 4, 5},
	})

	colGt3 := f("COL", ">", 3)
	col1Gt3 := f("COL1", ">", 3)

	table := []qframe.Clause{
		colGt3,
		or(col1Gt3, colGt3),
		and(col1Gt3, colGt3),
		and(col1Gt3, and(col1Gt3, colGt3)),
		or(and(col1Gt3, colGt3), col1Gt3),
		or(and(col1Gt3, col1Gt3), colGt3),
	}

	for i, c := range table {
		t.Run(fmt.Sprintf("Filter %d", i), func(t *testing.T) {
			b := c.Filter(a)
			assertErr(t, b.Err, "column does not exist")
		})
	}
}
