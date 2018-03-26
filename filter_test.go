package qframe_test

import (
	"fmt"
	"github.com/tobgu/qframe"
	"testing"
)

func f(column string, comparator string, arg interface{}) qframe.Filter {
	return qframe.Filter{Column: column, Comparator: comparator, Arg: arg}
}

func notf(column string, comparator string, arg interface{}) qframe.Filter {
	filter := f(column, comparator, arg)
	filter.Inverse = true
	return filter
}

func and(clauses ...qframe.FilterClause) qframe.AndClause {
	return qframe.And(clauses...)
}

func or(clauses ...qframe.FilterClause) qframe.OrClause {
	return qframe.Or(clauses...)
}

func not(clause qframe.FilterClause) qframe.NotClause {
	return qframe.Not(clause)
}

func TestFilter_Success(t *testing.T) {
	input := qframe.New(map[string]interface{}{
		"COL1": []int{1, 2, 3, 4, 5},
	})

	eq := func(x int) qframe.FilterClause {
		return f("COL1", "=", x)
	}

	table := []struct {
		name     string
		clause   qframe.FilterClause
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
			"Or with mixed nested or and clause",
			or(eq(1), or(eq(3), eq(4)), eq(5)),
			[]int{1, 3, 4, 5},
		},
		{
			"Nested single clause",
			or(and(eq(4))),
			[]int{4},
		},
		{
			"Not start",
			not(or(eq(1), eq(2))),
			[]int{3, 4, 5},
		},
		{
			"Not end",
			not(or(eq(4), eq(5))),
			[]int{1, 2, 3},
		},
		{
			"Not mixed",
			not(or(eq(4), eq(2))),
			[]int{1, 3, 5},
		},
		{
			"Not empty",
			not(eq(6)),
			[]int{1, 2, 3, 4, 5},
		},
		{
			"Not full",
			not(f("COL1", "<", 6)),
			[]int{},
		},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("Filter %s", tc.name), func(t *testing.T) {
			assertNotErr(t, tc.clause.Err())
			out := input.Filter(tc.clause)
			assertNotErr(t, out.Err)
			assertEquals(t, qframe.New(map[string]interface{}{"COL1": tc.expected}), out)
		})
	}
}

func TestFilter_ErrorColumnDoesNotExist(t *testing.T) {
	input := qframe.New(map[string]interface{}{
		"COL1": []int{1, 2, 3, 4, 5},
	})

	colGt3 := f("COL", ">", 3)
	col1Gt3 := f("COL1", ">", 3)

	table := []qframe.FilterClause{
		colGt3,
		or(col1Gt3, colGt3),
		and(col1Gt3, colGt3),
		and(col1Gt3, and(col1Gt3, colGt3)),
		or(and(col1Gt3, colGt3), col1Gt3),
		or(and(col1Gt3, col1Gt3), colGt3),
	}

	for i, c := range table {
		t.Run(fmt.Sprintf("Filter %d", i), func(t *testing.T) {
			out := input.Filter(c)
			assertErr(t, out.Err, "column does not exist")
		})
	}
}

func TestFilter_String(t *testing.T) {
	table := []struct {
		clause   qframe.FilterClause
		expected string
	}{
		{f("COL1", ">", 3), `[">", "COL1", 3]`},
		{f("COL1", ">", "3"), `[">", "COL1", "3"]`},
		{not(f("COL1", ">", 3)), `["!", [">", "COL1", 3]]`},
		{notf("COL1", ">", 3), `["!", [">", "COL1", 3]]`},
		{and(f("COL1", ">", 3)), `["and", [">", "COL1", 3]]`},
		{or(f("COL1", ">", 3)), `["or", [">", "COL1", 3]]`},
		{
			and(f("COL1", ">", 3), f("COL2", ">", 3)),
			`["and", [">", "COL1", 3], [">", "COL2", 3]]`,
		},
		{
			or(f("COL1", ">", 3), f("COL2", ">", 3)),
			`["or", [">", "COL1", 3], [">", "COL2", 3]]`,
		},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("String %s", tc.expected), func(t *testing.T) {
			assertNotErr(t, tc.clause.Err())
			if tc.expected != tc.clause.String() {
				t.Errorf("%s != %s", tc.expected, tc.clause.String())
			}
		})
	}
}
