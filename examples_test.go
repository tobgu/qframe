package qframe_test

import (
	"fmt"
	"math"
	"strings"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/config/groupby"
	"github.com/tobgu/qframe/config/newqf"
	"github.com/tobgu/qframe/function"
	"github.com/tobgu/qframe/types"
)

func ExampleQFrame_filterBuiltin() {
	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
	newF := f.Filter(qframe.Filter{Column: "COL1", Comparator: ">", Arg: 1})
	fmt.Println(newF)

	// Output:
	// COL1(i) COL2(s)
	// ------- -------
	//       2       b
	//       3       c
	//
	// Dims = 2 x 2
}

func ExampleQFrame_filterCustomFunc() {
	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
	isOdd := func(x int) bool { return x&1 > 0 }
	newF := f.Filter(qframe.Filter{Column: "COL1", Comparator: isOdd})
	fmt.Println(newF)

	// Output:
	// COL1(i) COL2(s)
	// ------- -------
	//       1       a
	//       3       c
	//
	// Dims = 2 x 2
}

func ExampleQFrame_filterWithOrClause() {
	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
	newF := f.Filter(qframe.Or(
		qframe.Filter{Column: "COL1", Comparator: ">", Arg: 2},
		qframe.Filter{Column: "COL2", Comparator: "=", Arg: "a"}))
	fmt.Println(newF)

	// Output:
	// COL1(i) COL2(s)
	// ------- -------
	//       1       a
	//       3       c
	//
	// Dims = 2 x 2
}

func ExampleQFrame_sortWithEnum() {
	f := qframe.New(
		map[string]interface{}{"COL1": []string{"abc", "def", "ghi"}, "COL2": []string{"a", "b", "c"}},
		newqf.Enums(map[string][]string{"COL2": {"c", "b", "a"}}))
	fmt.Println(f)
	fmt.Println("\nSorted according to enum spec:")
	fmt.Println(f.Sort(qframe.Order{Column: "COL2"}))
	// Output:
	// COL1(s) COL2(e)
	// ------- -------
	//     abc       a
	//     def       b
	//     ghi       c
	//
	// Dims = 2 x 3
	//
	// Sorted according to enum spec:
	// COL1(s) COL2(e)
	// ------- -------
	//     ghi       c
	//     def       b
	//     abc       a
	//
	// Dims = 2 x 3
}

func ExampleReadCSV() {
	input := `COL1,COL2
a,1.5
b,2.25
c,3.0`

	f := qframe.ReadCSV(strings.NewReader(input))
	fmt.Println(f)
	// Output:
	// COL1(s) COL2(f)
	// ------- -------
	//       a     1.5
	//       b    2.25
	//       c       3
	//
	// Dims = 2 x 3
}

func ExampleNew() {
	a, c := "a", "c"
	f := qframe.New(map[string]interface{}{
		"COL1": []int{1, 2, 3},
		"COL2": []float64{1.5, 2.5, math.NaN()},
		"COL3": []string{"a", "b", "c"},
		"COL4": []*string{&a, nil, &c},
		"COL5": []bool{false, false, true}},
		newqf.ColumnOrder("COL5", "COL4", "COL3", "COL2", "COL1"))
	fmt.Println(f)
	// Output:
	// COL5(b) COL4(s) COL3(s) COL2(f) COL1(i)
	// ------- ------- ------- ------- -------
	//   false       a       a     1.5       1
	//   false    null       b     2.5       2
	//    true       c       c    null       3
	//
	// Dims = 5 x 3
}

func ExampleQFrame_applyStrConcat() {
	// String concatenating COL2 and COL1.
	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
	f = f.Apply(
		qframe.Instruction{Fn: function.StrI, DstCol: "COL1", SrcCol1: "COL1"},
		qframe.Instruction{Fn: function.ConcatS, DstCol: "COL3", SrcCol1: "COL1", SrcCol2: "COL2"})
	fmt.Println(f.Select("COL3"))

	// Output:
	// COL3(s)
	// -------
	//      1a
	//      2b
	//      3c
	//
	// Dims = 1 x 3
}

func ExampleQFrame_applyConstant() {
	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}})
	f = f.Apply(qframe.Instruction{Fn: 1.5, DstCol: "COL2"})
	fmt.Println(f)

	// COL1(i) COL2(f)
	// ------- -------
	//       1     1.5
	//       2     1.5
	//       3     1.5
	//
	// Dims = 2 x 3
}

func ExampleQFrame_applyGenerator() {
	val := -1
	generator := func() int {
		val++
		return val
	}

	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}})
	f = f.Apply(qframe.Instruction{Fn: generator, DstCol: "COL2"})
	fmt.Println(f)

	// COL1(i) COL2(i)
	// ------- -------
	//       1       0
	//       2       1
	//       3       2
	//
	// Dims = 2 x 3
}

func ExampleQFrame_evalStrConcat() {
	// Same example as for apply but using Eval instead.
	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
	f = f.Eval("COL3", qframe.Expr("+", qframe.Expr("str", types.ColumnName("COL1")), types.ColumnName("COL2")))
	fmt.Println(f.Select("COL3"))

	// Output:
	// COL3(s)
	// -------
	//      1a
	//      2b
	//      3c
	//
	// Dims = 1 x 3
}

func ExampleQFrame_groupByAggregate() {
	intSum := func(xx []int) int {
		result := 0
		for _, x := range xx {
			result += x
		}
		return result
	}

	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 2, 3, 3}, "COL2": []string{"a", "b", "c", "a", "b"}})
	f = f.GroupBy(groupby.Columns("COL2")).Aggregate(qframe.Aggregation{Fn: intSum, Column: "COL1"})
	fmt.Println(f.Sort(qframe.Order{Column: "COL2"}))

	// Output:
	// COL2(s) COL1(i)
	// ------- -------
	//       a       4
	//       b       5
	//       c       2
	//
	// Dims = 2 x 3
}

func ExampleQFrame_view() {
	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}})
	v, _ := f.IntView("COL1")
	fmt.Println(v.Slice())

	// Output:
	// [1 2 3]
}

func ExampleQFrame_iter() {
	qf := qframe.New(map[string]interface{}{
		"COL1": []string{"a", "b", "c"},
		"COL2": []int{0, 1, 2},
		"COL3": []string{"d", "e", "f"},
		"COL4": []int{3, 4, 5},
	})
	named := qf.ColumnTypeMap()
	for _, col := range qf.ColumnNames() {
		if named[col] == types.Int {
			view := qf.MustIntView(col)
			for i := 0; i < view.Len(); i++ {
				fmt.Println(view.ItemAt(i))
			}
		}
	}

	// Output:
	// 0
	// 1
	// 2
	// 3
	// 4
	// 5
}

func ExampleQFrame_groupByCount() {
	qf := qframe.New(map[string]interface{}{
		"COL1": []string{"a", "b", "a", "b", "b", "c"},
		"COL2": []float64{0.1, 0.1, 0.2, 0.4, 0.5, 0.6},
	})

	g := qf.GroupBy(groupby.Columns("COL1"))
	qf = g.Aggregate(qframe.Aggregation{Fn: "count", Column: "COL2"}).Sort(qframe.Order{Column: "COL1"})

	fmt.Println(qf)

	// Output:
	// COL1(s) COL2(i)
	// ------- -------
	//       a       2
	//       b       3
	//       c       1
	//
	// Dims = 2 x 3
}

func ExampleQFrame_distinct() {
	qf := qframe.New(map[string]interface{}{
		"COL1": []string{"a", "b", "a", "b", "b", "c"},
		"COL2": []int{0, 1, 2, 4, 4, 6},
	})

	qf = qf.Distinct(groupby.Columns("COL1", "COL2")).Sort(qframe.Order{Column: "COL1"}, qframe.Order{Column: "COL2"})

	fmt.Println(qf)

	// Output:
	// COL1(s) COL2(i)
	// ------- -------
	//       a       0
	//       a       2
	//       b       1
	//       b       4
	//       c       6
	//
	// Dims = 2 x 5
}
