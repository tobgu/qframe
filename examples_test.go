package qframe_test

import (
	"github.com/tobgu/qframe"
	"fmt"
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

func ExampleQFrame_filterCustom() {
	f := qframe.New(map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []string{"a", "b", "c"}})
	isOdd := func(x int) bool { return x & 1 > 0 }
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
