package qframe_test

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/tobgu/qframe/config/rolling"

	"io"
	"log"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/aggregation"
	"github.com/tobgu/qframe/config/csv"
	"github.com/tobgu/qframe/config/eval"
	"github.com/tobgu/qframe/config/groupby"
	"github.com/tobgu/qframe/config/newqf"
	"github.com/tobgu/qframe/types"
)

func assertEquals(t *testing.T, expected, actual qframe.QFrame) {
	t.Helper()
	equal, reason := expected.Equals(actual)
	if !equal {
		t.Errorf("QFrames not equal, %s.\nexpected=\n%s\nactual=\n%s", reason, expected, actual)
	}
}

func assertNotErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
}

func assertErr(t *testing.T, err error, expectedErr string) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected error, was nil")
		return
	}

	if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(expectedErr)) {
		t.Errorf("Expected error to contain: %s, was: %s", expectedErr, err.Error())
	}
}

func assertTrue(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Error("Expected true")
	}
}

func TestQFrame_FilterAgainstConstant(t *testing.T) {
	table := []struct {
		name     string
		clause   qframe.FilterClause
		input    interface{}
		configs  []newqf.ConfigFunc
		expected interface{}
	}{
		{
			name:     "built in greater than",
			clause:   qframe.Filter{Column: "COL1", Comparator: ">", Arg: 3},
			input:    []int{1, 2, 3, 4, 5},
			expected: []int{4, 5}},
		{
			name:     "built in 'in' with int",
			clause:   qframe.Filter{Column: "COL1", Comparator: "in", Arg: []int{3, 5}},
			input:    []int{1, 2, 3, 4, 5},
			expected: []int{3, 5}},
		{
			name:     "built in 'in' with float (truncated to int)",
			clause:   qframe.Filter{Column: "COL1", Comparator: "in", Arg: []float64{3.4, 5.1}},
			input:    []int{1, 2, 3, 4, 5},
			expected: []int{3, 5}},
		{
			name:     "combined with OR",
			clause:   qframe.Or(qframe.Filter{Column: "COL1", Comparator: ">", Arg: 4}, qframe.Filter{Column: "COL1", Comparator: "<", Arg: 2}),
			input:    []int{1, 2, 3, 4, 5},
			expected: []int{1, 5}},
		{
			name:     "inverse",
			clause:   qframe.Filter{Column: "COL1", Comparator: ">", Arg: 4, Inverse: true},
			input:    []int{1, 2, 3, 4, 5},
			expected: []int{1, 2, 3, 4}},
		{
			name:     "all_bits",
			clause:   qframe.Filter{Column: "COL1", Comparator: "all_bits", Arg: 6},
			input:    []int{7, 2, 4, 1, 6},
			expected: []int{7, 6}},
		{
			name:     "all_bits inverse",
			clause:   qframe.Filter{Column: "COL1", Comparator: "all_bits", Arg: 6, Inverse: true},
			input:    []int{7, 2, 4, 1, 6},
			expected: []int{2, 4, 1}},
		{
			name:     "any_bits",
			clause:   qframe.Filter{Column: "COL1", Comparator: "any_bits", Arg: 6},
			input:    []int{7, 2, 4, 1, 6},
			expected: []int{7, 2, 4, 6}},
		{
			name:     "boolean equals",
			clause:   qframe.Filter{Column: "COL1", Comparator: "=", Arg: true},
			input:    []bool{true, false, true},
			expected: []bool{true, true}},
		{
			name: "enum custom function",
			clause: qframe.Filter{
				Column:     "COL1",
				Comparator: func(s *string) bool { return *s == "a" }},
			input:    []string{"a", "b", "c"},
			expected: []string{"a"},
			configs:  []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": {"a", "b", "c"}})}},
		{
			name: "float custom function",
			clause: qframe.Filter{
				Column:     "COL1",
				Comparator: func(f float64) bool { return f > 1.0 }},
			input:    []float64{1.0, 1.25},
			expected: []float64{1.25}},
		{
			name:     "int column against float arg (float will be truncated)",
			clause:   qframe.Filter{Column: "COL1", Comparator: ">=", Arg: 1.5},
			input:    []int{0, 1, 2},
			expected: []int{1, 2}},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Filter %d", i), func(t *testing.T) {
			input := qframe.New(map[string]interface{}{"COL1": tc.input})
			output := input.Filter(tc.clause)
			assertNotErr(t, output.Err)
			expected := qframe.New(map[string]interface{}{"COL1": tc.expected})
			assertEquals(t, expected, output)
		})
	}
}

func TestQFrame_FilterColConstNull(t *testing.T) {
	// For null columns all comparisons will always produce false except for != which will always produce true
	comparisons := []struct {
		operation   string
		expectCount int
	}{
		{operation: "<", expectCount: 1},
		{operation: ">=", expectCount: 1},
		{operation: "=", expectCount: 1},
		{operation: "!=", expectCount: 2},
	}

	a, b := "a", "b"
	table := []struct {
		name   string
		input  interface{}
		isEnum bool
		arg    interface{}
	}{
		{name: "string", input: []*string{&a, &b, nil}, arg: "b"},
		{name: "enum", input: []*string{&a, &b, nil}, arg: "b", isEnum: true},
		{name: "float", input: []float64{1.0, 2.0, math.NaN()}, arg: 2.0},
	}

	for _, comp := range comparisons {
		for _, tc := range table {
			t.Run(fmt.Sprintf("%s, %s", tc.name, comp.operation), func(t *testing.T) {
				enums := map[string][]string{}
				if tc.isEnum {
					enums["COL1"] = nil
				}
				input := qframe.New(map[string]interface{}{"COL1": tc.input}, newqf.Enums(enums))
				output := input.Filter(qframe.Filter{Column: "COL1", Comparator: comp.operation, Arg: tc.arg})
				assertNotErr(t, output.Err)
				if output.Len() != comp.expectCount {
					fmt.Println(output.String())
					t.Errorf("Unexpected frame length: %d", output.Len())
				}
			})
		}
	}
}

func TestQFrame_FilterColColNull(t *testing.T) {
	// For null columns all comparisons will always produce false except for != which will always produce true
	comparisons := []struct {
		operation   string
		expectCount int
	}{
		{operation: "<", expectCount: 0},
		{operation: ">=", expectCount: 1},
		{operation: "=", expectCount: 1},
		{operation: "!=", expectCount: 3},
	}

	a, b := "a", "b"
	table := []struct {
		name      string
		inputCol1 interface{}
		inputCol2 interface{}
		isEnum    bool
	}{
		{name: "string", inputCol1: []*string{&a, &b, nil, nil}, inputCol2: []*string{&a, nil, nil, &b}},
		{name: "enum", inputCol1: []*string{&a, &b, nil, nil}, inputCol2: []*string{&a, nil, nil, &b}, isEnum: true},
		{name: "enum", inputCol1: []float64{1.0, 2.0, math.NaN(), math.NaN()}, inputCol2: []float64{1.0, math.NaN(), math.NaN(), 2.0}},
	}

	for _, comp := range comparisons {
		for _, tc := range table {
			t.Run(fmt.Sprintf("%s, %s", tc.name, comp.operation), func(t *testing.T) {
				enums := map[string][]string{}
				if tc.isEnum {
					enums["COL1"] = nil
					enums["COL2"] = nil
				}
				input := qframe.New(map[string]interface{}{"COL1": tc.inputCol1, "COL2": tc.inputCol2}, newqf.Enums(enums))
				output := input.Filter(qframe.Filter{Column: "COL1", Comparator: comp.operation, Arg: col("COL2")})
				assertNotErr(t, output.Err)
				if output.Len() != comp.expectCount {
					fmt.Println(output.String())
					t.Errorf("Unexpected frame length: %d", output.Len())
				}
			})
		}
	}
}

func TestQFrame_FilterIsNull(t *testing.T) {
	a, b := "a", "b"
	table := []struct {
		input     interface{}
		expected  interface{}
		isEnum    bool
		inverse   bool
		operation string
	}{
		{operation: "isnull", input: []*string{&a, nil, nil, &b}, expected: []*string{nil, nil}},
		{operation: "isnotnull", input: []*string{&a, nil, nil, &b}, expected: []*string{nil, nil}, inverse: true},
		{operation: "isnotnull", input: []*string{&a, nil, nil, &b}, expected: []*string{&a, &b}},
		{operation: "isnull", input: []*string{&a, nil, nil, &b}, expected: []*string{&a, &b}, inverse: true},
		{operation: "isnull", input: []*string{&a, nil, nil, &b}, expected: []*string{nil, nil}, isEnum: true},
		{operation: "isnotnull", input: []*string{&a, nil, nil, &b}, expected: []*string{&a, &b}, isEnum: true},
		{operation: "isnull", input: []float64{1, math.NaN(), 2}, expected: []float64{math.NaN()}},
		{operation: "isnotnull", input: []float64{1, math.NaN(), 2}, expected: []float64{1, 2}},
		{operation: "isnull", input: []int{1, 2, 3}, expected: []int{}},
		{operation: "isnotnull", input: []int{1, 2, 3}, expected: []int{1, 2, 3}},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("%v, %s", reflect.TypeOf(tc.input), tc.operation), func(t *testing.T) {
			enums := map[string][]string{}
			if tc.isEnum {
				enums["COL1"] = nil
			}
			input := qframe.New(map[string]interface{}{"COL1": tc.input}, newqf.Enums(enums))
			expected := qframe.New(map[string]interface{}{"COL1": tc.expected}, newqf.Enums(enums))
			output := input.Filter(qframe.Filter{Column: "COL1", Comparator: tc.operation, Inverse: tc.inverse})
			assertNotErr(t, output.Err)
			assertEquals(t, expected, output)
		})
	}
}

func TestQFrame_FilterNullArg(t *testing.T) {
	// This should result in an error
	table := []struct {
		name   string
		input  interface{}
		isEnum bool
		arg    interface{}
	}{
		{name: "string", input: []string{"a"}, arg: nil},
		{name: "enum", input: []string{"a"}, arg: nil, isEnum: true},
		{name: "float", input: []float64{1.0}, arg: math.NaN()},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			enums := map[string][]string{}
			if tc.isEnum {
				enums["COL1"] = nil
			}

			input := qframe.New(map[string]interface{}{"COL1": tc.input}, newqf.Enums(enums))
			output := input.Filter(qframe.Filter{Column: "COL1", Comparator: "<", Arg: tc.arg})
			assertErr(t, output.Err, "filter")
		})
	}
}

func TestQFrame_FilterAgainstColumn(t *testing.T) {
	table := []struct {
		name       string
		comparator interface{}
		input      map[string]interface{}
		expected   map[string]interface{}
		configs    []newqf.ConfigFunc
	}{
		{
			name:       "built in int compare",
			comparator: ">",
			input:      map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []int{10, 1, 10}},
			expected:   map[string]interface{}{"COL1": []int{1, 3}, "COL2": []int{10, 10}}},
		{
			name:       "int with float compare possible",
			comparator: "=",
			input:      map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []float64{1.0, 2.5, 3.0}},
			expected:   map[string]interface{}{"COL1": []int{1, 3}, "COL2": []float64{1.0, 3.0}}},
		{
			name:       "float with int compare possible",
			comparator: "=",
			input:      map[string]interface{}{"COL1": []float64{1.0, 2.5, 3.0}, "COL2": []int{1, 2, 3}},
			expected:   map[string]interface{}{"COL1": []float64{1.0, 3.0}, "COL2": []int{1, 3}}},
		{
			name:       "int with float neq NaN compare possible",
			comparator: "!=",
			input:      map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []float64{1.0, math.NaN(), 3.0}},
			expected:   map[string]interface{}{"COL1": []int{2}, "COL2": []float64{math.NaN()}}},
		{
			name:       "float with int neq NaN compare possible",
			comparator: "!=",
			input:      map[string]interface{}{"COL1": []float64{1.0, math.NaN(), 3.0}, "COL2": []int{1, 2, 3}},
			expected:   map[string]interface{}{"COL1": []float64{math.NaN()}, "COL2": []int{2}}},
		{
			name:       "custom int compare",
			comparator: func(a, b int) bool { return a > b },
			input:      map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []int{10, 1, 10}},
			expected:   map[string]interface{}{"COL1": []int{1, 3}, "COL2": []int{10, 10}}},
		{
			name:       "built in bool compare",
			comparator: "=",
			input:      map[string]interface{}{"COL1": []bool{true, false, false}, "COL2": []bool{true, true, false}},
			expected:   map[string]interface{}{"COL1": []bool{true, false}, "COL2": []bool{true, false}}},
		{
			name:       "custom bool compare",
			comparator: func(a, b bool) bool { return a == b },
			input:      map[string]interface{}{"COL1": []bool{true, false, false}, "COL2": []bool{true, true, false}},
			expected:   map[string]interface{}{"COL1": []bool{true, false}, "COL2": []bool{true, false}}},
		{
			name:       "built in float compare",
			comparator: "<",
			input:      map[string]interface{}{"COL1": []float64{1, 2, 3}, "COL2": []float64{10, 1, 10}},
			expected:   map[string]interface{}{"COL1": []float64{2}, "COL2": []float64{1}}},
		{
			name:       "custom float compare",
			comparator: func(a, b float64) bool { return a < b },
			input:      map[string]interface{}{"COL1": []float64{1, 2, 3}, "COL2": []float64{10, 1, 10}},
			expected:   map[string]interface{}{"COL1": []float64{2}, "COL2": []float64{1}}},
		{
			name:       "built in string compare",
			comparator: "<",
			input:      map[string]interface{}{"COL1": []string{"a", "b", "c"}, "COL2": []string{"o", "a", "q"}},
			expected:   map[string]interface{}{"COL1": []string{"b"}, "COL2": []string{"a"}}},
		{
			name:       "custom string compare",
			comparator: func(a, b *string) bool { return *a < *b },
			input:      map[string]interface{}{"COL1": []string{"a", "b", "c"}, "COL2": []string{"o", "a", "q"}},
			expected:   map[string]interface{}{"COL1": []string{"b"}, "COL2": []string{"a"}}},
		{
			name:       "built in enum compare",
			comparator: "<",
			input:      map[string]interface{}{"COL1": []string{"a", "b", "c"}, "COL2": []string{"o", "a", "q"}},
			expected:   map[string]interface{}{"COL1": []string{"b"}, "COL2": []string{"a"}}},
		{
			name:       "custom enum compare",
			comparator: func(a, b *string) bool { return *a < *b },
			input:      map[string]interface{}{"COL1": []string{"a", "b", "c"}, "COL2": []string{"o", "a", "q"}},
			expected:   map[string]interface{}{"COL1": []string{"b"}, "COL2": []string{"a"}},
			configs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{
				"COL1": {"a", "b", "c", "o", "q"},
				"COL2": {"a", "b", "c", "o", "q"},
			})}},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("Filter %s", tc.name), func(t *testing.T) {
			input := qframe.New(tc.input, tc.configs...)
			output := input.Filter(qframe.Filter{Comparator: tc.comparator, Column: "COL2", Arg: col("COL1")})
			expected := qframe.New(tc.expected, tc.configs...)
			assertEquals(t, expected, output)
		})
	}
}

func TestQFrame_Sort(t *testing.T) {
	a, b := "a", "b"
	table := []struct {
		orders   []qframe.Order
		expected qframe.QFrame
		input    map[string]interface{}
		configs  []newqf.ConfigFunc
	}{
		{
			orders: []qframe.Order{{Column: "COL1"}},
			expected: qframe.New(map[string]interface{}{
				"COL1": []int{0, 1, 2, 3},
				"COL2": []int{3, 2, 1, 1}})},
		{
			orders: []qframe.Order{{Column: "COL1", Reverse: true}},
			expected: qframe.New(map[string]interface{}{
				"COL1": []int{3, 2, 1, 0},
				"COL2": []int{1, 1, 2, 3}})},
		{
			orders: []qframe.Order{{Column: "COL2"}, {Column: "COL1"}},
			expected: qframe.New(map[string]interface{}{
				"COL1": []int{2, 3, 1, 0},
				"COL2": []int{1, 1, 2, 3}})},
		{
			orders: []qframe.Order{{Column: "COL1"}},
			expected: qframe.New(map[string]interface{}{
				"COL1": []bool{false, true, true}}),
			input: map[string]interface{}{
				"COL1": []bool{true, false, true}}},
		{
			orders: []qframe.Order{{Column: "COL1"}},
			expected: qframe.New(map[string]interface{}{
				"COL1": []*string{nil, &b, &a}},
				newqf.Enums(map[string][]string{"COL1": {"b", "a"}})),
			input: map[string]interface{}{
				"COL1": []*string{&b, nil, &a}},
			configs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": {"b", "a"}})}},
		{
			orders: []qframe.Order{{Column: "COL1", NullLast: true}},
			expected: qframe.New(map[string]interface{}{
				"COL1": []*string{&b, &a, nil}},
				newqf.Enums(map[string][]string{"COL1": {"b", "a"}})),
			input: map[string]interface{}{
				"COL1": []*string{&b, nil, &a}},
			configs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": {"b", "a"}})}},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Sort %d", i), func(t *testing.T) {
			if tc.input == nil {
				tc.input = map[string]interface{}{
					"COL1": []int{0, 1, 3, 2},
					"COL2": []int{3, 2, 1, 1}}
			}
			a := qframe.New(tc.input, tc.configs...)
			b := a.Sort(tc.orders...)
			assertEquals(t, tc.expected, b)
		})
	}
}

func TestQFrame_SortNull(t *testing.T) {
	a, b, c := "a", "b", "c"
	stringIn := map[string]interface{}{
		"COL1": []*string{&b, nil, &a, nil, &c, &a, nil},
	}

	floatIn := map[string]interface{}{
		"COL1": []float64{1.0, math.NaN(), -1.0, math.NaN()},
	}

	table := []struct {
		in       map[string]interface{}
		orders   []qframe.Order
		expected map[string]interface{}
	}{
		{
			stringIn,
			[]qframe.Order{{Column: "COL1"}},
			map[string]interface{}{
				"COL1": []*string{nil, nil, nil, &a, &a, &b, &c},
			},
		},
		{
			stringIn,
			[]qframe.Order{{Column: "COL1", NullLast: true}},
			map[string]interface{}{
				"COL1": []*string{&a, &a, &b, &c, nil, nil, nil},
			},
		},
		{
			stringIn,
			[]qframe.Order{{Column: "COL1", Reverse: true}},
			map[string]interface{}{
				"COL1": []*string{&c, &b, &a, &a, nil, nil, nil},
			},
		},
		{
			floatIn,
			[]qframe.Order{{Column: "COL1"}},
			map[string]interface{}{
				"COL1": []float64{math.NaN(), math.NaN(), -1.0, 1.0},
			},
		},
		{
			floatIn,
			[]qframe.Order{{Column: "COL1", Reverse: true}},
			map[string]interface{}{
				"COL1": []float64{1.0, -1.0, math.NaN(), math.NaN()},
			},
		},
		{
			floatIn,
			[]qframe.Order{{Column: "COL1", NullLast: true}},
			map[string]interface{}{
				"COL1": []float64{-1.0, 1.0, math.NaN(), math.NaN()},
			},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Sort %d", i), func(t *testing.T) {
			in := qframe.New(tc.in)
			out := in.Sort(tc.orders...)
			assertNotErr(t, out.Err)
			assertEquals(t, qframe.New(tc.expected), out)
		})
	}
}

func TestQFrame_SortStability(t *testing.T) {
	a := qframe.New(map[string]interface{}{
		"COL1": []int{0, 1, 3, 2},
		"COL2": []int{1, 1, 1, 1},
	})

	table := []struct {
		orders   []qframe.Order
		expected qframe.QFrame
	}{
		{
			[]qframe.Order{{Column: "COL2", Reverse: true}, {Column: "COL1"}},
			qframe.New(map[string]interface{}{
				"COL1": []int{0, 1, 2, 3},
				"COL2": []int{1, 1, 1, 1}}),
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Sort %d", i), func(t *testing.T) {
			b := a.Sort(tc.orders...)
			assertEquals(t, tc.expected, b)
		})
	}
}

func TestQFrame_Distinct(t *testing.T) {
	table := []struct {
		input    map[string]interface{}
		expected map[string]interface{}
		columns  []string
	}{
		{
			input: map[string]interface{}{
				"COL1": []int{0, 1, 0, 1},
				"COL2": []int{0, 1, 0, 1}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1},
				"COL2": []int{0, 1}},
			columns: []string{"COL1", "COL2"},
		},
		{
			input: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			expected: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			columns: []string{"COL1", "COL2"},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Distinct %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			out := in.Distinct()
			assertEquals(t, qframe.New(tc.expected), out.Sort(colNamesToOrders("COL1", "COL2")...))
		})
	}
}

func incSlice(size, step int) []int {
	result := make([]int, size)
	for i := range result {
		result[i] = step * i
	}
	return result
}

func TestQFrame_GroupByAggregate(t *testing.T) {
	ownSum := func(col []int) int {
		result := 0
		for _, x := range col {
			result += x
		}
		return result
	}

	table := []struct {
		name         string
		input        map[string]interface{}
		expected     map[string]interface{}
		groupColumns []string
		aggregations []qframe.Aggregation
	}{
		{
			name: "built in aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 2},
				"COL2": []int{0, 0, 1, 1},
				"COL3": []int{1, 2, 5, 7}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1, 2},
				"COL2": []int{0, 1, 1},
				"COL3": []int{3, 5, 7}},
			groupColumns: []string{"COL1", "COL2"},
			aggregations: []qframe.Aggregation{{Fn: "sum", Column: "COL3"}},
		},
		{
			name: "built in max aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 1, 2},
				"COL2": []int{1, 2, 3, 5, 7}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1, 2},
				"COL2": []int{2, 5, 7}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "max", Column: "COL2"}},
		},
		{
			name: "built in min aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 1, 2},
				"COL2": []int{1, 2, 3, 5, 7}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1, 2},
				"COL2": []int{1, 3, 7}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "min", Column: "COL2"}},
		},
		{
			name: "combined max and min aggregation",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 1, 2},
				"COL2": []int{1, 2, 3, 5, 7}},
			expected: map[string]interface{}{
				"COL1":     []int{0, 1, 2},
				"min_COL2": []int{1, 3, 7},
				"max_COL2": []int{2, 5, 7}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{
				{Fn: "max", Column: "COL2", As: "max_COL2"},
				{Fn: "min", Column: "COL2", As: "min_COL2"},
			},
		},
		{
			name: "user defined aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 1},
				"COL2": []int{1, 2, 5, 7}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1},
				"COL2": []int{3, 12}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: ownSum, Column: "COL2"}},
		},
		{
			name: "empty qframe",
			input: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			expected: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "sum", Column: "COL2"}},
		},
		{
			name: "empty max qframe",
			input: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			expected: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "max", Column: "COL2"}},
		},
		{
			name: "empty min qframe",
			input: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			expected: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "min", Column: "COL2"}},
		},
		{
			// This will trigger hash table relocations
			name: "high cardinality grouping column",
			input: map[string]interface{}{
				"COL1": incSlice(1000, 1),
				"COL2": incSlice(1000, 2)},
			expected: map[string]interface{}{
				"COL1": incSlice(1000, 1),
				"COL2": incSlice(1000, 2)},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "sum", Column: "COL2"}},
		},
		{
			name:         "aggregate booleans over all rows",
			input:        map[string]interface{}{"COL1": []bool{true, false, true}},
			expected:     map[string]interface{}{"COL1": []bool{true}},
			groupColumns: []string{},
			aggregations: []qframe.Aggregation{{Fn: "majority", Column: "COL1"}},
		},
		{
			name:         "group by booleans",
			input:        map[string]interface{}{"COL1": []bool{true, false, true}, "COL2": []int{1, 2, 3}},
			expected:     map[string]interface{}{"COL1": []bool{false, true}, "COL2": []int{2, 4}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "sum", Column: "COL2"}},
		},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("GroupByAggregate %s", tc.name), func(t *testing.T) {
			in := qframe.New(tc.input)
			out := in.GroupBy(groupby.Columns(tc.groupColumns...)).Aggregate(tc.aggregations...)

			assertEquals(t, qframe.New(tc.expected), out.Sort(colNamesToOrders(tc.groupColumns...)...))
		})
	}
}

func TestQFrame_GroupByAggregateFloats(t *testing.T) {
	ownSum := func(col []float64) float64 {
		result := 0.0
		for _, x := range col {
			result += x
		}
		return result
	}

	table := []struct {
		name         string
		input        map[string]interface{}
		expected     map[string]interface{}
		groupColumns []string
		aggregations []qframe.Aggregation
	}{
		{
			name: "built in aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 2},
				"COL2": []int{0, 0, 1, 1},
				"COL3": []float64{1.0, 2.0, 5.0, 7.0}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1, 2},
				"COL2": []int{0, 1, 1},
				"COL3": []float64{3.0, 5.0, 7.0}},
			groupColumns: []string{"COL1", "COL2"},
			aggregations: []qframe.Aggregation{{Fn: "sum", Column: "COL3"}},
		},
		{
			name: "built in count aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 1, 2},
				"COL2": []float64{1.0, 2.0, 3.0, 5.0, 7.0}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1, 2},
				"COL2": []int{2, 2, 1}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "count", Column: "COL2"}},
		},
		{
			name: "built in max aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 1, 2},
				"COL2": []float64{1.0, 2.0, 3.0, 5.0, 7.0}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1, 2},
				"COL2": []float64{2.0, 5.0, 7.0}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "max", Column: "COL2"}},
		},
		{
			name: "built in min aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 1, 2},
				"COL2": []float64{1.0, 2.0, 3.0, 5.0, 7.0}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1, 2},
				"COL2": []float64{1.0, 3.0, 7.0}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "min", Column: "COL2"}},
		},
		{
			name: "user defined aggregation function",
			input: map[string]interface{}{
				"COL1": []int{0, 0, 1, 1},
				"COL2": []float64{1.0, 2.0, 5.0, 7.0}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1},
				"COL2": []float64{3.0, 12.0}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: ownSum, Column: "COL2"}},
		},
		{
			name: "empty qframe",
			input: map[string]interface{}{
				"COL1": []int{},
				"COL2": []float64{}},
			expected: map[string]interface{}{
				"COL1": []int{},
				"COL2": []float64{}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "sum", Column: "COL2"}},
		},
		{
			name: "empty max qframe",
			input: map[string]interface{}{
				"COL1": []int{},
				"COL2": []float64{}},
			expected: map[string]interface{}{
				"COL1": []int{},
				"COL2": []float64{}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "max", Column: "COL2"}},
		},
		{
			name: "empty min qframe",
			input: map[string]interface{}{
				"COL1": []int{},
				"COL2": []float64{}},
			expected: map[string]interface{}{
				"COL1": []int{},
				"COL2": []float64{}},
			groupColumns: []string{"COL1"},
			aggregations: []qframe.Aggregation{{Fn: "min", Column: "COL2"}},
		},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("GroupByAggregate %s", tc.name), func(t *testing.T) {
			in := qframe.New(tc.input)
			out := in.GroupBy(groupby.Columns(tc.groupColumns...)).Aggregate(tc.aggregations...)

			assertEquals(t, qframe.New(tc.expected), out.Sort(colNamesToOrders(tc.groupColumns...)...))
		})
	}
}

func TestQFrame_RollingWindow(t *testing.T) {
	sum := func(col []int) int {
		result := 0
		for _, x := range col {
			result += x
		}
		return result
	}

	table := []struct {
		name     string
		input    map[string]interface{}
		expected map[string]interface{}
		fn       interface{}
		configs  []rolling.ConfigFunc
	}{
		{
			name:     "default one element window",
			input:    map[string]interface{}{"source": []int{1, 2, 3}},
			expected: map[string]interface{}{"destination": []int{1, 2, 3}},
			fn:       sum,
		},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("Rolling %s", tc.name), func(t *testing.T) {
			in := qframe.New(tc.input)

			out := in.Rolling(tc.fn, "destination", "source")

			assertEquals(t, qframe.New(tc.expected), out.Select("destination"))
		})
	}
}

func colNamesToOrders(colNames ...string) []qframe.Order {
	result := make([]qframe.Order, len(colNames))
	for i, name := range colNames {
		result[i] = qframe.Order{Column: name}
	}
	return result
}

func TestQFrame_Select(t *testing.T) {
	table := []struct {
		input      map[string]interface{}
		expected   map[string]interface{}
		selectCols []string
	}{
		{
			input: map[string]interface{}{
				"COL1": []int{0, 1},
				"COL2": []int{1, 2}},
			expected: map[string]interface{}{
				"COL1": []int{0, 1}},
			selectCols: []string{"COL1"},
		},
		{
			input: map[string]interface{}{
				"COL1": []int{0, 1},
				"COL2": []int{1, 2}},
			expected:   map[string]interface{}{},
			selectCols: []string{},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Select %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			out := in.Select(tc.selectCols...)
			assertEquals(t, qframe.New(tc.expected), out)
		})
	}
}

func TestQFrame_Slice(t *testing.T) {
	table := []struct {
		input    map[string]interface{}
		expected map[string]interface{}
		start    int
		end      int
		err      string
	}{
		{
			input: map[string]interface{}{
				"COL1": []float64{0.0, 1.5, 2.5, 3.5},
				"COL2": []int{1, 2, 3, 4}},
			expected: map[string]interface{}{
				"COL1": []float64{1.5, 2.5},
				"COL2": []int{2, 3}},
			start: 1,
			end:   3},
		{
			input: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			expected: map[string]interface{}{
				"COL1": []int{},
				"COL2": []int{}},
			start: 0,
			end:   0},
		{
			input: map[string]interface{}{"COL1": []int{1, 2}},
			start: -1,
			end:   0,
			err:   "start must be non negative"},
		{
			input: map[string]interface{}{"COL1": []int{1, 2}},
			start: 0,
			end:   3,
			err:   "end must not be greater than"},
		{
			input: map[string]interface{}{"COL1": []int{1, 2}},
			start: 2,
			end:   1,
			err:   "start must not be greater than end"},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Slice %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			out := in.Slice(tc.start, tc.end)
			if tc.err != "" {
				assertErr(t, out.Err, tc.err)
			} else {
				assertEquals(t, qframe.New(tc.expected), out)
			}
		})
	}
}

func TestQFrame_ReadCSV(t *testing.T) {
	/*
		Pandas reference
		>>> data = """
		... foo,bar,baz,qux
		... ccc,,,www
		... aaa,3.25,7,"""
		>>> pd.read_csv(StringIO(data))
		   foo   bar  baz  qux
		0  ccc   NaN  NaN  www
		1  aaa  3.25  7.0  NaN
	*/
	a, b, c, empty := "a", "b", "c", ""
	table := []struct {
		name             string
		inputHeaders     []string
		inputData        string
		emptyNull        bool
		ignoreEmptyLines bool
		expected         map[string]interface{}
		types            map[string]string
		expectedErr      string
		delimiter        byte
		rowDelimiter     string
	}{
		{
			name:         "base",
			inputHeaders: []string{"foo", "bar"},
			inputData:    "1,2\n3,4\n",
			expected: map[string]interface{}{
				"foo": []int{1, 3},
				"bar": []int{2, 4}},
		},
		{
			name:         "tab delimiter",
			inputHeaders: []string{"foo", "bar"},
			inputData:    "1\t2\n3\t4\n",
			expected: map[string]interface{}{
				"foo": []int{1, 3},
				"bar": []int{2, 4}},
			delimiter: '\t',
		},
		{
			name:             "empty lines ignored, multiple columns",
			inputHeaders:     []string{"foo", "bar"},
			inputData:        "1,2\n\n3,4\n",
			ignoreEmptyLines: true,
			expected: map[string]interface{}{
				"foo": []int{1, 3},
				"bar": []int{2, 4}},
		},
		{
			name:         "column count mismatch results in error",
			inputHeaders: []string{"foo", "bar"},
			inputData:    "1,2\n33\n3,4\n",
			expectedErr:  "Wrong number of columns",
		},
		{
			name:             "empty lines kept, single column",
			inputHeaders:     []string{"foo"},
			inputData:        "1\n\n3\n",
			ignoreEmptyLines: false,
			expected: map[string]interface{}{
				"foo": []float64{1, math.NaN(), 3}},
		},
		{
			name:         "mixed",
			inputHeaders: []string{"int", "float", "bool", "string"},
			inputData:    "1,2.5,true,hello\n10,20.5,false,\"bye,\n bye\"",
			expected: map[string]interface{}{
				"int":    []int{1, 10},
				"float":  []float64{2.5, 20.5},
				"bool":   []bool{true, false},
				"string": []string{"hello", "bye,\n bye"}},
		},
		{
			name:         "null string",
			inputHeaders: []string{"foo", "bar"},
			inputData:    "a,b\n,c",
			emptyNull:    true,
			expected: map[string]interface{}{
				"foo": []*string{&a, nil},
				"bar": []*string{&b, &c}},
		},
		{
			name:         "empty string",
			inputHeaders: []string{"foo", "bar"},
			inputData:    "a,b\n,c",
			emptyNull:    false,
			expected: map[string]interface{}{
				"foo": []*string{&a, &empty},
				"bar": []*string{&b, &c}},
		},
		{
			name:         "NaN float",
			inputHeaders: []string{"foo", "bar"},
			inputData:    "1.5,3.0\n,2.0",
			expected: map[string]interface{}{
				"foo": []float64{1.5, math.NaN()},
				"bar": []float64{3.0, 2.0}},
		},
		{
			name:         "Int to float type success",
			inputHeaders: []string{"foo"},
			inputData:    "3\n2",
			expected:     map[string]interface{}{"foo": []float64{3.0, 2.0}},
			types:        map[string]string{"foo": "float"},
		},
		{
			name:         "Bool to string success",
			inputHeaders: []string{"foo"},
			inputData:    "true\nfalse",
			expected:     map[string]interface{}{"foo": []string{"true", "false"}},
			types:        map[string]string{"foo": "string"},
		},
		{
			name:         "Int to string success",
			inputHeaders: []string{"foo"},
			inputData:    "123\n456",
			expected:     map[string]interface{}{"foo": []string{"123", "456"}},
			types:        map[string]string{"foo": "string"},
		},
		{
			name:         "Float to int failure",
			inputHeaders: []string{"foo"},
			inputData:    "1.23\n4.56",
			expectedErr:  "int",
			types:        map[string]string{"foo": "int"},
		},
		{
			name:         "String to bool failure",
			inputHeaders: []string{"foo"},
			inputData:    "abc\ndef",
			expectedErr:  "bool",
			types:        map[string]string{"foo": "bool"},
		},
		{
			name:         "String to float failure",
			inputHeaders: []string{"foo"},
			inputData:    "abc\ndef",
			expectedErr:  "float",
			types:        map[string]string{"foo": "float"},
		},
		{
			name:         "Enum with null value",
			inputHeaders: []string{"foo"},
			inputData:    "a\n\nc",
			types:        map[string]string{"foo": "enum"},
			emptyNull:    true,
			expected:     map[string]interface{}{"foo": []*string{&a, nil, &c}},
		},
		{
			name:         "CRLF",
			rowDelimiter: "\r\n",
			inputHeaders: []string{"a_string", "b_number", "c_string"},
			inputData:    "abc,1,cde\r\n,1,cde\r\nabc,1,\r\n",
			emptyNull:    false,
			expected: map[string]interface{}{
				"a_string": []string{"abc", "", "abc"},
				"b_number": []int{1, 1, 1},
				"c_string": []string{"cde", "cde", ""}},
		},
		{
			name:         "Duplicate column error",
			inputHeaders: []string{"foo", "bar", "foo"},
			inputData:    "a,b,c",
			expectedErr:  "Duplicate columns",
		},
		{
			name:         "CRLF combined with quotes",
			inputHeaders: []string{"foo"},
			inputData:    "\"a\"\r\n\"b\"\r\n",
			expected: map[string]interface{}{
				"foo": []string{"a", "b"},
			},
		},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("ReadCSV %s", tc.name), func(t *testing.T) {
			if tc.delimiter == 0 {
				tc.delimiter = ','
			}

			if tc.rowDelimiter == "" {
				tc.rowDelimiter = "\n"
			}

			input := strings.Join(tc.inputHeaders, string([]byte{tc.delimiter})) + tc.rowDelimiter + tc.inputData
			out := qframe.ReadCSV(strings.NewReader(input),
				csv.EmptyNull(tc.emptyNull),
				csv.Types(tc.types),
				csv.IgnoreEmptyLines(tc.ignoreEmptyLines),
				csv.Delimiter(tc.delimiter))
			if tc.expectedErr != "" {
				assertErr(t, out.Err, tc.expectedErr)
			} else {
				assertNotErr(t, out.Err)

				enums := make(map[string][]string)
				for k, v := range tc.types {
					if v == "enum" {
						enums[k] = nil
					}
				}

				assertEquals(t, qframe.New(tc.expected, newqf.ColumnOrder(tc.inputHeaders...), newqf.Enums(enums)), out)
			}
		})
	}
}

// EOFReader is a mock to simulate io.Reader implementation that returns data together with err == io.EOF.
type EOFReader struct {
	s      string
	isRead bool
}

func (r *EOFReader) Read(b []byte) (int, error) {
	if r.isRead {
		return 0, io.EOF
	}

	if len(b) < len(r.s) {
		// This is just a mock, don't bother supporting more complicated cases
		log.Fatalf("Buffer len too short for string: %d < %d", len(b), len(r.s))
	}

	count := copy(b, []byte(r.s))
	r.isRead = true
	return count, io.EOF
}

func TestQFrame_ReadCSVCombinedReadAndEOF(t *testing.T) {
	input := `abc,def
1,2
3,4
`
	out := qframe.ReadCSV(&EOFReader{s: input})
	expected := qframe.New(map[string]interface{}{"abc": []int{1, 3}, "def": []int{2, 4}}, newqf.ColumnOrder("abc", "def"))
	assertEquals(t, expected, out)
}

func TestQFrame_ReadCSVNoRowsNoTypes(t *testing.T) {
	// Should be possible to test an empty, non typed column against anything.
	input := `abc,def`

	t.Run("Empty column comparable to anything when not typed", func(t *testing.T) {
		out := qframe.ReadCSV(strings.NewReader(input))
		assertNotErr(t, out.Err)

		// Filtering
		out = out.Filter(qframe.Filter{Column: "abc", Comparator: ">", Arg: "b"})
		assertNotErr(t, out.Err)

		// Aggregation
		e := qframe.Expr("abs", types.ColumnName("abc"))
		assertNotErr(t, e.Err())
		out = out.Eval("abc", e)
		assertNotErr(t, out.Err)
	})

	t.Run("Empty column not comparable to anything when typed", func(t *testing.T) {
		out := qframe.ReadCSV(strings.NewReader(input), csv.Types(map[string]string{"abc": "int"}))
		out = out.Filter(qframe.Filter{Column: "abc", Comparator: ">", Arg: "b"})
		assertErr(t, out.Err, "type")
	})
}

func TestQFrame_ReadCSVNoHeader(t *testing.T) {
	input := `1,2`

	out := qframe.ReadCSV(strings.NewReader(input), csv.Headers([]string{"abc", "def"}))
	assertNotErr(t, out.Err)

	expected := qframe.New(map[string]interface{}{"abc": []int{1}, "def": []int{2}})
	assertNotErr(t, out.Err)
	assertEquals(t, expected, out)
}

func TestQFrame_Enum(t *testing.T) {
	mon, tue, wed, thu, fri, sat, sun := "mon", "tue", "wed", "thu", "fri", "sat", "sun"
	t.Run("Applies specified order", func(t *testing.T) {
		input := `day
tue
mon
sat
wed
sun
thu
mon
thu

`
		out := qframe.ReadCSV(
			strings.NewReader(input),
			csv.EmptyNull(true),
			csv.Types(map[string]string{"day": "enum"}),
			csv.EnumValues(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))
		out = out.Sort(qframe.Order{Column: "day"})
		expected := qframe.New(
			map[string]interface{}{"day": []*string{nil, &mon, &mon, &tue, &wed, &thu, &thu, &sat, &sun}},
			newqf.Enums(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))

		assertNotErr(t, out.Err)
		assertEquals(t, expected, out)
	})

	t.Run("Orders given for non-enum columns results in error", func(t *testing.T) {
		input := `day
tue
`
		out := qframe.ReadCSV(
			strings.NewReader(input),
			csv.Types(map[string]string{"day": "enum"}),
			csv.EnumValues(map[string][]string{"week": {"foo", "bar"}}))
		assertErr(t, out.Err, "Enum values specified for non enum column")
	})

	t.Run("Wont accept unknown values in strict mode", func(t *testing.T) {
		input := `day
tue
mon
foo
`
		out := qframe.ReadCSV(
			strings.NewReader(input),
			csv.Types(map[string]string{"day": "enum"}),
			csv.EnumValues(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))

		assertErr(t, out.Err, "unknown enum value")
	})

	t.Run("Fails with too high cardinality column", func(t *testing.T) {
		input := make([]string, 0)
		for i := 0; i < 256; i++ {
			input = append(input, strconv.Itoa(i))
		}

		out := qframe.New(
			map[string]interface{}{"foo": input},
			newqf.Enums(map[string][]string{"foo": nil}))

		assertErr(t, out.Err, "max cardinality")
	})

	t.Run("Fails when enum values specified for non enum column", func(t *testing.T) {
		input := `day
tue
`

		out := qframe.ReadCSV(
			strings.NewReader(input),
			csv.EnumValues(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))

		assertErr(t, out.Err, "specified for non enum column")
	})

	t.Run("Wont accept unknown filter values in strict mode", func(t *testing.T) {
		input := `day
tue
mon
`
		out := qframe.ReadCSV(
			strings.NewReader(input),
			csv.Types(map[string]string{"day": "enum"}),
			csv.EnumValues(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))
		out = out.Filter(qframe.Filter{Column: "day", Comparator: ">", Arg: "foo"})
		assertErr(t, out.Err, "unknown enum value")
	})

	t.Run("Will accept unknown filter values in non-strict mode", func(t *testing.T) {
		input := `day
tue
mon
`
		out := qframe.ReadCSV(
			strings.NewReader(input),
			csv.Types(map[string]string{"day": "enum"}))
		out = out.Filter(qframe.Filter{Column: "day", Comparator: ">", Arg: "foo"})
		assertNotErr(t, out.Err)
	})

	t.Run("Will accept and eval to true for neq and unknown filter value in non-strict mode", func(t *testing.T) {
		input := `day
tue
mon
`
		out := qframe.ReadCSV(
			strings.NewReader(input),
			csv.Types(map[string]string{"day": "enum"}))
		out = out.Filter(qframe.Filter{Column: "day", Comparator: "!=", Arg: "foo"})
		assertNotErr(t, out.Err)
		assertTrue(t, out.Len() == 2)
	})
}

func TestQFrame_ReadCSVMissingColumnName(t *testing.T) {
	input := `,COL2
a,1.5`
	expectedIn := `COL,COL2
a,1.5`

	out := qframe.ReadCSV(strings.NewReader(input), csv.MissingColumnNameAlias("COL"))
	expected := qframe.ReadCSV(strings.NewReader(expectedIn))
	assertNotErr(t, out.Err)
	assertEquals(t, expected, out)
}

func TestQFrame_ReadCSVDuplicateColumnName(t *testing.T) {
	input := `COL,COL,COL,COL,COL,KOL,KOL
	a,1.5,1.6,1.7,1.8,1.9,2.0`

	expectedIn := `COL,COL0,COL1,COL2,COL3,KOL,KOL0
	a,1.5,1.6,1.7,1.8,1.9,2.0`

	out := qframe.ReadCSV(strings.NewReader(input), csv.RenameDuplicateColumns(true))
	expected := qframe.ReadCSV(strings.NewReader(expectedIn))
	assertNotErr(t, out.Err)
	assertEquals(t, expected, out)
}

func TestQFrame_ReadCSVDuplicateAndEmptyColumnName(t *testing.T) {
	input := `,
a,1.5`

	expectedIn := `COL,COL0
a,1.5`

	out := qframe.ReadCSV(strings.NewReader(input), csv.RenameDuplicateColumns(true), csv.MissingColumnNameAlias("COL"))
	expected := qframe.ReadCSV(strings.NewReader(expectedIn))
	assertNotErr(t, out.Err)
	assertEquals(t, expected, out)
}

func TestQFrame_ReadJSON(t *testing.T) {
	/*
		>>> pd.DataFrame.from_records([dict(a=1.5), dict(a=None)])
			 a
		0  1.5
		1  NaN
		>>> pd.DataFrame.from_records([dict(a=1), dict(a=None)])
			 a
		0  1.0
		1  NaN
		>>> pd.DataFrame.from_records([dict(a=1), dict(a=2)])
		   a
		0  1
		1  2
		>>> pd.DataFrame.from_records([dict(a='foo'), dict(a=None)])
			  a
		0   foo
		1  None
		>>> pd.DataFrame.from_records([dict(a=1.5), dict(a='N/A')])
			 a
		0  1.5
		1  N/A
		>>> x = pd.DataFrame.from_records([dict(a=1.5), dict(a='N/A')])
		>>> x.ix[0]
		a    1.5
		Name: 0, dtype: object
	*/
	testString := "FOO"
	table := []struct {
		input    string
		expected map[string]interface{}
	}{
		{
			input: `[
				{"STRING1": "a", "INT1": 1, "FLOAT1": 1.5, "BOOL1": true},
				{"STRING1": "b", "INT1": 2, "FLOAT1": 2.5, "BOOL1": false}]`,
			expected: map[string]interface{}{
				// NOTE: The integers become floats if not explicitly typed
				"STRING1": []string{"a", "b"}, "INT1": []float64{1, 2}, "FLOAT1": []float64{1.5, 2.5}, "BOOL1": []bool{true, false}},
		},
		{
			input: `[{"STRING1": "FOO"}, {"STRING1": null}]`,
			expected: map[string]interface{}{
				"STRING1": []*string{&testString, nil}},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("FromJSON %d", i), func(t *testing.T) {
			out := qframe.ReadJSON(strings.NewReader(tc.input))
			assertNotErr(t, out.Err)
			assertEquals(t, qframe.New(tc.expected), out)
		})
	}
}

func TestQFrame_ToCSV_ColOrder(t *testing.T) {
	table := []struct {
		input    string
		config   []string
		expected string
		err      string
	}{
		{
			input: `COL1,COL2,COL3
1a,2a,3a
1b,2b,3b`,
			config: []string{"COL1", "COL3", "COL2"},
			expected: `COL1,COL3,COL2
1a,3a,2a
1b,3b,2b`,
			err: "",
		},
		{
			input: `COL1,COL2,COL3
1a,2a,3a
1b,2b,3b`,
			config:   []string{"COL1", "COLX", "COL2"},
			err:      "COLX: column does not exist in QFrame",
			expected: "",
		},
		{
			input: `COL1,COL2,COL3
1a,2a,3a
1b,2b,3b`,
			config:   []string{"COL1", "COL3", "COL2", "COL4"},
			err:      "wrong number of columns: expected: 3",
			expected: "",
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("ToCSV (ordered) %d", i), func(t *testing.T) {
			in := qframe.ReadCSV(strings.NewReader(tc.input))
			assertNotErr(t, in.Err)
			buf := new(bytes.Buffer)
			err := in.ToCSV(buf, csv.Columns(tc.config))
			if tc.err == "" {
				assertNotErr(t, err)
				output := strings.TrimSpace(buf.String())
				if output != tc.expected {
					t.Errorf("CSV columns not in order. \nGot:\n|%s|\nExpected:\n|%s|", output, tc.expected)
				}
			} else {
				assertErr(t, err, tc.err)
			}
		})
	}
}

func TestQFrame_ToCSV(t *testing.T) {
	table := []struct {
		input    map[string]interface{}
		expected string
		header   bool
	}{
		{
			input: map[string]interface{}{
				"STRING1": []string{"a", "b,c"}, "INT1": []int{1, 2}, "FLOAT1": []float64{1.5, 2.5}, "BOOL1": []bool{true, false}},
			expected: `BOOL1,FLOAT1,INT1,STRING1
true,1.5,1,a
false,2.5,2,"b,c"
`,
			header: true,
		},
		{
			input: map[string]interface{}{
				"STRING1": []string{"a", "b,c"}, "INT1": []int{1, 2}, "FLOAT1": []float64{1.5, 2.5}, "BOOL1": []bool{true, false}},
			expected: `true,1.5,1,a
false,2.5,2,"b,c"
`,
			header: false,
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("ToCSV %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			assertNotErr(t, in.Err)

			buf := new(bytes.Buffer)
			err := in.ToCSV(buf,
				csv.Header(tc.header),
			)
			assertNotErr(t, err)

			result := buf.String()
			if result != tc.expected {
				t.Errorf("QFrames not equal. \nGot:\n%s\nExpected:\n%s", result, tc.expected)
			}
		})
	}
}

func TestQFrame_ToFromJSON(t *testing.T) {
	config := []newqf.ConfigFunc{newqf.Enums(map[string][]string{"ENUM": {"aa", "bb"}})}
	data := map[string]interface{}{
		"STRING1": []string{"añ", "bö☺	"}, "FLOAT1": []float64{1.5, 2.5}, "BOOL1": []bool{true, false}, "ENUM": []string{"aa", "bb"}}
	originalDf := qframe.New(data, config...)
	assertNotErr(t, originalDf.Err)

	buf := new(bytes.Buffer)
	err := originalDf.ToJSON(buf)
	assertNotErr(t, err)

	// Map order should be consistent across calls
	for i := 0; i < 10; i++ {
		buf2 := new(bytes.Buffer)
		err := originalDf.ToJSON(buf2)
		assertNotErr(t, err)
		if buf.String() != buf2.String() {
			t.Errorf("%s != %s", buf.String(), buf.String())
		}
	}

	jsonDf := qframe.ReadJSON(buf, config...)
	assertNotErr(t, jsonDf.Err)
	assertEquals(t, originalDf, jsonDf)
}

func TestQFrame_ToJSONNaN(t *testing.T) {
	buf := new(bytes.Buffer)

	// Test the special case NaN, this can currently be encoded but not
	// decoded by the JSON parsers.
	data := map[string]interface{}{"FLOAT1": []float64{1.5, math.NaN()}}
	originalDf := qframe.New(data)
	assertNotErr(t, originalDf.Err)

	err := originalDf.ToJSON(buf)
	assertNotErr(t, err)
	expected := `[{"FLOAT1":1.5},{"FLOAT1":null}]`
	if buf.String() != expected {
		t.Errorf("Not equal: %s ||| %s", buf.String(), expected)
	}
}

func TestQFrame_ToJSONInt(t *testing.T) {
	// The ints should not have decimals when turned into JSON
	data := map[string]interface{}{"INT": []int{1, 2}}
	originalDf := qframe.New(data)
	assertNotErr(t, originalDf.Err)

	buf := new(bytes.Buffer)
	err := originalDf.ToJSON(buf)
	assertNotErr(t, err)
	if buf.String() != `[{"INT":1},{"INT":2}]` {
		t.Errorf("Unexpected JSON string: %s", buf.String())
	}
}

func TestQFrame_FilterEnum(t *testing.T) {
	a, b, c, d, e := "a", "b", "c", "d", "e"
	enums := newqf.Enums(map[string][]string{"COL1": {"a", "b", "c", "d", "e"}})
	in := qframe.New(map[string]interface{}{
		"COL1": []*string{&b, &c, &a, nil, &e, &d, nil}}, enums)

	table := []struct {
		clause   qframe.FilterClause
		expected map[string]interface{}
	}{
		{
			qframe.Filter{Column: "COL1", Comparator: ">", Arg: "b"},
			map[string]interface{}{"COL1": []*string{&c, &e, &d}},
		},
		{
			qframe.Filter{Column: "COL1", Comparator: "in", Arg: []string{"a", "b"}},
			map[string]interface{}{"COL1": []*string{&b, &a}},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Filter enum %d", i), func(t *testing.T) {
			expected := qframe.New(tc.expected, enums)
			out := in.Filter(tc.clause)
			assertEquals(t, expected, out)
		})
	}
}

func TestQFrame_FilterString(t *testing.T) {
	a, b, c, d, e := "a", "b", "c", "d", "e"
	withNil := map[string]interface{}{"COL1": []*string{&b, &c, &a, nil, &e, &d, nil}}

	table := []struct {
		input    map[string]interface{}
		clause   qframe.FilterClause
		expected map[string]interface{}
	}{
		{
			withNil,
			qframe.Filter{Column: "COL1", Comparator: ">", Arg: "b"},
			map[string]interface{}{"COL1": []*string{&c, &e, &d}},
		},
		{
			withNil,
			qframe.Filter{Column: "COL1", Comparator: "<", Arg: "b"},
			map[string]interface{}{"COL1": []*string{&a}},
		},
		{
			withNil,
			qframe.Filter{Column: "COL1", Comparator: "!=", Arg: "a"},
			map[string]interface{}{"COL1": []*string{&b, &c, nil, &e, &d, nil}},
		},
		{
			withNil,
			qframe.Filter{Column: "COL1", Comparator: "like", Arg: "b"},
			map[string]interface{}{"COL1": []*string{&b}},
		},
		{
			withNil,
			qframe.Filter{Column: "COL1", Comparator: "in", Arg: []string{"a", "b"}},
			map[string]interface{}{"COL1": []*string{&b, &a}},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Filter string %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			expected := qframe.New(tc.expected)
			out := in.Filter(tc.clause)
			assertEquals(t, expected, out)
		})
	}
}

func TestQFrame_LikeFilterString(t *testing.T) {
	col1 := []string{"ABC", "AbC", "DEF", "ABCDEF", "abcdef", "FFF", "abc$def", "défåäöΦ"}

	// Add a couple of fields to be able to verify functionality for high cardinality enums
	for i := 0; i < 200; i++ {
		col1 = append(col1, fmt.Sprintf("foo%dbar", i))
	}

	data := map[string]interface{}{"COL1": col1}
	for _, enums := range []map[string][]string{{}, {"COL1": nil}} {
		table := []struct {
			comparator string
			arg        string
			expected   []string
		}{
			// like
			{"like", ".*EF.*", []string{"DEF", "ABCDEF"}},
			{"like", "%EF%", []string{"DEF", "ABCDEF"}},
			{"like", "AB%", []string{"ABC", "ABCDEF"}},
			{"like", "%F", []string{"DEF", "ABCDEF", "FFF"}},
			{"like", "ABC", []string{"ABC"}},
			{"like", "défåäöΦ", []string{"défåäöΦ"}},
			{"like", "%éfåäöΦ", []string{"défåäöΦ"}},
			{"like", "défå%", []string{"défåäöΦ"}},
			{"like", "%éfåäö%", []string{"défåäöΦ"}},
			{"like", "abc$def", []string{}},
			{"like", regexp.QuoteMeta("abc$def"), []string{"abc$def"}},
			{"like", "%180%", []string{"foo180bar"}},

			// ilike
			{"ilike", ".*ef.*", []string{"DEF", "ABCDEF", "abcdef", "abc$def"}},
			{"ilike", "ab%", []string{"ABC", "AbC", "ABCDEF", "abcdef", "abc$def"}},
			{"ilike", "%f", []string{"DEF", "ABCDEF", "abcdef", "FFF", "abc$def"}},
			{"ilike", "%ef%", []string{"DEF", "ABCDEF", "abcdef", "abc$def"}},
			{"ilike", "défÅäöΦ", []string{"défåäöΦ"}},
			{"ilike", "%éFåäöΦ", []string{"défåäöΦ"}},
			{"ilike", "défå%", []string{"défåäöΦ"}},
			{"ilike", "%éfåäÖ%", []string{"défåäöΦ"}},
			{"ilike", "ABC$def", []string{}},
			{"ilike", regexp.QuoteMeta("abc$DEF"), []string{"abc$def"}},
			{"ilike", "%180%", []string{"foo180bar"}},
		}

		for _, tc := range table {
			t.Run(fmt.Sprintf("Enum %t, %s %s", len(enums) > 0, tc.comparator, tc.arg), func(t *testing.T) {
				in := qframe.New(data, newqf.Enums(enums))
				expected := qframe.New(map[string]interface{}{"COL1": tc.expected}, newqf.Enums(enums))
				out := in.Filter(qframe.Filter{Column: "COL1", Comparator: tc.comparator, Arg: tc.arg})
				assertEquals(t, expected, out)
			})
		}
	}
}

func TestQFrame_String(t *testing.T) {
	a := qframe.New(map[string]interface{}{
		"COLUMN1": []string{"Long content", "a", "b", "c"},
		"COL2":    []int{3, 2, 1, 12345678910},
	}, newqf.ColumnOrder("COL2", "COLUMN1"))

	expected := `COL2(i) COLUMN1(s)
------- ----------
      3 Long co...
      2          a
      1          b
1234...          c

Dims = 2 x 4`

	if expected != a.String() {
		if len(expected) != len(a.String()) {
			t.Errorf("Different lengths: %d != %d", len(expected), len(a.String()))
		}
		t.Errorf("\n%s\n != \n%s\n", expected, a.String())
	}
}

func TestQFrame_ByteSize(t *testing.T) {
	a := qframe.New(map[string]interface{}{
		"COL1": []string{"a", "b"},
		"COL2": []int{3, 2},
		"COL3": []float64{3.5, 2.0},
		"COL4": []bool{true, false},
		"COL5": []string{"1", "2"},
	}, newqf.Enums(map[string][]string{"COL5": nil}))
	totalSize := a.ByteSize()

	// This is not so much of as a test as lock down on behavior to detect changes
	expectedSize := 740
	if totalSize != expectedSize {
		t.Errorf("Unexpected byte size: %d != %d", totalSize, expectedSize)
	}

	assertTrue(t, a.Select("COL1", "COL2", "COL3", "COL4").ByteSize() < totalSize)
	assertTrue(t, a.Select("COL2", "COL3", "COL4", "COL5").ByteSize() < totalSize)
	assertTrue(t, a.Select("COL1", "COL3", "COL4", "COL5").ByteSize() < totalSize)
	assertTrue(t, a.Select("COL1", "COL2", "COL4", "COL5").ByteSize() < totalSize)
	assertTrue(t, a.Select("COL1", "COL2", "COL3", "COL5").ByteSize() < totalSize)
}

func TestQFrame_CopyColumn(t *testing.T) {
	input := qframe.New(map[string]interface{}{
		"COL1": []string{"a", "b"},
		"COL2": []int{3, 2},
	})

	expectedNew := qframe.New(map[string]interface{}{
		"COL1": []string{"a", "b"},
		"COL2": []int{3, 2},
		"COL3": []int{3, 2},
	})

	expectedReplace := qframe.New(map[string]interface{}{
		"COL1": []int{3, 2},
		"COL2": []int{3, 2},
	})

	assertEquals(t, expectedNew, input.Copy("COL3", "COL2"))
	assertEquals(t, expectedReplace, input.Copy("COL1", "COL2"))
}

func TestQFrame_ApplyZeroArg(t *testing.T) {
	a, b := "a", "b"
	table := []struct {
		name     string
		expected interface{}
		fn       interface{}
	}{
		{name: "int fn", expected: []int{2, 2}, fn: func() int { return 2 }},
		{name: "int const", expected: []int{3, 3}, fn: 3},
		{name: "float fn", expected: []float64{2.5, 2.5}, fn: func() float64 { return 2.5 }},
		{name: "float const", expected: []float64{3.5, 3.5}, fn: 3.5},
		{name: "bool fn", expected: []bool{true, true}, fn: func() bool { return true }},
		{name: "bool const", expected: []bool{false, false}, fn: false},
		{name: "string fn", expected: []*string{&a, &a}, fn: func() *string { return &a }},
		{name: "string const", expected: []*string{&b, &b}, fn: &b},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			input := map[string]interface{}{"COL1": []int{3, 2}}
			in := qframe.New(input)
			input["COL2"] = tc.expected
			expected := qframe.New(input)
			out := in.Apply(qframe.Instruction{Fn: tc.fn, DstCol: "COL2"})
			assertEquals(t, expected, out)
		})
	}
}

func TestQFrame_ApplySingleArgIntToInt(t *testing.T) {
	input := qframe.New(map[string]interface{}{
		"COL1": []int{3, 2},
	})

	expectedNew := qframe.New(map[string]interface{}{
		"COL1": []int{6, 4},
	})

	assertEquals(t, expectedNew, input.Apply(qframe.Instruction{Fn: func(a int) int { return 2 * a }, DstCol: "COL1", SrcCol1: "COL1"}))
}

func TestQFrame_ApplySingleArgStringToBool(t *testing.T) {
	input := qframe.New(map[string]interface{}{
		"COL1": []string{"a", "aa", "aaa"},
	})

	expectedNew := qframe.New(map[string]interface{}{
		"COL1":    []string{"a", "aa", "aaa"},
		"IS_LONG": []bool{false, false, true},
	})

	assertEquals(t, expectedNew, input.Apply(qframe.Instruction{Fn: func(x *string) bool { return len(*x) > 2 }, DstCol: "IS_LONG", SrcCol1: "COL1"}))
}

func toUpper(x *string) *string {
	if x == nil {
		return x
	}
	result := strings.ToUpper(*x)
	return &result
}

func TestQFrame_ApplySingleArgString(t *testing.T) {
	a, b := "a", "b"
	A, B := "A", "B"
	input := qframe.New(map[string]interface{}{
		"COL1": []*string{&a, &b, nil},
	})

	expectedNew := qframe.New(map[string]interface{}{
		"COL1": []*string{&A, &B, nil},
	})

	// General function
	assertEquals(t, expectedNew, input.Apply(qframe.Instruction{Fn: toUpper, DstCol: "COL1", SrcCol1: "COL1"}))

	// Built in function
	assertEquals(t, expectedNew, input.Apply(qframe.Instruction{Fn: "ToUpper", DstCol: "COL1", SrcCol1: "COL1"}))
}

func TestQFrame_ApplySingleArgEnum(t *testing.T) {
	a, b := "a", "b"
	A, B := "A", "B"
	input := qframe.New(
		map[string]interface{}{"COL1": []*string{&a, &b, nil, &a}},
		newqf.Enums(map[string][]string{"COL1": nil}))

	expectedData := map[string]interface{}{"COL1": []*string{&A, &B, nil, &A}}
	expectedNewGeneral := qframe.New(expectedData)
	expectedNewBuiltIn := qframe.New(expectedData, newqf.Enums(map[string][]string{"COL1": nil}))

	// General function
	assertEquals(t, expectedNewGeneral, input.Apply(qframe.Instruction{Fn: toUpper, DstCol: "COL1", SrcCol1: "COL1"}))

	// Builtin function
	assertEquals(t, expectedNewBuiltIn, input.Apply(qframe.Instruction{Fn: "ToUpper", DstCol: "COL1", SrcCol1: "COL1"}))
}

func TestQFrame_ApplyToCopyColumn(t *testing.T) {
	a, b := "a", "b"
	input := qframe.New(map[string]interface{}{
		"COL1": []string{a, b}})

	expectedNew := qframe.New(map[string]interface{}{
		"COL1": []string{a, b},
		"COL2": []string{a, b}})

	assertEquals(t, expectedNew, input.Apply(qframe.Instruction{Fn: types.ColumnName("COL1"), DstCol: "COL2"}))
}

func TestQFrame_ApplyDoubleArg(t *testing.T) {
	table := []struct {
		name     string
		input    map[string]interface{}
		expected interface{}
		fn       interface{}
		enums    map[string][]string
	}{
		{
			name:     "int",
			input:    map[string]interface{}{"COL1": []int{3, 2}, "COL2": []int{30, 20}},
			expected: []int{33, 22},
			fn:       func(a, b int) int { return a + b }},
		{
			name:     "string",
			input:    map[string]interface{}{"COL1": []string{"a", "b"}, "COL2": []string{"x", "y"}},
			expected: []string{"ax", "by"},
			fn:       func(a, b *string) *string { result := *a + *b; return &result }},
		{
			name:     "enum",
			input:    map[string]interface{}{"COL1": []string{"a", "b"}, "COL2": []string{"x", "y"}},
			expected: []string{"ax", "by"},
			fn:       func(a, b *string) *string { result := *a + *b; return &result },
			enums:    map[string][]string{"COL1": nil, "COL2": nil}},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			in := qframe.New(tc.input, newqf.Enums(tc.enums))
			tc.input["COL3"] = tc.expected
			expected := qframe.New(tc.input, newqf.Enums(tc.enums))
			out := in.Apply(qframe.Instruction{Fn: tc.fn, DstCol: "COL3", SrcCol1: "COL1", SrcCol2: "COL2"})
			assertEquals(t, expected, out)
		})
	}
}

func TestQFrame_FilteredApply(t *testing.T) {
	plus1 := func(a int) int { return a + 1 }
	table := []struct {
		name         string
		input        map[string]interface{}
		expected     map[string]interface{}
		instructions []qframe.Instruction
		clauses      qframe.FilterClause
	}{
		{
			name:         "null fills for rows that dont match filter when destination column is new",
			input:        map[string]interface{}{"COL1": []int{3, 2, 1}},
			instructions: []qframe.Instruction{{Fn: plus1, DstCol: "COL3", SrcCol1: "COL1"}, {Fn: plus1, DstCol: "COL3", SrcCol1: "COL3"}},
			expected:     map[string]interface{}{"COL1": []int{3, 2, 1}, "COL3": []int{5, 4, 0}},
			clauses:      qframe.Filter{Comparator: ">", Column: "COL1", Arg: 1}},
		{
			// One could question whether this is the desired behaviour or not. The alternative
			// would be to preserve the existing values but that would cause a lot of inconsistencies
			// when the result column type differs from the source column type for example. What would
			// the preserved value be in that case? Preserving the existing behaviour could be achieved
			// by using a temporary column that indexes which columns to modify and not. Perhaps this
			// should be built in at some point.
			name:         "null fills rows that dont match filter when destination column is existing",
			input:        map[string]interface{}{"COL1": []int{3, 2, 1}},
			instructions: []qframe.Instruction{{Fn: plus1, DstCol: "COL1", SrcCol1: "COL1"}},
			expected:     map[string]interface{}{"COL1": []int{4, 3, 0}},
			clauses:      qframe.Filter{Comparator: ">", Column: "COL1", Arg: 1}},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			in := qframe.New(tc.input)
			expected := qframe.New(tc.expected)
			out := in.FilteredApply(tc.clauses, tc.instructions...)
			assertEquals(t, expected, out)
		})
	}
}

func TestQFrame_AggregateStrings(t *testing.T) {
	table := []struct {
		enums map[string][]string
	}{
		{map[string][]string{"COL2": nil}},
		{map[string][]string{}},
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("Enum %t", len(tc.enums) > 0), func(t *testing.T) {
			input := qframe.New(map[string]interface{}{
				"COL1": []string{"a", "b", "a", "b", "a"},
				"COL2": []string{"x", "p", "y", "q", "z"},
			}, newqf.Enums(tc.enums))
			expected := qframe.New(map[string]interface{}{"COL1": []string{"a", "b"}, "COL2": []string{"x,y,z", "p,q"}})
			out := input.GroupBy(groupby.Columns("COL1")).Aggregate(qframe.Aggregation{Fn: aggregation.StrJoin(","), Column: "COL2"})
			assertEquals(t, expected, out.Sort(qframe.Order{Column: "COL1"}))
		})
	}
}

func sum(c []int) int {
	result := 0
	for _, v := range c {
		result += v
	}
	return result
}

func TestQFrame_AggregateGroupByNull(t *testing.T) {
	a, b := "a", "b"
	for _, groupByNull := range []bool{false, true} {
		for _, column := range []string{"COL1", "COL2", "COL3"} {
			t.Run(fmt.Sprintf("%s %v", column, groupByNull), func(t *testing.T) {
				input := qframe.New(map[string]interface{}{
					"COL1": []*string{&a, &b, nil, &a, &b, nil},
					"COL2": []*string{&a, &b, nil, &a, &b, nil},
					"COL3": []float64{1, 2, math.NaN(), 1, 2, math.NaN()},
					"COL4": []int{1, 2, 3, 10, 20, 30},
				}, newqf.Enums(map[string][]string{"COL2": nil}))

				col4 := []int{3, 30, 11, 22}
				if groupByNull {
					// Here we expect the nil/NaN columns to have been aggregated together
					col4 = []int{33, 11, 22}
				}
				expected := qframe.New(map[string]interface{}{"COL4": col4})

				out := input.GroupBy(groupby.Columns(column), groupby.Null(groupByNull)).Aggregate(qframe.Aggregation{Fn: sum, Column: "COL4"})
				assertEquals(t, expected, out.Sort(colNamesToOrders(column, "COL4")...).Select("COL4"))
			})
		}
	}
}

func TestQFrame_NewWithConstantVal(t *testing.T) {
	a := "a"
	table := []struct {
		name     string
		input    interface{}
		expected interface{}
		enums    map[string][]string
	}{
		{
			name:     "int",
			input:    qframe.ConstInt{Val: 33, Count: 2},
			expected: []int{33, 33}},
		{
			name:     "float",
			input:    qframe.ConstFloat{Val: 33.5, Count: 2},
			expected: []float64{33.5, 33.5}},
		{
			name:     "bool",
			input:    qframe.ConstBool{Val: true, Count: 2},
			expected: []bool{true, true}},
		{
			name:     "string",
			input:    qframe.ConstString{Val: &a, Count: 2},
			expected: []string{"a", "a"}},
		{
			name:     "string null",
			input:    qframe.ConstString{Val: nil, Count: 2},
			expected: []*string{nil, nil}},
		{
			name:     "enum",
			input:    qframe.ConstString{Val: &a, Count: 2},
			expected: []string{"a", "a"},
			enums:    map[string][]string{"COL1": nil}},
		{
			name:     "enum null",
			input:    qframe.ConstString{Val: nil, Count: 2},
			expected: []*string{nil, nil},
			enums:    map[string][]string{"COL1": nil}},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			in := qframe.New(map[string]interface{}{"COL1": tc.input}, newqf.Enums(tc.enums))
			expected := qframe.New(map[string]interface{}{"COL1": tc.expected}, newqf.Enums(tc.enums))
			assertEquals(t, expected, in)
		})
	}
}

func TestQFrame_NewErrors(t *testing.T) {
	longCol := make([]string, 256)
	for i := range longCol {
		longCol[i] = fmt.Sprintf("%d", i)
	}

	table := []struct {
		input   map[string]interface{}
		configs []newqf.ConfigFunc
		err     string
	}{
		{
			input: map[string]interface{}{"": []int{1}},
			err:   "must not be empty"},
		{
			input: map[string]interface{}{"'foo'": []int{1}},
			err:   `must not be quoted: 'foo'`},
		{
			input: map[string]interface{}{`"foo"`: []int{1}},
			err:   `must not be quoted: "foo"`},
		{
			input: map[string]interface{}{"$foo": []int{1}},
			err:   "must not start with $"},
		{
			input:   map[string]interface{}{"COL1": longCol},
			configs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": nil})},
			err:     "enum max cardinality"},
		{
			input:   map[string]interface{}{"COL1": longCol},
			configs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": nil})},
			err:     "enum max cardinality"},
		{
			input:   map[string]interface{}{"COL1": longCol},
			configs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL2": nil})},
			err:     "unknown enum columns"},
		{
			input:   map[string]interface{}{"COL1": []int{1}, "COL2": []int{2}},
			configs: []newqf.ConfigFunc{newqf.ColumnOrder("COL1")},
			err:     "number of columns and columns order length do not match"},
		{
			input:   map[string]interface{}{"COL1": []int{1}, "COL2": []int{2}},
			configs: []newqf.ConfigFunc{newqf.ColumnOrder("COL1", "COL3")},
			err:     `column "COL3" in column order does not exist`},
		{
			input: map[string]interface{}{"COL1": []int8{1}},
			err:   `unknown column data type`},
		{
			input: map[string]interface{}{"COL1": []int{1}, "COL2": []int{2, 3}},
			err:   `different lengths on columns not allowed`},
	}

	for _, tc := range table {
		t.Run(tc.err, func(t *testing.T) {
			f := qframe.New(tc.input, tc.configs...)
			assertErr(t, f.Err, tc.err)
		})
	}
}

func TestQFrame_OperationErrors(t *testing.T) {
	// Catch all test case for various errors caused by invalid input parameters
	// to various functions on the QFrame.
	table := []struct {
		name    string
		fn      func(f qframe.QFrame) error
		err     string
		configs []newqf.ConfigFunc
		input   map[string]interface{}
	}{
		{
			name: "Copy with invalid destination column name",
			fn:   func(f qframe.QFrame) error { return f.Copy("$A", "COL1").Err },
			err:  "must not start with $"},
		{
			name: "Apply with invalid destination column name",
			fn:   func(f qframe.QFrame) error { return f.Apply(qframe.Instruction{Fn: 1, DstCol: "$A"}).Err },
			err:  "must not start with $"},
		{
			name: "Set eval func with invalid name",
			fn: func(f qframe.QFrame) error {
				ctx := eval.NewDefaultCtx()
				return ctx.SetFunc("$foo", func(i int) int { return i })
			},
			err: "must not start with $"},
		{
			name: "Missing function in eval",
			fn: func(f qframe.QFrame) error {
				expr := qframe.Expr("foo", types.ColumnName("COL1"))
				return f.Eval("COL3", expr).Err
			},
			err: "Could not find Int function"},
		{
			name: "Error in lhs of composed expression",
			fn: func(f qframe.QFrame) error {
				expr := qframe.Expr("+",
					qframe.Expr("foo", types.ColumnName("COL1")),
					qframe.Expr("abs", types.ColumnName("COL2")))
				return f.Eval("COL3", expr).Err
			},
			err: "Could not find Int function"},
		{
			name: "Error in rhs of composed expression",
			fn: func(f qframe.QFrame) error {
				expr := qframe.Expr("+",
					qframe.Expr("abs", types.ColumnName("COL2")),
					qframe.Expr("foo", types.ColumnName("COL1")))
				return f.Eval("COL3", expr).Err
			},
			err: "Could not find Int function"},
		{
			name: "Zero clause OR filter not allowed",
			fn:   func(f qframe.QFrame) error { return f.Filter(qframe.Or()).Err },
			err:  "zero subclauses not allowed"},
		{
			name: "Zero clause AND filter not allowed",
			fn:   func(f qframe.QFrame) error { return f.Filter(qframe.And()).Err },
			err:  "zero subclauses not allowed"},
		{
			name: "Group by missing column",
			fn:   func(f qframe.QFrame) error { return f.GroupBy(groupby.Columns("FOO")).Err },
			err:  "unknown column"},
		{
			name: "Aggregate on missing column",
			fn: func(f qframe.QFrame) error {
				return f.GroupBy(groupby.Columns("COL1")).Aggregate(qframe.Aggregation{Fn: "sum", Column: "FOO"}).Err
			},
			err: "unknown column"},
		{
			name: "Aggregate on column part of the group by expression is not allowed",
			fn: func(f qframe.QFrame) error {
				return f.GroupBy(groupby.Columns("COL1")).Aggregate(qframe.Aggregation{Fn: "sum", Column: "COL1"}).Err
			},
			err: "cannot aggregate on column that is part of group by"},
		{
			name:    "Filter using unknown operation, enum",
			input:   map[string]interface{}{"COL1": []string{"a", "b"}},
			configs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": {"a", "b"}})},
			fn: func(f qframe.QFrame) error {
				return f.Filter(qframe.Filter{Comparator: ">>>", Column: "COL1", Arg: "c"}).Err
			},
			err: "unknown comparison operator"},
		{
			name:    "Filter against unknown value, enum",
			input:   map[string]interface{}{"COL1": []string{"a", "b"}},
			configs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": {"a", "b"}})},
			fn: func(f qframe.QFrame) error {
				return f.Filter(qframe.Filter{Comparator: ">", Column: "COL1", Arg: "c"}).Err
			},
			err: "unknown enum value"},
		{
			name:  "Filter using unknown operator, float",
			input: map[string]interface{}{"COL1": []float64{1.0}},
			fn: func(f qframe.QFrame) error {
				return f.Filter(qframe.Filter{Comparator: ">>>", Column: "COL1", Arg: 1.0}).Err
			},
			err: "invalid comparison operator"},
		{
			name:  "Filter against wrong type, float",
			input: map[string]interface{}{"COL1": []float64{1.0}},
			fn: func(f qframe.QFrame) error {
				return f.Filter(qframe.Filter{Comparator: ">", Column: "COL1", Arg: "foo"}).Err
			},
			err: "invalid comparison value type"},
		{
			name:  "Filter against wrong type, string",
			input: map[string]interface{}{"COL1": []string{"a"}},
			fn: func(f qframe.QFrame) error {
				return f.Filter(qframe.Filter{Comparator: ">", Column: "COL1", Arg: 1.0}).Err
			},
			err: "invalid comparison value type"},
		{
			name:  "Filter on missing column",
			input: map[string]interface{}{"COL1": []string{"a"}},
			fn: func(f qframe.QFrame) error {
				return f.Filter(qframe.Filter{Comparator: "=", Column: "FOO", Arg: "a"}).Err
			},
			err: "unknown column"},
		{
			name:  "Filter against missing argument column",
			input: map[string]interface{}{"COL1": []string{"a"}},
			fn: func(f qframe.QFrame) error {
				return f.Filter(qframe.Filter{Comparator: "=", Column: "COL1", Arg: types.ColumnName("COL2")}).Err
			},
			err: "unknown argument column"},
		{
			name:  "Distinct on missing column",
			input: map[string]interface{}{"COL1": []string{"a"}},
			fn:    func(f qframe.QFrame) error { return f.Distinct(groupby.Columns("COL2")).Err },
			err:   "unknown column"},
		{
			name:  "Select on missing column",
			input: map[string]interface{}{"COL1": []string{"a"}},
			fn:    func(f qframe.QFrame) error { return f.Select("COL2").Err },
			err:   "unknown column"},
		{
			name:  "Copy with missing column",
			input: map[string]interface{}{"COL1": []string{"a"}},
			fn:    func(f qframe.QFrame) error { return f.Copy("COL3", "COL2").Err },
			err:   "unknown column"},
		{
			name:  "Unknown sort column",
			input: map[string]interface{}{"COL1": []string{"a"}},
			fn:    func(f qframe.QFrame) error { return f.Sort(qframe.Order{Column: "COL2"}).Err },
			err:   "unknown column"},
		{
			name:  "Get view for wrong type",
			input: map[string]interface{}{"COL1": []string{"a"}},
			fn: func(f qframe.QFrame) error {
				_, err := f.FloatView("COL1")
				return err
			},
			err: "invalid column type"},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			if tc.input == nil {
				tc.input = map[string]interface{}{"COL1": []int{1, 2, 3}, "COL2": []int{11, 12, 13}}
			}
			f := qframe.New(tc.input, tc.configs...)
			err := tc.fn(f)
			assertErr(t, err, tc.err)
		})
	}
}

func TestQFrame_Equals(t *testing.T) {
	table := []struct {
		name              string
		input             map[string]interface{}
		comparatee        map[string]interface{}
		inputConfigs      []newqf.ConfigFunc
		comparateeConfigs []newqf.ConfigFunc
		expected          bool
	}{
		{
			name:       "Equality basic",
			input:      map[string]interface{}{"COL1": []int{1}, "COL2": []int{1}},
			comparatee: map[string]interface{}{"COL1": []int{1}, "COL2": []int{1}},
			expected:   true},
		{
			name:       "Equality of zero column",
			input:      map[string]interface{}{},
			comparatee: map[string]interface{}{},
			expected:   true},
		{
			name:       "Equality of empty column",
			input:      map[string]interface{}{"COL1": []int{}},
			comparatee: map[string]interface{}{"COL1": []int{}},
			expected:   true},
		{
			name:       "Inequality empty vs non-empty column",
			input:      map[string]interface{}{"COL1": []int{}},
			comparatee: map[string]interface{}{"COL1": []int{1}},
			expected:   false},
		{
			name:       "Inequality different columns",
			input:      map[string]interface{}{"COL1": []int{}},
			comparatee: map[string]interface{}{"COL2": []int{}},
			expected:   false},
		{
			name:       "Inequality different number of columns",
			input:      map[string]interface{}{"COL1": []int{1}},
			comparatee: map[string]interface{}{"COL1": []int{1}, "COL2": []int{1}},
			expected:   false},
		{
			name:       "Inequality different column content",
			input:      map[string]interface{}{"COL1": []int{1, 2}},
			comparatee: map[string]interface{}{"COL1": []int{2, 1}},
			expected:   false},
		{
			name:              "Equality between different enum types as long as elements are the same",
			input:             map[string]interface{}{"COL1": []string{"a", "b"}},
			inputConfigs:      []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": {"a", "b"}})},
			comparatee:        map[string]interface{}{"COL1": []string{"a", "b"}},
			comparateeConfigs: []newqf.ConfigFunc{newqf.Enums(map[string][]string{"COL1": {"c", "b", "a"}})},
			expected:          true},
		{
			// Not sure if this is the way it should work, just documenting the current behaviour
			name:              "Inequality with same content but different column order",
			input:             map[string]interface{}{"COL1": []string{"a", "b"}, "COL2": []string{"aa", "bb"}},
			inputConfigs:      []newqf.ConfigFunc{newqf.ColumnOrder("COL1", "COL2")},
			comparatee:        map[string]interface{}{"COL1": []string{"a", "b"}, "COL2": []string{"aa", "bb"}},
			comparateeConfigs: []newqf.ConfigFunc{newqf.ColumnOrder("COL2", "COL1")},
			expected:          false},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			in := qframe.New(tc.input, tc.inputConfigs...)
			comp := qframe.New(tc.comparatee, tc.comparateeConfigs...)
			eq, reason := in.Equals(comp)
			if eq != tc.expected {
				t.Errorf("Actual: %v, expected: %v, reason: %s", eq, tc.expected, reason)
			}
		})
	}
}

func TestQFrame_FloatView(t *testing.T) {
	input := qframe.New(map[string]interface{}{"COL1": []float64{1.5, 0.5, 3.0}})
	input = input.Sort(qframe.Order{Column: "COL1"})
	expected := []float64{0.5, 1.5, 3.0}

	v, err := input.FloatView("COL1")
	assertNotErr(t, err)

	s := v.Slice()
	assertTrue(t, v.Len() == len(expected))
	assertTrue(t, len(s) == len(expected))
	assertTrue(t, (v.ItemAt(0) == s[0]) && (s[0] == expected[0]))
	assertTrue(t, (v.ItemAt(1) == s[1]) && (s[1] == expected[1]))
	assertTrue(t, (v.ItemAt(2) == s[2]) && (s[2] == expected[2]))
}

func TestQFrame_StringView(t *testing.T) {
	a, b := "a", "b"
	input := qframe.New(map[string]interface{}{"COL1": []*string{&a, nil, &b}})
	input = input.Sort(qframe.Order{Column: "COL1"})
	expected := []*string{nil, &a, &b}

	v, err := input.StringView("COL1")
	assertNotErr(t, err)

	s := v.Slice()
	assertTrue(t, v.Len() == len(expected))
	assertTrue(t, len(s) == len(expected))

	// Nil, check pointers
	assertTrue(t, (v.ItemAt(0) == s[0]) && (s[0] == expected[0]))

	// !Nil, check values
	assertTrue(t, (*v.ItemAt(1) == *s[1]) && (*s[1] == *expected[1]))
	assertTrue(t, (*v.ItemAt(2) == *s[2]) && (*s[2] == *expected[2]))
}

func TestQFrame_EnumView(t *testing.T) {
	a, b := "a", "b"
	input := qframe.New(map[string]interface{}{"COL1": []*string{&a, nil, &b}}, newqf.Enums(map[string][]string{"COL1": {"a", "b"}}))
	input = input.Sort(qframe.Order{Column: "COL1"})
	expected := []*string{nil, &a, &b}

	v, err := input.EnumView("COL1")
	assertNotErr(t, err)

	s := v.Slice()
	assertTrue(t, v.Len() == len(expected))
	assertTrue(t, len(s) == len(expected))

	// Nil, check pointers
	assertTrue(t, (v.ItemAt(0) == s[0]) && (s[0] == expected[0]))

	// !Nil, check values
	assertTrue(t, (*v.ItemAt(1) == *s[1]) && (*s[1] == *expected[1]))
	assertTrue(t, (*v.ItemAt(2) == *s[2]) && (*s[2] == *expected[2]))
}

func col(c string) types.ColumnName {
	return types.ColumnName(c)
}

func TestQFrame_EvalSuccess(t *testing.T) {
	table := []struct {
		name         string
		expr         qframe.Expression
		dstCol       string
		input        map[string]interface{}
		expected     interface{}
		customFn     interface{}
		customFnName string
		enums        map[string][]string
	}{
		{
			name:     "column copying",
			expr:     qframe.Val(col("COL1")),
			input:    map[string]interface{}{"COL1": []int{1, 2}},
			dstCol:   "COL2",
			expected: []int{1, 2}},
		{
			name:     "column constant fill",
			expr:     qframe.Val(3),
			input:    map[string]interface{}{"COL1": []int{1, 2}},
			dstCol:   "COL2",
			expected: []int{3, 3}},
		{
			name:     "column nil fill",
			expr:     qframe.Val(nil),
			input:    map[string]interface{}{"COL1": []int{1, 2}},
			dstCol:   "COL2",
			expected: []*string{nil, nil}},
		{
			name:     "int col plus col",
			expr:     qframe.Expr("+", col("COL1"), col("COL2")),
			input:    map[string]interface{}{"COL1": []int{1, 2}, "COL2": []int{3, 4}},
			expected: []int{4, 6}},
		{
			name:     "int col plus const minus const",
			expr:     qframe.Expr("-", qframe.Expr("+", col("COL1"), 10), qframe.Val(1)),
			input:    map[string]interface{}{"COL1": []int{1, 2}},
			expected: []int{10, 11}},
		{
			name:     "string plus itoa int",
			expr:     qframe.Expr("+", col("COL1"), qframe.Expr("str", col("COL2"))),
			input:    map[string]interface{}{"COL1": []string{"a", "b"}, "COL2": []int{1, 2}},
			expected: []string{"a1", "b2"}},
		{
			name:     "string plus string literal",
			expr:     qframe.Expr("+", col("COL1"), qframe.Val("A")),
			input:    map[string]interface{}{"COL1": []string{"a", "b"}},
			expected: []string{"aA", "bA"}},
		{
			name:         "float custom func",
			expr:         qframe.Expr("pythagoras", col("COL1"), col("COL2")),
			input:        map[string]interface{}{"COL1": []float64{1, 2}, "COL2": []float64{1, 3}},
			expected:     []float64{math.Sqrt(2), math.Sqrt(4 + 9)},
			customFn:     func(x, y float64) float64 { return math.Sqrt(x*x + y*y) },
			customFnName: "pythagoras"},
		{
			name:     "bool col and col",
			expr:     qframe.Expr("&", col("COL1"), col("COL2")),
			input:    map[string]interface{}{"COL1": []bool{true, false}, "COL2": []bool{true, true}},
			expected: []bool{true, false}},
		{
			name:     "enum col plus col",
			expr:     qframe.Expr("+", col("COL1"), col("COL2")),
			input:    map[string]interface{}{"COL1": []string{"a", "b"}, "COL2": []string{"A", "B"}},
			expected: []string{"aA", "bB"},
			enums:    map[string][]string{"COL1": nil, "COL2": nil}},
		{
			name:     "enum col plus string col, cast string to enum needed",
			expr:     qframe.Expr("+", qframe.Expr("str", col("COL1")), col("COL2")),
			input:    map[string]interface{}{"COL1": []string{"a", "b"}, "COL2": []string{"A", "B"}},
			expected: []string{"aA", "bB"},
			enums:    map[string][]string{"COL1": nil}},
		{
			name:     "abs of float sum",
			expr:     qframe.Expr("abs", qframe.Expr("+", col("COL1"), col("COL2"))),
			input:    map[string]interface{}{"COL1": []float64{1, 2}, "COL2": []float64{-3, -2}},
			expected: []float64{2, 0}},
		{
			name:     "chained multi argument evaluation - three arguments",
			expr:     qframe.Expr("/", col("COL1"), col("COL2"), col("COL3")),
			input:    map[string]interface{}{"COL1": []float64{18}, "COL2": []float64{2}, "COL3": []float64{3}},
			dstCol:   "COL4",
			expected: []float64{3}},
		{
			name:     "chained multi argument evaluation - four arguments including constant",
			expr:     qframe.Expr("/", col("COL1"), col("COL2"), col("COL3"), 3.0),
			input:    map[string]interface{}{"COL1": []float64{18}, "COL2": []float64{2}, "COL3": []float64{3}},
			dstCol:   "COL4",
			expected: []float64{1}},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			conf := make([]eval.ConfigFunc, 0)
			if tc.customFn != nil {
				ctx := eval.NewDefaultCtx()
				err := ctx.SetFunc(tc.customFnName, tc.customFn)
				assertNotErr(t, err)
				conf = append(conf, eval.EvalContext(ctx))
			}

			if tc.dstCol == "" {
				tc.dstCol = "COL3"
			}
			in := qframe.New(tc.input, newqf.Enums(tc.enums))
			tc.input[tc.dstCol] = tc.expected
			expected := qframe.New(tc.input, newqf.Enums(tc.enums))

			assertNotErr(t, tc.expr.Err())
			out := in.Eval(tc.dstCol, tc.expr, conf...)

			assertEquals(t, expected, out)
		})
	}
}

func TestQFrame_Typing(t *testing.T) {
	qf := qframe.New(map[string]interface{}{
		"ints":    []int{1, 2},
		"bools":   []bool{true, false},
		"floats":  []float64{1, 0},
		"strings": []string{"a", "b"},
		"enums":   []string{"a", "b"},
	},
		newqf.Enums(map[string][]string{"enums": {"a", "b"}}),
		newqf.ColumnOrder("ints", "bools", "floats", "strings", "enums"),
	)
	assertTrue(t, qf.ColumnTypeMap()["ints"] == types.Int)
	assertTrue(t, qf.ColumnTypes()[0] == types.Int)
	assertTrue(t, qf.ColumnTypeMap()["bools"] == types.Bool)
	assertTrue(t, qf.ColumnTypes()[1] == types.Bool)
	assertTrue(t, qf.ColumnTypeMap()["floats"] == types.Float)
	assertTrue(t, qf.ColumnTypes()[2] == types.Float)
	assertTrue(t, qf.ColumnTypeMap()["strings"] == types.String)
	assertTrue(t, qf.ColumnTypes()[3] == types.String)
	assertTrue(t, qf.ColumnTypeMap()["enums"] == types.Enum)
	assertTrue(t, qf.ColumnTypes()[4] == types.Enum)
}

func TestQFrame_WithRows(t *testing.T) {
	input := qframe.New(map[string]interface{}{"COL1": []int{11, 22, 33}})
	expected := qframe.New(map[string]interface{}{
		"ROWNUMS": []int{0, 1, 2},
		"COL1":    []int{11, 22, 33}})
	assertEquals(t, expected, input.WithRowNums("ROWNUMS"))
}

func assertContains(t *testing.T, actual, expected string) {
	t.Helper()
	if !strings.Contains(actual, expected) {
		t.Errorf("Could not find: %s, in: %s", expected, actual)
	}
}

func TestDoc(t *testing.T) {
	// This is just a verification that something is printed rather than a proper test.
	doc := qframe.Doc()
	assertContains(t, doc, "context")
	assertContains(t, doc, "Single arg")
	assertContains(t, doc, "Double arg")
	assertContains(t, doc, "bool")
	assertContains(t, doc, "enum")
	assertContains(t, doc, "float")
	assertContains(t, doc, "int")
	assertContains(t, doc, "string")
	assertContains(t, doc, "filters")
	assertContains(t, doc, "aggregations")
}

func TestQFrame_AppendSuccess(t *testing.T) {
	f1 := qframe.New(map[string]interface{}{"COL1": []int{11, 22}})
	f2 := qframe.New(map[string]interface{}{"COL1": []int{33}})
	f3 := qframe.New(map[string]interface{}{"COL1": []int{44, 55}})
	expected := qframe.New(map[string]interface{}{"COL1": []int{11, 22, 33, 44, 55}})
	assertEquals(t, expected, f1.Append(f2, f3))
}

func assertContainsQFrame(t *testing.T, frames []qframe.QFrame, frame qframe.QFrame) {
	t.Helper()
	for _, f := range frames {
		if ok, _ := frame.Equals(f); ok {
			return
		}
	}
	t.Errorf("%v does not contain %v", frames, frame)
}

func TestQFrame_GroupByQFrames(t *testing.T) {
	f := qframe.New(map[string]interface{}{
		"COL1": []int{1, 1, 2, 3, 3},
		"COL2": []int{10, 11, 20, 30, 31},
	})

	ff, err := f.GroupBy(groupby.Columns("COL1")).QFrames()
	assertNotErr(t, err)
	assertTrue(t, len(ff) == 3)
	assertContainsQFrame(t, ff, qframe.New(map[string]interface{}{"COL1": []int{1, 1}, "COL2": []int{10, 11}}))
	assertContainsQFrame(t, ff, qframe.New(map[string]interface{}{"COL1": []int{2}, "COL2": []int{20}}))
	assertContainsQFrame(t, ff, qframe.New(map[string]interface{}{"COL1": []int{3, 3}, "COL2": []int{30, 31}}))
}
