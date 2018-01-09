package qframe_test

import (
	"bytes"
	"fmt"
	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/filter"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func assertEquals(t *testing.T, expected, actual qframe.QFrame) {
	t.Helper()
	equal, reason := expected.Equals(actual)
	if !equal {
		t.Errorf("QFrames not equal, %s.\nexpected=%s\nactual=%s", reason, expected, actual)
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

	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected error to contain: %s, was: %s", expectedErr, err.Error())
	}
}

func TestQFrame_Filter(t *testing.T) {
	a := qframe.New(map[string]interface{}{
		"COL.1": []int{1, 2, 3, 4, 5},
	})

	table := []struct {
		filters  []filter.Filter
		expected qframe.QFrame
	}{
		{
			[]filter.Filter{{Column: "COL.1", Comparator: ">", Arg: 3}},
			qframe.New(map[string]interface{}{"COL.1": []int{4, 5}}),
		},
		{
			[]filter.Filter{
				{Column: "COL.1", Comparator: ">", Arg: 4},
				{Column: "COL.1", Comparator: "<", Arg: 2}},
			qframe.New(map[string]interface{}{"COL.1": []int{1, 5}}),
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Filter %d", i), func(t *testing.T) {
			b := a.Filter(tc.filters...)
			assertEquals(t, tc.expected, b)
		})
	}
}

func TestQFrame_Sort(t *testing.T) {
	a := qframe.New(map[string]interface{}{
		"COL.1": []int{0, 1, 3, 2},
		"COL.2": []int{3, 2, 1, 1},
	})

	table := []struct {
		orders   []qframe.Order
		expected qframe.QFrame
	}{
		{
			[]qframe.Order{{Column: "COL.1"}},
			qframe.New(map[string]interface{}{
				"COL.1": []int{0, 1, 2, 3},
				"COL.2": []int{3, 2, 1, 1}}),
		},
		{
			[]qframe.Order{{Column: "COL.1", Reverse: true}},
			qframe.New(map[string]interface{}{
				"COL.1": []int{3, 2, 1, 0},
				"COL.2": []int{1, 1, 2, 3}}),
		},
		{
			[]qframe.Order{{Column: "COL.2"}, {Column: "COL.1"}},
			qframe.New(map[string]interface{}{
				"COL.1": []int{2, 3, 1, 0},
				"COL.2": []int{1, 1, 2, 3}}),
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Sort %d", i), func(t *testing.T) {
			b := a.Sort(tc.orders...)
			assertEquals(t, tc.expected, b)
		})
	}
}

func TestQFrame_SortNull(t *testing.T) {
	a, b, c := "a", "b", "c"
	stringIn := map[string]interface{}{
		"COL.1": []*string{&b, nil, &a, nil, &c, &a, nil},
	}

	floatIn := map[string]interface{}{
		"COL.1": []float64{1.0, math.NaN(), -1.0, math.NaN()},
	}

	table := []struct {
		in       map[string]interface{}
		orders   []qframe.Order
		expected map[string]interface{}
	}{
		{
			stringIn,
			[]qframe.Order{{Column: "COL.1"}},
			map[string]interface{}{
				"COL.1": []*string{nil, nil, nil, &a, &a, &b, &c},
			},
		},
		{
			stringIn,
			[]qframe.Order{{Column: "COL.1", Reverse: true}},
			map[string]interface{}{
				"COL.1": []*string{&c, &b, &a, &a, nil, nil, nil},
			},
		},
		{
			floatIn,
			[]qframe.Order{{Column: "COL.1"}},
			map[string]interface{}{
				"COL.1": []float64{math.NaN(), math.NaN(), -1.0, 1.0},
			},
		},
		{
			floatIn,
			[]qframe.Order{{Column: "COL.1", Reverse: true}},
			map[string]interface{}{
				"COL.1": []float64{1.0, -1.0, math.NaN(), math.NaN()},
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
		"COL.1": []int{0, 1, 3, 2},
		"COL.2": []int{1, 1, 1, 1},
	})

	table := []struct {
		orders   []qframe.Order
		expected qframe.QFrame
	}{
		{
			[]qframe.Order{{Column: "COL.2", Reverse: true}, {Column: "COL.1"}},
			qframe.New(map[string]interface{}{
				"COL.1": []int{0, 1, 2, 3},
				"COL.2": []int{1, 1, 1, 1}}),
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
				"COL.1": []int{0, 1, 0, 1},
				"COL.2": []int{0, 1, 0, 1}},
			expected: map[string]interface{}{
				"COL.1": []int{0, 1},
				"COL.2": []int{0, 1}},
			columns: []string{"COL.1", "COL.2"},
		},
		{
			input: map[string]interface{}{
				"COL.1": []int{},
				"COL.2": []int{}},
			expected: map[string]interface{}{
				"COL.1": []int{},
				"COL.2": []int{}},
			columns: []string{"COL.1", "COL.2"},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Distinct %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			out := in.Distinct()
			assertEquals(t, qframe.New(tc.expected), out)
		})
	}
}

func TestQFrame_GroupByAggregate(t *testing.T) {
	table := []struct {
		input        map[string]interface{}
		expected     map[string]interface{}
		groupColumns []string
		aggregations []string
	}{
		{
			input: map[string]interface{}{
				"COL.1": []int{0, 0, 1, 2},
				"COL.2": []int{0, 0, 1, 1},
				"COL.3": []int{1, 2, 5, 7}},
			expected: map[string]interface{}{
				"COL.1": []int{0, 1, 2},
				"COL.2": []int{0, 1, 1},
				"COL.3": []int{3, 5, 7}},
			groupColumns: []string{"COL.1", "COL.2"},
			aggregations: []string{"sum", "COL.3"},
		},
		{
			input: map[string]interface{}{
				"COL.1": []int{},
				"COL.2": []int{}},
			expected: map[string]interface{}{
				"COL.1": []int{},
				"COL.2": []int{}},
			groupColumns: []string{"COL.1"},
			aggregations: []string{"sum", "COL.2"},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("GroupByAggregate %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			out := in.GroupBy(tc.groupColumns...).Aggregate(tc.aggregations...)
			assertEquals(t, qframe.New(tc.expected), out)
		})
	}
}

func TestQFrame_Select(t *testing.T) {
	table := []struct {
		input      map[string]interface{}
		expected   map[string]interface{}
		selectCols []string
	}{
		{
			input: map[string]interface{}{
				"COL.1": []int{0, 1},
				"COL.2": []int{1, 2}},
			expected: map[string]interface{}{
				"COL.1": []int{0, 1}},
			selectCols: []string{"COL.1"},
		},
		{
			input: map[string]interface{}{
				"COL.1": []int{0, 1},
				"COL.2": []int{1, 2}},
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
	}{
		{
			input: map[string]interface{}{
				"COL.1": []float64{0.0, 1.5, 2.5, 3.5},
				"COL.2": []int{1, 2, 3, 4}},
			expected: map[string]interface{}{
				"COL.1": []float64{1.5, 2.5},
				"COL.2": []int{2, 3}},
			start: 1,
			end:   3,
		},
		{
			input: map[string]interface{}{
				"COL.1": []int{},
				"COL.2": []int{}},
			expected: map[string]interface{}{
				"COL.1": []int{},
				"COL.2": []int{}},
			start: 0,
			end:   0,
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Slice %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			out := in.Slice(tc.start, tc.end)
			assertEquals(t, qframe.New(tc.expected), out)
		})
	}
}

func TestQFrame_ReadCsv(t *testing.T) {
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
		name         string
		inputHeaders []string
		inputData    string
		emptyNull    bool
		expected     map[string]interface{}
		types        map[string]string
		expectedErr  string
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
			name:         "mixed",
			inputHeaders: []string{"int", "float", "bool", "string"},
			inputData:    "1,2.5,true,hello\n10,20.5,false,\"bye, bye\"",
			expected: map[string]interface{}{
				"int":    []int{1, 10},
				"float":  []float64{2.5, 20.5},
				"bool":   []bool{true, false},
				"string": []string{"hello", "bye, bye"}},
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
	}

	for _, tc := range table {
		t.Run(fmt.Sprintf("ReadCsv %s", tc.name), func(t *testing.T) {
			input := strings.Join(tc.inputHeaders, ",") + "\n" + tc.inputData
			out := qframe.ReadCsv(strings.NewReader(input), qframe.EmptyNull(tc.emptyNull), qframe.Types(tc.types))
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

				assertEquals(t, qframe.New(tc.expected, qframe.ColumnOrder(tc.inputHeaders...), qframe.Enums(enums)), out)
			}
		})
	}
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
		out := qframe.ReadCsv(
			strings.NewReader(input),
			qframe.EmptyNull(true),
			qframe.Types(map[string]string{"day": "enum"}),
			qframe.EnumValues(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))
		out = out.Sort(qframe.Order{Column: "day"})
		expected := qframe.New(
			map[string]interface{}{"day": []*string{nil, &mon, &mon, &tue, &wed, &thu, &thu, &sat, &sun}},
			qframe.Enums(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))

		assertNotErr(t, out.Err)
		assertEquals(t, expected, out)
	})

	t.Run("Wont accept unknown values in strict mode", func(t *testing.T) {
		input := `day
tue
mon
foo
`
		out := qframe.ReadCsv(
			strings.NewReader(input),
			qframe.Types(map[string]string{"day": "enum"}),
			qframe.EnumValues(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))

		assertErr(t, out.Err, "unknown enum value")
	})

	t.Run("Fails with too high cardinality column", func(t *testing.T) {
		input := make([]string, 0)
		for i := 0; i < 256; i++ {
			input = append(input, strconv.Itoa(i))
		}

		out := qframe.New(
			map[string]interface{}{"foo": input},
			qframe.Enums(map[string][]string{"foo": nil}))

		assertErr(t, out.Err, "max cardinality")
	})

	t.Run("Fails when enum values specified for non enum column", func(t *testing.T) {
		input := `day
tue
`

		out := qframe.ReadCsv(
			strings.NewReader(input),
			qframe.EnumValues(map[string][]string{"day": {mon, tue, wed, thu, fri, sat, sun}}))

		assertErr(t, out.Err, "specified for non enum column")
	})
}

func TestQFrame_ReadJson(t *testing.T) {
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
			input: `{"STRING1": ["a", "b"], "INT1": [1, 2], "FLOAT1": [1.5, 2.5], "BOOL1": [true, false]}`,
			expected: map[string]interface{}{
				"STRING1": []string{"a", "b"}, "INT1": []int{1, 2}, "FLOAT1": []float64{1.5, 2.5}, "BOOL1": []bool{true, false}},
		},
		{
			input:    `{"STRING1": ["FOO", null]}`,
			expected: map[string]interface{}{"STRING1": []*string{&testString, nil}},
		},
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
			out := qframe.ReadJson(strings.NewReader(tc.input))
			assertNotErr(t, out.Err)
			assertEquals(t, qframe.New(tc.expected), out)
		})
	}
}

func TestQFrame_ToCsv(t *testing.T) {
	table := []struct {
		input    map[string]interface{}
		expected string
	}{
		{
			input: map[string]interface{}{
				"STRING1": []string{"a", "b,c"}, "INT1": []int{1, 2}, "FLOAT1": []float64{1.5, 2.5}, "BOOL1": []bool{true, false}},
			expected: `BOOL1,FLOAT1,INT1,STRING1
true,1.5,1,a
false,2.5,2,"b,c"
`,
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("ToCsv %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			assertNotErr(t, in.Err)

			buf := new(bytes.Buffer)
			err := in.ToCsv(buf)
			assertNotErr(t, err)

			result := buf.String()
			if result != tc.expected {
				t.Errorf("QFrames not equal, %s ||| %s", result, tc.expected)
			}
		})
	}
}

func TestQFrame_ToFromJSON(t *testing.T) {
	config := []qframe.ConfigFunc{qframe.Enums(map[string][]string{"ENUM": {"aa", "bb"}})}
	table := []struct {
		orientation string
		configFuncs []qframe.ConfigFunc
	}{
		{orientation: "records"},
		{orientation: "columns"},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("ToFromJSON %d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			data := map[string]interface{}{
				"STRING1": []string{"añ", "bö☺	"}, "FLOAT1": []float64{1.5, 2.5}, "BOOL1": []bool{true, false}, "ENUM": []string{"aa", "bb"}}
			originalDf := qframe.New(data, config...)
			assertNotErr(t, originalDf.Err)

			err := originalDf.ToJson(buf, tc.orientation)
			assertNotErr(t, err)

			jsonDf := qframe.ReadJson(buf, config...)
			assertNotErr(t, jsonDf.Err)
			assertEquals(t, originalDf, jsonDf)
		})
	}
}

func TestQFrame_ToJSONNaN(t *testing.T) {
	table := []struct {
		orientation string
		expected    string
	}{
		{orientation: "records", expected: `[{"FLOAT1":1.5},{"FLOAT1":NaN}]`},
		{orientation: "columns", expected: `{"FLOAT1":[1.5,NaN]}`},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("ToFromJSON %d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Test the special case NaN, this can currently be encoded but not
			// decoded by the JSON parsers.
			data := map[string]interface{}{"FLOAT1": []float64{1.5, math.NaN()}}
			originalDf := qframe.New(data)
			assertNotErr(t, originalDf.Err)

			err := originalDf.ToJson(buf, tc.orientation)
			assertNotErr(t, err)
			if buf.String() != tc.expected {
				t.Errorf("Not equal: %s ||| %s", buf.String(), tc.expected)
			}
		})
	}
}

func TestQFrame_FilterEnum(t *testing.T) {
	a, b, c, d, e := "a", "b", "c", "d", "e"
	enums := qframe.Enums(map[string][]string{"COL1": {"a", "b", "c", "d", "e"}})
	in := qframe.New(map[string]interface{}{
		"COL1": []*string{&b, &c, &a, nil, &e, &d, nil}}, enums)

	table := []struct {
		filters  []filter.Filter
		expected map[string]interface{}
	}{
		{
			[]filter.Filter{{Column: "COL1", Comparator: ">", Arg: "b"}},
			map[string]interface{}{"COL1": []*string{&c, &e, &d}},
		},
		{
			[]filter.Filter{{Column: "COL1", Comparator: "<", Arg: "b"}},
			map[string]interface{}{"COL1": []*string{&a, nil, nil}},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Filter enum %d", i), func(t *testing.T) {
			expected := qframe.New(tc.expected, enums)
			out := in.Filter(tc.filters...)
			assertEquals(t, expected, out)
		})
	}
}

func TestQFrame_FilterString(t *testing.T) {
	a, b, c, d, e := "a", "b", "c", "d", "e"
	withNil := map[string]interface{}{"COL1": []*string{&b, &c, &a, nil, &e, &d, nil}}

	table := []struct {
		input    map[string]interface{}
		filters  []filter.Filter
		expected map[string]interface{}
	}{
		{
			withNil,
			[]filter.Filter{{Column: "COL1", Comparator: ">", Arg: "b"}},
			map[string]interface{}{"COL1": []*string{&c, &e, &d}},
		},
		{
			withNil,
			[]filter.Filter{{Column: "COL1", Comparator: "<", Arg: "b"}},
			map[string]interface{}{"COL1": []*string{&a, nil, nil}},
		},
		{
			withNil,
			[]filter.Filter{{Column: "COL1", Comparator: "like", Arg: "b"}},
			map[string]interface{}{"COL1": []*string{&b}},
		},
	}

	for i, tc := range table {
		t.Run(fmt.Sprintf("Filter string %d", i), func(t *testing.T) {
			in := qframe.New(tc.input)
			expected := qframe.New(tc.expected)
			out := in.Filter(tc.filters...)
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
			comparator filter.Comparator
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
				in := qframe.New(data, qframe.Enums(enums))
				expected := qframe.New(map[string]interface{}{"COL1": tc.expected}, qframe.Enums(enums))
				out := in.Filter(filter.Filter{Column: "COL1", Comparator: tc.comparator, Arg: tc.arg})
				assertEquals(t, expected, out)
			})
		}
	}
}
