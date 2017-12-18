package dataframe_test

import (
	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
	qf "github.com/tobgu/go-qcache/dataframe"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"reflect"
	"strings"
	"testing"
)

func TestDataGotaFrame_Filter(t *testing.T) {
	a := dataframe.New(
		series.New([]string{"b", "a", "b", "c", "d"}, series.String, "COL.1"),
		series.New([]int{1, 2, 4, 5, 4}, series.Int, "COL.2"),
		series.New([]float64{3.0, 4.0, 5.3, 3.2, 1.2}, series.Float, "COL.3"),
	)
	table := []struct {
		filters []dataframe.F
		expDf   dataframe.DataFrame
	}{
		{
			[]dataframe.F{{"COL.2", series.GreaterEq, 4}},
			dataframe.New(
				series.New([]string{"b", "c", "d"}, series.String, "COL.1"),
				series.New([]int{4, 5, 4}, series.Int, "COL.2"),
				series.New([]float64{5.3, 3.2, 1.2}, series.Float, "COL.3"),
			),
		},
	}

	for i, tc := range table {
		b := a.Filter(tc.filters...)

		if b.Err != nil {
			t.Errorf("Test: %d\nError:%v", i, b.Err)
		}

		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Types(), b.Types()) {
			t.Errorf("Test: %d\nDifferent types:\nA:%v\nB:%v", i, tc.expDf.Types(), b.Types())
		}
		// Check that the colnames are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Names(), b.Names()) {
			t.Errorf("Test: %d\nDifferent colnames:\nA:%v\nB:%v", i, tc.expDf.Names(), b.Names())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(tc.expDf.Records(), b.Records()) {
			t.Errorf("Test: %d\nDifferent values:\nA:%v\nB:%v", i, tc.expDf.Records(), b.Records())
		}
	}
}

func TestQCacheFrame_Filter(t *testing.T) {
	a := qf.New(map[string]interface{}{
		"COL.1": []int{1, 2, 3, 4, 5},
	})

	table := []struct {
		filters []filter.Filter
		expDf   qf.DataFrame
	}{
		{
			[]filter.Filter{{Column: "COL.1", Comparator: ">", Arg: 3}},
			qf.New(map[string]interface{}{"COL.1": []int{4, 5}}),
		},
		{
			[]filter.Filter{
				{Column: "COL.1", Comparator: ">", Arg: 4},
				{Column: "COL.1", Comparator: "<", Arg: 2}},
			qf.New(map[string]interface{}{"COL.1": []int{1, 5}}),
		},
	}

	for i, tc := range table {
		b := a.Filter(tc.filters...)
		equal, reason := tc.expDf.Equals(b)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s", i, reason)
		}
	}
}

func TestQCacheFrame_Sort(t *testing.T) {
	a := qf.New(map[string]interface{}{
		"COL.1": []int{0, 1, 3, 2},
		"COL.2": []int{3, 2, 1, 1},
	})

	table := []struct {
		orders []qf.Order
		expDf  qf.DataFrame
	}{
		{
			[]qf.Order{{Column: "COL.1"}},
			qf.New(map[string]interface{}{
				"COL.1": []int{0, 1, 2, 3},
				"COL.2": []int{3, 2, 1, 1}}),
		},
		{
			[]qf.Order{{Column: "COL.1", Reverse: true}},
			qf.New(map[string]interface{}{
				"COL.1": []int{3, 2, 1, 0},
				"COL.2": []int{1, 1, 2, 3}}),
		},
		{
			[]qf.Order{{Column: "COL.2"}, {Column: "COL.1"}},
			qf.New(map[string]interface{}{
				"COL.1": []int{2, 3, 1, 0},
				"COL.2": []int{1, 1, 2, 3}}),
		},
	}

	for i, tc := range table {
		b := a.Sort(tc.orders...)
		equal, reason := tc.expDf.Equals(b)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s", i, reason)
		}
	}
}

func TestQCacheFrame_SortStability(t *testing.T) {
	a := qf.New(map[string]interface{}{
		"COL.1": []int{0, 1, 3, 2},
		"COL.2": []int{1, 1, 1, 1},
	})

	table := []struct {
		orders []qf.Order
		expDf  qf.DataFrame
	}{
		{
			[]qf.Order{{Column: "COL.2", Reverse: true}, {Column: "COL.1"}},
			qf.New(map[string]interface{}{
				"COL.1": []int{0, 1, 2, 3},
				"COL.2": []int{1, 1, 1, 1}}),
		},
	}

	for i, tc := range table {
		b := a.Sort(tc.orders...)
		equal, reason := tc.expDf.Equals(b)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s", i, reason)
		}
	}
}

func TestQCacheFrame_Distinct(t *testing.T) {
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
		in := qf.New(tc.input)
		out := in.Distinct()
		expDf := qf.New(tc.expected)
		equal, reason := out.Equals(expDf)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s, %s", i, reason, out)
		}
	}
}

func TestQCacheFrame_GroupByAggregate(t *testing.T) {
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
		in := qf.New(tc.input)
		out := in.GroupBy(tc.groupColumns...).Aggregate(tc.aggregations...)
		expDf := qf.New(tc.expected)
		equal, reason := out.Equals(expDf)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s, %s", i, reason, out)
		}
	}
}

func TestQCacheFrame_Select(t *testing.T) {
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
		in := qf.New(tc.input)
		out := in.Select(tc.selectCols...)
		expDf := qf.New(tc.expected)
		equal, reason := out.Equals(expDf)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s, %s", i, reason, out)
		}
	}
}

func TestQCacheFrame_Slice(t *testing.T) {
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
		in := qf.New(tc.input)
		out := in.Slice(tc.start, tc.end)
		expDf := qf.New(tc.expected)
		equal, reason := out.Equals(expDf)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s, %s", i, reason, out)
		}
	}
}

func TestQCacheFrame_FromCsv(t *testing.T) {
	table := []struct {
		input    string
		expected map[string]interface{}
	}{
		{
			input: "foo,bar\n1,2\n3,4\n",
			expected: map[string]interface{}{
				"foo": []int{1, 3},
				"bar": []int{2, 4}},
		},
		{
			input: "int,float,bool,string\n1,2.5,true,hello\n10,20.5,false,\"bye, bye\"",
			expected: map[string]interface{}{
				"int":    []int{1, 10},
				"float":  []float64{2.5, 20.5},
				"bool":   []bool{true, false},
				"string": []string{"hello", "bye, bye"}},
		},
	}

	for i, tc := range table {
		out := qf.FromCsv(strings.NewReader(tc.input), map[string]qf.ColumnType{})
		if out.Err != nil {
			t.Errorf("error in FromCsv: %s", out.Err.Error())
		}

		expDf := qf.New(tc.expected)
		equal, reason := out.Equals(expDf)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s, %s", i, reason, out)
		}
	}
}

func TestQCacheFrame_FromJsonRecords(t *testing.T) {

}

func TestQCacheFrame_FromJsonSeries(t *testing.T) {
	table := []struct {
		input    string
		expected map[string]interface{}
	}{
		{
			input: `{"STRING1": ["a", "b"], "INT1": [1, 2], "FLOAT1": [1.5, 2.5], "BOOL1": [true, false]}`,
			expected: map[string]interface{}{
				"STRING1": []string{"a", "b"}, "INT1": []int{1, 2}, "FLOAT1": []float64{1.5, 2.5}, "BOOL1": []bool{true, false}},
		},
	}

	for i, tc := range table {
		out := qf.FromJson(strings.NewReader(tc.input))
		if out.Err != nil {
			t.Errorf("error in FromJson: %s", out.Err.Error())
		}

		expDf := qf.New(tc.expected)
		equal, reason := out.Equals(expDf)
		if !equal {
			t.Errorf("TC %d: Dataframes not equal, %s, %s", i, reason, out)
		}
	}
}
