package dataframe_test

import (
	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
	qf "github.com/tobgu/go-qcache/dataframe"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"reflect"
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
