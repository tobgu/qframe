package qframe_test

import (
	"bytes"
	stdcsv "encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"testing"

	qf "github.com/tobgu/qframe"
	"github.com/tobgu/qframe/config/csv"
	"github.com/tobgu/qframe/config/groupby"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/types"
)

func genInts(seed int64, size int) []int {
	result := make([]int, size)
	rand.Seed(seed)
	if seed == noSeed {
		// Sorted slice
		for ix := range result {
			result[ix] = ix
		}
	} else {
		// Random slice
		for ix := range result {
			result[ix] = rand.Intn(size)
		}
	}

	return result
}

func genIntsWithCardinality(seed int64, size, cardinality int) []int {
	result := genInts(seed, size)
	for i, x := range result {
		result[i] = x % cardinality
	}

	return result
}

func genStringsWithCardinality(seed int64, size, cardinality, strLen int) []string {
	baseStr := "abcdefghijklmnopqrstuvxyz"[:strLen]
	result := make([]string, size)
	for i, x := range genIntsWithCardinality(seed, size, cardinality) {
		result[i] = fmt.Sprintf("%s%d", baseStr, x)
	}
	return result
}

const noSeed int64 = 0
const seed1 int64 = 1
const seed2 int64 = 2
const seed3 int64 = 3
const seed4 int64 = 4
const frameSize = 100000

func exampleIntFrame(size int) qf.QFrame {
	return qf.New(map[string]interface{}{
		"S1": genInts(seed1, size),
		"S2": genInts(seed2, size),
		"S3": genInts(seed3, size),
		"S4": genInts(seed4, size)})
}

func BenchmarkQFrame_FilterIntBuiltIn(b *testing.B) {
	data := exampleIntFrame(frameSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newData := data.Filter(qf.Or(
			qf.Filter{Column: "S1", Comparator: "<", Arg: frameSize / 10},
			qf.Filter{Column: "S2", Comparator: "<", Arg: frameSize / 10},
			qf.Filter{Column: "S3", Comparator: ">", Arg: int(0.9 * frameSize)}))

		if newData.Len() != 27142 {
			b.Errorf("Length was %d, Err: %s", newData.Len(), newData.Err)
		}
	}
}

func lessThan(limit int) func(int) bool {
	return func(x int) bool { return x < limit }
}

func greaterThan(limit int) func(int) bool {
	return func(x int) bool { return x > limit }
}

func BenchmarkQFrame_FilterIntGeneral(b *testing.B) {
	data := exampleIntFrame(frameSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newData := data.Filter(qf.Or(
			qf.Filter{Column: "S1", Comparator: lessThan(frameSize / 10)},
			qf.Filter{Column: "S2", Comparator: lessThan(frameSize / 10)},
			qf.Filter{Column: "S3", Comparator: greaterThan(int(0.9 * frameSize))}))

		if newData.Len() != 27142 {
			b.Errorf("Length was %d, Err: %s", newData.Len(), newData.Err)
		}
	}
}

func rangeSlice(size int) []int {
	result := make([]int, size)
	for i := 0; i < size; i++ {
		result[i] = i
	}
	return result
}

func BenchmarkQFrame_FilterIntBuiltinIn(b *testing.B) {
	data := exampleIntFrame(frameSize)
	slice := rangeSlice(frameSize / 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newData := data.Filter(qf.Filter{Column: "S1", Comparator: "in", Arg: slice})
		if newData.Err != nil {
			b.Errorf("Length was Err: %s", newData.Err)
		}
	}
}

func intInFilter(input []int) func(int) bool {
	set := make(map[int]struct{}, len(input))
	for _, x := range input {
		set[x] = struct{}{}
	}

	return func(x int) bool {
		_, ok := set[x]
		return ok
	}
}

func BenchmarkQFrame_FilterIntGeneralIn(b *testing.B) {
	data := exampleIntFrame(frameSize)
	slice := rangeSlice(frameSize / 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newData := data.Filter(qf.Filter{Column: "S1", Comparator: intInFilter(slice)})
		if newData.Err != nil {
			b.Errorf("Length was Err: %s", newData.Err)
		}
	}
}

func BenchmarkQFrame_FilterNot(b *testing.B) {
	data := qf.New(map[string]interface{}{
		"S1": genInts(seed1, frameSize)})
	f := qf.Filter{Column: "S1", Comparator: "<", Arg: frameSize - frameSize/10, Inverse: true}

	b.Run("qframe", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			newData := data.Filter(f)
			if newData.Len() != 9882 {
				b.Errorf("Length was %d", newData.Len())
			}
		}
	})

	b.Run("filter", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			clause := qf.Not(qf.Filter(filter.Filter{Column: "S1", Comparator: "<", Arg: frameSize - frameSize/10}))
			newData := data.Filter(clause)
			if newData.Len() != 9882 {
				b.Errorf("Length was %d", newData.Len())
			}
		}
	})
}

func BenchmarkQFrame_Sort(b *testing.B) {
	data := qf.New(map[string]interface{}{
		"S1": genInts(seed1, frameSize),
		"S2": genInts(seed2, frameSize),
		"S3": genInts(seed3, frameSize),
		"S4": genInts(seed4, frameSize)})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		newData := data.Sort(qf.Order{Column: "S1"}, qf.Order{Column: "S2", Reverse: true})
		if newData.Err != nil {
			b.Errorf("Unexpected sort error: %s", newData.Err)
		}
	}
}

func BenchmarkQFrame_Sort1Col(b *testing.B) {
	data := qf.New(map[string]interface{}{
		"S1": genInts(seed1, frameSize),
		"S2": genInts(seed2, frameSize),
		"S3": genInts(seed3, frameSize),
		"S4": genInts(seed4, frameSize)})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		newData := data.Sort(qf.Order{Column: "S1"})
		if newData.Err != nil {
			b.Errorf("Unexpected sort error: %s", newData.Err)
		}
	}
}

func BenchmarkQFrame_SortSorted(b *testing.B) {
	data := qf.New(map[string]interface{}{
		"S1": genInts(noSeed, frameSize),
		"S2": genInts(noSeed, frameSize),
		"S3": genInts(noSeed, frameSize),
		"S4": genInts(noSeed, frameSize)})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		newData := data.Sort(qf.Order{Column: "S1"}, qf.Order{Column: "S2", Reverse: true})
		if newData.Err != nil {
			b.Errorf("Unexpected sort error: %s", newData.Err)
		}
	}
}

func csvBytes(rowCount int) []byte {
	buf := new(bytes.Buffer)
	writer := stdcsv.NewWriter(buf)
	writer.Write([]string{"INT1", "INT2", "FLOAT1", "FLOAT2", "BOOL1", "STRING1", "STRING2"})
	for i := 0; i < rowCount; i++ {
		writer.Write([]string{"123", "1234567", "5.2534", "9834543.25", "true", fmt.Sprintf("Foo bar baz %d", i%10000), "ABCDEFGHIJKLMNOPQRSTUVWXYZ"})
	}
	writer.Flush()

	csvBytes, _ := ioutil.ReadAll(buf)
	return csvBytes
}

func csvEnumBytes(rowCount, cardinality int) []byte {
	buf := new(bytes.Buffer)
	writer := stdcsv.NewWriter(buf)
	writer.Write([]string{"COL1", "COL2"})
	for i := 0; i < rowCount; i++ {
		writer.Write([]string{
			fmt.Sprintf("Foo bar baz %d", i%cardinality),
			fmt.Sprintf("AB%d", i%cardinality)})
	}
	writer.Flush()

	csvBytes, _ := ioutil.ReadAll(buf)
	return csvBytes
}

func BenchmarkQFrame_ReadCSV(b *testing.B) {
	rowCount := 100000
	input := csvBytes(rowCount)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(input)
		df := qf.ReadCSV(r, csv.RowCountHint(rowCount))
		if df.Err != nil {
			b.Errorf("Unexpected CSV error: %s", df.Err)
		}

		if df.Len() != rowCount {
			b.Errorf("Unexpected size: %d", df.Len())
		}
	}
}

func BenchmarkQFrame_ReadCSVEnum(b *testing.B) {
	rowCount := 100000
	cardinality := 20
	input := csvEnumBytes(rowCount, cardinality)

	for _, t := range []string{"enum"} {
		b.Run(fmt.Sprintf("Type %s", t), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := bytes.NewReader(input)
				df := qf.ReadCSV(r, csv.Types(map[string]string{"COL1": t, "COL2": t}))
				if df.Err != nil {
					b.Errorf("Unexpected CSV error: %s", df.Err)
				}

				if df.Len() != rowCount {
					b.Errorf("Unexpected size: %d", df.Len())
				}
			}
		})
	}
}

func jsonRecords(rowCount int) []byte {
	record := map[string]interface{}{
		"INT1":    123,
		"INT2":    1234567,
		"FLOAT1":  5.2534,
		"FLOAT2":  9834543.25,
		"BOOL1":   true,
		"STRING1": "Foo bar baz",
		"STRING2": "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}
	records := make([]map[string]interface{}, rowCount)
	for i := range records {
		records[i] = record
	}

	result, err := json.Marshal(records)
	if err != nil {
		panic(err)
	}
	return result
}

func intSlice(value, size int) []int {
	result := make([]int, size)
	for i := range result {
		result[i] = value
	}

	return result
}

func floatSlice(value float64, size int) []float64 {
	result := make([]float64, size)
	for i := range result {
		result[i] = value
	}

	return result
}

func boolSlice(value bool, size int) []bool {
	result := make([]bool, size)
	for i := range result {
		result[i] = value
	}

	return result
}

func stringSlice(value string, size int) []string {
	result := make([]string, size)
	for i := range result {
		result[i] = value
	}

	return result
}

func exampleData(rowCount int) map[string]interface{} {
	return map[string]interface{}{
		"INT1":    intSlice(123, rowCount),
		"INT2":    intSlice(1234567, rowCount),
		"FLOAT1":  floatSlice(5.2534, rowCount),
		"FLOAT2":  floatSlice(9834543.25, rowCount),
		"BOOL1":   boolSlice(false, rowCount),
		"STRING1": stringSlice("Foo bar baz", rowCount),
		"STRING2": stringSlice("ABCDEFGHIJKLMNOPQRSTUVWXYZ", rowCount)}
}

func BenchmarkQFrame_FromJSONRecords(b *testing.B) {
	rowCount := 10000
	input := jsonRecords(rowCount)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(input)
		df := qf.ReadJSON(r)
		if df.Err != nil {
			b.Errorf("Unexpected JSON error: %s", df.Err)
		}

		if df.Len() != rowCount {
			b.Errorf("Unexpected size: %d", df.Len())
		}
	}
}

func BenchmarkQFrame_ToCSV(b *testing.B) {
	rowCount := 100000
	input := exampleData(rowCount)
	df := qf.New(input)
	if df.Err != nil {
		b.Errorf("Unexpected New error: %s", df.Err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		err := df.ToCSV(buf)
		if err != nil {
			b.Errorf("Unexpected ToCSV error: %s", err)
		}
	}
}

func toJSON(b *testing.B) {
	rowCount := 100000
	input := exampleData(rowCount)
	df := qf.New(input)
	if df.Err != nil {
		b.Errorf("Unexpected New error: %s", df.Err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		err := df.ToJSON(buf)
		if err != nil {
			b.Errorf("Unexpected ToCSV error: %s", err)
		}
	}
}

func BenchmarkQFrame_ToJSONRecords(b *testing.B) {
	toJSON(b)
}

func BenchmarkQFrame_FilterEnumVsString(b *testing.B) {
	rowCount := 100000
	cardinality := 9
	input := csvEnumBytes(rowCount, cardinality)

	table := []struct {
		types         map[string]string
		column        string
		filter        string
		expectedCount int
		comparator    string
	}{
		{
			types:         map[string]string{"COL1": "enum", "COL2": "enum"},
			column:        "COL1",
			filter:        "Foo bar baz 5",
			expectedCount: 55556,
		},
		{
			types:         map[string]string{},
			column:        "COL1",
			filter:        "Foo bar baz 5",
			expectedCount: 55556,
		},
		{
			types:         map[string]string{},
			column:        "COL2",
			filter:        "AB5",
			expectedCount: 55556,
		},
		{
			types:         map[string]string{},
			column:        "COL1",
			filter:        "%bar baz 5%",
			expectedCount: 11111,
			comparator:    "like",
		},
		{
			types:         map[string]string{},
			column:        "COL1",
			filter:        "%bar baz 5%",
			expectedCount: 11111,
			comparator:    "ilike",
		},
		{
			types:         map[string]string{"COL1": "enum", "COL2": "enum"},
			column:        "COL1",
			filter:        "%bar baz 5%",
			expectedCount: 11111,
			comparator:    "ilike",
		},
	}
	for _, tc := range table {
		r := bytes.NewReader(input)
		df := qf.ReadCSV(r, csv.Types(tc.types))
		if tc.comparator == "" {
			tc.comparator = "<"
		}

		b.Run(fmt.Sprintf("Filter %s %s, enum: %t", tc.filter, tc.comparator, len(tc.types) > 0), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				newDf := df.Filter(qf.Filter{Comparator: tc.comparator, Column: tc.column, Arg: tc.filter})
				if newDf.Len() != tc.expectedCount {
					b.Errorf("Unexpected count: %d, expected: %d", newDf.Len(), tc.expectedCount)
				}
			}
		})
	}
}

func benchApply(b *testing.B, name string, input qf.QFrame, fn interface{}) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := input.Apply(qf.Instruction{Fn: fn, DstCol: "COL1", SrcCol1: "COL1"})
			if result.Err != nil {
				b.Errorf("Err: %d, %s", result.Len(), result.Err)
			}
		}
	})

}

func BenchmarkQFrame_ApplyStringToString(b *testing.B) {
	rowCount := 100000
	cardinality := 9
	input := csvEnumBytes(rowCount, cardinality)
	r := bytes.NewReader(input)
	df := qf.ReadCSV(r)

	benchApply(b, "Instruction with custom function", df, toUpper)
	benchApply(b, "Instruction with builtin function", df, "ToUpper")
}

func BenchmarkQFrame_ApplyEnum(b *testing.B) {
	rowCount := 100000
	cardinality := 9
	input := csvEnumBytes(rowCount, cardinality)
	r := bytes.NewReader(input)
	df := qf.ReadCSV(r, csv.Types(map[string]string{"COL1": "enum"}))

	benchApply(b, "Instruction with custom function", df, toUpper)
	benchApply(b, "Instruction with built in function", df, "ToUpper")
	benchApply(b, "Instruction int function (for reference)", df, func(x *string) int { return len(*x) })
}

func BenchmarkQFrame_IntView(b *testing.B) {
	f := qf.New(map[string]interface{}{"S1": genInts(seed1, frameSize)}).Sort(qf.Order{Column: "S1"})
	v, err := f.IntView("S1")
	if err != nil {
		b.Error(err)
	}

	b.Run("For loop", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := 0
			for j := 0; j < v.Len(); j++ {
				result += v.ItemAt(j)
			}

			// Don't allow the result to be optimized away
			if result == 0 {
				b.Fail()
			}
		}
	})

	b.Run("Slice", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := 0
			for _, j := range v.Slice() {
				result += j
			}

			// Don't allow the result to be optimized away
			if result == 0 {
				b.Fail()
			}
		}
	})

}

func BenchmarkQFrame_StringView(b *testing.B) {
	rowCount := 100000
	cardinality := 9
	input := csvEnumBytes(rowCount, cardinality)
	r := bytes.NewReader(input)
	f := qf.ReadCSV(r).Sort(qf.Order{Column: "COL1"})
	v, err := f.StringView("COL1")
	if err != nil {
		b.Error(err)
	}

	b.Run("For loop", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var last *string
			for j := 0; j < v.Len(); j++ {
				last = v.ItemAt(j)
			}

			// Don't allow the result to be optimized away
			if len(*last) == 0 {
				b.Fail()
			}
		}
	})

	b.Run("Slice", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var last *string
			for _, j := range v.Slice() {
				last = j
			}

			// Don't allow the result to be optimized away
			if len(*last) == 0 {
				b.Fail()
			}
		}
	})
}

func BenchmarkQFrame_EvalInt(b *testing.B) {
	df := exampleIntFrame(100000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := df.Eval("RESULT", qf.Expr("+", qf.Expr("+", types.ColumnName("S1"), types.ColumnName("S2")), qf.Val(2)))
		if result.Err != nil {
			b.Errorf("Err: %d, %s", result.Len(), result.Err)
		}
	}
}

func BenchmarkGroupBy(b *testing.B) {
	table := []struct {
		name         string
		size         int
		cardinality1 int
		cardinality2 int
		cardinality3 int
		cols         []string
	}{
		{name: "single col", size: 100000, cardinality1: 1000, cardinality2: 10, cardinality3: 2, cols: []string{"COL1"}},
		{name: "triple col", size: 100000, cardinality1: 1000, cardinality2: 10, cardinality3: 2, cols: []string{"COL1", "COL2", "COL3"}},
		{name: "high cardinality", size: 100000, cardinality1: 50000, cardinality2: 1, cardinality3: 1, cols: []string{"COL1"}},
		{name: "low cardinality", size: 100000, cardinality1: 5, cardinality2: 1, cardinality3: 1, cols: []string{"COL1"}},
		{name: "small frame", size: 100, cardinality1: 20, cardinality2: 1, cardinality3: 1, cols: []string{"COL1"}},
	}

	for _, tc := range table {
		for _, dataType := range []string{"string", "integer"} {
			b.Run(fmt.Sprintf("%s dataType=%s", tc.name, dataType), func(b *testing.B) {
				var input map[string]interface{}
				if dataType == "integer" {
					input = map[string]interface{}{
						"COL1": genIntsWithCardinality(seed1, tc.size, tc.cardinality1),
						"COL2": genIntsWithCardinality(seed2, tc.size, tc.cardinality2),
						"COL3": genIntsWithCardinality(seed3, tc.size, tc.cardinality3),
					}
				} else {
					input = map[string]interface{}{
						"COL1": genStringsWithCardinality(seed1, tc.size, tc.cardinality1, 10),
						"COL2": genStringsWithCardinality(seed2, tc.size, tc.cardinality2, 10),
						"COL3": genStringsWithCardinality(seed3, tc.size, tc.cardinality3, 10),
					}
				}
				df := qf.New(input)
				b.ReportAllocs()
				b.ResetTimer()
				var stats qf.GroupStats
				for i := 0; i < b.N; i++ {
					grouper := df.GroupBy(groupby.Columns(tc.cols...))
					if grouper.Err != nil {
						b.Errorf(grouper.Err.Error())
					}
					stats = grouper.Stats
				}

				_ = stats
				// b.Logf("Stats: %#v", stats)

				/*
					// Remember to put -alloc_space there otherwise it will be empty since no space is used anymore
					go tool pprof -alloc_space qframe.test mem_singlegroup.prof/

					(pprof) web
					(pprof) list insertEntry

				*/
			})
		}
	}
}

func BenchmarkDistinctNull(b *testing.B) {
	inputLen := 100000
	input := make([]*string, inputLen)
	foo := "foo"
	input[0] = &foo
	df := qf.New(map[string]interface{}{"COL1": input})

	table := []struct {
		groupByNull bool
		expectedLen int
	}{
		{groupByNull: false, expectedLen: inputLen},
		{groupByNull: true, expectedLen: 2},
	}

	for _, tc := range table {
		b.Run(fmt.Sprintf("groupByNull=%v", tc.groupByNull), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := df.Distinct(groupby.Columns("COL1"), groupby.Null(tc.groupByNull))
				if out.Err != nil {
					b.Errorf(out.Err.Error())

				}
				if tc.expectedLen != out.Len() {
					b.Errorf("%d != %d", tc.expectedLen, out.Len())
				}
			}
		})
	}
}

/*
Go 1.7

go test -bench=.
tpp
go tool pprof dataframe.test filter_cpu.out

Initial results:
BenchmarkDataFrame_Filter-2     	      30	  40542568 ns/op	 7750730 B/op	  300134 allocs/op
BenchmarkQCacheFrame_Filter-2   	     300	   3997702 ns/op	  991720 B/op	      14 allocs/op

After converting bool index to int index before subsetting:
BenchmarkDataFrame_Filter-2     	      30	  40330898 ns/op	 7750731 B/op	  300134 allocs/op
BenchmarkQCacheFrame_Filter-2   	     500	   2631666 ns/op	 2098409 B/op	      38 allocs/op

Only evolve indexes, don't realize the dataframe (note that the tests tests are running slower in general,
the BenchmarkDataFrame_Filter is the exact same as above):
BenchmarkDataFrame_Filter-2     	      30	  46309948 ns/op	 7750730 B/op	  300134 allocs/op
BenchmarkQCacheFrame_Filter-2   	    1000	   2083198 ns/op	  606505 B/op	      29 allocs/op

Initial sorting implementation using built in interface-based sort.Sort. Not sure if this is actually
OK going forward since the Sort is not guaranteed to be stable.
BenchmarkDataFrame_Sort-2     	       5	 245155627 ns/op	50547024 B/op	     148 allocs/op
BenchmarkQFrame_Sort-2        	      20	  78297649 ns/op	  401504 B/op	       3 allocs/op

Sorting using a copy of the stdlib Sort but with the Interface switched to a concrete type. A fair
bit quicker but not as quick as expected.
BenchmarkDataFrame_Filter-2   	      30	  46760882 ns/op	 7750731 B/op	  300134 allocs/op
BenchmarkQFrame_Filter-2      	    1000	   2062230 ns/op	  606504 B/op	      29 allocs/op
BenchmarkDataFrame_Sort-2     	       5	 242068573 ns/op	50547024 B/op	     148 allocs/op
BenchmarkQFrame_Sort-2        	      30	  50057905 ns/op	  401408 B/op	       1 allocs/op

Sorting done using above copy but using stable sort for all but the last order by column.
BenchmarkDataFrame_Filter-2   	      30	  44818293 ns/op	 7750731 B/op	  300134 allocs/op
BenchmarkQFrame_Filter-2      	    1000	   2126636 ns/op	  606505 B/op	      29 allocs/op
BenchmarkDataFrame_Sort-2     	       5	 239796901 ns/op	50547024 B/op	     148 allocs/op
BenchmarkQFrame_Sort-2        	      10	 119140365 ns/op	  401408 B/op	       1 allocs/op

Test using timsort instead of built in sort, gives stability by default. Better, but slightly disappointing.
BenchmarkDataFrame_Filter-2   	      30	  44576205 ns/op	 7750731 B/op	  300134 allocs/op
BenchmarkQFrame_Filter-2      	    1000	   2121513 ns/op	  606504 B/op	      29 allocs/op
BenchmarkDataFrame_Sort-2     	       5	 245788389 ns/op	50547024 B/op	     148 allocs/op
BenchmarkQFrame_Sort-2        	      20	  94122521 ns/op	 3854980 B/op	      25 allocs/op

// timsort
BenchmarkDataFrame_Filter-2    	      30	  47960157 ns/op	 7750731 B/op	  300134 allocs/op
BenchmarkQFrame_Filter-2       	    1000	   2174167 ns/op	  606504 B/op	      29 allocs/op
BenchmarkDataFrame_Sort-2      	       5	 281561310 ns/op	50547024 B/op	     148 allocs/op
BenchmarkQFrame_Sort-2         	      20	  98123611 ns/op	 3854984 B/op	      25 allocs/op
BenchmarkQFrame_Sort1Col-2     	      30	  45322479 ns/op	 2128192 B/op	      13 allocs/op
BenchmarkQFrame_SortSorted-2   	     300	   4428537 ns/op	 2011788 B/op	       9 allocs/op

// stdlib specific
BenchmarkDataFrame_Filter-2    	      20	  50015836 ns/op	 7750730 B/op	  300134 allocs/op
BenchmarkQFrame_Filter-2       	     500	   2205289 ns/op	  606504 B/op	      29 allocs/op
BenchmarkDataFrame_Sort-2      	       5	 270738781 ns/op	50547024 B/op	     148 allocs/op
BenchmarkQFrame_Sort-2         	      10	 137043496 ns/op	  401408 B/op	       1 allocs/op
BenchmarkQFrame_Sort1Col-2     	      50	  30669308 ns/op	  401408 B/op	       1 allocs/op
BenchmarkQFrame_SortSorted-2   	      50	  28217092 ns/op	  401408 B/op	       1 allocs/op

// stdlib
BenchmarkDataFrame_Filter-2    	      30	  50137069 ns/op	 7750731 B/op	  300134 allocs/op
BenchmarkQFrame_Filter-2       	    1000	   2308053 ns/op	  606504 B/op	      29 allocs/op
BenchmarkDataFrame_Sort-2      	       5	 288688150 ns/op	50547024 B/op	     148 allocs/op
BenchmarkQFrame_Sort-2         	      10	 206407019 ns/op	  401536 B/op	       3 allocs/op
BenchmarkQFrame_Sort1Col-2     	      30	  46005496 ns/op	  401472 B/op	       2 allocs/op
BenchmarkQFrame_SortSorted-2   	      20	  54300644 ns/op	  401536 B/op	       3 allocs/op

// stdlib specific + co-locate data to sort on, ~2x speedup compared to separate index
BenchmarkDataFrame_Filter-2    	      30	  46678558 ns/op	 7750731 B/op	  300134 allocs/op
BenchmarkQFrame_Filter-2       	    1000	   2218767 ns/op	  606504 B/op	      29 allocs/op
BenchmarkDataFrame_Sort-2      	       5	 254261311 ns/op	50547024 B/op	     148 allocs/op
BenchmarkQFrame_Sort-2         	      20	  68903882 ns/op	 3612672 B/op	       3 allocs/op
BenchmarkQFrame_Sort1Col-2     	     100	  15970577 ns/op	 2007040 B/op	       2 allocs/op
BenchmarkQFrame_SortSorted-2   	     100	  14389450 ns/op	 3612672 B/op	       3 allocs/op

// Different sort implementation that likely performs better for multi column sort but
// slightly worse for singe column sort.
BenchmarkQFrame_Sort-2         	      30	  47600788 ns/op	  401626 B/op	       4 allocs/op
BenchmarkQFrame_Sort1Col-2     	      30	  43807643 ns/op	  401472 B/op	       3 allocs/op
BenchmarkQFrame_SortSorted-2   	      50	  24775838 ns/op	  401536 B/op	       4 allocs/op

// Initial CSV implementation for int, 4 x 100000.
BenchmarkQFrame_IntFromCSV-2      	      20	  55921060 ns/op	30167012 B/op	     261 allocs/op
BenchmarkDataFrame_IntFromCSV-2   	       5	 243541282 ns/op	41848809 B/op	  900067 allocs/op

// Type detecting CSV implementation, 100000 x "123", "1234567", "5.2534", "9834543.25", "true", "Foo bar baz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
BenchmarkQFrame_IntFromCSV-2   	      10	 101362864 ns/op	87707785 B/op	  200491 allocs/op

// JSON, 10000 rows
BenchmarkDataFrame_ReadJSON-2          	      10	 176107262 ns/op	24503045 B/op	  670112 allocs/op
BenchmarkQFrame_FromJSONRecords-2   	      10	 117408651 ns/op	15132420 B/op	  430089 allocs/op
BenchmarkQFrame_FromJSONColumns-2   	      10	 104641079 ns/op	15342302 B/op	  220842 allocs/op

// JSON with easyjson generated unmarshal
BenchmarkQFrame_FromJSONColumns-2   	      50	  24764232 ns/op	 6730738 B/op	   20282 allocs/op

// ToCSV, vanilla implementation based on stdlib csv, 100000 records
BenchmarkQFrame_ToCSV-2   	       5	 312478023 ns/op	26365360 B/op	  600017 allocs/op

// ToJSON, performance is not super impressive... 100000 records
BenchmarkQFrame_ToJSONRecords-2   	       2	 849280921 ns/op	181573400 B/op	 3400028 allocs/op
BenchmarkQFrame_ToJSONColumns-2   	       5	 224702680 ns/op	33782697 B/op	     513 allocs/op

// Testing jsoniter with some success
BenchmarkQFrame_ToJSONRecords-2   	       2	 646738504 ns/op	137916264 B/op	 3600006 allocs/op
BenchmarkQFrame_ToJSONColumns-2   	      20	  99932317 ns/op	34144682 B/op	     490 allocs/op

// Python, as a comparison, with corresponding list of dictionaries:
>>> import json
>>> import time
>>> t0 = time.time(); j = json.dumps(x); time.time() - t0
0.33017611503601074
>>> import ujson
>>> t0 = time.time(); j = ujson.dumps(x); time.time() - t0
0.17484211921691895

// Custom encoder for JSON records, now we're talking
BenchmarkQFrame_ToJSONRecords-2   	      20	  87437635 ns/op	53638858 B/op	      35 allocs/op
BenchmarkQFrame_ToJSONColumns-2   	      10	 102566155 ns/op	37746546 B/op	     547 allocs/op

// Reuse string pointers when reading CSV
Before:
BenchmarkQFrame_ReadCSV-2   	      10	 119385221 ns/op	92728576 B/op	  400500 allocs/op

After:
BenchmarkQFrame_ReadCSV-2   	      10	 108917111 ns/op	86024686 B/op	   20790 allocs/op

// Initial CSV read Enum, 2 x 100000 cells with cardinality 20
BenchmarkQFrame_ReadCSVEnum/Type_enum-2         	      50	  28081769 ns/op	19135232 B/op	     213 allocs/op
BenchmarkQFrame_ReadCSVEnum/Type_string-2       	      50	  28563580 ns/op	20526743 B/op	     238 allocs/op

Total saving 1,4 Mb in line with what was expected given that one byte is used per entry instead of eight

// Enum vs string filtering
BenchmarkQFrame_FilterEnumVsString/Test_0-2         	    2000	    714369 ns/op	  335888 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Test_1-2         	    1000	   1757913 ns/op	  335888 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Test_2-2         	    1000	   1792186 ns/op	  335888 B/op	       3 allocs/op

// Initial "(i)like" matching of strings using regexes

Case sensitive:
BenchmarkQFrame_FilterEnumVsString/Test_3-2         	     100	  11765579 ns/op	  162600 B/op	      74 allocs/op

Case insensitive:
BenchmarkQFrame_FilterEnumVsString/Test_4-2         	      30	  41680939 ns/op	  163120 B/op	      91 allocs/op

// Remove the need for regexp in many cases:
BenchmarkQFrame_FilterEnumVsString/Filter_Foo_bar_baz_5_<-2         	    2000	    692662 ns/op	  335888 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_Foo_bar_baz_5_<#01-2      	    1000	   1620056 ns/op	  335893 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_AB5_<-2                   	    1000	   1631806 ns/op	  335888 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_like-2        	     500	   3245751 ns/op	  155716 B/op	       4 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_ilike-2       	     100	  11418693 ns/op	  155873 B/op	       8 allocs/op

// Enum string matching, speedy:
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_ilike,_enum:_false-2      	     100	  11583233 ns/op	  155792 B/op	       8 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_ilike,_enum:_true-2       	    2000	    729671 ns/op	  155989 B/op	      13 allocs/op

// Inverse (not) filtering:
BenchmarkQFrame_FilterNot-2   	    2000	    810831 ns/op	  147459 B/op	       2 allocs/op

// Performance tweak for single, simple, clause statements to put them on par with calling the
// Qframe Filter function directly

// Before
BenchmarkQFrame_FilterNot/qframe-2         	    2000	    716280 ns/op	  147465 B/op	       2 allocs/op
BenchmarkQFrame_FilterNot/filter-2         	    2000	   1158211 ns/op	  516161 B/op	       4 allocs/op

// After
BenchmarkQFrame_FilterNot/qframe-2         	    2000	    713147 ns/op	  147465 B/op	       2 allocs/op
BenchmarkQFrame_FilterNot/filter-2         	    2000	    726766 ns/op	  147521 B/op	       3 allocs/op

// Restructure string column to use a byte blob with offsets and lengths
BenchmarkQFrame_ReadCSV-2       	      20	  85906027 ns/op	84728656 B/op	     500 allocs/op

// Fix string clause to make better use of the new string blob structure:
BenchmarkQFrame_FilterEnumVsString/Filter_Foo_bar_baz_5_<,_enum:_true-2         	    2000	    691081 ns/op	  335888 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_Foo_bar_baz_5_<,_enum:_false-2        	    1000	   1902665 ns/op	  335889 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_AB5_<,_enum:_false-2                  	    1000	   1935237 ns/op	  335888 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_like,_enum:_false-2       	     500	   3855434 ns/op	  155680 B/op	       4 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_ilike,_enum:_false-2      	     100	  11881963 ns/op	  155792 B/op	       8 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_ilike,_enum:_true-2       	    2000	    691971 ns/op	  155824 B/op	       9 allocs/op

// Compare string to upper, first as general custom function, second as specialized built in function.
BenchmarkQFrame_ApplyStringToString/Apply_with_custom_function-2         	      30	  42895890 ns/op	17061043 B/op	  400020 allocs/op
BenchmarkQFrame_ApplyStringToString/Apply_with_built_in_function-2       	     100	  12163217 ns/op	 2107024 B/op	       7 allocs/op

// Compare apply for enums
BenchmarkQFrame_ApplyEnum/Apply_with_custom_function-2         	      50	  38505068 ns/op	15461041 B/op	  300020 allocs/op
BenchmarkQFrame_ApplyEnum/Apply_with_built_in_function-2       	  300000	      3566 ns/op	    1232 B/op	      23 allocs/op
BenchmarkQFrame_ApplyEnum/Apply_int_function_(for_reference)-2 	    1000	   1550604 ns/op	  803491 B/op	       6 allocs/op

// The difference in using built in filter vs general filter func passed as argument. Basically the overhead of a function
// call for each row. Smaller than I would have thought actually.
BenchmarkQFrame_FilterIntBuiltIn-2   	    1000	   1685483 ns/op	  221184 B/op	       2 allocs/op
BenchmarkQFrame_FilterIntGeneral-2   	     500	   2631678 ns/op	  221239 B/op	       5 allocs/op

// Only minor difference in performance between built in and general filtering here. Map access dominates
// the execution time.
BenchmarkQFrame_FilterIntGeneralIn-2   	     500	   3321307 ns/op	  132571 B/op	      10 allocs/op
BenchmarkQFrame_FilterIntBuiltinIn-2   	     500	   3055410 ns/op	  132591 B/op	      10 allocs/op

// Without the sort the slice version is actually a bit faster even though it allocates a new slice and iterates
// over the data twice.
BenchmarkQFrame_IntView/For_loop-2         	    2000	    763169 ns/op	       0 B/op	       0 allocs/op
BenchmarkQFrame_IntView/Slice-2            	    2000	    806672 ns/op	  802816 B/op	       1 allocs/op

BenchmarkQFrame_StringView/For_loop-2         	     200	   6242471 ns/op	 1600000 B/op	  100000 allocs/op
BenchmarkQFrame_StringView/Slice-2            	     100	  14006634 ns/op	 4002816 B/op	  200001 allocs/op

// Same as above but modified to work with enums in COL1
BenchmarkQFrame_StringView/For_loop-2         	    1000	   1651190 ns/op	       0 B/op	       0 allocs/op
BenchmarkQFrame_StringView/Slice-2            	     500	   2697675 ns/op	  802816 B/op	       1 allocs/op

// Most of the time is spent in icolumn.Apply2
BenchmarkQFrame_EvalInt-2   	     500	   2461435 ns/op	 2416968 B/op	      69 allocs/op

// Hash based group by and distinct
BenchmarkGroupBy/single_col_dataType=string-2         	     100	  15649028 ns/op	 2354704 B/op	    7012 allocs/op
BenchmarkGroupBy/single_col_dataType=integer-2        	     200	   9231345 ns/op	 2354672 B/op	    7012 allocs/op
BenchmarkGroupBy/triple_col_dataType=string-2         	      20	  61141105 ns/op	 5300345 B/op	   49990 allocs/op
BenchmarkGroupBy/triple_col_dataType=integer-2        	      50	  28986440 ns/op	 5300250 B/op	   49990 allocs/op
BenchmarkGroupBy/high_cardinality_dataType=string-2   	      30	  36929671 ns/op	10851690 B/op	   62115 allocs/op
BenchmarkGroupBy/high_cardinality_dataType=integer-2  	      50	  28362647 ns/op	10851660 B/op	   62115 allocs/op
BenchmarkGroupBy/low_cardinality_dataType=string-2    	     100	  12705659 ns/op	 3194024 B/op	     114 allocs/op
BenchmarkGroupBy/low_cardinality_dataType=integer-2   	     200	   7764495 ns/op	 3193995 B/op	     114 allocs/op
BenchmarkGroupBy/small_frame_dataType=string-2        	  100000	     18085 ns/op	    5736 B/op	      62 allocs/op
BenchmarkGroupBy/small_frame_dataType=integer-2       	  100000	     12313 ns/op	    5704 B/op	      62 allocs/op
P
BenchmarkDistinctNull/groupByNull=false-2         	      30	  38197889 ns/op	15425856 B/op	      13 allocs/op
BenchmarkDistinctNull/groupByNull=true-2          	     100	  10925589 ns/op	 1007945 B/op	      10 allocs/op
*/
