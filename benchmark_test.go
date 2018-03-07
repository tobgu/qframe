package qframe_test

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
	qf "github.com/tobgu/qframe"
	"github.com/tobgu/qframe/filter"
	"io/ioutil"
	"math/rand"
	"testing"
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

const noSeed int64 = 0
const seed1 int64 = 1
const seed2 int64 = 2
const seed3 int64 = 3
const seed4 int64 = 4
const frameSize = 100000

func BenchmarkDataFrame_Filter(b *testing.B) {
	data := dataframe.New(
		series.New(genInts(seed1, frameSize), series.Int, "S1"),
		series.New(genInts(seed2, frameSize), series.Int, "S2"),
		series.New(genInts(seed3, frameSize), series.Int, "S3"),
		series.New(genInts(seed4, frameSize), series.Int, "S4"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newData := data.Filter(
			dataframe.F{Colname: "S1", Comparator: series.Less, Comparando: frameSize / 10},
			dataframe.F{Colname: "S2", Comparator: series.Less, Comparando: frameSize / 10},
			dataframe.F{Colname: "S3", Comparator: series.Greater, Comparando: int(0.9 * frameSize)})
		if newData.Nrow() != 27142 {
			b.Errorf("Length was %d", newData.Nrow())
		}
	}
}

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
		newData := data.Filter(
			filter.Filter{Column: "S1", Comparator: "<", Arg: frameSize / 10},
			filter.Filter{Column: "S2", Comparator: "<", Arg: frameSize / 10},
			filter.Filter{Column: "S3", Comparator: ">", Arg: int(0.9 * frameSize)})

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
		newData := data.Filter(
			filter.Filter{Column: "S1", Comparator: lessThan(frameSize / 10)},
			filter.Filter{Column: "S2", Comparator: lessThan(frameSize / 10)},
			filter.Filter{Column: "S3", Comparator: greaterThan(int(0.9 * frameSize))})

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
		newData := data.Filter(filter.Filter{Column: "S1", Comparator: "in", Arg: slice})
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
		newData := data.Filter(filter.Filter{Column: "S1", Comparator: intInFilter(slice)})
		if newData.Err != nil {
			b.Errorf("Length was Err: %s", newData.Err)
		}
	}
}

func BenchmarkQFrame_FilterNot(b *testing.B) {
	data := qf.New(map[string]interface{}{
		"S1": genInts(seed1, frameSize)})
	f := filter.Filter{Column: "S1", Comparator: "<", Arg: frameSize - frameSize/10, Inverse: true}

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
			f := qf.Not(qf.Filter(filter.Filter{Column: "S1", Comparator: "<", Arg: frameSize - frameSize/10}))
			newData := f.Filter(data)
			if newData.Len() != 9882 {
				b.Errorf("Length was %d", newData.Len())
			}
		}
	})
}

func BenchmarkDataFrame_Sort(b *testing.B) {
	data := dataframe.New(
		series.New(genInts(seed1, frameSize), series.Int, "S1"),
		series.New(genInts(seed2, frameSize), series.Int, "S2"),
		series.New(genInts(seed3, frameSize), series.Int, "S3"),
		series.New(genInts(seed4, frameSize), series.Int, "S4"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newData := data.Arrange(dataframe.Sort("S1"), dataframe.RevSort("S2"))
		if newData.Err != nil {
			b.Errorf("Unexpected sort error: %s", newData.Err)
		}
	}
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
	writer := csv.NewWriter(buf)
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
	writer := csv.NewWriter(buf)
	writer.Write([]string{"COL.1", "COL.2"})
	for i := 0; i < rowCount; i++ {
		writer.Write([]string{
			fmt.Sprintf("Foo bar baz %d", i%cardinality),
			fmt.Sprintf("AB%d", i%cardinality)})
	}
	writer.Flush()

	csvBytes, _ := ioutil.ReadAll(buf)
	return csvBytes
}

func BenchmarkQFrame_ReadCsv(b *testing.B) {
	rowCount := 100000
	input := csvBytes(rowCount)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(input)
		df := qf.ReadCsv(r)
		if df.Err != nil {
			b.Errorf("Unexpected CSV error: %s", df.Err)
		}

		if df.Len() != rowCount {
			b.Errorf("Unexpected size: %d", df.Len())
		}
	}
}

func BenchmarkQFrame_ReadCsvEnum(b *testing.B) {
	rowCount := 100000
	cardinality := 20
	input := csvEnumBytes(rowCount, cardinality)

	for _, t := range []string{"enum"} {
		b.Run(fmt.Sprintf("Type %s", t), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := bytes.NewReader(input)
				df := qf.ReadCsv(r, qf.Types(map[string]string{"COL.1": t, "COL.2": t}))
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

func BenchmarkDataFrame_ReadCSV(b *testing.B) {
	rowCount := 100000
	input := csvBytes(rowCount)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(input)
		df := dataframe.ReadCSV(r)
		if df.Err != nil {
			b.Errorf("Unexpected CSV error: %s", df.Err)
		}

		if df.Nrow() != rowCount {
			b.Errorf("Unexpected size: %d", df.Nrow())
		}
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

func jsonColumns(rowCount int) []byte {
	record := exampleData(rowCount)
	result, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}
	return result
}

func BenchmarkDataFrame_ReadJSON(b *testing.B) {
	rowCount := 10000
	input := jsonRecords(rowCount)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(input)
		df := dataframe.ReadJSON(r)
		if df.Err != nil {
			b.Errorf("Unexpected JSON error: %s", df.Err)
		}

		if df.Nrow() != rowCount {
			b.Errorf("Unexpected size: %d", df.Nrow())
		}
	}
}

func BenchmarkQFrame_FromJSONRecords(b *testing.B) {
	rowCount := 10000
	input := jsonRecords(rowCount)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(input)
		df := qf.ReadJson(r)
		if df.Err != nil {
			b.Errorf("Unexpected JSON error: %s", df.Err)
		}

		if df.Len() != rowCount {
			b.Errorf("Unexpected size: %d", df.Len())
		}
	}
}

func BenchmarkQFrame_FromJSONColumns(b *testing.B) {
	rowCount := 10000
	input := jsonColumns(rowCount)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(input)
		df := qf.ReadJson(r)
		if df.Err != nil {
			b.Errorf("Unexpected JSON error: %s", df.Err)
		}

		if df.Len() != rowCount {
			b.Errorf("Unexpected size: %d", df.Len())
		}
	}
}

func BenchmarkQFrame_ToCsv(b *testing.B) {
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
		err := df.ToCsv(buf)
		if err != nil {
			b.Errorf("Unexpected ToCsv error: %s", err)
		}
	}
}

func toJson(b *testing.B, orient string) {
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
		err := df.ToJson(buf, orient)
		if err != nil {
			b.Errorf("Unexpected ToCsv error: %s", err)
		}
	}
}

func BenchmarkQFrame_ToJsonRecords(b *testing.B) {
	toJson(b, "records")
}

func BenchmarkQFrame_ToJsonColumns(b *testing.B) {
	toJson(b, "columns")
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
			types:         map[string]string{"COL.1": "enum", "COL.2": "enum"},
			column:        "COL.1",
			filter:        "Foo bar baz 5",
			expectedCount: 55556,
		},
		{
			types:         map[string]string{},
			column:        "COL.1",
			filter:        "Foo bar baz 5",
			expectedCount: 55556,
		},
		{
			types:         map[string]string{},
			column:        "COL.2",
			filter:        "AB5",
			expectedCount: 55556,
		},
		{
			types:         map[string]string{},
			column:        "COL.1",
			filter:        "%bar baz 5%",
			expectedCount: 11111,
			comparator:    "like",
		},
		{
			types:         map[string]string{},
			column:        "COL.1",
			filter:        "%bar baz 5%",
			expectedCount: 11111,
			comparator:    "ilike",
		},
		{
			types:         map[string]string{"COL.1": "enum", "COL.2": "enum"},
			column:        "COL.1",
			filter:        "%bar baz 5%",
			expectedCount: 11111,
			comparator:    "ilike",
		},
	}
	for _, tc := range table {
		r := bytes.NewReader(input)
		df := qf.ReadCsv(r, qf.Types(tc.types))
		if tc.comparator == "" {
			tc.comparator = "<"
		}

		b.Run(fmt.Sprintf("Filter %s %s, enum: %t", tc.filter, tc.comparator, len(tc.types) > 0), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				newDf := df.Filter(filter.Filter{Comparator: tc.comparator, Column: tc.column, Arg: tc.filter})
				if newDf.Len() != tc.expectedCount {
					b.Errorf("Unexpected count: %d, expected: %d", newDf.Len(), tc.expectedCount)
				}
			}
		})
	}
}

func benchAssign(b *testing.B, name string, input qf.QFrame, fn interface{}) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := input.Assign(qf.Instruction{Fn: fn, DstCol: "COL.1", SrcCol1: "COL.1"})
			if result.Err != nil {
				b.Errorf("Err: %s, %s", result.Len(), result.Err)
			}
		}
	})

}

func BenchmarkQFrame_AssignStringToString(b *testing.B) {
	rowCount := 100000
	cardinality := 9
	input := csvEnumBytes(rowCount, cardinality)
	r := bytes.NewReader(input)
	df := qf.ReadCsv(r)

	benchAssign(b, "Instruction with custom function", df, toUpper)
	benchAssign(b, "Instruction with builtin function", df, "ToUpper")
}

func BenchmarkQFrame_AssignEnum(b *testing.B) {
	rowCount := 100000
	cardinality := 9
	input := csvEnumBytes(rowCount, cardinality)
	r := bytes.NewReader(input)
	df := qf.ReadCsv(r, qf.Types(map[string]string{"COL.1": "enum"}))

	benchAssign(b, "Instruction with custom function", df, toUpper)
	benchAssign(b, "Instruction with built in function", df, "ToUpper")
	benchAssign(b, "Instruction int function (for reference)", df, func(x *string) (int, error) { return len(*x), nil })
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
	f := qf.ReadCsv(r).Sort(qf.Order{Column: "COL.1"})
	v, err := f.StringView("COL.1")
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

/*
Go 1.7

go test -bench=.
tpp
go tool pprof dataframe.test filter_cpu.out

Initial results:
BenchmarkDataFrame_Filter-2     	      30	  40542568 ns/op	 7750730 B/op	  300134 allocs/op
BenchmarkQCacheFrame_Filter-2   	     300	   3997702 ns/op	  991720 B/op	      14 allocs/op

After converting bool index to int index before subseting:
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
BenchmarkQFrame_IntFromCsv-2      	      20	  55921060 ns/op	30167012 B/op	     261 allocs/op
BenchmarkDataFrame_IntFromCsv-2   	       5	 243541282 ns/op	41848809 B/op	  900067 allocs/op

// Type detecting CSV implementation, 100000 x "123", "1234567", "5.2534", "9834543.25", "true", "Foo bar baz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
BenchmarkQFrame_IntFromCsv-2   	      10	 101362864 ns/op	87707785 B/op	  200491 allocs/op

// JSON, 10000 rows
BenchmarkDataFrame_ReadJSON-2          	      10	 176107262 ns/op	24503045 B/op	  670112 allocs/op
BenchmarkQFrame_FromJSONRecords-2   	      10	 117408651 ns/op	15132420 B/op	  430089 allocs/op
BenchmarkQFrame_FromJSONColumns-2   	      10	 104641079 ns/op	15342302 B/op	  220842 allocs/op

// JSON with easyjson generated unmarshal
BenchmarkQFrame_FromJSONColumns-2   	      50	  24764232 ns/op	 6730738 B/op	   20282 allocs/op

// ToCsv, vanilla implementation based on stdlib csv, 100000 records
BenchmarkQFrame_ToCsv-2   	       5	 312478023 ns/op	26365360 B/op	  600017 allocs/op

// ToJson, performance is not super impressive... 100000 records
BenchmarkQFrame_ToJsonRecords-2   	       2	 849280921 ns/op	181573400 B/op	 3400028 allocs/op
BenchmarkQFrame_ToJsonColumns-2   	       5	 224702680 ns/op	33782697 B/op	     513 allocs/op

// Testing jsoniter with some success
BenchmarkQFrame_ToJsonRecords-2   	       2	 646738504 ns/op	137916264 B/op	 3600006 allocs/op
BenchmarkQFrame_ToJsonColumns-2   	      20	  99932317 ns/op	34144682 B/op	     490 allocs/op

// Python, as a comparison, with corresponding list of dictionaries:
>>> import json
>>> import time
>>> t0 = time.time(); j = json.dumps(x); time.time() - t0
0.33017611503601074
>>> import ujson
>>> t0 = time.time(); j = ujson.dumps(x); time.time() - t0
0.17484211921691895

// Custom encoder for JSON records, now we're talking
BenchmarkQFrame_ToJsonRecords-2   	      20	  87437635 ns/op	53638858 B/op	      35 allocs/op
BenchmarkQFrame_ToJsonColumns-2   	      10	 102566155 ns/op	37746546 B/op	     547 allocs/op

// Reuse string pointers when reading CSV
Before:
BenchmarkQFrame_ReadCsv-2   	      10	 119385221 ns/op	92728576 B/op	  400500 allocs/op

After:
BenchmarkQFrame_ReadCsv-2   	      10	 108917111 ns/op	86024686 B/op	   20790 allocs/op

// Initial CSV read Enum, 2 x 100000 cells with cardinality 20
BenchmarkQFrame_ReadCsvEnum/Type_enum-2         	      50	  28081769 ns/op	19135232 B/op	     213 allocs/op
BenchmarkQFrame_ReadCsvEnum/Type_string-2       	      50	  28563580 ns/op	20526743 B/op	     238 allocs/op

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

// Performance tweak for single, simple, clauses statements to put them on par with calling the
// Qframe Filter function directly

// Before
BenchmarkQFrame_FilterNot/qframe-2         	    2000	    716280 ns/op	  147465 B/op	       2 allocs/op
BenchmarkQFrame_FilterNot/filter-2         	    2000	   1158211 ns/op	  516161 B/op	       4 allocs/op

// After
BenchmarkQFrame_FilterNot/qframe-2         	    2000	    713147 ns/op	  147465 B/op	       2 allocs/op
BenchmarkQFrame_FilterNot/filter-2         	    2000	    726766 ns/op	  147521 B/op	       3 allocs/op

// Restructure string column to use a byte blob with offsets and lengths
BenchmarkQFrame_ReadCsv-2       	      20	  85906027 ns/op	84728656 B/op	     500 allocs/op

// Fix string filters to make better use of the new string blob structure:
BenchmarkQFrame_FilterEnumVsString/Filter_Foo_bar_baz_5_<,_enum:_true-2         	    2000	    691081 ns/op	  335888 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_Foo_bar_baz_5_<,_enum:_false-2        	    1000	   1902665 ns/op	  335889 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_AB5_<,_enum:_false-2                  	    1000	   1935237 ns/op	  335888 B/op	       3 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_like,_enum:_false-2       	     500	   3855434 ns/op	  155680 B/op	       4 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_ilike,_enum:_false-2      	     100	  11881963 ns/op	  155792 B/op	       8 allocs/op
BenchmarkQFrame_FilterEnumVsString/Filter_%bar_baz_5%_ilike,_enum:_true-2       	    2000	    691971 ns/op	  155824 B/op	       9 allocs/op

// Compare string to upper, first as general custom function, second as specialized built in function.
BenchmarkQFrame_AssignStringToString/Assign_with_custom_function-2         	      30	  42895890 ns/op	17061043 B/op	  400020 allocs/op
BenchmarkQFrame_AssignStringToString/Assign_with_built_in_function-2       	     100	  12163217 ns/op	 2107024 B/op	       7 allocs/op

// Compare assign for enums
BenchmarkQFrame_AssignEnum/Assign_with_custom_function-2         	      50	  38505068 ns/op	15461041 B/op	  300020 allocs/op
BenchmarkQFrame_AssignEnum/Assign_with_built_in_function-2       	  300000	      3566 ns/op	    1232 B/op	      23 allocs/op
BenchmarkQFrame_AssignEnum/Assign_int_function_(for_reference)-2 	    1000	   1550604 ns/op	  803491 B/op	       6 allocs/op

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

// Same as above but modified to work with enums in COL.1
BenchmarkQFrame_StringView/For_loop-2         	    1000	   1651190 ns/op	       0 B/op	       0 allocs/op
BenchmarkQFrame_StringView/Slice-2            	     500	   2697675 ns/op	  802816 B/op	       1 allocs/op

*/
