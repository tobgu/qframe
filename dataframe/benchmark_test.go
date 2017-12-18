package dataframe_test

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
	qf "github.com/tobgu/go-qcache/dataframe"
	"github.com/tobgu/go-qcache/dataframe/filter"
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

func BenchmarkQFrame_Filter(b *testing.B) {
	data := qf.New(map[string]interface{}{
		"S1": genInts(seed1, frameSize),
		"S2": genInts(seed2, frameSize),
		"S3": genInts(seed3, frameSize),
		"S4": genInts(seed4, frameSize)})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newData := data.Filter(
			filter.Filter{Column: "S1", Comparator: "<", Arg: frameSize / 10},
			filter.Filter{Column: "S2", Comparator: "<", Arg: frameSize / 10},
			filter.Filter{Column: "S3", Comparator: ">", Arg: int(0.9 * frameSize)})

		if newData.Len() != 27142 {
			b.Errorf("Length was %d", newData.Len())
		}
	}
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
		writer.Write([]string{"123", "1234567", "5.2534", "9834543.25", "true", "Foo bar baz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"})
	}
	writer.Flush()

	csvBytes, _ := ioutil.ReadAll(buf)
	return csvBytes
}

func BenchmarkQFrame_FromCsv(b *testing.B) {
	rowCount := 100000
	input := csvBytes(rowCount)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(input)
		df := qf.FromCsv(r)
		if df.Err != nil {
			b.Errorf("Unexpected CSV error: %s", df.Err)
		}

		if df.Len() != rowCount {
			b.Errorf("Unexpected size: %d", df.Len())
		}
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

func jsonColumns(rowCount int) []byte {
	record := map[string]interface{}{
		"INT1":    intSlice(123, rowCount),
		"INT2":    intSlice(1234567, rowCount),
		"FLOAT1":  floatSlice(5.2534, rowCount),
		"FLOAT2":  floatSlice(9834543.25, rowCount),
		"BOOL1":   boolSlice(false, rowCount),
		"STRING1": stringSlice("Foo bar baz", rowCount),
		"STRING2": stringSlice("ABCDEFGHIJKLMNOPQRSTUVWXYZ", rowCount)}

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
		df := qf.FromJson(r)
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
		df := qf.FromJson(r)
		if df.Err != nil {
			b.Errorf("Unexpected JSON error: %s", df.Err)
		}

		if df.Len() != rowCount {
			b.Errorf("Unexpected size: %d", df.Len())
		}
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
*/
