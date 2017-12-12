package dataframe_test

import (
	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
	qf "github.com/tobgu/go-qcache/dataframe"
	"github.com/tobgu/go-qcache/dataframe/filter"
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


/*
Go 1.7

go test -bench=.
go test -bench=BenchmarkQCacheFrame_Filter -cpuprofile filter_cpu.out
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

*/
