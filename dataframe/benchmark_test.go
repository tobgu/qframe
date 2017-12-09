package dataframe_test

import (
	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
	qf "github.com/tobgu/go-qcache/dataframe"
	"math/rand"
	"testing"
)

func genInts(seed int64, size int) []int {
	rand.Seed(seed)
	result := make([]int, size)
	for ix := range result {
		result[ix] = rand.Intn(size)
	}

	return result
}

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

func BenchmarkQCacheFrame_Filter(b *testing.B) {
	data := qf.New(map[string]interface{}{
		"S1": genInts(seed1, frameSize),
		"S2": genInts(seed2, frameSize),
		"S3": genInts(seed3, frameSize),
		"S4": genInts(seed4, frameSize)})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newData := data.Filter(
			qf.SimpleFilter{Column: "S1", Comparator: "<", Arg: frameSize / 10},
			qf.SimpleFilter{Column: "S2", Comparator: "<", Arg: frameSize / 10},
			qf.SimpleFilter{Column: "S3", Comparator: ">", Arg: int(0.9 * frameSize)})

		if newData.Len() != 27142 {
			b.Errorf("Length was %d", newData.Len())
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
*/
