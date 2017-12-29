package fseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/internal/series"
	"strconv"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]float64) float64{}

var filterFuncs = map[filter.Comparator]func(index.Int, []float64, interface{}, index.Bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
}

func (s Series) StringAt(i int) string {
	return strconv.FormatFloat(s.data[i], 'f', -1, 64)
}

func (s Series) AppendByteStringAt(buf []byte, i int) []byte {
	return strconv.AppendFloat(buf, s.data[i], 'f', -1, 64)
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonFloat64(s.subset(index).data)
}

func (s Series) Equals(index index.Int, other series.Series, otherIndex index.Int) bool {
	otherI, ok := other.(Series)
	if !ok {
		return false
	}

	for ix, x := range index {
		if s.data[x] != otherI.data[otherIndex[ix]] {
			return false
		}
	}

	return true
}

// TODO: Handle NaN in comparisons, etc.
