package sseries

import (
	"encoding/json"
	"github.com/tobgu/go-qcache/dataframe/filter"
	"github.com/tobgu/go-qcache/dataframe/internal/index"
	"github.com/tobgu/go-qcache/dataframe/internal/io"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]string) string{}

var filterFuncs = map[filter.Comparator]func(index.Int, []string, interface{}, index.Bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
}

func (s Series) StringAt(i int) string {
	return s.data[i]
}

func (s Series) AppendByteStringAt(buf []byte, i int) []byte {
	buf = append(buf, '"')
	buf = append(buf, s.data[i]...)
	buf = append(buf, '"')
	return buf
}

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonString(s.subset(index).data)
}
