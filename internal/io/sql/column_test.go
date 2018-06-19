package sql

import (
	"math"
	"testing"
)

func assertEqual(t *testing.T, expected, actual interface{}) {
	t.Helper()
	if expected != actual {
		t.Errorf("%v != %v", expected, actual)
	}
}

func TestColumn(t *testing.T) {

	// Column with two NULL values
	col := &Column{}
	col.Scan(0.0)
	col.Scan(nil)
	col.Scan(2.0)
	col.Scan(nil)
	data := col.Data().([]float64)
	assertEqual(t, 4, len(data))
	assertEqual(t, data[0], 0.0)
	assertEqual(t, true, math.IsNaN(data[1]))
	assertEqual(t, data[2], 2.0)
	assertEqual(t, true, math.IsNaN(data[3]))

	// Column with NULL values at the head
	col = &Column{}
	col.Scan(nil)
	col.Scan(nil)
	col.Scan(0.0)
	col.Scan(1.0)
	data = col.Data().([]float64)
	assertEqual(t, 4, len(data))

	// Column with all NULL values
	col = &Column{}
	col.Scan(nil)
	col.Scan(nil)
	col.Scan(nil)
	col.Scan(nil)
	assertEqual(t, nil, col.Data())

}

func TestColumnCoercion(t *testing.T) {
	col := &Column{}
	col.coerce = Int64ToBool(col)
	col.Scan(int64(1))
	col.Scan(int64(0))
	col.Scan(int64(1))
	col.Scan(int64(0))
	data := col.Data().([]bool)
	assertEqual(t, 4, len(data))
	assertEqual(t, true, data[0])
	assertEqual(t, false, data[1])
	assertEqual(t, true, data[2])
	assertEqual(t, false, data[3])
}

func BenchmarkColumn(b *testing.B) {
	col := &Column{}
	for n := 0; n < b.N; n++ {
		col.Scan(1.0)
	}
}
