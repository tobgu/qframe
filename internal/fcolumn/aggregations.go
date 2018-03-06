package fcolumn

var aggregations = map[string]func([]float64) float64{
	"sum": sum,
}

func sum(values []float64) float64 {
	result := 0.0
	for _, v := range values {
		result += v
	}
	return result
}
