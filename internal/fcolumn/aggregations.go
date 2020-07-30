package fcolumn

import "math"

var aggregations = map[string]func([]float64) float64{
	"max": max,
	"min": min,
	"sum": sum,
	"avg": avg,
}

func sum(values []float64) float64 {
	result := 0.0
	for _, v := range values {
		result += v
	}
	return result
}

func avg(values []float64) float64 {
	result := 0.0
	for _, v := range values {
		result += v
	}

	return result / float64(len(values))
}

func max(values []float64) float64 {
	result := values[0]
	for _, v := range values[1:] {
		result = math.Max(result, v)
	}
	return result
}

func min(values []float64) float64 {
	result := values[0]
	for _, v := range values[1:] {
		result = math.Min(result, v)
	}
	return result
}
