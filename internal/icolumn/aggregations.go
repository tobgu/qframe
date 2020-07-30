package icolumn

import "github.com/tobgu/qframe/internal/math/integer"

var aggregations = map[string]func([]int) int{
	"sum": sum,
	"max": max,
	"min": min,
}

func sum(values []int) int {
	result := 0
	for _, v := range values {
		result += v
	}
	return result
}

func max(values []int) int {
	result := values[0]
	for _, v := range values[1:] {
		result = integer.Max(result, v)
	}
	return result
}

func min(values []int) int {
	result := values[0]
	for _, v := range values[1:] {
		result = integer.Min(result, v)
	}
	return result
}
