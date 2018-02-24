package iseries

func sum(values []int) int {
	result := 0
	for _, v := range values {
		result += v
	}
	return result
}

var aggregations = map[string]func([]int) int{
	"sum": sum,
}
