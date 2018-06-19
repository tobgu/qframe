package float

import (
	"math"
)

func Round(n float64) int {
	return int(n + math.Copysign(0.5, n))
}

func Fixed(num float64, precision int) float64 {
	i := math.Pow(10, float64(precision))
	return float64(Round(num*i)) / i
}
