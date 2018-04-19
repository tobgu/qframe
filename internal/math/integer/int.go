package integer

import "math"

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Pow2(exp int) int {
	return int(math.Pow(2, float64(exp)))
}
