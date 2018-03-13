package function

import "strconv"

func AbsI(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func PlusI(x, y int) int {
	return x + y
}

func MinusI(x, y int) int {
	return x - y
}

func MulI(x, y int) int {
	return x * y
}

func DivI(x, y int) int {
	return x / y
}

func StrI(x int) *string {
	result := strconv.Itoa(x)
	return &result
}

func FloatI(x int) float64 {
	return float64(x)
}

func BoolI(x int) bool {
	return x != 0
}
