package function

import "strconv"

// AbsI returns the absolute value of x.
func AbsI(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// PlusI returns x + y.
func PlusI(x, y int) int {
	return x + y
}

// MinusI returns x - y.
func MinusI(x, y int) int {
	return x - y
}

// MulI returns x * y.
func MulI(x, y int) int {
	return x * y
}

// DivI returns x / y. y == 0 will cause panic.
func DivI(x, y int) int {
	return x / y
}

// StrI returns the string representation of x.
func StrI(x int) *string {
	result := strconv.Itoa(x)
	return &result
}

// FloatI casts x to float.
func FloatI(x int) float64 {
	return float64(x)
}

// BoolI returns bool representation of x. x == 0 => false, all other values result in true.
func BoolI(x int) bool {
	return x != 0
}
