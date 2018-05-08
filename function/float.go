package function

import "fmt"

// PlusF returns x + y.
func PlusF(x, y float64) float64 {
	return x + y
}

// MinusF returns x - y.
func MinusF(x, y float64) float64 {
	return x - y
}

// MulF returns x * y.
func MulF(x, y float64) float64 {
	return x * y
}

// DivF returns x / y. y == 0 will cause panic.
func DivF(x, y float64) float64 {
	return x / y
}

// StrF returns the string representation of x.
func StrF(x float64) *string {
	result := fmt.Sprintf("%f", x)
	return &result
}

// IntF casts x to int.
func IntF(x float64) int {
	return int(x)
}
