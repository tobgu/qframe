package function

import "fmt"

func PlusF(x, y float64) float64 {
	return x + y
}

func MinusF(x, y float64) float64 {
	return x - y
}

func MulF(x, y float64) float64 {
	return x * y
}

func DivF(x, y float64) float64 {
	return x / y
}

func StrF(x float64) *string {
	result := fmt.Sprintf("%f", x)
	return &result
}

func IntF(x float64) int {
	return int(x)
}
