package function

func PlusF(x, y float64) (float64, error) {
	return x + y, nil
}

func MinusF(x, y float64) (float64, error) {
	return x - y, nil
}

func MulF(x, y float64) (float64, error) {
	return x * y, nil
}

func DivF(x, y float64) (float64, error) {
	return x / y, nil
}
