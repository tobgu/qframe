package function

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
