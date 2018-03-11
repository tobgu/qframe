package function

func NotB(x bool) bool {
	return !x
}

func AndB(x, y bool) bool {
	return x && y
}

func OrB(x, y bool) bool {
	return x || y
}

func XorB(x, y bool) bool {
	return (x && !y) || (!x && y)
}

func NandB(x, y bool) bool {
	return !x || !y
}
