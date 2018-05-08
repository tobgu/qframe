package function

import "strconv"

// NotB returns the inverse of x
func NotB(x bool) bool {
	return !x
}

// AndB returns the logical conjunction of x and y.
func AndB(x, y bool) bool {
	return x && y
}

// OrB returns the logical disjunction of x and y.
func OrB(x, y bool) bool {
	return x || y
}

// XorB returns the exclusive disjunction of x and y
func XorB(x, y bool) bool {
	return (x && !y) || (!x && y)
}

// NandB returns the inverse logical conjunction of x and b.
func NandB(x, y bool) bool {
	return !AndB(x, y)
}

// StrB returns the string representation of x.
func StrB(x bool) *string {
	result := strconv.FormatBool(x)
	return &result
}

// IntB casts x to int. true => 1 and false => 0.
func IntB(x bool) int {
	if x {
		return 1
	}

	return 0
}
