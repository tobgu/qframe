package function

import "strings"

func nilSafe(f func(string) string) func(*string) *string {
	return func(s *string) *string {
		if s == nil {
			return nil
		}

		result := f(*s)
		return &result
	}
}

var UpperS = nilSafe(strings.ToUpper)
var LowerS = nilSafe(strings.ToLower)

func ConcatS(x, y *string) *string {
	if x == nil {
		return y
	}

	if y == nil {
		return x
	}

	result := *x + *y
	return &result
}
