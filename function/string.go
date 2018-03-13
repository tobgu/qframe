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

func StrS(s *string) *string {
	// Seemingly useless but this can be used to convert enum columns to string
	// columns so that the two can be used as input to other functions. It is
	// currently not possible to combine enum and string as input.
	return s
}

func LenS(s *string) int {
	if s == nil {
		return 0
	}

	return len(*s)
}

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
