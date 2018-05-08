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

// UpperS returns the upper case representation of s.
var UpperS = nilSafe(strings.ToUpper)

// LowerS returns the lower case representation of s.
var LowerS = nilSafe(strings.ToLower)

// StrS returns s.
//
// This may appear useless but this can be used to convert enum columns to string
// columns so that the two can be used as input to other functions. It is
// currently not possible to combine enum and string as input.
func StrS(s *string) *string {
	return s
}

// LenS returns the length of s.
func LenS(s *string) int {
	if s == nil {
		return 0
	}

	return len(*s)
}

// ConcatS returns the concatenation of x and y.
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
