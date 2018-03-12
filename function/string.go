package function

import "strings"

func nilSafe(f func(string) string) func(*string) (*string, error) {
	return func(s *string) (*string, error) {
		if s == nil {
			return nil, nil
		}

		result := f(*s)
		return &result, nil
	}
}

var UpperS = nilSafe(strings.ToUpper)
var LowerS = nilSafe(strings.ToLower)

func ConcatS(x, y *string) (*string, error) {
	if x == nil {
		return y, nil
	}

	if y == nil {
		return x, nil
	}

	result := *x + *y
	return &result, nil
}
