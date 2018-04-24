package aggregation

import "strings"

// StrJoin creates a function that joins a slice of strings into
// a single string using the provided separator.
// It is provided as an example and can be used in aggregations
// on string and enum columns.
func StrJoin(sep string) func([]*string) *string {
	return func(input []*string) *string {
		s := make([]string, 0, len(input))
		for _, sPtr := range input {
			if sPtr != nil {
				s = append(s, *sPtr)
			}
		}

		result := strings.Join(s, sep)
		return &result
	}
}
