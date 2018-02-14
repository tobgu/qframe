package aggregation

import "strings"

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
