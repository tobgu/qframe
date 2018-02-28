package strings

type StringSet map[string]struct{}

func NewStringSet(input []string) StringSet {
	result := make(StringSet, len(input))
	for _, s := range input {
		result[s] = struct{}{}
	}

	return result
}

func (ss StringSet) Contains(s string) bool {
	_, ok := ss[s]
	return ok
}
