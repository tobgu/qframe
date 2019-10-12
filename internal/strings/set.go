package strings

type StringSet map[string]struct{}

func NewEmptyStringSet() StringSet {
	return make(StringSet)
}

func NewStringSet(input []string) StringSet {
	result := make(StringSet, len(input))
	for _, s := range input {
		result.Add(s)
	}

	return result
}

func (ss StringSet) Contains(s string) bool {
	_, ok := ss[s]
	return ok
}

func (ss StringSet) Add(s string) {
	ss[s] = struct{}{}
}

func (ss StringSet) AsSlice() []string {
	result := make([]string, 0, len(ss))
	for k := range ss {
		result = append(result, k)
	}

	return result
}
