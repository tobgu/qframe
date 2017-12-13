package index

type Int []uint32

type Bool []bool

func NewBool(size int) Bool {
	return make(Bool, size)
}

func NewAscending(size int) Int {
	newIndex := make(Int, size)
	for i := range newIndex {
		newIndex[i] = uint32(i)
	}

	return newIndex
}

func (ix Int) Filter(bIx Bool) Int {
	result := make(Int, 0)
	for i, b := range bIx {
		if b {
			result = append(result, ix[i])
		}
	}

	return result
}

func (ix Int) Len() int {
	return len(ix)
}

func (ix Int) Copy() Int {
	newIndex := make(Int, len(ix))
	copy(newIndex, ix)
	return newIndex
}
