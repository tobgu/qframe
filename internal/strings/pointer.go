package strings

import "fmt"

type Pointer uint64

type StringBlob struct {
	Pointers []Pointer
	Data     []byte
}

const nullBit = 0x8000000000000000

func NewPointer(offset, length int, isNull bool) Pointer {
	result := Pointer(offset<<28 | length)
	if isNull {
		result |= nullBit
	}
	return result
}

func (p Pointer) Offset() int {
	return int(p>>28) & 0x7FFFFFFFF
}

func (p Pointer) Len() int {
	return int(p) & 0xFFFFFFF
}

func (p Pointer) IsNull() bool {
	return p&nullBit > 0
}

// NB: Not safe to use the returned string when the b is no longer referenced.
func (p Pointer) Apply(b []byte) *string {
	if p.IsNull() {
		return nil
	}
	s := UnsafeBytesToString(b[p.Offset() : p.Offset()+p.Len()])
	return &s
}

func (p Pointer) String() string {
	return fmt.Sprintf("{offset: %d, len: %d, isNull: %v}",
		p.Offset(), p.Len(), p.IsNull())
}
