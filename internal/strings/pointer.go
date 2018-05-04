package strings

import "fmt"

// Pointer identifies a string within a StringBlob.
// Max individual string size 2^28 byte ~ 268 Mb
// Max total size 2^35 byte ~ 34 Gb
type Pointer uint64

// StringBlob represents a set of strings.
// The underlying data is stored in a byte blob which can be interpreted through
// the pointers which identifies the start and end of individual strings in the blob.
//
// This structure is used instead of a slice of strings or a slice of
// string pointers is to avoid that the GC has to scan all pointers which
// takes quite some time with large/many live frames.
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

func (p Pointer) String() string {
	return fmt.Sprintf("{offset: %d, len: %d, isNull: %v}",
		p.Offset(), p.Len(), p.IsNull())
}
