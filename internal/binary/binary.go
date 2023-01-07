package binary

import (
	"encoding/binary"
	"golang.org/x/exp/constraints"
	"io"
)

type number interface {
	constraints.Integer | constraints.Float
}

func Read[T number | bool](r io.Reader) (T, error) {
	// The choice of little endian is arbitrary, make sure to use the same in decode!
	var result T
	err := binary.Read(r, binary.LittleEndian, &result)
	return result, err
}

func Write[T number | bool](w io.Writer, n T) error {
	return binary.Write(w, binary.LittleEndian, n)
}
