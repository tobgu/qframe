package index

import (
	"fmt"
	qfbinary "github.com/tobgu/qframe/internal/binary"
	"io"
)

type Bool []bool

func NewBool(size int) Bool {
	return make(Bool, size)
}

func (ix Bool) Len() int {
	return len(ix)
}

type Int []uint32

func NewAscending(size uint32) Int {
	newIndex := make(Int, size)
	for i := range newIndex {
		newIndex[i] = uint32(i)
	}

	return newIndex
}

func (ix Int) Filter(bIx Bool) Int {
	count := 0
	for _, b := range bIx {
		if b {
			count++
		}
	}

	result := make(Int, 0, count)
	for i, b := range bIx {
		if b {
			result = append(result, ix[i])
		}
	}

	return result
}

func (ix Int) ByteSize() int {
	return 4 * cap(ix)
}

func (ix Int) Len() int {
	return len(ix)
}

func (ix Int) Copy() Int {
	newIndex := make(Int, len(ix))
	copy(newIndex, ix)
	return newIndex
}

func (ix Int) ToQBin(w io.Writer) error {
	err := qfbinary.Write(w, uint32(ix.Len()))
	if err != nil {
		return fmt.Errorf("error writing index length: %w", err)
	}

	_, err = w.Write(qfbinary.UnsafeByteSlice(ix))
	if err != nil {
		return fmt.Errorf("error writing index bytes: %w", err)
	}

	return nil
}

func ReadIntIxFromQBin(r io.Reader) (Int, error) {
	ixLen, err := qfbinary.Read[uint32](r)
	if err != nil {
		return nil, fmt.Errorf("error reading index length: %w", err)
	}

	ix := make(Int, ixLen)
	_, err = io.ReadFull(r, qfbinary.UnsafeByteSlice(ix))
	if err != nil {
		return nil, fmt.Errorf("error reading index bytes: %w", err)
	}

	return ix, nil
}
