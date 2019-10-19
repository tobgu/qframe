package hash

import (
	"math/rand"
	"unsafe"
)

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

type stringStruct struct {
	str unsafe.Pointer
	len int
}

// MemHash holds a buffer of bytes and allows calculation of a hash over the bytes.
type MemHash struct {
	buf []byte
}

// Reset resets the memory buffer
func (h *MemHash) Reset() {
	h.buf = h.buf[:0]
}

// Hash returns the hash over all bytes in the buffer
func (h *MemHash) Hash() uint32 {
	ss := (*stringStruct)(unsafe.Pointer(&h.buf))
	return uint32(memhash(ss.str, 0, uintptr(ss.len)))
}

// WriteByte appends a byte to the buffer
func (h *MemHash) WriteByte(b byte) {
	h.buf = append(h.buf, b)
}

// Write appends the byte slice to the buffer
func (h *MemHash) Write(bb []byte) {
	h.buf = append(h.buf, bb...)
}

// WriteRand32 writes four random bytes to the buffer
func (h *MemHash) WriteRand32() {
	var nullHashBytes [4]byte
	hashBytes := nullHashBytes[:]
	rand.Read(hashBytes)
	h.Write(hashBytes)
}
