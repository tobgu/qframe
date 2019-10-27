package hash

import (
	"unsafe"
)

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, seed, s uintptr) uintptr

type stringStruct struct {
	str unsafe.Pointer
	len int
}

func HashBytes(bb []byte, seed uint64) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&bb))
	return uint64(memhash(ss.str, uintptr(seed), uintptr(ss.len)))
}
