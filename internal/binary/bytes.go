package binary

import "unsafe"

func UnsafeByteSlice[T any](src []T) []byte {
	var firstPtr *byte
	if len(src) > 0 {
		firstPtr = (*byte)(unsafe.Pointer(&src[0]))
	}

	var zero T
	return unsafe.Slice(firstPtr, uintptr(len(src))*unsafe.Sizeof(zero))
}
