package io

import (
	"reflect"
	"strconv"
	"unsafe"
)

func parseInt(b []byte) (i int64, err error) {
	s := unsafeBytesToString(b)
	return strconv.ParseInt(s, 10, 64)
}

func parseFloat(b []byte) (float64, error) {
	s := unsafeBytesToString(b)
	return strconv.ParseFloat(s, 64)
}

func parseBool(b []byte) (bool, error) {
	return strconv.ParseBool(unsafeBytesToString(b))
}

func unsafeBytesToString(in []byte) string {
	src := *(*reflect.SliceHeader)(unsafe.Pointer(&in))
	dst := reflect.StringHeader{
		Data: src.Data,
		Len:  src.Len,
	}
	s := *(*string)(unsafe.Pointer(&dst))
	return s
}

func QuotedBytes(s string) []byte {
	result := make([]byte, 0, len(s)+2)
	result = append(result, byte('"'))
	result = append(result, []byte(s)...)
	return append(result, byte('"'))
}
