package strings

import (
	"reflect"
	"strconv"
	"unicode"
	"unicode/utf8"
	"unsafe"
)

func ParseInt(b []byte) (i int64, err error) {
	s := UnsafeBytesToString(b)
	return strconv.ParseInt(s, 10, 64)
}

func ParseFloat(b []byte) (float64, error) {
	s := UnsafeBytesToString(b)
	return strconv.ParseFloat(s, 64)
}

func ParseBool(b []byte) (bool, error) {
	return strconv.ParseBool(UnsafeBytesToString(b))
}

func UnsafeBytesToString(in []byte) string {
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

// This is a modified, zero alloc, version of the stdlib function strings.ToUpper.
// The passed in byte buffer is used to hold the converted string. The returned
// string is not safe to use when bP goes out of scope and the content may
// be overwritten upon next call to this function.
func ToUpper(bP *[]byte, s string) string {
	// nbytes is the number of bytes encoded in b.
	var nbytes int

	var b []byte
	for i, c := range s {
		r := unicode.ToUpper(c)
		if r == c {
			continue
		}

		if len(*bP) >= len(s)+utf8.UTFMax {
			b = *bP
		} else {
			b = make([]byte, len(s)+utf8.UTFMax)
		}
		nbytes = copy(b, s[:i])
		if r >= 0 {
			if r <= utf8.RuneSelf {
				b[nbytes] = byte(r)
				nbytes++
			} else {
				nbytes += utf8.EncodeRune(b[nbytes:], r)
			}
		}

		if c == utf8.RuneError {
			// RuneError is the result of either decoding
			// an invalid sequence or '\uFFFD'. Determine
			// the correct number of bytes we need to advance.
			_, w := utf8.DecodeRuneInString(s[i:])
			i += w
		} else {
			i += utf8.RuneLen(c)
		}

		s = s[i:]
		break
	}

	if b == nil {
		return s
	}

	for _, c := range s {
		r := unicode.ToUpper(c)

		// common case
		if (0 <= r && r <= utf8.RuneSelf) && nbytes < len(b) {
			b[nbytes] = byte(r)
			nbytes++
			continue
		}

		// b is not big enough or r is not a ASCII rune.
		if r >= 0 {
			if nbytes+utf8.UTFMax >= len(b) {
				// Grow the buffer.
				nb := make([]byte, 2*len(b))
				copy(nb, b[:nbytes])
				b = nb
			}
			nbytes += utf8.EncodeRune(b[nbytes:], r)
		}
	}

	*bP = b
	return UnsafeBytesToString(b[:nbytes])
}

// InterfaceSliceToStringSlice converts a slice of interface{} to a slice of strings.
// If the input is not a slice of interface{} it is returned unmodified. If the input
// slice does not consist of strings (only) the input is returned unmodified.
func InterfaceSliceToStringSlice(input interface{}) interface{} {
	ifSlice, ok := input.([]interface{})
	if !ok {
		return input
	}

	result := make([]string, len(ifSlice))
	for i, intfc := range ifSlice {
		s, ok := intfc.(string)
		if !ok {
			return input
		}
		result[i] = s
	}

	return result
}
