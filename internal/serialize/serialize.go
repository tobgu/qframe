package serialize

import (
	"reflect"
	"unicode"
	"unicode/utf8"
	"unsafe"
)

const chars = "0123456789abcdef"

func AppendQuotedString(buf []byte, str string) []byte {
	// String escape code is highly inspired by the escape code in easyjson.
	buf = append(buf, '"')
	p := 0
	// last non-escape symbol
	for i := 0; i < len(str); {
		c := str[i]

		if c != '\\' && c != '"' && c >= 0x20 && c < utf8.RuneSelf {
			// single-width character, no escaping is required
			i++
			continue
		}

		if c < utf8.RuneSelf {
			// single-with character, need to escape
			buf = append(buf, str[p:i]...)

			switch c {
			case '\t':
				buf = append(buf, `\t`...)
			case '\r':
				buf = append(buf, `\r`...)
			case '\n':
				buf = append(buf, `\n`...)
			case '\\':
				buf = append(buf, `\\`...)
			case '"':
				buf = append(buf, `\"`...)
			default:
				buf = append(buf, `\u00`...)
				buf = append(buf, chars[c>>4])
				buf = append(buf, chars[c&0xf])
			}

			i++
			p = i
			continue
		}

		// broken utf
		runeValue, runeWidth := utf8.DecodeRuneInString(str[i:])
		if runeValue == utf8.RuneError && runeWidth == 1 {
			buf = append(buf, str[p:i]...)
			buf = append(buf, `\ufffd`...)
			i++
			p = i
			continue
		}

		// jsonp stuff - tab separator and line separator
		if runeValue == '\u2028' || runeValue == '\u2029' {
			buf = append(buf, str[p:i]...)
			buf = append(buf, `\u202`...)
			buf = append(buf, chars[runeValue&0xf])
			i += runeWidth
			p = i
			continue
		}
		i += runeWidth
	}

	buf = append(buf, str[p:]...)
	buf = append(buf, '"')
	return buf
}

// This is a modified, zero alloc, version of the stdlib function strings.ToUpper.
// The passed in byte buffer is used to hold the converted string. The returned
// string is not safe to use when bP goes out of scope and the content may
// be overwritten upon next call to this function.
func ToUpper(bP *[]byte, sP *string) string {
	// nbytes is the number of bytes encoded in b.
	var nbytes int

	var b []byte
	s := *sP
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
	return unsafeBytesToString(b[:nbytes])
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
