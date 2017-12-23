package sseries

import (
	"encoding/json"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/io"
	"unicode/utf8"
)

// TODO: Probably need a more general aggregation pattern, int -> float (average for example)
var aggregations = map[string]func([]string) string{}

var filterFuncs = map[filter.Comparator]func(index.Int, []string, interface{}, index.Bool) error{
	filter.Gt: gt,
	filter.Lt: lt,
}

func (s Series) StringAt(i int) string {
	return s.data[i]
}

const chars = "0123456789abcdef"

func (s Series) AppendByteStringAt(buf []byte, i int) []byte {
	// String escape code is highly inspired by the escape code in easyjson.
	buf = append(buf, '"')

	str := s.data[i]
	p := 0 // last non-escape symbol

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

func (s Series) Marshaler(index index.Int) json.Marshaler {
	return io.JsonString(s.subset(index).data)
}
