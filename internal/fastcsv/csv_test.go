package fastcsv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

func toStrings(bs [][]byte) []string {
	strs := make([]string, 0, len(bs))
	for _, b := range bs {
		strs = append(strs, string(b))
	}
	return strs
}

func quote(strs []string) []string {
	out := make([]string, 0, len(strs))
	for _, s := range strs {
		out = append(out, fmt.Sprintf("\"%s\"", s))
	}
	return out
}

func compareLine(line [][]byte, wanted ...string) error {
	if len(line) != len(wanted) {
		return fmt.Errorf(
			"Wanted [%s]; got [%s]",
			strings.Join(quote(wanted), ", "),
			strings.Join(quote(toStrings(line)), ", "),
		)
	}
	for i, s := range toStrings(line) {
		if s != wanted[i] {
			return fmt.Errorf(
				"Mismatch at item %d; wanted '%s'; got '%s'",
				i,
				wanted[i],
				s,
			)
		}
	}
	return nil
}

func TestRead(t *testing.T) {
	testCases := []struct {
		Title     string
		Input     string
		Wanted    [][]string
		BufferCap int
	}{{
		Title:  "OneRow",
		Input:  "abc,def,ghi",
		Wanted: [][]string{{"abc", "def", "ghi"}},
	}, {
		Title:  "MultipleLines",
		Input:  "abc,def\n1234,56",
		Wanted: [][]string{{"abc", "def"}, {"1234", "56"}},
	}, {
		Title:  "QuotedField",
		Input:  "\"abc\",\"123\",\"456\"",
		Wanted: [][]string{{"abc", "123", "456"}},
	}, {
		Title:  "QuotedFieldMultipleLines",
		Input:  "\"abc\",\"123\"\n\"def\",\"456\"",
		Wanted: [][]string{{"abc", "123"}, {"def", "456"}},
	}, {
		Title:  "SomeQuoted",
		Input:  `hello,"hello2",hello3`,
		Wanted: [][]string{{"hello", "hello2", "hello3"}},
	}, {
		Title:  "QuotedFieldsWithComma",
		Input:  "\"a,b,c\",\"d,e,f\"",
		Wanted: [][]string{{"a,b,c", "d,e,f"}},
	}, {
		Title:  "QuotedFieldsWithNewLine",
		Input:  "\"a\nb\nc\"",
		Wanted: [][]string{{"a\nb\nc"}},
	}, {
		Title:  "QuotedFieldsWithEscapedQuotes",
		Input:  "\"a\"\"b\"",
		Wanted: [][]string{{"a\"b"}},
	}, {
		Title:  "QuotedFieldsWithConsecutiveEscapedQuotes",
		Input:  "\"\"\"\"\"a\"\"\"\"\"",
		Wanted: [][]string{{"\"\"a\"\""}},
	}, {
		Title:  "QuotedFieldsWithEscapeQuotesAndMultipleLines",
		Input:  "abc,\"1\"\"\n2\"",
		Wanted: [][]string{{"abc", "1\"\n2"}},
	}, {
		Title:  "QuotedFieldsWithConsecutiveEscapedQuotesAndMultipleLines",
		Input:  "abc,\"\"\"\"\"a\"\"\"\"\nb\"",
		Wanted: [][]string{{"abc", "\"\"a\"\"\nb"}},
	}, {
		Title:     "QuotedFieldsWithLinesLongerThanBuffer",
		Input:     "\"abc\",\"def\",\"ghi\"",
		Wanted:    [][]string{{"abc", "def", "ghi"}},
		BufferCap: 4,
	}, {
		Title:  "TrailingNewline",
		Input:  "a,b,c\n",
		Wanted: [][]string{{"a", "b", "c"}},
	}, {
		Title:  "EmptyMiddleLine",
		Input:  "a,b\n\nc,d",
		Wanted: [][]string{{"a", "b"}, {""}, {"c", "d"}},
	}, {
		Title:  "CRLF",
		Input:  "a,b,c\r\nd,e,f",
		Wanted: [][]string{{"a", "b", "c"}, {"d", "e", "f"}},
	}}

	for _, testCase := range testCases {
		t.Run(testCase.Title, func(t *testing.T) {
			r := Reader{
				fields: fields{
					// initialize with a deliberately small buffer so we get
					// good coverage of i/o buffering
					buffer: bufferedReader{
						r:    strings.NewReader(testCase.Input),
						data: make([]byte, 0, testCase.BufferCap),
					},
					delimiter: ',',
				},
				fieldsBuffer: make([][]byte, 0, 16),
			}
			for i, wantedLine := range testCase.Wanted {
				fields, err := r.Read()
				if err != nil {
					t.Fatalf("Unexpected error on line %d: %v", i+1, err)
				}
				if err := compareLine(fields, wantedLine...); err != nil {
					t.Fatalf("Mismatch on line %d: %v", i+1, err)
				}
			}
			if _, err := r.Read(); err != io.EOF {
				t.Fatal("Wanted io.EOF; got:", err)
			}
		})
	}
}

func BenchmarkRead(b *testing.B) {
	data, err := ioutil.ReadFile("testdata/fl_insurance.csv")
	if err != nil {
		b.Fatal(err)
	}
	quotedData, err := ioutil.ReadFile("testdata/fl_insurance_quoted.csv")
	if err != nil {
		b.Fatal(err)
	}

	b.Run("StdCSV", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := csv.NewReader(bytes.NewReader(data))
			for {
				if _, err := r.Read(); err != nil {
					if err == io.EOF {
						break
					}
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("FastCSV", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := NewReader(bytes.NewReader(data), ',')
			for {
				if _, err := r.Read(); err != nil {
					if err == io.EOF {
						break
					}
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("StdCSVQuoted", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := csv.NewReader(bytes.NewReader(quotedData))
			for {
				if _, err := r.Read(); err != nil {
					if err == io.EOF {
						break
					}
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("FastCSVQuoted", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := NewReader(bytes.NewReader(quotedData), ',')
			for {
				if _, err := r.Read(); err != nil {
					if err == io.EOF {
						break
					}
					b.Fatal(err)
				}
			}
		}
	})
}
