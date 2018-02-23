package eseries

import "fmt"

// Helper type for multi value filtering
type bitset [4]uint64

func (s *bitset) set(val enumVal) {
	s[val>>6] |= 1 << (val & 0x3F)
}

func (s *bitset) isSet(val enumVal) bool {
	return s[val>>6]&(1<<(val&0x3F)) > 0
}

func (s *bitset) String() string {
	return fmt.Sprintf("%X %X %X %X", s[3], s[2], s[1], s[0])
}
