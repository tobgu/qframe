// Copyright 2018 Ulf Adams
// Modifications copyright 2019 Caleb Spare
//
// The contents of this file may be used under the terms of the Apache License,
// Version 2.0.
//
//    (See accompanying file LICENSE or copy at
//     http://www.apache.org/licenses/LICENSE-2.0)
//
// Unless required by applicable law or agreed to in writing, this software
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
//
// The code in this file is part of a Go translation of the C code written by
// Ulf Adams which may be found at https://github.com/ulfjack/ryu. That source
// code is licensed under Apache 2.0 and this code is derivative work thereof.

package ryu

import (
	"math"
	"math/bits"
)

// dec32 is a floating decimal type representing m * 10^e.
type dec32 struct {
	m uint32
	e int32
}

func (d dec32) append(b []byte, neg bool) []byte {
	// Step 5: Print the decimal representation.
	if neg {
		b = append(b, '-')
	}

	out := d.m
	outLen := decimalLen32(out)
	bufLen := outLen
	if bufLen > 1 {
		bufLen++ // extra space for '.'
	}

	// Print the decimal digits.
	n := len(b)
	b = append(b, make([]byte, bufLen)...)
	for i := 0; i < outLen-1; i++ {
		b[n+outLen-i] = '0' + byte(out%10)
		out /= 10
	}
	b[n] = '0' + byte(out%10)

	// Print the '.' if needed.
	if outLen > 1 {
		b[n+1] = '.'
	}

	// Print the exponent.
	b = append(b, 'e')
	exp := d.e + int32(outLen) - 1
	if exp < 0 {
		b = append(b, '-')
		exp = -exp
	} else {
		// Unconditionally print a + here to match strconv's formatting.
		b = append(b, '+')
	}
	// Always print two digits to match strconv's formatting.
	d1 := exp % 10
	d0 := exp / 10
	b = append(b, '0'+byte(d0), '0'+byte(d1))

	return b
}

func float32ToDecimalExactInt(mant, exp uint32) (d dec32, ok bool) {
	e := exp - bias32
	if e > mantBits32 {
		return d, false
	}
	shift := mantBits32 - e
	mant |= 1 << mantBits32 // implicit 1
	d.m = mant >> shift
	if d.m<<shift != mant {
		return d, false
	}
	for d.m%10 == 0 {
		d.m /= 10
		d.e++
	}
	return d, true
}

func float32ToDecimal(mant, exp uint32) dec32 {
	var e2 int32
	var m2 uint32
	if exp == 0 {
		// We subtract 2 so that the bounds computation has
		// 2 additional bits.
		e2 = 1 - bias32 - mantBits32 - 2
		m2 = mant
	} else {
		e2 = int32(exp) - bias32 - mantBits32 - 2
		m2 = uint32(1)<<mantBits32 | mant
	}
	even := m2&1 == 0
	acceptBounds := even

	// Step 2: Determine the interval of valid decimal representations.
	var (
		mv      = 4 * m2
		mp      = 4*m2 + 2
		mmShift = boolToUint32(mant != 0 || exp <= 1)
		mm      = 4*m2 - 1 - mmShift
	)

	// Step 3: Convert to a decimal power base using 64-bit arithmetic.
	var (
		vr, vp, vm        uint32
		e10               int32
		vmIsTrailingZeros bool
		vrIsTrailingZeros bool
		lastRemovedDigit  uint8
	)
	if e2 >= 0 {
		q := log10Pow2(e2)
		e10 = int32(q)
		k := pow5InvNumBits32 + pow5Bits(int32(q)) - 1
		i := -e2 + int32(q) + k
		vr = mulPow5InvDivPow2(mv, q, i)
		vp = mulPow5InvDivPow2(mp, q, i)
		vm = mulPow5InvDivPow2(mm, q, i)
		if q != 0 && (vp-1)/10 <= vm/10 {
			// We need to know one removed digit even if we are not
			// going to loop below. We could use q = X - 1 above,
			// except that would require 33 bits for the result, and
			// we've found that 32-bit arithmetic is faster even on
			// 64-bit machines.
			l := pow5InvNumBits32 + pow5Bits(int32(q-1)) - 1
			lastRemovedDigit = uint8(mulPow5InvDivPow2(mv, q-1, -e2+int32(q-1)+l) % 10)
		}
		if q <= 9 {
			// The largest power of 5 that fits in 24 bits is 5^10,
			// but q <= 9 seems to be safe as well. Only one of mp,
			// mv, and mm can be a multiple of 5, if any.
			if mv%5 == 0 {
				vrIsTrailingZeros = multipleOfPowerOfFive32(mv, q)
			} else if acceptBounds {
				vmIsTrailingZeros = multipleOfPowerOfFive32(mm, q)
			} else if multipleOfPowerOfFive32(mp, q) {
				vp--
			}
		}
	} else {
		q := log10Pow5(-e2)
		e10 = int32(q) + e2
		i := -e2 - int32(q)
		k := pow5Bits(i) - pow5NumBits32
		j := int32(q) - k
		vr = mulPow5DivPow2(mv, uint32(i), j)
		vp = mulPow5DivPow2(mp, uint32(i), j)
		vm = mulPow5DivPow2(mm, uint32(i), j)
		if q != 0 && (vp-1)/10 <= vm/10 {
			j = int32(q) - 1 - (pow5Bits(i+1) - pow5NumBits32)
			lastRemovedDigit = uint8(mulPow5DivPow2(mv, uint32(i+1), j) % 10)
		}
		if q <= 1 {
			// {vr,vp,vm} is trailing zeros if {mv,mp,mm} has at
			// least q trailing 0 bits. mv = 4 * m2, so it always
			// has at least two trailing 0 bits.
			vrIsTrailingZeros = true
			if acceptBounds {
				// mm = mv - 1 - mmShift, so it has 1 trailing 0 bit
				// iff mmShift == 1.
				vmIsTrailingZeros = mmShift == 1
			} else {
				// mp = mv + 2, so it always has at least one
				// trailing 0 bit.
				vp--
			}
		} else if q < 31 {
			vrIsTrailingZeros = multipleOfPowerOfTwo32(mv, q-1)
		}
	}

	// Step 4: Find the shortest decimal representation
	// in the interval of valid representations.
	var removed int32
	var out uint32
	if vmIsTrailingZeros || vrIsTrailingZeros {
		// General case, which happens rarely (~4.0%).
		for vp/10 > vm/10 {
			vmIsTrailingZeros = vmIsTrailingZeros && vm%10 == 0
			vrIsTrailingZeros = vrIsTrailingZeros && lastRemovedDigit == 0
			lastRemovedDigit = uint8(vr % 10)
			vr /= 10
			vp /= 10
			vm /= 10
			removed++
		}
		if vmIsTrailingZeros {
			for vm%10 == 0 {
				vrIsTrailingZeros = vrIsTrailingZeros && lastRemovedDigit == 0
				lastRemovedDigit = uint8(vr % 10)
				vr /= 10
				vp /= 10
				vm /= 10
				removed++
			}
		}
		if vrIsTrailingZeros && lastRemovedDigit == 5 && vr%2 == 0 {
			// Round even if the exact number is .....50..0.
			lastRemovedDigit = 4
		}
		out = vr
		// We need to take vr + 1 if vr is outside bounds
		// or we need to round up.
		if (vr == vm && (!acceptBounds || !vmIsTrailingZeros)) || lastRemovedDigit >= 5 {
			out++
		}
	} else {
		// Specialized for the common case (~96.0%). Percentages below
		// are relative to this. Loop iterations below (approximately):
		// 0: 13.6%, 1: 70.7%, 2: 14.1%, 3: 1.39%, 4: 0.14%, 5+: 0.01%
		for vp/10 > vm/10 {
			lastRemovedDigit = uint8(vr % 10)
			vr /= 10
			vp /= 10
			vm /= 10
			removed++
		}
		// We need to take vr + 1 if vr is outside bounds
		// or we need to round up.
		out = vr + boolToUint32(vr == vm || lastRemovedDigit >= 5)
	}

	return dec32{m: out, e: e10 + removed}
}

func decimalLen32(u uint32) int {
	// Function precondition: u is not a 10-digit number.
	// (9 digits are sufficient for round-tripping.)
	// This benchmarked faster than the log2 approach used for uint64s.
	assert(u < 1000000000, "too big")
	switch {
	case u >= 100000000:
		return 9
	case u >= 10000000:
		return 8
	case u >= 1000000:
		return 7
	case u >= 100000:
		return 6
	case u >= 10000:
		return 5
	case u >= 1000:
		return 4
	case u >= 100:
		return 3
	case u >= 10:
		return 2
	default:
		return 1
	}
}

func mulShift32(m uint32, mul uint64, shift int32) uint32 {
	assert(shift > 32, "shift > 32")

	hi, lo := bits.Mul64(uint64(m), mul)
	shiftedSum := (lo >> uint(shift)) + (hi << uint(64-shift))
	assert(shiftedSum <= math.MaxUint32, "shiftedSum <= math.MaxUint32")
	return uint32(shiftedSum)
}

func mulPow5InvDivPow2(m, q uint32, j int32) uint32 {
	return mulShift32(m, pow5InvSplit32[q], j)
}

func mulPow5DivPow2(m, i uint32, j int32) uint32 {
	return mulShift32(m, pow5Split32[i], j)
}

func pow5Factor32(v uint32) uint32 {
	for n := uint32(0); ; n++ {
		q, r := v/5, v%5
		if r != 0 {
			return n
		}
		v = q
	}
}

// multipleOfPowerOfFive32 reports whether v is divisible by 5^p.
func multipleOfPowerOfFive32(v, p uint32) bool {
	return pow5Factor32(v) >= p
}

// multipleOfPowerOfTwo32 reports whether v is divisible by 2^p.
func multipleOfPowerOfTwo32(v, p uint32) bool {
	return uint32(bits.TrailingZeros32(v)) >= p
}
