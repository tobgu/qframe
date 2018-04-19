package murmur3

import (
	"unsafe"
	"math/rand"
)

/*
The below is highly inspired by the 32 bit hash found in
github.com/spaolacci/murmur3. See license below.

LICENSE
-------
Copyright 2013, Sébastien Paolacci.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the library nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

const (
	c1_32 uint32 = 0xcc9e2d51
	c2_32 uint32 = 0x1b873593
)

type Murm32 struct {
	totLen int
	hash uint32
	buf [4]byte
	bufSize int8
}

func (m *Murm32) WriteByte(b byte) {
	m.buf[m.bufSize] = b
	m.bufSize++
	m.flushBufIfNeeded()
}

func (m *Murm32) flushBufIfNeeded() {
	if m.bufSize == int8(len(m.buf)) {
		m.Write(m.buf[:])
		m.bufSize = 0
	}
}

func (m *Murm32) Write(data []byte) {
	h1 := m.hash
	nblocks := len(data) / 4
	m.totLen += len(data)
	var p uintptr
	if len(data) > 0 {
		p = uintptr(unsafe.Pointer(&data[0]))
	}
	p1 := p + uintptr(4*nblocks)
	for ; p < p1; p += 4 {
		k1 := *(*uint32)(unsafe.Pointer(p))

		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // rotl32(h1, 13)
		h1 = h1*4 + h1 + 0xe6546b64
	}

	tail := data[nblocks*4:]
	for _, d := range tail {
		m.buf[m.bufSize] = d
		m.bufSize++
		m.flushBufIfNeeded()
	}

	m.hash = h1
}

func (m *Murm32) Reset() {
	m.bufSize = 0
	m.hash = 0
	m.totLen = 0
}

func (m *Murm32) Hash() uint32 {
	var k1 uint32
	h1 := m.hash
	switch m.bufSize {
	case 3:
		k1 ^= uint32(m.buf[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(m.buf[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(m.buf[0])
		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	case 0:
		// Nothing to do
	default:
		panic("Unexpected buf length")
	}

	h1 ^= uint32(m.totLen)

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16
	return h1
}

func (m *Murm32) WriteFourRandomBytes() {
	var nullHashBytes [4]byte
	hashBytes := nullHashBytes[:]
	rand.Read(hashBytes)
	m.Write(hashBytes)
}
