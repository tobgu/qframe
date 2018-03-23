package hashgrouper

import (
	"bytes"
	"math/rand"
)

func WriteFourRandomBytes(buf *bytes.Buffer) {
	var nullHashBytes [4]byte
	hashBytes := nullHashBytes[:]
	rand.Read(hashBytes)
	buf.Write(hashBytes)
}
