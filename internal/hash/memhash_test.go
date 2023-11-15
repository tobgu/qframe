package hash_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/tobgu/qframe/internal/hash"
)

const noSeed int64 = 0
const seed1 int64 = 1
const seed2 int64 = 2
const seed3 int64 = 3

func genInts(seed int64, size int) []int {
	result := make([]int, size)
	r := rand.New(rand.NewSource(seed))
	if seed == noSeed {
		// Sorted slice
		for ix := range result {
			result[ix] = ix
		}
	} else {
		// Random slice
		for ix := range result {
			result[ix] = r.Intn(size)
		}
	}

	return result
}

func genIntsWithCardinality(seed int64, size, cardinality int) []int {
	result := genInts(seed, size)
	for i, x := range result {
		result[i] = x % cardinality
	}

	return result
}

func genStringsWithCardinality(seed int64, size, cardinality, strLen int) []string {
	baseStr := "abcdefghijklmnopqrstuvxyz"[:strLen]
	result := make([]string, size)
	for i, x := range genIntsWithCardinality(seed, size, cardinality) {
		result[i] = fmt.Sprintf("%s%d", baseStr, x)
	}
	return result
}

func Test_StringDistribution(t *testing.T) {
	size := 100000
	strs1 := genStringsWithCardinality(seed1, 100000, 1000, 10)
	strs2 := genStringsWithCardinality(seed2, 100000, 10, 10)
	strs3 := genStringsWithCardinality(seed3, 100000, 2, 10)

	hashCounter := make(map[uint32]int)
	stringCounter := make(map[string]int)
	for i := 0; i < size; i++ {
		val := hash.HashBytes([]byte(strs1[i]), 0)
		val = hash.HashBytes([]byte(strs2[i]), val)
		val = hash.HashBytes([]byte(strs3[i]), val)
		h := uint32(val)
		hashCounter[h] += 1
		stringCounter[strs1[i]+strs2[i]+strs3[i]] += 1
	}

	// For this input the hash is perfect so we expect the same number
	// of different hashes as actual values.
	if len(hashCounter) < len(stringCounter) {
		t.Errorf("Unexpected hash count: %d, %d", len(hashCounter), len(stringCounter))
	}
}

func Test_SmallIntDistribution(t *testing.T) {
	result := make(map[uint64]uint64)
	for i := 1; i < 177; i++ {
		val := hash.HashBytes([]byte{0, 0, 0, 0, 0, 0, 0, byte(i)}, 0)
		result[val] = result[val] + 1
	}

	if len(result) != 176 {
		t.Errorf("%d: %v", len(result), result)
	}
}
