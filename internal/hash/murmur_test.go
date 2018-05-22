package hash_test

import (
	"fmt"
	"github.com/tobgu/qframe/internal/hash"
	"math/rand"
	"testing"
)

const noSeed int64 = 0
const seed1 int64 = 1
const seed2 int64 = 2
const seed3 int64 = 3

func genInts(seed int64, size int) []int {
	result := make([]int, size)
	rand.Seed(seed)
	if seed == noSeed {
		// Sorted slice
		for ix := range result {
			result[ix] = ix
		}
	} else {
		// Random slice
		for ix := range result {
			result[ix] = rand.Intn(size)
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

func TestMurm32_Hash(t *testing.T) {
	size := 100000
	strs1 := genStringsWithCardinality(seed1, 100000, 1000, 10)
	strs2 := genStringsWithCardinality(seed2, 100000, 10, 10)
	strs3 := genStringsWithCardinality(seed3, 100000, 2, 10)

	hasher := hash.Murm32{}
	hashCounter := make(map[uint32]int)
	stringCounter := make(map[string]int)
	for i := 0; i < size; i++ {
		hasher.Reset()
		hasher.Write([]byte(strs1[i]))
		hasher.Write([]byte(strs2[i]))
		hasher.Write([]byte(strs3[i]))
		h := hasher.Hash()
		hashCounter[h] += 1
		stringCounter[strs1[i]+strs2[i]+strs3[i]] += 1
	}

	// For this input the hash is perfect so we expect the same number
	// of different hashes as actual values.
	if len(hashCounter) < len(stringCounter) {
		t.Errorf("Unexpected hash count: %d, %d", len(hashCounter), len(stringCounter))
	}
}
