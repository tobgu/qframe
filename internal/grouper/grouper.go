package grouper

import (
	"bytes"
	"github.com/spaolacci/murmur3"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
)

const (
	// Constants for multiplication: random odd 64-bit numbers.
	m1 = 16877499708836156737
	m2 = 2820277070424839065
	m3 = 9497967016996688599
)

// Taken from/highly inspired by stdlib runtime/hash64.go, see GO-LICENCE in root directory
func memhash64(x, seed uint64) uint64 {
	h := seed
	h ^= (x | (x + 4)) << 32
	h = rotl31(h*m1) * m2
	h ^= h >> 29
	h *= m3
	h ^= h >> 32
	return h
}

func rotl31(x uint64) uint64 {
	return (x << 31) | (x >> (64 - 31))
}

type entry struct {
	hash uint64
	ix   index.Int
}

func equals(comparables []column.Comparable, i, j uint32) bool {
	for _, c := range comparables {
		if c.Compare(i, j) != column.Equal {
			return false
		}
	}
	return true
}

func insertEntry(i uint32, hash uint64, entries []entry, comparables []column.Comparable) (bool, int) {
	newGroup := false
	entriesLen := uint64(len(entries))
	startPos := hash % entriesLen

	// Find entry
	var destEntry *entry
	collisions := 0
	for pos := startPos; destEntry == nil; pos = (pos + 1) % entriesLen {
		e := &entries[pos]
		if e.ix == nil || e.hash == hash && equals(comparables, i, e.ix[0]) {
			destEntry = e
		} else {
			collisions++
		}
	}

	// Update entry
	if destEntry.ix == nil {
		// Eden entry
		destEntry.ix = index.Int{i}
		destEntry.hash = hash
		newGroup = true
	} else {
		// Existing entry
		destEntry.ix = append(destEntry.ix, i)
	}

	return newGroup, collisions
}

func newHash(i uint32, comparables []column.Comparable, buf *bytes.Buffer) uint64 {
	buf.Reset()
	for _, c := range comparables {
		c.HashBytes(i, buf)
	}

	return murmur3.Sum64(buf.Bytes())
}

const maxLoadFactor = 0.5

func reLocateEntries(oldEntries []entry) ([]entry, int) {
	newLen := uint64(2 * len(oldEntries))
	result := make([]entry, newLen)
	collisions := 0
	for _, e := range oldEntries {
		for pos := e.hash % newLen; ; pos = (pos + 1) % newLen {
			newEntry := &result[pos]
			if newEntry.ix == nil {
				newEntry.ix = e.ix
				newEntry.hash = e.hash
				break
			}
			collisions++
		}
	}
	return result, collisions
}

type GroupStats struct {
	RelocationCount      int
	RelocationCollisions int
	InsertCollisions     int
	LoadFactor           float64
}

func Groups(ix index.Int, comparables []column.Comparable) ([]index.Int, GroupStats) {
	// Initial length is arbitrary
	groupCount := 0
	entries := make([]entry, (len(ix)/10)+10)
	stats := GroupStats{}
	var collisions int

	hashBytes := new(bytes.Buffer)
	for _, i := range ix {
		if float64(groupCount)/float64(len(entries)) > maxLoadFactor {
			entries, collisions = reLocateEntries(entries)
			stats.RelocationCollisions += collisions
			stats.RelocationCount++
		}

		hash := newHash(i, comparables, hashBytes)
		newGroup, collisions := insertEntry(i, hash, entries, comparables)
		stats.InsertCollisions += collisions
		if newGroup {
			groupCount++
		}
	}

	result := make([]index.Int, 0, groupCount)
	for _, e := range entries {
		if len(e.ix) > 0 {
			result = append(result, e.ix)
		}
	}

	stats.LoadFactor = float64(groupCount) / float64(len(entries))

	// fmt.Println(fmt.Sprintf("Hash count: %d, Hash sum count: %v, Group len: %d", len(hashes), len(hashSums), len(result)))
	return result, stats
}
