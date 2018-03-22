package hashgrouper

import (
	"bytes"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
)

type entry struct {
	hash uint32
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

func insertEntry(i uint32, hash uint32, entries []entry, comparables []column.Comparable) (bool, int) {
	newGroup := false
	entriesLen := uint64(len(entries))
	startPos := uint64(hash) % entriesLen

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

func newHash(i uint32, comparables []column.Comparable, buf *bytes.Buffer) uint32 {
	buf.Reset()
	for _, c := range comparables {
		c.HashBytes(i, buf)
	}
	return murmur32(buf.Bytes())
}

const maxLoadFactor = 0.5

func reLocateEntries(oldEntries []entry) ([]entry, int) {
	newLen := uint32(2 * len(oldEntries))
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
