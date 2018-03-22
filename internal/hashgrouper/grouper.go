package hashgrouper

import (
	"bytes"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/index"
)

type groupEntry struct {
	ix       index.Int
	hash     uint32
	firstPos uint32
	occupied bool
}

func equals(comparables []column.Comparable, i, j uint32) bool {
	for _, c := range comparables {
		if c.Compare(i, j) != column.Equal {
			return false
		}
	}
	return true
}

func insertEntry(i, hash uint32, entries []groupEntry, comparables []column.Comparable, collectIx bool) (bool, int) {
	// Find entry
	entriesLen := uint64(len(entries))
	startPos := uint64(hash) % entriesLen
	var destEntry *groupEntry
	collisions := 0
	for pos := startPos; destEntry == nil; pos = (pos + 1) % entriesLen {
		e := &entries[pos]
		if !e.occupied || e.hash == hash && equals(comparables, i, e.firstPos) {
			destEntry = e
		} else {
			collisions++
		}
	}

	// Update entry
	newGroup := false
	if !destEntry.occupied {
		// Eden entry
		destEntry.hash = hash
		destEntry.firstPos = i
		destEntry.occupied = true
		newGroup = true
	} else {
		// Existing entry
		if collectIx {
			// Small hack to reduce number of allocations under some circumstances. Delay
			// creation of index slice until there are at least two entries in the group
			// since we store the first position in a separate variable on the entry anyway.
			if destEntry.ix == nil {
				destEntry.ix = index.Int{destEntry.firstPos, i}
			} else {
				destEntry.ix = append(destEntry.ix, i)
			}
		}
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

func reLocateEntries(oldEntries []groupEntry) ([]groupEntry, int) {
	newLen := uint32(2 * len(oldEntries))
	result := make([]groupEntry, newLen)
	collisions := 0
	for _, e := range oldEntries {
		for pos := e.hash % newLen; ; pos = (pos + 1) % newLen {
			if !result[pos].occupied {
				result[pos] = e
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
	GroupCount           int
	LoadFactor           float64
}

func groupIndex(ix index.Int, comparables []column.Comparable, collectIx bool) ([]groupEntry, GroupStats) {
	// Initial length is arbitrary
	groupCount := 0
	entries := make([]groupEntry, (len(ix)/10)+10)
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
		newGroup, collisions := insertEntry(i, hash, entries, comparables, collectIx)
		stats.InsertCollisions += collisions
		if newGroup {
			groupCount++
		}
	}

	stats.LoadFactor = float64(groupCount) / float64(len(entries))
	stats.GroupCount = groupCount
	return entries, stats
}

func GroupBy(ix index.Int, comparables []column.Comparable) ([]index.Int, GroupStats) {
	entries, stats := groupIndex(ix, comparables, true)
	result := make([]index.Int, 0, stats.GroupCount)
	for _, e := range entries {
		if e.occupied {
			if e.ix == nil {
				result = append(result, index.Int{e.firstPos})
			} else {
				result = append(result, e.ix)
			}
		}
	}

	return result, stats
}

func Distinct(ix index.Int, comparables []column.Comparable) index.Int {
	entries, stats := groupIndex(ix, comparables, false)
	result := make(index.Int, 0, stats.GroupCount)
	for _, e := range entries {
		if e.occupied {
			result = append(result, e.firstPos)
		}
	}

	return result
}
