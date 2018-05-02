package bcolumn

import (
	"github.com/tobgu/qframe/internal/index"
)

// Code generated from template/... DO NOT EDIT

func eq(index index.Int, column []bool, comp bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] == comp
		}
	}
}

func neq(index index.Int, column []bool, comp bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] != comp
		}
	}
}

func eq2(index index.Int, column []bool, compCol []bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] == compCol[pos]
		}
	}
}

func neq2(index index.Int, column []bool, compCol []bool, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] != compCol[pos]
		}
	}
}
