package ecolumn

import (
	"github.com/tobgu/qframe/internal/index"
)

// Code generated from template/filters.go DO NOT EDIT

func lt(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() < comparatee.compVal()
		}
	}
}

func lte(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() <= comparatee.compVal()
		}
	}
}

func gt(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() > comparatee.compVal()
		}
	}
}

func gte(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() >= comparatee.compVal()
		}
	}
}

func eq(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() == comparatee.compVal()
		}
	}
}

func neq(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() != comparatee.compVal()
		}
	}
}

func lt2(index index.Int, col, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() < col2[index[i]].compVal()
		}
	}
}

func lte2(index index.Int, col, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() <= col2[index[i]].compVal()
		}
	}
}

func gt2(index index.Int, col, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() > col2[index[i]].compVal()
		}
	}
}

func gte2(index index.Int, col, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() >= col2[index[i]].compVal()
		}
	}
}

func eq2(index index.Int, col, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() == col2[index[i]].compVal()
		}
	}
}

func neq2(index index.Int, col, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() != col2[index[i]].compVal()
		}
	}
}
