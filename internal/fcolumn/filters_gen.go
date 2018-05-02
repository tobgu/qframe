package fcolumn

import (
	"github.com/tobgu/qframe/internal/index"
)

// Code generated from template/... DO NOT EDIT

func lt(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] < comp
		}
	}
}

func lte(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] <= comp
		}
	}
}

func gt(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] > comp
		}
	}
}

func gte(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] >= comp
		}
	}
}

func eq(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] == comp
		}
	}
}

func neq(index index.Int, column []float64, comp float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] != comp
		}
	}
}

func lt2(index index.Int, column []float64, compCol []float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] < compCol[pos]
		}
	}
}

func lte2(index index.Int, column []float64, compCol []float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] <= compCol[pos]
		}
	}
}

func gt2(index index.Int, column []float64, compCol []float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] > compCol[pos]
		}
	}
}

func gte2(index index.Int, column []float64, compCol []float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] >= compCol[pos]
		}
	}
}

func eq2(index index.Int, column []float64, compCol []float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] == compCol[pos]
		}
	}
}

func neq2(index index.Int, column []float64, compCol []float64, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] != compCol[pos]
		}
	}
}
