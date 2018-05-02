package scolumn

import (
	"github.com/tobgu/qframe/internal/index"
)

// Code generated from template/... DO NOT EDIT

func lt(index index.Int, c Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := c.stringAt(index[i])
			bIndex[i] = !isNull && s < comparatee
		}
	}

	return nil
}

func lte(index index.Int, c Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := c.stringAt(index[i])
			bIndex[i] = !isNull && s <= comparatee
		}
	}

	return nil
}

func gt(index index.Int, c Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := c.stringAt(index[i])
			bIndex[i] = !isNull && s > comparatee
		}
	}

	return nil
}

func gte(index index.Int, c Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := c.stringAt(index[i])
			bIndex[i] = !isNull && s >= comparatee
		}
	}

	return nil
}

func eq(index index.Int, c Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := c.stringAt(index[i])
			bIndex[i] = !isNull && s == comparatee
		}
	}

	return nil
}

func lt2(index index.Int, col, col2 Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := col.stringAt(index[i])
			s2, isNull2 := col2.stringAt(index[i])
			bIndex[i] = !isNull && !isNull2 && s < s2
		}
	}
	return nil
}

func lte2(index index.Int, col, col2 Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := col.stringAt(index[i])
			s2, isNull2 := col2.stringAt(index[i])
			bIndex[i] = !isNull && !isNull2 && s <= s2
		}
	}
	return nil
}

func gt2(index index.Int, col, col2 Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := col.stringAt(index[i])
			s2, isNull2 := col2.stringAt(index[i])
			bIndex[i] = !isNull && !isNull2 && s > s2
		}
	}
	return nil
}

func gte2(index index.Int, col, col2 Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := col.stringAt(index[i])
			s2, isNull2 := col2.stringAt(index[i])
			bIndex[i] = !isNull && !isNull2 && s >= s2
		}
	}
	return nil
}

func eq2(index index.Int, col, col2 Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := col.stringAt(index[i])
			s2, isNull2 := col2.stringAt(index[i])
			bIndex[i] = !isNull && !isNull2 && s == s2
		}
	}
	return nil
}
