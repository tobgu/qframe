package scolumn

import (
	"github.com/tobgu/qframe/internal/index"
)

// Code generated from template/filters.go DO NOT EDIT

func lt(index index.Int, s Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			bIndex[i] = !isNull && s < comparatee
		}
	}

	return nil
}

func lte(index index.Int, s Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			bIndex[i] = !isNull && s <= comparatee
		}
	}

	return nil
}

func gt(index index.Int, s Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			bIndex[i] = !isNull && s > comparatee
		}
	}

	return nil
}

func gte(index index.Int, s Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			bIndex[i] = !isNull && s >= comparatee
		}
	}

	return nil
}

func eq(index index.Int, s Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := s.stringAt(index[i])
			bIndex[i] = !isNull && s == comparatee
		}
	}

	return nil
}
