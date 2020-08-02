package qframe

import (
	"github.com/tobgu/qframe/internal/bcolumn"
	"github.com/tobgu/qframe/internal/ecolumn"
	"github.com/tobgu/qframe/internal/fcolumn"
	"github.com/tobgu/qframe/internal/icolumn"
	"github.com/tobgu/qframe/internal/scolumn"
	"github.com/tobgu/qframe/qerrors"
)

// Code generated from template/... DO NOT EDIT

// IntView provides a "view" into an int column and can be used for access to individual elements.
type IntView struct {
	icolumn.View
}

// IntView returns a view into an int column identified by name.
//
// colName - Name of the column.
//
// Returns an error if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) IntView(colName string) (IntView, error) {
	namedColumn, ok := qf.columnsByName[colName]
	if !ok {
		return IntView{}, qerrors.New("IntView", "unknown column: %s", colName)
	}

	col, ok := namedColumn.Column.(icolumn.Column)
	if !ok {
		return IntView{}, qerrors.New(
			"IntView",
			"invalid column type, expected: %s, was: %s", "int", namedColumn.DataType())
	}

	return IntView{View: col.View(qf.index)}, nil
}

// MustIntView returns a view into an int column identified by name.
//
// colName - Name of the column.
//
// Panics if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) MustIntView(colName string) IntView {
	view, err := qf.IntView(colName)
	if err != nil {
		panic(qerrors.Propagate("MustIntView", err))
	}
	return view
}

// FloatView provides a "view" into an float column and can be used for access to individual elements.
type FloatView struct {
	fcolumn.View
}

// FloatView returns a view into an float column identified by name.
//
// colName - Name of the column.
//
// Returns an error if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) FloatView(colName string) (FloatView, error) {
	namedColumn, ok := qf.columnsByName[colName]
	if !ok {
		return FloatView{}, qerrors.New("FloatView", "unknown column: %s", colName)
	}

	col, ok := namedColumn.Column.(fcolumn.Column)
	if !ok {
		return FloatView{}, qerrors.New(
			"FloatView",
			"invalid column type, expected: %s, was: %s", "float", namedColumn.DataType())
	}

	return FloatView{View: col.View(qf.index)}, nil
}

// MustFloatView returns a view into an float column identified by name.
//
// colName - Name of the column.
//
// Panics if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) MustFloatView(colName string) FloatView {
	view, err := qf.FloatView(colName)
	if err != nil {
		panic(qerrors.Propagate("MustFloatView", err))
	}
	return view
}

// BoolView provides a "view" into an bool column and can be used for access to individual elements.
type BoolView struct {
	bcolumn.View
}

// BoolView returns a view into an bool column identified by name.
//
// colName - Name of the column.
//
// Returns an error if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) BoolView(colName string) (BoolView, error) {
	namedColumn, ok := qf.columnsByName[colName]
	if !ok {
		return BoolView{}, qerrors.New("BoolView", "unknown column: %s", colName)
	}

	col, ok := namedColumn.Column.(bcolumn.Column)
	if !ok {
		return BoolView{}, qerrors.New(
			"BoolView",
			"invalid column type, expected: %s, was: %s", "bool", namedColumn.DataType())
	}

	return BoolView{View: col.View(qf.index)}, nil
}

// MustBoolView returns a view into an bool column identified by name.
//
// colName - Name of the column.
//
// Panics if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) MustBoolView(colName string) BoolView {
	view, err := qf.BoolView(colName)
	if err != nil {
		panic(qerrors.Propagate("MustBoolView", err))
	}
	return view
}

// StringView provides a "view" into an string column and can be used for access to individual elements.
type StringView struct {
	scolumn.View
}

// StringView returns a view into an string column identified by name.
//
// colName - Name of the column.
//
// Returns an error if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) StringView(colName string) (StringView, error) {
	namedColumn, ok := qf.columnsByName[colName]
	if !ok {
		return StringView{}, qerrors.New("StringView", "unknown column: %s", colName)
	}

	col, ok := namedColumn.Column.(scolumn.Column)
	if !ok {
		return StringView{}, qerrors.New(
			"StringView",
			"invalid column type, expected: %s, was: %s", "string", namedColumn.DataType())
	}

	return StringView{View: col.View(qf.index)}, nil
}

// MustStringView returns a view into an string column identified by name.
//
// colName - Name of the column.
//
// Panics if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) MustStringView(colName string) StringView {
	view, err := qf.StringView(colName)
	if err != nil {
		panic(qerrors.Propagate("MustStringView", err))
	}
	return view
}

// EnumView provides a "view" into an enum column and can be used for access to individual elements.
type EnumView struct {
	ecolumn.View
}

// EnumView returns a view into an enum column identified by name.
//
// colName - Name of the column.
//
// Returns an error if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) EnumView(colName string) (EnumView, error) {
	namedColumn, ok := qf.columnsByName[colName]
	if !ok {
		return EnumView{}, qerrors.New("EnumView", "unknown column: %s", colName)
	}

	col, ok := namedColumn.Column.(ecolumn.Column)
	if !ok {
		return EnumView{}, qerrors.New(
			"EnumView",
			"invalid column type, expected: %s, was: %s", "enum", namedColumn.DataType())
	}

	return EnumView{View: col.View(qf.index)}, nil
}

// MustEnumView returns a view into an enum column identified by name.
//
// colName - Name of the column.
//
// Panics if the column is missing or of wrong type.
// Time complexity O(1).
func (qf QFrame) MustEnumView(colName string) EnumView {
	view, err := qf.EnumView(colName)
	if err != nil {
		panic(qerrors.Propagate("MustEnumView", err))
	}
	return view
}
