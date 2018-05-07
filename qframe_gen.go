package qframe

import (
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/bcolumn"
	"github.com/tobgu/qframe/internal/ecolumn"
	"github.com/tobgu/qframe/internal/fcolumn"
	"github.com/tobgu/qframe/internal/icolumn"
	"github.com/tobgu/qframe/internal/scolumn"
	"reflect"
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
		return IntView{}, errors.New("IntView", "no such column: %s", colName)
	}

	col, ok := namedColumn.Column.(icolumn.Column)
	if !ok {
		return IntView{}, errors.New(
			"IntView",
			"invalid column type, expected %s, was: %s", "int", namedColumn.DataType(),
			reflect.TypeOf(namedColumn.Column))
	}

	return IntView{View: col.View(qf.index)}, nil
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
		return FloatView{}, errors.New("FloatView", "no such column: %s", colName)
	}

	col, ok := namedColumn.Column.(fcolumn.Column)
	if !ok {
		return FloatView{}, errors.New(
			"FloatView",
			"invalid column type, expected %s, was: %s", "float", namedColumn.DataType(),
			reflect.TypeOf(namedColumn.Column))
	}

	return FloatView{View: col.View(qf.index)}, nil
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
		return BoolView{}, errors.New("BoolView", "no such column: %s", colName)
	}

	col, ok := namedColumn.Column.(bcolumn.Column)
	if !ok {
		return BoolView{}, errors.New(
			"BoolView",
			"invalid column type, expected %s, was: %s", "bool", namedColumn.DataType(),
			reflect.TypeOf(namedColumn.Column))
	}

	return BoolView{View: col.View(qf.index)}, nil
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
		return StringView{}, errors.New("StringView", "no such column: %s", colName)
	}

	col, ok := namedColumn.Column.(scolumn.Column)
	if !ok {
		return StringView{}, errors.New(
			"StringView",
			"invalid column type, expected %s, was: %s", "string", namedColumn.DataType(),
			reflect.TypeOf(namedColumn.Column))
	}

	return StringView{View: col.View(qf.index)}, nil
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
		return EnumView{}, errors.New("EnumView", "no such column: %s", colName)
	}

	col, ok := namedColumn.Column.(ecolumn.Column)
	if !ok {
		return EnumView{}, errors.New(
			"EnumView",
			"invalid column type, expected %s, was: %s", "enum", namedColumn.DataType(),
			reflect.TypeOf(namedColumn.Column))
	}

	return EnumView{View: col.View(qf.index)}, nil
}
