package qplot

import (
	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/types"
)

// isNumCol checks to see if column contains a numeric
// type and may be plotted.
func isNumCol(col string, qf qframe.QFrame) bool {
	cType, ok := qf.ColumnTypeMap()[col]
	if !ok {
		return false
	}
	switch cType {
	case types.Float:
		return true
	case types.Int:
		return true
	}
	return false
}
