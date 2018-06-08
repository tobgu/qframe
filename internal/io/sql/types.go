package sql

import (
	"fmt"
	"reflect"

	"github.com/tobgu/qframe/internal/bcolumn"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/ecolumn"
	"github.com/tobgu/qframe/internal/fcolumn"
	"github.com/tobgu/qframe/internal/icolumn"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/scolumn"
)

type SQLConfig struct {
	// Raw SQL statement which must return
	// appropriate types which can be inferred
	// and loaded into a new QFrame.
	Query string
}

type ArgBuilder func(ix index.Int, i int) interface{}

func NewArgBuilder(col column.Column) (ArgBuilder, error) {
	switch c := col.(type) {
	case bcolumn.Column:
		return func(ix index.Int, i int) interface{} {
			return c.View(ix).ItemAt(i)
		}, nil
	case icolumn.Column:
		return func(ix index.Int, i int) interface{} {
			return c.View(ix).ItemAt(i)
		}, nil
	case fcolumn.Column:
		return func(ix index.Int, i int) interface{} {
			return c.View(ix).ItemAt(i)
		}, nil
	case scolumn.Column:
		return func(ix index.Int, i int) interface{} {
			return c.View(ix).ItemAt(i)
		}, nil
	case ecolumn.Column:
		return func(ix index.Int, i int) interface{} {
			return c.View(ix).ItemAt(i)
		}, nil
	}
	return nil, fmt.Errorf("bad column type: %s", reflect.TypeOf(col).Name())
}
