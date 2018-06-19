package sql

import (
	"fmt"
	"reflect"

	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/bcolumn"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/ecolumn"
	"github.com/tobgu/qframe/internal/fcolumn"
	"github.com/tobgu/qframe/internal/icolumn"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/scolumn"
)

type SQLConfig struct {
	// Query is a Raw SQL statement which must return
	// appropriate types which can be inferred
	// and loaded into a new QFrame.
	Query string
	// Incrementing indicates the PostgreSQL variant
	// of parameter markers will be used, e.g. $1..$2.
	// The default style is ?..?.
	Incrementing bool
	// Table is the name of the table to be used
	// for generating an INSERT statement.
	Table string
	// EscapeChar is a rune which column and table
	// names will be escaped with. PostgreSQL and SQLite
	// both accept double quotes "" while MariaDB/MySQL
	// only accept backticks.
	EscapeChar rune
	// CoerceMap is a map of columns to perform explicit
	// type coercion on.
	CoerceMap map[string]CoerceFunc
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
	return nil, errors.New("NewArgBuilder", fmt.Sprintf("bad column type: %s", reflect.TypeOf(col).Name()))
}
