package sql

import (
	"reflect"

	"github.com/tobgu/qframe/qerrors"
)

// CoerceFunc returns a function that does an explicit
// type cast from one input type and sets an internal
// column type.
type CoerceFunc func(c *Column) func(t interface{}) error

// Int64ToBool casts an int64 type into a boolean. This
// is useful for casting columns in SQLite which stores
// BOOL as INT types natively.
func Int64ToBool(c *Column) func(t interface{}) error {
	return func(t interface{}) error {
		v, ok := t.(int64)
		if !ok {
			return qerrors.New(
				"Coercion Int64ToBool", "type %s is not int64", reflect.TypeOf(t).Kind())
		}
		c.Bool(v != 0)
		return nil
	}
}
