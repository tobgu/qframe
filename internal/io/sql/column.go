package sql

import (
	"fmt"
	"math"
	"reflect"

	"github.com/tobgu/qframe/errors"
)

// Column implements the sql.Scanner interface
// and allows arbitrary data types to be loaded from
// any database/sql/driver into a QFrame.
type Column struct {
	kind  reflect.Kind
	nulls int
	// pointer to the data slice which
	// contains the inferred data type
	ptr  interface{}
	data struct {
		Ints    []int
		Floats  []float64
		Bools   []bool
		Strings []*string
	}
	coerce func(t interface{}) error
}

// Null appends a new Null value to
// the underlying column data.
func (c *Column) Null() error {
	// If we haven't inferred the type of
	// data we are scanning simply count
	// the number of NULL values we receive.
	// The only scenario this will happen is
	// when the first returned values are NULL.
	if c.kind == reflect.Invalid {
		c.nulls++
		return nil
	}
	switch c.kind {
	case reflect.Float64:
		c.data.Floats = append(c.data.Floats, math.NaN())
	case reflect.String:
		c.data.Strings = append(c.data.Strings, nil)
	default:
		return errors.New("Column Null", fmt.Sprintf("non-nullable type: %s", c.kind))
	}
	return nil
}

// Int adds a new int to the underlying data slice
func (c *Column) Int(i int) {
	if c.ptr == nil {
		c.kind = reflect.Int
		c.ptr = &c.data.Ints
	}
	c.data.Ints = append(c.data.Ints, i)
}

// Float adds a new float to the underlying data slice
func (c *Column) Float(f float64) {
	if c.ptr == nil {
		c.kind = reflect.Float64
		c.ptr = &c.data.Floats
		// add any NULL floats previously scanned
		if c.nulls > 0 {
			for i := 0; i < c.nulls; i++ {
				c.data.Floats = append(c.data.Floats, math.NaN())
			}
			c.nulls = 0
		}
	}
	c.data.Floats = append(c.data.Floats, f)
}

// String adds a new string to the underlying data slice
func (c *Column) String(s string) {
	if c.ptr == nil {
		c.kind = reflect.String
		c.ptr = &c.data.Strings
		// add any NULL strings previously scanned
		if c.nulls > 0 {
			for i := 0; i < c.nulls; i++ {
				c.data.Strings = append(c.data.Strings, nil)
			}
			c.nulls = 0
		}
	}
	c.data.Strings = append(c.data.Strings, &s)
}

// Bool adds a new bool to the underlying data slice
func (c *Column) Bool(b bool) {
	if c.ptr == nil {
		c.kind = reflect.Bool
		c.ptr = &c.data.Bools
	}
	c.data.Bools = append(c.data.Bools, b)
}

// Scan implements the sql.Scanner interface
func (c *Column) Scan(t interface{}) error {
	if c.coerce != nil {
		return c.coerce(t)
	}
	switch v := t.(type) {
	case bool:
		c.Bool(v)
	case string:
		c.String(v)
	case int64:
		c.Int(int(v))
	case []uint8:
		c.String(string(v))
	case float64:
		c.Float(v)
	case nil:
		err := c.Null()
		if err != nil {
			return err
		}
	default:
		return errors.New(
			"Column Scan", "unsupported scan type: %s", reflect.ValueOf(t).Kind())
	}
	return nil
}

// Data returns the underlying data slice
func (c *Column) Data() interface{} {
	if c.ptr == nil {
		return nil
	}
	// *[]<T> -> []<T>
	return reflect.ValueOf(c.ptr).Elem().Interface()
}
