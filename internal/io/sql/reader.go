package sql

import (
	"database/sql"

	"github.com/tobgu/qframe/types"
)

// args returns a slice of Scanner for the SQL
// package. TODO: These pointer allocations seem
// unnecessary but I'm unsure how else to do it.
func args(cols []*Column) (ptrs []interface{}) {
	for _, col := range cols {
		ptrs = append(ptrs, col)
	}
	return ptrs
}

// Column holds an embeded struct of possible underlying
// data types. Each call to the Scan() will append a
// new value. Each Column is only expected to be used
// one time during a call to ReadSQL.
// TODO: Only supporting types returned from sqlite
// at the moment, need to test with other drivers.
type Column struct {
	Name string
	data struct {
		Ints    []int
		Floats  []float64
		Bools   []bool
		Strings []string
	}
}

// Data returns the first underlying DataSlice
func (c *Column) Data() types.DataSlice {
	switch {
	case len(c.data.Ints) > 0:
		return c.data.Ints
	case len(c.data.Floats) > 0:
		return c.data.Floats
	case len(c.data.Bools) > 0:
		return c.data.Bools
	case len(c.data.Strings) > 0:
		return c.data.Strings
	}
	return nil
}

// Scan reads the SQL driver type into an underlying
// DataSlice. Only a single DataType should ever be
// be scanned through the lifetime of a Column.
func (c *Column) Scan(t interface{}) error {
	switch v := t.(type) {
	case int64:
		c.data.Ints = append(c.data.Ints, int(v))
	case float64:
		c.data.Floats = append(c.data.Floats, v)
	case bool:
		c.data.Bools = append(c.data.Bools, v)
	case []uint8:
		c.data.Strings = append(c.data.Strings, string(v))
	}
	return nil
}

// ReadSQL returns a named map of types.DataSlice for consumption
// by the qframe.New constructor.
func ReadSQL(rows *sql.Rows) (map[string]types.DataSlice, error) {
	var columns []*Column
	for rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		// Allocate columns for the returning query
		if columns == nil {
			cols, err := rows.Columns()
			if err != nil {
				return nil, err
			}
			for _, name := range cols {
				columns = append(columns, &Column{Name: name})
			}
		}
		// Scan the result into our columns
		err := rows.Scan(args(columns)...)
		if err != nil {
			return nil, err
		}
	}
	result := map[string]types.DataSlice{}
	for _, column := range columns {
		result[column.Name] = column.Data()
	}
	return result, nil
}
