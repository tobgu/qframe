package sql

import (
	"database/sql"

	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/types"
)

// ReadSQL returns a named map of types.DataSlice for consumption
// by the qframe.New constructor.
func ReadSQL(rows *sql.Rows, conf SQLConfig) (map[string]types.DataSlice, []string, error) {
	var (
		columns  []interface{}
		colNames []string
	)
	for rows.Next() {
		// Allocate columns for the returning query
		if columns == nil {
			names, err := rows.Columns()
			if err != nil {
				return nil, colNames, errors.New("ReadSQL Columns", err.Error())
			}
			for _, name := range names {
				col := &Column{}
				if conf.CoerceMap != nil {
					fn, ok := conf.CoerceMap[name]
					if ok {
						col.coerce = fn(col)
					}
				}
				columns = append(columns, col)
			}
			// ensure any column in the coercion map
			// exists in the resulting columns or return
			// an error explicitly.
			if conf.CoerceMap != nil {
			checkMap:
				for name, _ := range conf.CoerceMap {
					for _, colName := range colNames {
						if name == colName {
							continue checkMap
						}
						return nil, colNames, errors.New("ReadSQL Columns", "column %s does not exist to coerce", name)
					}
				}
			}
			colNames = names
		}
		// Scan the result into our columns
		err := rows.Scan(columns...)
		if err != nil {
			return nil, colNames, errors.New("ReadSQL Scan", err.Error())
		}
	}
	result := map[string]types.DataSlice{}
	for i, column := range columns {
		result[colNames[i]] = column.(*Column).Data()
	}
	return result, colNames, nil
}
