package sql

import (
	"database/sql"

	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/types"
)

// ReadSQL returns a named map of types.DataSlice for consumption
// by the qframe.New constructor.
func ReadSQL(rows *sql.Rows) (map[string]types.DataSlice, []string, error) {
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
			for i := 0; i < len(names); i++ {
				columns = append(columns, &Column{})
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
