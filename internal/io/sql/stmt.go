package sql

import (
	"bytes"
)

// Insert generates a SQL insert statement
// for each colName.
func Insert(table string, colNames []string) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("INSERT INTO \"")
	buf.WriteString(table)
	buf.WriteString("\" (")
	for i, name := range colNames {
		buf.WriteString(name)
		if i+1 < len(colNames) {
			buf.WriteString(",")
		}
	}
	buf.WriteString(") VALUES (")
	for i := range colNames {
		buf.WriteString("?")
		if i+1 < len(colNames) {
			buf.WriteString(",")
		}
	}
	buf.WriteString(");")
	return buf.String()
}
