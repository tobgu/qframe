package sql

import (
	"testing"
)

func TestInsert(t *testing.T) {
	// Unescaped
	query := Insert([]string{"COL1", "COL2"}, SQLConfig{Table: "test"})
	expected := `INSERT INTO test (COL1,COL2) VALUES (?,?);`
	assertEqual(t, expected, query)

	// Double quote escaped
	query = Insert([]string{"COL1", "COL2"}, SQLConfig{
		Table: "test", EscapeChar: '"'})
	expected = "INSERT INTO \"test\" (\"COL1\",\"COL2\") VALUES (?,?);"
	assertEqual(t, expected, query)

	// Backtick escaped
	query = Insert([]string{"COL1", "COL2"}, SQLConfig{
		Table: "test", EscapeChar: '`'})
	expected = "INSERT INTO `test` (`COL1`,`COL2`) VALUES (?,?);"
	assertEqual(t, expected, query)
}
