package sql

import (
	qsqlio "github.com/tobgu/qframe/internal/io/sql"
)

type coerceType int

const (
	_ coerceType = iota
	// Int64ToBool casts an int64 type into a bool,
	// useful for handling SQLite INT -> BOOL.
	Int64ToBool
	StringToFloat
)

// CoercePair casts the scanned value in Column
// to another type.
type CoercePair struct {
	Column string
	Type   coerceType
}

func coerceFunc(cType coerceType) qsqlio.CoerceFunc {
	switch cType {
	case Int64ToBool:
		return qsqlio.Int64ToBool
	case StringToFloat:
		return qsqlio.StringToFloat
	}
	return nil
}

// Config holds configuration parameters for reading/writing to/from a SQL DB.
type Config = qsqlio.SQLConfig

// StatementFunc returns a query for a SQL Prepare() method
type StatementFunc = qsqlio.StatementFunc

var (
	// Insert creates a dialect specific INSERT statement
	// for writing to a SQL database. This is the default.
	Insert = qsqlio.Insert
)

// ConfigFunc manipulates a Config object.
type ConfigFunc func(*Config)

// NewConfig creates a new config object.
func NewConfig(ff []ConfigFunc) Config {
	// defaults
	conf := Config{
		Statement: Insert,
	}
	for _, f := range ff {
		f(&conf)
	}
	return conf
}

// Query is a Raw SQL statement which must return
// appropriate types which can be inferred
// and loaded into a new QFrame.
func Query(query string) ConfigFunc {
	return func(c *Config) {
		c.Query = query
	}
}

// Table is the name of the table to be used
// for generating an INSERT statement.
func Table(table string) ConfigFunc {
	return func(c *Config) {
		c.Table = table
	}
}

// Postgres configures the query builder
// to generate SQL that is compatible with
// PostgreSQL. See github.com/lib/pq
func Postgres() ConfigFunc {
	return func(c *Config) {
		EscapeChar('"')(c)
		Incrementing()(c)
	}
}

// SQLite configures the query builder to
// generate SQL that is compatible with
// SQLite3. See github.com/mattn/go-sqlite3
func SQLite() ConfigFunc {
	return func(c *Config) {
		EscapeChar('"')(c)
	}
}

// MySQL configures the query builder to
// generate SQL that is compatible with MySQL/MariaDB
// See github.com/go-sql-driver/mysql
func MySQL() ConfigFunc {
	return func(c *Config) {
		EscapeChar('`')(c)
	}
}

// Incrementing indicates the PostgreSQL variant
// of parameter markers will be used, e.g. $1..$2.
// The default style is ?..?.
func Incrementing() ConfigFunc {
	return func(c *Config) {
		c.Incrementing = true
	}
}

// EscapeChar is a rune which column and table
// names will be escaped with. PostgreSQL and SQLite
// both accept double quotes "" while MariaDB/MySQL
// only accept backticks.
func EscapeChar(r rune) ConfigFunc {
	return func(c *Config) {
		c.EscapeChar = r
	}
}

// Coerce accepts a map of column names that
// will be cast explicitly into the desired type.
func Coerce(pairs ...CoercePair) ConfigFunc {
	return func(c *Config) {
		c.CoerceMap = map[string]qsqlio.CoerceFunc{}
		for _, pair := range pairs {
			c.CoerceMap[pair.Column] = coerceFunc(pair.Type)
		}
	}
}

// Precision sets the precision float64 types will
// be rounded to when read from SQL.
func Precision(i int) ConfigFunc {
	return func(c *Config) {
		c.Precision = i
	}
}

// Statement is a function that returns a raw SQL statement
// to be passed into a sql.Tx function to generate a prepared
// statement prior to writing data into the database.
func Statement(fn StatementFunc) ConfigFunc {
	return func(c *Config) {
		c.Statement = fn
	}
}

// RawStatement is a convience function to return a raw query
// for generating a prepared statement.
func RawStatement(stmt string) StatementFunc {
	return func([]string, Config) string {
		return stmt
	}
}
