package sql

import (
	qsqlio "github.com/tobgu/qframe/internal/io/sql"
)

type Config qsqlio.SQLConfig

type ConfigFunc func(*Config)

func NewConfig(ff []ConfigFunc) Config {
	conf := Config{}
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

// MariaDB configures the query builder to
// generate SQL that is compatible MariaDB
// and MySQL. See github.com/go-sql-driver/mysql
func MariaDB() ConfigFunc {
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
