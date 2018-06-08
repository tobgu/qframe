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

func Query(query string) ConfigFunc {
	return func(c *Config) {
		c.Query = query
	}
}
