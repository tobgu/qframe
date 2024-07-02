package csv

import (
	qfio "github.com/tobgu/qframe/internal/io"
	"github.com/tobgu/qframe/types"
)

// Config holds configuration for reading CSV files into QFrames.
// It should be considered a private implementation detail and should never be
// referenced or used directly outside of the QFrame code. To manipulate it
// use the functions returning ConfigFunc below.
type Config qfio.CSVConfig

// ConfigFunc is a function that operates on a Config object.
type ConfigFunc func(*Config)

// NewConfig creates a new Config object.
// This function should never be called from outside QFrame.
func NewConfig(ff []ConfigFunc) Config {
	conf := Config{Delimiter: ','}
	for _, f := range ff {
		f(&conf)
	}
	return conf
}

// EmptyNull configures if empty strings should be considered as empty strings (default) or null.
//
// emptyNull - If set to true empty string will be translated to null.
func EmptyNull(emptyNull bool) ConfigFunc {
	return func(c *Config) {
		c.EmptyNull = emptyNull
	}
}

// MissingColumnNameAlias sets the name to be used for empty columns name with given string
func MissingColumnNameAlias(MissingColumnNameAlias string) ConfigFunc {
	return func(c *Config) {
		c.MissingColumnNameAlias = MissingColumnNameAlias
	}
}

// RenameDuplicateColumns configures if duplicate column names should have the column index appended to the column name to resolve the conflict.
func RenameDuplicateColumns(RenameDuplicateColumns bool) ConfigFunc {
	return func(c *Config) {
		c.RenameDuplicateColumns = RenameDuplicateColumns
	}
}

// IgnoreEmptyLines configures if a line without any characters should be ignored or interpreted
// as a zero length string.
//
// IgnoreEmptyLines - If set to true empty lines will not produce any data.
func IgnoreEmptyLines(ignoreEmptyLines bool) ConfigFunc {
	return func(c *Config) {
		c.IgnoreEmptyLines = ignoreEmptyLines
	}
}

// Delimiter configures the delimiter/separator between columns.
// Only byte representable delimiters are supported. Default is ','.
//
// delimiter - The delimiter to use.
func Delimiter(delimiter byte) ConfigFunc {
	return func(c *Config) {
		c.Delimiter = delimiter
	}
}

// Types is used set types for certain columns.
// If types are not given a best effort attempt will be done to auto detected the type.
//
// typs - map column name -> type name. For a list of type names see package qframe/types.
func Types(typs map[string]string) ConfigFunc {
	return func(c *Config) {
		c.Types = make(map[string]types.DataType, len(typs))
		for k, v := range typs {
			c.Types[k] = types.DataType(v)
		}
	}
}

// EnumValues is used to list the possible values and internal order of these values for an enum column.
//
// values - map column name -> list of valid values.
//
// Enum columns that do not specify the values are automatically assigned values based on the content
// of the column. The ordering between these values is undefined. It hence doesn't make much sense to
// sort a QFrame on an enum column unless the ordering has been specified.
//
// Note that the column must be listed as having an enum type (using Types above) for this option to take effect.
func EnumValues(values map[string][]string) ConfigFunc {
	return func(c *Config) {
		c.EnumVals = make(map[string][]string)
		for k, v := range values {
			c.EnumVals[k] = v
		}
	}
}

// RowCountHint can be used to provide an indication of the number of rows
// in the CSV. In some cases this will help allocating buffers more efficiently
// and improve import times.
//
// rowCount - The number of rows.
func RowCountHint(rowCount int) ConfigFunc {
	return func(c *Config) {
		c.RowCountHint = rowCount
	}
}

// Headers can be used to specify the header names for a CSV file without header.
//
// header - Slice with column names.
func Headers(headers []string) ConfigFunc {
	return func(c *Config) {
		c.Headers = headers
	}
}

// ToConfig holds configuration for writing CSV files
type ToConfig qfio.ToCsvConfig

// ToConfigFunc is a function that operates on a ToConfig object.
type ToConfigFunc func(*ToConfig)

// NewConfig creates a new ToConfig object.
// This function should never be called from outside QFrame.
func NewToConfig(ff []ToConfigFunc) ToConfig {
	conf := ToConfig{Header: true} //Default
	for _, f := range ff {
		f(&conf)
	}
	return conf
}

// Header indicates whether or not the CSV file should be written with a header.
// Default is true.
func Header(header bool) ToConfigFunc {
	return func(c *ToConfig) {
		c.Header = header
	}
}

// Columns holds the order to write CSV columns.
func Columns(cols []string) ToConfigFunc {
	return func(c *ToConfig) {
		c.Columns = cols
	}
}
