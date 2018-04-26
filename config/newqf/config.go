package newqf

// Config holds configuration for creating new QFrames using the New constructor.
// It should be considered a private implementation detail and should never be
// referenced or used directly outside of the QFrame code. To manipulate it
// use the functions returning ConfigFunc below.
type Config struct {
	ColumnOrder []string
	EnumColumns map[string][]string
}

// ConfigFunc is a function that operates on a Config object.
type ConfigFunc func(c *Config)

// NewConfig creates a new Config object.
// This function should never be called from outside QFrame.
func NewConfig(fns []ConfigFunc) *Config {
	// TODO: This function returns a pointer while most of the other returns values. Decide which way to do it.
	config := &Config{}
	for _, fn := range fns {
		fn(config)
	}
	return config
}

// ColumnOrder provides the order in which columns are displayed, etc.
func ColumnOrder(columns ...string) ConfigFunc {
	return func(c *Config) {
		c.ColumnOrder = make([]string, len(columns))
		copy(c.ColumnOrder, columns)
	}
}

// Enums lists columns that should be considered enums.
// The map key specifies the columns name, the value if there is a fixed set of
// values and their internal ordering. If value is nil or empty list the values
// will be derived from the columns content and the ordering unspecified.
func Enums(columns map[string][]string) ConfigFunc {
	return func(c *Config) {
		c.EnumColumns = make(map[string][]string)
		for k, v := range columns {
			c.EnumColumns[k] = v
		}
	}
}
