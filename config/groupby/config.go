package groupby

// Config holds configuration for group by operations on QFrames.
// It should be considered a private implementation detail and should never be
// referenced or used directly outside of the QFrame code. To manipulate it
// use the functions returning ConfigFunc below.
type Config struct {
	Columns     []string
	GroupByNull bool
	// dropNulls?
}

// ConfigFunc is a function that operates on a Config object.
type ConfigFunc func(c *Config)

// NewConfig creates a new Config object.
// This function should never be called from outside QFrame.
func NewConfig(configFns []ConfigFunc) Config {
	var config Config
	for _, f := range configFns {
		f(&config)
	}

	return config
}

// Columns sets the columns by which the data should be grouped.
// Leaving this configuration option out will group on all columns in the QFrame.
//
// The order of columns does not matter from a functional point of view but
// it may impact execution time a bit. For optimal performance order columns
// according to type with the following priority:
// 1. int
// 2. float
// 3. enum/bool
// 4. string
func Columns(columns ...string) ConfigFunc {
	return func(c *Config) {
		c.Columns = columns
	}
}

// Null configures if Na/nulls should be grouped together or not.
// Default is false (eg. don't group null/NaN).
func Null(b bool) ConfigFunc {
	return func(c *Config) {
		c.GroupByNull = b
	}
}
