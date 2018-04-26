package eval

type Config struct {
	Ctx *Context
}

// ConfigFunc is a function that operates on a Config object.
type ConfigFunc func(*Config)

// NewConfig creates a new Config object.
// This function should never be called from outside QFrame.
func NewConfig(ff ...ConfigFunc) Config {
	result := Config{}
	for _, f := range ff {
		f(&result)
	}

	if result.Ctx == nil {
		result.Ctx = NewDefaultCtx()
	}

	return result
}

// EvalContext sets the evaluation context to use.
func EvalContext(ctx *Context) ConfigFunc {
	return func(c *Config) {
		c.Ctx = ctx
	}
}
