package rolling

import "github.com/tobgu/qframe/qerrors"

// DataValue can be any of int/float/*string/bool, eg. any type that a column may take.
type DataValue = interface{}

// IntervalFunc is a function taking two parameters of the same DataValue and returning boolean stating if
// the two values are part of the same interval or not.
//
// For example, x and y within one unit from each other (with x assumed to be <= y):
type IntervalFunc = interface{}

// It should be considered a private implementation detail and should never be
// referenced or used directly outside of the QFrame code. To manipulate it
// use the functions returning ConfigFunc below.
type Config struct {
	PadValue        DataValue
	IntervalColName string
	IntervalFunc    IntervalFunc
	WindowSize      int
	Position        string // center/start/end
}

// ConfigFunc is a function that operates on a Config object.
type ConfigFunc func(c *Config)

func NewConfig(ff []ConfigFunc) (Config, error) {
	c := Config{
		WindowSize: 1,
		Position:   "center",
	}

	for _, fn := range ff {
		fn(&c)
	}

	if c.WindowSize <= 0 {
		return c, qerrors.New("Rolling config", "Window size must be positive, was %d", c.WindowSize)
	}

	if c.Position != "center" && c.Position != "start" && c.Position != "end" {
		return c, qerrors.New("Rolling config", "Position must be center/start/end, was %s", c.Position)
	}

	if c.IntervalFunc != nil && c.WindowSize != 1 {
		return c, qerrors.New("Rolling config", "Cannot set both interval function and window size")
	}

	return c, nil
}

// PadValue can be used to set the value to use in the beginning and/or end of the column to fill out any values
// where fewer than WindowSize values are available.
func PadValue(v DataValue) ConfigFunc {
	return func(c *Config) {
		c.PadValue = v
	}
}

// IntervalFunction can be used to set a dynamic interval based on the content of another column.
// QFrame will include all rows from the start row of the window until (but not including) the first row that is
// not part of the interval according to 'fn'. The first parameter passed to 'fn' is always the value at the start
// of the window.
//
// For example, lets say that you have a time series with millisecond resolution integer timestamps in column 'ts'
// and values in column 'value' that you would like to compute a rolling average over a minute for.
//
// In this case:
// col = "ts", fn = func(tsStart, tsEnd int) bool { return tsEnd < tsStart + int(time.Minute / time.Millisecond)}
func IntervalFunction(colName string, fn IntervalFunc) ConfigFunc {
	return func(c *Config) {
		c.IntervalColName = colName
		c.IntervalFunc = fn
	}
}

// WindowSize is used to set the size of the Window. By default this is 1.
func WindowSize(s int) ConfigFunc {
	return func(c *Config) {
		c.WindowSize = s
	}
}

// Position is used to set where in window the resulting value should be inserted.
// Valid values: start/center/end
// Default value: center
func Position(p string) ConfigFunc {
	return func(c *Config) {
		c.Position = p
	}
}
