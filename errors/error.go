package errors

import "fmt"

// Error holds data identifying an error that occurred
// while executing a qframe operation.
type Error struct {
	source    error
	operation string
	reason    string
}

// Error returns a string representation of the error.
func (e Error) Error() string {
	result := e.operation
	if e.reason != "" {
		result += ": " + e.reason
	}

	if e.source != nil {
		result += fmt.Sprintf(" (%s)", e.source)
	}

	return result
}

// New creates a new error instance.
func New(operation, reason string, params ...interface{}) Error {
	return Error{operation: operation, reason: fmt.Sprintf(reason, params...)}
}

// Propagate propagates an existing error with added context.
func Propagate(operation string, err error) Error {
	return Error{operation: operation, source: err}
}

// Error types:
//   - Type error
//   - Input error (which would basically always be the case...)
