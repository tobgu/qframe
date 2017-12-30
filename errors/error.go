package errors

import "fmt"

type Error struct {
	source    error
	operation string
	reason    string
}

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

func New(operation, reason string, params ...interface{}) Error {
	return Error{operation: operation, reason: fmt.Sprintf(reason, params...)}
}

func Propagate(operation string, err error) Error {
	return Error{operation: operation, source: err}
}
