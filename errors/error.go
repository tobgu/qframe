package errors

import "fmt"

type Error struct {
	source    error
	operation string
	reason    string
}

func (e Error) Error() string {
	return e.reason
}

func New(operation, reason string, params ...interface{}) Error {
	return Error{operation: operation, reason: fmt.Sprintf(reason, params...)}
}

func Propagate(operation string, err error) Error {
	return Error{operation: operation, source: err}
}
