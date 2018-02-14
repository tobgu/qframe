package aggregation

type Aggregation struct {
	Fn     interface{}
	Column string
}

func New(fn interface{}, column string) Aggregation {
	return Aggregation{Fn: fn, Column: column}
}
