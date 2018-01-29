package qframe

import "github.com/tobgu/qframe/errors"

type Select []interface{}

func (s Select) Select(qf QFrame) QFrame {
	if s == nil {
		// Equivalent of select *
		return qf
	}

	columns := make([]string, len(s))
	for i, item := range s {
		column, ok := item.(string)
		if !ok {
			qf.Err = errors.New("Select", "unknown operation in select: %v", item)
		}
		columns[i] = column
	}

	return qf.Select(columns...)
}
