package qcache

import (
	"encoding/json"
	"fmt"
	"github.com/kniren/gota/dataframe"
)

type Clause []interface{}

type Query struct {
	Select   Clause   `json:"select,omitempty"`
	Where    Clause   `json:"where,omitempty"`
	OrderBy  []string `json:"order_by,omitempty"`
	GroupBy  []string `json:"group_by,omitempty"`
	Distinct []string `json:"distinct,omitempty"`
	Offset   int      `json:"offset,omitempty"`
	Limit    int      `json:"limit,omitempty"`
	From     *Query   `json:"from,omitempty"`
}

type QFrame struct {
	dataFrame *dataframe.DataFrame
}

func (c *Clause) strings() ([]string, error) {
	result := make([]string, 0, len(*c))
	for _, p := range *c {
		s, ok := p.(string)
		if !ok {
			return nil, fmt.Errorf("%v was not a string", p)
		}

		result = append(result, s)
	}

	return result, nil
}

func (f *QFrame) Query(qString string) (*QFrame, error) {
	query := Query{}
	err := json.Unmarshal([]byte(qString), &query)
	if err != nil {
		return nil, err
	}

//	columns, err := query.Select.strings()
//	if err != nil {
//		return nil, err
//	}

	newDf := *f.dataFrame
//	if len(columns) > 0 {
//		newDf = f.dataFrame.Select(columns...)
//	}

	if newDf.Err != nil {
		return nil, newDf.Err
	}

	return &QFrame{&newDf}, nil
}
