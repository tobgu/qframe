package io

import (
	"encoding/json"
	"fmt"
	"io"
)

type JsonRecords []map[string]interface{}

type JsonColumns map[string]json.RawMessage

func fillInts(col []int, records JsonRecords, colName string) error {
	for i := range col {
		record := records[i]
		value, ok := record[colName]
		if !ok {
			return fmt.Errorf("missing value for column %s, row %d", colName, i)
		}

		intValue, ok := value.(int)
		if !ok {
			return fmt.Errorf("wrong type for column %s, row %d, expected int", colName, i)
		}
		col[i] = intValue
	}

	return nil
}

func fillFloats(col []float64, records JsonRecords, colName string) error {
	for i := range col {
		record := records[i]
		value, ok := record[colName]
		if !ok {
			return fmt.Errorf("missing value for column %s, row %d", colName, i)
		}

		floatValue, ok := value.(float64)
		if !ok {
			return fmt.Errorf("wrong type for column %s, row %d, expected float", colName, i)
		}
		col[i] = floatValue
	}

	return nil
}

func fillBools(col []bool, records JsonRecords, colName string) error {
	for i := range col {
		record := records[i]
		value, ok := record[colName]
		if !ok {
			return fmt.Errorf("wrong type for column %s, row %d", colName, i)
		}

		boolValue, ok := value.(bool)
		if !ok {
			return fmt.Errorf("wrong type for column %s, row %d, expected bool", colName, i)
		}
		col[i] = boolValue
	}

	return nil
}

func fillStrings(col []*string, records JsonRecords, colName string) error {
	for i := range col {
		record := records[i]
		value, ok := record[colName]
		if !ok {
			return fmt.Errorf("wrong type for column %s, row %d", colName, i)
		}

		switch t := value.(type) {
		case string:
			col[i] = &t
		case nil:
			col[i] = nil
		default:
			return fmt.Errorf("wrong type for column %s, row %d, expected int", colName, i)
		}
	}

	return nil
}

func jsonRecordsToData(records JsonRecords) (map[string]interface{}, error) {
	result := map[string]interface{}{}
	if len(records) == 0 {
		return result, nil
	}

	r0 := records[0]
	for colName, value := range r0 {
		switch t := value.(type) {
		case int:
			col := make([]int, len(records))
			if err := fillInts(col, records, colName); err != nil {
				return nil, err
			}
			result[colName] = col
		case float64:
			col := make([]float64, len(records))
			if err := fillFloats(col, records, colName); err != nil {
				return nil, err
			}
			result[colName] = col
		case bool:
			col := make([]bool, len(records))
			if err := fillBools(col, records, colName); err != nil {
				return nil, err
			}
			result[colName] = col
		case nil, string:
			col := make([]*string, len(records))
			if err := fillStrings(col, records, colName); err != nil {
				return nil, err
			}
			result[colName] = col
		default:
			return nil, fmt.Errorf("unknown type of %s", t)
		}
	}
	return result, nil
}

// UnmarshalJson transforms JSON containing data records or columns into a map of columns
// that can be used to create a QFrame.
func UnmarshalJson(r io.Reader) (map[string]interface{}, error) {
	var records JsonRecords
	decoder := json.NewDecoder(r)
	err := decoder.Decode(&records)
	if err != nil {
		return nil, err
	}

	return jsonRecordsToData(records)
}
