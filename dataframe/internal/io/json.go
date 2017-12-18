package io

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

//go:generate easyjson -in=$GOFILE -output_filename json_gen.go

type JsonRecords []map[string]interface{}

type JsonSeries map[string]json.RawMessage

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

func fillStrings(col []string, records JsonRecords, colName string) error {
	for i := range col {
		record := records[i]
		value, ok := record[colName]
		if !ok {
			return fmt.Errorf("wrong type for column %s, row %d", colName, i)
		}

		stringValue, ok := value.(string)
		if !ok {
			return fmt.Errorf("wrong type for column %s, row %d, expected int", colName, i)
		}
		col[i] = stringValue
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
		case string:
			col := make([]string, len(records))
			if err := fillStrings(col, records, colName); err != nil {
				return nil, err
			}
			result[colName] = col
		default:
			return nil, fmt.Errorf("unknown type of %s!", t)
		}
	}
	return result, nil
}

//easyjson:json
type JsonInt []int

//easyjson:json
type JsonFloat []float64

//easyjson:json
type JsonBool []bool

//easyjson:json
type JsonString []string

func UnmarshalJson(r io.Reader) (map[string]interface{}, error) {
	br := bufio.NewReader(r)
	bytes, err := br.Peek(1)
	if err != nil {
		return nil, err
	}

	if bytes[0] == []byte(`{`)[0] {
		var series JsonSeries
		decoder := json.NewDecoder(br)
		err = decoder.Decode(&series)
		if err != nil {
			return nil, err
		}

		result := make(map[string]interface{}, len(series))
		for colName, rawValue := range series {
			intDest := []int{}
			if err = json.Unmarshal(rawValue, &intDest); err == nil {
				result[colName] = intDest
				continue
			}

			floatDest := []float64{}
			if err = json.Unmarshal(rawValue, &floatDest); err == nil {
				result[colName] = floatDest
				continue
			}

			boolDest := []bool{}
			if err = json.Unmarshal(rawValue, &boolDest); err == nil {
				result[colName] = boolDest
				continue
			}

			strDest := []string{}
			if err = json.Unmarshal(rawValue, &strDest); err == nil {
				result[colName] = strDest
				continue
			}

			if err != nil {
				return nil, fmt.Errorf("json decoding could not find matching type for column %s", colName)
			}
		}

		return result, nil
	}

	if bytes[0] == []byte(`[`)[0] {
		var records JsonRecords
		decoder := json.NewDecoder(br)
		err = decoder.Decode(&records)
		if err != nil {
			return nil, err
		}

		return jsonRecordsToData(records)
	}

	return nil, fmt.Errorf("unrecognized start of JSON document: %v", bytes[0])
}
