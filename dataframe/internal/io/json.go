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

func jsonRecordsToData(r JsonRecords) (map[string]interface{}, error) {
	// TODO
	return map[string]interface{}{}, nil
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
		err = decoder.Decode(records)
		if err != nil {
			return nil, err
		}

		return jsonRecordsToData(records)
	}

	return nil, fmt.Errorf("unrecognized start of JSON document: %v", bytes[0])
}
