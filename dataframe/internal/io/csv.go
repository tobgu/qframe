package io

import (
	"bitbucket.org/weberc2/fastcsv"
	"io"
)

// Helper type to slice column bytes into individual elements
type bytePointer struct {
	start uint32
	end   uint32
}

// TODO: Take type map
func FromCsv(reader io.Reader) (map[string]interface{}, error) {
	r := csv.NewReader(reader)
	byteHeader, err := r.Read()
	if err != nil {
		return nil, err
	}

	headers := make([]string, len(byteHeader))
	colPointers := make([][]bytePointer, len(headers))
	for i := range headers {
		headers[i] = string(byteHeader[i])
		colPointers[i] = []bytePointer{}
	}

	// All bytes in a column
	colBytes := make([][]byte, len(headers))

	for r.Next() {
		// TODO: What happens when the number of columns differ from number of
		//       headers. When the number of columns is zero?
		if r.Err() != nil {
			return nil, r.Err()
		}

		for i, col := range r.Fields() {
			start := len(colBytes[i])
			colBytes[i] = append(colBytes[i], col...)
			colPointers[i] = append(colPointers[i], bytePointer{start: uint32(start), end: uint32(len(colBytes[i]))})
		}
	}

	// TODO: Perhaps series should be a slice instead with a map indexing into it...
	dataMap := make(map[string]interface{}, len(headers))
	for i, header := range headers {
		data, err := columnToData(colBytes[i], colPointers[i])
		if err != nil {
			return nil, err
		}

		dataMap[header] = data
	}

	return dataMap, nil
}

// Convert bytes to data columns, try, in turn int, float, bool and last string.
func columnToData(bytes []byte, pointers []bytePointer) (interface{}, error) {
	// TODO: Take type hint and err if type cannot be applied

	// Int
	intData := make([]int, 0, len(pointers))
	var err error
	for _, p := range pointers {
		x, intErr := parseInt(bytes[p.start:p.end])
		if intErr != nil {
			err = intErr
			break
		}
		intData = append(intData, int(x))
	}

	if err == nil {
		return intData, nil
	}

	// Float
	err = nil
	floatData := make([]float64, 0, len(pointers))
	for _, p := range pointers {
		x, floatErr := parseFloat(bytes[p.start:p.end])
		if floatErr != nil {
			err = floatErr
			break
		}
		floatData = append(floatData, x)
	}

	if err == nil {
		return floatData, nil
	}

	// Bool
	err = nil
	boolData := make([]bool, 0, len(pointers))
	for _, p := range pointers {
		x, boolErr := parseBool(bytes[p.start:p.end])
		if boolErr != nil {
			err = boolErr
			break
		}
		boolData = append(boolData, x)
	}

	if err == nil {
		return boolData, nil
	}

	// String
	stringData := make([]string, 0, len(pointers))
	for _, p := range pointers {
		stringData = append(stringData, string(bytes[p.start:p.end]))
	}

	// TODO: Might want some sort of categorial like here for low cardinality strings,
	//       could be achieved with a map caching strings.
	/*
		stringData := make([]string, 0, len(pointers))
		strings := map[string]string{}
		for _, p := range pointers {
			b := bytes[p.start:p.end]
			s, ok := strings[string(b)]
			if !ok {
				s = string(b)
				strings[s] = s
			}
			stringData = append(stringData, s)
		}
	*/

	return stringData, nil
}
