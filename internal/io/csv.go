package io

import (
	"bitbucket.org/weberc2/fastcsv"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/eseries"
	"github.com/tobgu/qframe/types"
	"io"
	"math"
)

// Helper type to slice column bytes into individual elements
type bytePointer struct {
	start uint32
	end   uint32
}

// TODO: Take type map
func ReadCsv(reader io.Reader, emptyNull bool, types map[string]types.DataType) (map[string]interface{}, []string, error) {
	r := csv.NewReader(reader)
	byteHeader, err := r.Read()
	if err != nil {
		return nil, nil, err
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
			return nil, nil, r.Err()
		}

		for i, col := range r.Fields() {
			start := len(colBytes[i])
			colBytes[i] = append(colBytes[i], col...)
			colPointers[i] = append(colPointers[i], bytePointer{start: uint32(start), end: uint32(len(colBytes[i]))})
		}
	}

	dataMap := make(map[string]interface{}, len(headers))
	for i, header := range headers {
		data, err := columnToData(colBytes[i], colPointers[i], emptyNull, types[header])
		if err != nil {
			return nil, nil, err
		}

		dataMap[header] = data
	}

	return dataMap, headers, nil
}

// Convert bytes to data columns, try, in turn int, float, bool and last string.
func columnToData(bytes []byte, pointers []bytePointer, emptyNull bool, dataType types.DataType) (interface{}, error) {
	var err error

	if dataType == types.Int || dataType == types.None {
		intData := make([]int, 0, len(pointers))
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

		if dataType == types.Int {
			return nil, errors.Propagate("Create int column", err)
		}
	}

	if dataType == types.Float || dataType == types.None {
		err = nil
		floatData := make([]float64, 0, len(pointers))
		for _, p := range pointers {
			if p.start == p.end {
				floatData = append(floatData, math.NaN())
				continue
			}

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

		if dataType == types.Float {
			return nil, errors.Propagate("Create float column", err)
		}
	}

	if dataType == types.Bool || dataType == types.None {
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

		if dataType == types.Bool {
			return nil, errors.Propagate("Create bool column", err)
		}
	}

	if dataType == types.String || dataType == types.None {
		stringMap := make(map[string]*string)
		stringData := make([]*string, 0, len(pointers))
		for _, p := range pointers {
			if p.start == p.end && emptyNull {
				stringData = append(stringData, nil)
			} else {
				// Reuse pointers to strings that have already occurred, good
				// way to save some memory and allocations for columns with
				// recurring strings. Less good for high cardinality columns.
				// If this is a problem the decision of whether to use it or
				// not could probably be made dynamic.
				if sp, ok := stringMap[string(bytes[p.start:p.end])]; ok {
					stringData = append(stringData, sp)
				} else {
					s := string(bytes[p.start:p.end])
					stringMap[s] = &s
					stringData = append(stringData, &s)
				}
			}
		}

		return stringData, nil
	}

	if dataType == types.Enum {
		factory, err := eseries.NewFactory(nil, len(pointers))
		if err != nil {
			return nil, err
		}

		for _, p := range pointers {
			if p.start == p.end && emptyNull {
				factory.AppendNil()
			} else {
				err := factory.AppendByteString(bytes[p.start:p.end])
				if err != nil {
					return nil, errors.Propagate("Create column", err)
				}
			}
		}

		return factory.ToSeries(), nil
	}

	return nil, errors.New("Create column", "unknown data type: %s", dataType)
}
