package io

import (
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/internal/ecolumn"
	"github.com/tobgu/qframe/internal/fastcsv"
	"github.com/tobgu/qframe/internal/strings"
	"github.com/tobgu/qframe/types"
	"io"
	"math"
)

// Helper type to slice column bytes into individual elements
type bytePointer struct {
	start uint32
	end   uint32
}

type CsvConfig struct {
	EmptyNull        bool
	IgnoreEmptyLines bool
	Delimiter        byte
	Types            map[string]types.DataType
	EnumVals         map[string][]string
}

func isEmptyLine(fields [][]byte) bool {
	return len(fields) == 1 && len(fields[0]) == 0
}

func ReadCsv(reader io.Reader, conf CsvConfig) (map[string]interface{}, []string, error) {
	r := fastcsv.NewReader(reader, conf.Delimiter)
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

	row := 1
	for r.Next() {
		if r.Err() != nil {
			return nil, nil, r.Err()
		}

		row++
		fields := r.Fields()
		if len(fields) != len(headers) {
			if isEmptyLine(fields) && conf.IgnoreEmptyLines {
				continue
			}

			return nil, nil, errors.New("ReadCsv", "Wrong number of columns on line %d, expected %d, was %d",
				row, len(headers), len(fields))
		}

		if isEmptyLine(fields) && conf.IgnoreEmptyLines {
			continue
		}

		for i, col := range fields {
			start := len(colBytes[i])
			colBytes[i] = append(colBytes[i], col...)
			colPointers[i] = append(colPointers[i], bytePointer{start: uint32(start), end: uint32(len(colBytes[i]))})
		}
	}

	dataMap := make(map[string]interface{}, len(headers))
	for i, header := range headers {
		data, err := columnToData(colBytes[i], colPointers[i], header, conf)
		if err != nil {
			return nil, nil, err
		}

		dataMap[header] = data
	}

	if len(conf.EnumVals) > 0 {
		return nil, nil, errors.New("Read csv", "Enum values specified for non enum column")
	}

	return dataMap, headers, nil
}

// Convert bytes to data columns, try, in turn int, float, bool and last string.
func columnToData(bytes []byte, pointers []bytePointer, colName string, conf CsvConfig) (interface{}, error) {
	var err error
	dataType := conf.Types[colName]

	if dataType == types.Int || dataType == types.None {
		intData := make([]int, 0, len(pointers))
		for _, p := range pointers {
			x, intErr := strings.ParseInt(bytes[p.start:p.end])
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

			x, floatErr := strings.ParseFloat(bytes[p.start:p.end])
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
			x, boolErr := strings.ParseBool(bytes[p.start:p.end])
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
		stringPointers := make([]strings.Pointer, len(pointers))
		for i, p := range pointers {
			if p.start == p.end && conf.EmptyNull {
				stringPointers[i] = strings.NewPointer(int(p.start), 0, true)
			} else {
				stringPointers[i] = strings.NewPointer(int(p.start), int(p.end-p.start), false)
			}
		}

		return strings.StringBlob{Pointers: stringPointers, Data: bytes}, nil
	}

	if dataType == types.Enum {
		values := conf.EnumVals[colName]
		delete(conf.EnumVals, colName)
		factory, err := ecolumn.NewFactory(values, len(pointers))
		if err != nil {
			return nil, err
		}

		for _, p := range pointers {
			if p.start == p.end && conf.EmptyNull {
				factory.AppendNil()
			} else {
				err := factory.AppendByteString(bytes[p.start:p.end])
				if err != nil {
					return nil, errors.Propagate("Create column", err)
				}
			}
		}

		return factory.ToColumn(), nil
	}

	return nil, errors.New("Create column", "unknown data type: %s", dataType)
}
