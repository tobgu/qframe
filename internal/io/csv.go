package io

import (
	"fmt"
	"io"
	"math"

	"github.com/tobgu/qframe/internal/ecolumn"
	"github.com/tobgu/qframe/internal/fastcsv"
	"github.com/tobgu/qframe/internal/ncolumn"
	"github.com/tobgu/qframe/internal/strings"
	"github.com/tobgu/qframe/qerrors"
	"github.com/tobgu/qframe/types"
)

// Helper type to slice column bytes into individual elements
type bytePointer struct {
	start uint32
	end   uint32
}

// For reading  CSV
type CSVConfig struct {
	EmptyNull              bool
	IgnoreEmptyLines       bool
	Delimiter              byte
	Types                  map[string]types.DataType
	EnumVals               map[string][]string
	RowCountHint           int
	Headers                []string
	RenameDuplicateColumns bool
	MissingColumnNameAlias string
}

// For writing CSV
type ToCsvConfig struct {
	Header  bool
	Columns []string
}

func isEmptyLine(fields [][]byte) bool {
	return len(fields) == 1 && len(fields[0]) == 0
}

func ReadCSV(reader io.Reader, conf CSVConfig) (map[string]interface{}, []string, error) {
	r := fastcsv.NewReader(reader, conf.Delimiter)
	headers := conf.Headers
	if len(headers) == 0 {
		byteHeader, err := r.Read()
		if err != nil {
			return nil, nil, qerrors.Propagate("ReadCSV read header", err)
		}

		headers = make([]string, len(byteHeader))
		for i := range headers {
			headers[i] = string(byteHeader[i])
		}
	}

	colPointers := make([][]bytePointer, len(headers))
	for i := range headers {
		colPointers[i] = []bytePointer{}
	}

	// All bytes in a column
	colBytes := make([][]byte, len(headers))

	row := 1
	nonEmptyRows := 0
	for r.Next() {
		if r.Err() != nil {
			return nil, nil, qerrors.Propagate("ReadCSV read body", r.Err())
		}

		row++
		fields := r.Fields()
		if len(fields) != len(headers) {
			if isEmptyLine(fields) && conf.IgnoreEmptyLines {
				continue
			}

			return nil, nil, qerrors.New("ReadCSV", "Wrong number of columns on line %d, expected %d, was %d",
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

		nonEmptyRows++
		if nonEmptyRows == 1000 && conf.RowCountHint > 2000 {
			// This is an optimization that can reduce allocations and copying if the number
			// of rows is provided. Not a huge impact but 5 - 10 % faster for big CSVs.
			resizeColBytes(colBytes, nonEmptyRows, conf.RowCountHint)
			resizeColPointers(colPointers, conf.RowCountHint)
		}
	}

	if conf.MissingColumnNameAlias != "" {
		headers = addAliasToMissingColumnNames(headers, conf.MissingColumnNameAlias)

	}

	if conf.RenameDuplicateColumns {
		headers = renameDuplicateColumns(headers)

	}

	dataMap := make(map[string]interface{}, len(headers))
	for i, header := range headers {
		data, err := columnToData(colBytes[i], colPointers[i], header, conf)
		if err != nil {
			return nil, nil, qerrors.Propagate("ReadCSV convert data", err)
		}

		dataMap[header] = data
	}

	if len(conf.EnumVals) > 0 {
		return nil, nil, qerrors.New("ReadCsv", "Enum values specified for non enum column")
	}

	if len(headers) > len(dataMap) {
		duplicates := make([]string, 0)
		headerSet := strings.NewEmptyStringSet()
		for _, h := range headers {
			if headerSet.Contains(h) {
				duplicates = append(duplicates, h)
			} else {
				headerSet.Add(h)
			}
		}
		return nil, nil, qerrors.New("ReadCsv", "Duplicate columns detected: %v", duplicates)
	}
	return dataMap, headers, nil
}

func resizeColPointers(pointers [][]bytePointer, sizeHint int) {
	for i, p := range pointers {
		if cap(p) < sizeHint {
			newP := make([]bytePointer, 0, sizeHint)
			newP = append(newP, p...)
			pointers[i] = newP
		}
	}
}

func resizeColBytes(bytes [][]byte, currentRowCount, sizeHint int) {
	for i, b := range bytes {
		// Estimate final size by using current size + 20%
		estimatedCap := int(1.2 * float64(len(b)) * (float64(sizeHint) / float64(currentRowCount)))
		if cap(b) < estimatedCap {
			newB := make([]byte, 0, estimatedCap)
			newB = append(newB, b...)
			bytes[i] = newB
		}
	}
}

func renameDuplicateColumns(headers []string) []string {
	headersMap := make(map[string]int)
	// loop through column names and add the index of first occurrence to the  headersMap
	// any occurrence after first is considered duplicate.
	for i, h := range headers {
		_, ok := headersMap[h]
		if !ok {
			headersMap[h] = i
		}
	}
	// iterate through all column names and rename the duplicates with candidateName
	for i, h := range headers {
		index, ok := headersMap[h]
		if ok && i != index {
			counter := 0
			for {
				candidateName := headers[i] + fmt.Sprint(counter)
				_, ok = headersMap[candidateName]
				if ok {
					counter++
				} else {
					headers[i] = candidateName
					headersMap[headers[i]] = i
					break
				}
			}
		}
	}
	return headers

}

// Handle Missing Columnnames
func addAliasToMissingColumnNames(headers []string, alias string) []string {
	for i, name := range headers {
		if name == "" {
			headers[i] = alias
		}
	}
	return headers
}

// Convert bytes to data columns, try, in turn int, float, bool and last string.
func columnToData(bytes []byte, pointers []bytePointer, colName string, conf CSVConfig) (interface{}, error) {
	var err error
	dataType := conf.Types[colName]

	if len(pointers) == 0 && dataType == types.None {
		return ncolumn.Column{}, nil
	}

	if dataType == types.Int || dataType == types.None {
		intData := make([]int, 0, len(pointers))
		for _, p := range pointers {
			x, intErr := strings.ParseInt(bytes[p.start:p.end])
			if intErr != nil {
				err = intErr
				break
			}
			intData = append(intData, x)
		}

		if err == nil {
			return intData, nil
		}

		if dataType == types.Int {
			return nil, qerrors.Propagate("Create int column", err)
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
			return nil, qerrors.Propagate("Create float column", err)
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
			return nil, qerrors.Propagate("Create bool column", err)
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
					return nil, qerrors.Propagate("Create column", err)
				}
			}
		}

		return factory.ToColumn(), nil
	}

	return nil, qerrors.New("Create column", "unknown data type: %s", dataType)
}
