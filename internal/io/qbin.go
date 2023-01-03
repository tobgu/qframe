package io

import (
	"errors"
	"fmt"
	qfbinary "github.com/tobgu/qframe/internal/binary"
	"github.com/tobgu/qframe/internal/column"
	"github.com/tobgu/qframe/internal/fcolumn"
	"github.com/tobgu/qframe/internal/icolumn"
	"github.com/tobgu/qframe/internal/index"
	"github.com/tobgu/qframe/internal/ncolumn"
	"github.com/tobgu/qframe/internal/scolumn"
	"io"
	"math"
)

type QBinColumn struct {
	Name   string
	Column column.Column
}

type QBinFrame struct {
	Index   index.Int
	Columns []QBinColumn
}

const magicNumber = uint32(0xfa570001) // Last two bytes could be used for version incompatibility detection

type qbinColumnType uint8

const (
	qbinColumnTypeInteger qbinColumnType = iota
	qbinColumnTypeString
	qbinColumnTypeFloat
	qbinColumnTypeUndefined
)

func ReadQBin(r io.Reader) (*QBinFrame, error) {
	readMagic, err := qfbinary.Read[uint32](r)
	if err != nil {
		return nil, fmt.Errorf("error reading magic number: %w", err)
	}

	if readMagic != magicNumber {
		return nil, fmt.Errorf("error, invalid magic number, expected %x, was %x", magicNumber, readMagic)
	}

	ix, err := index.ReadIntIxFromQBin(r)
	if err != nil {
		return nil, fmt.Errorf("error reading index: %w", err)
	}

	columns, err := readQBinColumns(r)
	if err != nil {
		return nil, fmt.Errorf("error reading columns: %w", err)
	}

	// Sanity check, should be at the end of the reader
	trailingBytes := make([]byte, 1)
	n, err := r.Read(trailingBytes)
	if n > 0 {
		return nil, fmt.Errorf("unexpected trailing bytes while reading QBin")
	}

	if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("unexpected error while checking trailing bytes, expected EOF, was: %w", err)
	}

	return &QBinFrame{
		Index:   ix,
		Columns: columns,
	}, nil
}

func readQBinColumns(r io.Reader) ([]QBinColumn, error) {
	columnCount, err := qfbinary.Read[uint32](r)
	if err != nil {
		return nil, fmt.Errorf("error reading column count: %w", err)
	}

	result := make([]QBinColumn, columnCount)
	for i := range result {
		nameLen, err := qfbinary.Read[uint16](r)
		if err != nil {
			return nil, fmt.Errorf("error reading column name length: %w", err)
		}

		nameBytes := make([]byte, nameLen)
		_, err = io.ReadFull(r, nameBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading column name: %w", err)
		}
		name := string(nameBytes)

		columnType, err := qfbinary.Read[qbinColumnType](r)
		if err != nil {
			return nil, fmt.Errorf("error reading column type: %w", err)
		}

		var col column.Column
		switch columnType {
		case qbinColumnTypeInteger:
			col, err = icolumn.ReadQBin(r)
		case qbinColumnTypeString:
			col, err = scolumn.ReadQBin(r)
		case qbinColumnTypeFloat:
			col, err = fcolumn.ReadQBin(r)
		case qbinColumnTypeUndefined:
			col, err = ncolumn.ReadQBin(r)
		default:
			return nil, fmt.Errorf("unexpected column type: %d", col)
		}

		if err != nil {
			return nil, fmt.Errorf("error reading data for column %s: %w", name, err)
		}

		result[i] = QBinColumn{
			Name:   name,
			Column: col,
		}
	}

	return result, nil
}

func writeQBinColumns(cols []QBinColumn, w io.Writer) error {
	if len(cols) > math.MaxUint32 {
		// Unlikely, but why not...
		return fmt.Errorf("too many columns in qframe to Qbin encode")
	}

	err := qfbinary.Write(w, uint32(len(cols)))
	if err != nil {
		return fmt.Errorf("error writing column count: %w", err)
	}

	for _, col := range cols {
		// Nothing fancy here to avoid allocations atm. Could perhaps be worth it for very
		// wide and very short qframes but no such use cases for me atm.
		nameBytes := []byte(col.Name)
		if len(nameBytes) > math.MaxUint16 {
			return fmt.Errorf("too long column name %s, cannot QBin encode", col.Name)
		}
		err = qfbinary.Write(w, uint16(len(nameBytes)))
		if err != nil {
			return fmt.Errorf("error writing column name length: %w", err)
		}

		_, err = w.Write(nameBytes)
		if err != nil {
			return fmt.Errorf("error writing column name '%s': %w", col.Name, err)
		}

		var columnType qbinColumnType
		switch t := col.Column.(type) {
		case icolumn.Column:
			columnType = qbinColumnTypeInteger
		case scolumn.Column:
			columnType = qbinColumnTypeString
		case fcolumn.Column:
			columnType = qbinColumnTypeFloat
		case ncolumn.Column:
			columnType = qbinColumnTypeUndefined
		default:
			return fmt.Errorf("unexpected column type: %s", t.DataType())
		}

		err := qfbinary.Write[qbinColumnType](w, columnType)
		if err != nil {
			return fmt.Errorf("error writing column type: %w", err)
		}

		err = col.Column.ToQBin(w)
		if err != nil {
			return fmt.Errorf("error writing column data for column %s: %w", col.Name, err)
		}
	}

	return nil
}

func WriteQBin(qbf QBinFrame, w io.Writer) error {
	err := qfbinary.Write(w, magicNumber)
	if err != nil {
		return err
	}

	err = qbf.Index.ToQBin(w)
	if err != nil {
		return err
	}

	return writeQBinColumns(qbf.Columns, w)
}
