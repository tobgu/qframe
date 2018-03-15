package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/tobgu/qframe/errors"
	bgenerator "github.com/tobgu/qframe/internal/bcolumn/generator"
	egenerator "github.com/tobgu/qframe/internal/ecolumn/generator"
	fgenerator "github.com/tobgu/qframe/internal/fcolumn/generator"
	igenerator "github.com/tobgu/qframe/internal/icolumn/generator"
	"go/format"
	"os"
)

/*
Simple code generator used in various places to reduce code duplication
*/

func main() {
	dstFile := flag.String("dst-file", "", "File that the code should be generated to")
	source := flag.String("source", "", "Which package that code should be generated for")
	flag.Parse()

	if *dstFile == "" {
		panic("Destination file must be given")
	}

	var fn func() (*bytes.Buffer, error)
	switch *source {
	case "ifilter":
		fn = igenerator.GenerateFilters
	case "ffilter":
		fn = fgenerator.GenerateFilters
	case "bfilter":
		fn = bgenerator.GenerateFilters
	case "efilter":
		fn = egenerator.GenerateFilters
	default:
		panic(fmt.Sprintf("Unknown source: \"%s\"", *source))
	}

	buf, err := fn()
	if err != nil {
		panic(err)
	}

	if err := writeFile(buf, *dstFile); err != nil {
		panic(err)
	}

	fmt.Println("Successfully wrote ", *dstFile)
}

func writeFile(buf *bytes.Buffer, file string) error {
	if file == "" {
		return errors.New("writeFile", "Output file must be specified")
	}

	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// The equivalent of "go fmt" before writing content
	src := buf.Bytes()
	fmtSrc, err := format.Source(src)
	if err != nil {
		os.Stdout.WriteString(string(src))
		return errors.Propagate("Format error", err)
	}

	_, err = f.Write(fmtSrc)
	return err
}
