package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"os"

	"github.com/tobgu/qframe/qerrors"
	bgenerator "github.com/tobgu/qframe/internal/bcolumn"
	egenerator "github.com/tobgu/qframe/internal/ecolumn"
	fgenerator "github.com/tobgu/qframe/internal/fcolumn"
	igenerator "github.com/tobgu/qframe/internal/icolumn"
	qfgenerator "github.com/tobgu/qframe/internal/qframe/generator"
	sgenerator "github.com/tobgu/qframe/internal/scolumn"
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

	generators := map[string]func() (*bytes.Buffer, error){
		"idoc":    igenerator.GenerateDoc,
		"ifilter": igenerator.GenerateFilters,
		"fdoc":    fgenerator.GenerateDoc,
		"ffilter": fgenerator.GenerateFilters,
		"bdoc":    bgenerator.GenerateDoc,
		"bfilter": bgenerator.GenerateFilters,
		"edoc":    egenerator.GenerateDoc,
		"efilter": egenerator.GenerateFilters,
		"sdoc":    sgenerator.GenerateDoc,
		"sfilter": sgenerator.GenerateFilters,
		"qframe":  qfgenerator.GenerateQFrame,
	}

	generator, ok := generators[*source]
	if !ok {
		panic(fmt.Sprintf("Unknown source: \"%s\"", *source))
	}

	buf, err := generator()
	if err != nil {
		panic(err)
	}

	if err := writeFile(buf, *dstFile); err != nil {
		panic(err)
	}
}

func writeFile(buf *bytes.Buffer, file string) error {
	if file == "" {
		return qerrors.New("writeFile", "Output file must be specified")
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
		return qerrors.Propagate("Format error", err)
	}

	_, err = f.Write(fmtSrc)
	return err
}
