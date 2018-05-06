package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	r := csv.NewReader(os.Stdin)
	for {
		row, err := r.Read()
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
		for i := range row {
			row[i] = fmt.Sprintf("\"%s\"", row[i])
		}
		fmt.Fprintln(os.Stdout, strings.Join(row, ","))
	}
}
