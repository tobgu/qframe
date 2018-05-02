package template

import (
	"bytes"
)

const BasicColConstComparison = `
func {{.name}}(index index.Int, column []{{.dataType}}, comp {{.dataType}}, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = column[index[i]] {{.operator}} comp
		}
	}
}
`

const BasicColColComparison = `
func {{.name}}(index index.Int, column []{{.dataType}}, compCol []{{.dataType}}, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			pos := index[i]
			bIndex[i] = column[pos] {{.operator}} compCol[pos]
		}
	}
}
`

func GenerateFilters(pkgName string, specs []Spec) (*bytes.Buffer, error) {
	return Generate(pkgName, specs, []string{"github.com/tobgu/qframe/internal/index"})
}
