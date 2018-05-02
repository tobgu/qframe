package generator

import (
	"bytes"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/template"
)

//go:generate qfgenerate -source=sfilter -dst-file=../filters_gen.go

const basicColConstComparison = `
func {{.name}}(index index.Int, c Column, comparatee string, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := c.stringAt(index[i])
			bIndex[i] = !isNull && s {{.operator}} comparatee
		}
	}

	return nil
}
`

const basicColColComparison = `
func {{.name}}(index index.Int, col, col2 Column, bIndex index.Bool) error {
	for i, x := range bIndex {
		if !x {
			s, isNull := col.stringAt(index[i])
			s2, isNull2 := col2.stringAt(index[i])
			bIndex[i] = !isNull && !isNull2 && s {{.operator}} s2
		}
	}
	return nil
}
`

func spec(name, operator, templateStr string) template.Spec {
	return template.Spec{
		Name:     name,
		Template: templateStr,
		Values:   map[string]interface{}{"name": name, "operator": operator}}
}

func colConstComparison(name, operator string) template.Spec {
	return spec(name, operator, basicColConstComparison)
}

func colColComparison(name, operator string) template.Spec {
	return spec(name, operator, basicColColComparison)
}

func GenerateFilters() (*bytes.Buffer, error) {
	// If adding more filters here make sure to also add a reference to them
	// in the corresponding filter map so that they can be looked up.
	return template.GenerateFilters("scolumn", []template.Spec{
		colConstComparison("lt", filter.Lt),
		colConstComparison("lte", filter.Lte),
		colConstComparison("gt", filter.Gt),
		colConstComparison("gte", filter.Gte),
		colConstComparison("eq", "=="), // Go eq ("==") differs from qframe eq ("=")
		colColComparison("lt2", filter.Lt),
		colColComparison("lte2", filter.Lte),
		colColComparison("gt2", filter.Gt),
		colColComparison("gte2", filter.Gte),
		colColComparison("eq2", "=="), // Go eq ("==") differs from qframe eq ("=")
	})
}
