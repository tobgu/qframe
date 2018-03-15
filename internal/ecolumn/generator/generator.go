package generator

import (
	"bytes"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/template"
)

//go:generate qfgenerate -source=efilter -dst-file=../filters_gen.go

const basicColConstComparison = `
func {{.name}}(index index.Int, column []enumVal, comparatee enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			enum := column[index[i]]
			bIndex[i] = enum.compVal() {{.operator}} comparatee.compVal()
		}
	}
}
`

const basicColColComparison = `
func {{.name}}(index index.Int, col, col2 []enumVal, bIndex index.Bool) {
	for i, x := range bIndex {
		if !x {
			bIndex[i] = col[index[i]].compVal() {{.operator}} col2[index[i]].compVal()
		}
	}
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
	// TODO: Decide how to handle NaN in comparisons.

	// If adding more filters here make sure to also add a reference to them
	// in the corresponding filter map so that they can be looked up.
	return template.Generate("ecolumn", []template.Spec{
		colConstComparison("lt", filter.Lt),
		colConstComparison("lte", filter.Lte),
		colConstComparison("gt", filter.Gt),
		colConstComparison("gte", filter.Gte),
		colConstComparison("eq", "=="), // Go eq ("==") differs from qframe eq ("=")
		colConstComparison("neq", filter.Neq),
		colColComparison("lt2", filter.Lt),
		colColComparison("lte2", filter.Lte),
		colColComparison("gt2", filter.Gt),
		colColComparison("gte2", filter.Gte),
		colColComparison("eq2", "=="), // Go eq ("==") differs from qframe eq ("=")
		colColComparison("neq2", filter.Neq),
	})
}
