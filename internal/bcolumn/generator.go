package bcolumn

import (
	"bytes"

	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qframe/internal/maps"
	"github.com/tobgu/qframe/internal/template"
)

//go:generate qfgenerate -source=bfilter -dst-file=filters_gen.go
//go:generate qfgenerate -source=bdoc -dst-file=doc_gen.go

func spec(name, operator, templateStr string) template.Spec {
	return template.Spec{
		Name:     name,
		Template: templateStr,
		Values:   map[string]interface{}{"name": name, "dataType": "bool", "operator": operator}}
}

func colConstComparison(name, operator string) template.Spec {
	return spec(name, operator, template.BasicColConstComparison)
}

func colColComparison(name, operator string) template.Spec {
	return spec(name, operator, template.BasicColColComparison)
}

func GenerateFilters() (*bytes.Buffer, error) {
	// If adding more filters here make sure to also add a reference to them
	// in the corresponding filter map so that they can be looked up.
	return template.GenerateFilters("bcolumn", []template.Spec{
		colConstComparison("eq", "=="), // Go eq ("==") differs from qframe eq ("=")
		colConstComparison("neq", filter.Neq),
		colColComparison("eq2", "=="), // Go eq ("==") differs from qframe eq ("=")
		colColComparison("neq2", filter.Neq),
	})
}

func GenerateDoc() (*bytes.Buffer, error) {
	return template.GenerateDocs(
		"bcolumn",
		"Boolean",
		maps.StringKeys(filterFuncs, filterFuncs2),
		maps.StringKeys(aggregations))
}
