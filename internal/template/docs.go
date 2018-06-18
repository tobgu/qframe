package template

import "bytes"

const DocTemplate = `
func Doc() string {
	return "{{.typeName}}\n" +
"=======\n" +
"\n  Built in filters\n" +
"  ----------------\n" +
{{ range $name := .filters }}"  {{$name}}\n" +
{{ end }}
"\n  Built in aggregations\n" +
"  ---------------------\n" +
{{ range $name := .aggregations }}"  {{$name}}\n" +
{{ end }}"\n"
}
`

func GenerateDocs(pkgName, typeName string, filters, aggregations []string) (*bytes.Buffer, error) {
	values := map[string]interface{}{
		"typeName":     typeName,
		"filters":      filters,
		"aggregations": aggregations}

	return Generate(pkgName, []Spec{{Name: "filterdocs", Template: DocTemplate, Values: values}}, []string{})
}
