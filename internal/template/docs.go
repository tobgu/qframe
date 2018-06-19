package template

import "bytes"

const DocTemplate = `
func Doc() string {
	return "\n Built in filters\n" +
{{ range $name := .filters }}"  {{$name}}\n" +
{{ end }}
"\n Built in aggregations\n" +
{{ range $name := .aggregations }}"  {{$name}}\n" +
{{ end }}"\n"
}
`

func GenerateDocs(pkgName string, filters, aggregations []string) (*bytes.Buffer, error) {
	values := map[string]interface{}{
		"filters":      filters,
		"aggregations": aggregations}

	return Generate(pkgName, []Spec{{Name: "filterdocs", Template: DocTemplate, Values: values}}, []string{})
}
