package template

import (
	"bytes"
	"io"
	"text/template"
)

const HeaderTemplate = `
package {{.pkgName}}

import (
{{ range $_, $imp := .imports }}
"{{$imp}}"{{ end }}
)

// Code generated from template/... DO NOT EDIT
`

type Spec struct {
	Name     string
	Template string
	Values   map[string]interface{}
}

func render(name, templateStr string, templateData interface{}, dst io.Writer) error {
	t := template.New(name)
	t, err := t.Parse(templateStr)
	if err != nil {
		return err
	}

	err = t.Execute(dst, templateData)
	if err != nil {
		return err
	}

	return nil
}

func Generate(pkgName string, specs []Spec, imports []string) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	values := map[string]interface{}{"pkgName": pkgName, "imports": imports}
	renderValues := append([]Spec{{Name: "header", Template: HeaderTemplate, Values: values}}, specs...)
	for _, v := range renderValues {
		if err := render(v.Name, v.Template, v.Values, &buf); err != nil {
			return nil, err
		}
	}

	return &buf, nil
}
