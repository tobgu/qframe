package template

import (
	"bytes"
	"io"
	"text/template"
)

// TODO: How do we want to handle any additional imports?
const HeaderTemplate = `
package {{.pkgName}}

import (
	"github.com/tobgu/qframe/internal/index"
)

// Code generated from template/filters.go DO NOT EDIT

`

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
			bIndex[i] = column[pos] > compCol[pos]
		}
	}
}
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

func Generate(pkgName string, specs []Spec) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	renderValues := append([]Spec{{Name: "header", Template: HeaderTemplate, Values: map[string]interface{}{"pkgName": pkgName}}}, specs...)
	for _, v := range renderValues {
		if err := render(v.Name, v.Template, v.Values, &buf); err != nil {
			return nil, err
		}
	}

	return &buf, nil
}
