package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

func main() {
	crdDir := "config/crd/bases"
	if len(os.Args) > 1 {
		crdDir = os.Args[1]
	}

	files, err := filepath.Glob(filepath.Join(crdDir, "*.yaml"))
	if err != nil {
		log.Fatal(err)
	}
	if len(files) == 0 {
		log.Fatalf("no CRD YAML files found in %s", crdDir)
	}
	sort.Strings(files)

	var crds []apiextensionsv1.CustomResourceDefinition
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatal(err)
		}
		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal(data, &crd); err != nil {
			log.Fatalf("parse %s: %v", file, err)
		}
		crds = append(crds, crd)
	}
	sort.Slice(crds, func(i, j int) bool {
		return crds[i].Spec.Names.Kind < crds[j].Spec.Names.Kind
	})

	var out bytes.Buffer
	fmt.Fprintln(&out, "# CRD Reference")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "Generated from `config/crd/bases/*.yaml`.")
	fmt.Fprintln(&out)

	for _, crd := range crds {
		renderCRD(&out, crd)
	}

	if _, err := os.Stdout.Write(out.Bytes()); err != nil {
		log.Fatal(err)
	}
}

func renderCRD(out *bytes.Buffer, crd apiextensionsv1.CustomResourceDefinition) {
	fmt.Fprintf(out, "## %s\n\n", crd.Spec.Names.Kind)
	fmt.Fprintf(out, "- API group: `%s`\n", crd.Spec.Group)
	fmt.Fprintf(out, "- Scope: `%s`\n", crd.Spec.Scope)
	fmt.Fprintf(out, "- Plural: `%s`\n", crd.Spec.Names.Plural)
	if len(crd.Spec.Names.ShortNames) > 0 {
		fmt.Fprintf(out, "- Short names: `%s`\n", strings.Join(crd.Spec.Names.ShortNames, "`, `"))
	}
	fmt.Fprintln(out)

	for _, version := range crd.Spec.Versions {
		if !version.Served || version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			continue
		}
		versionTags := []string{"served"}
		if version.Storage {
			versionTags = append(versionTags, "storage")
		}
		fmt.Fprintf(out, "### %s (%s)\n\n", version.Name, strings.Join(versionTags, ", "))
		renderSection(out, "Spec", "spec", schemaProperty(version.Schema.OpenAPIV3Schema, "spec"))
		renderSection(out, "Status", "status", schemaProperty(version.Schema.OpenAPIV3Schema, "status"))
	}
}

func renderSection(out *bytes.Buffer, title, root string, schema *apiextensionsv1.JSONSchemaProps) {
	if schema == nil {
		return
	}
	fmt.Fprintf(out, "#### %s\n\n", title)
	fmt.Fprintln(out, "| Field | Type | Required | Default | Enum | Description |")
	fmt.Fprintln(out, "| --- | --- | --- | --- | --- | --- |")
	renderProperties(out, root, schema)
	fmt.Fprintln(out)
}

func renderProperties(out *bytes.Buffer, prefix string, schema *apiextensionsv1.JSONSchemaProps) {
	required := map[string]bool{}
	for _, name := range schema.Required {
		required[name] = true
	}

	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		prop := schema.Properties[name]
		path := prefix + "." + name
		fmt.Fprintf(out, "| `%s` | `%s` | %s | %s | %s | %s |\n",
			path,
			markdownEscape(schemaType(prop)),
			yesNo(required[name]),
			markdownEscape(defaultValue(prop)),
			markdownEscape(enumValues(prop)),
			markdownEscape(prop.Description),
		)
		if shouldRecurse(prop) {
			childSchema := &prop
			if prop.Items != nil && prop.Items.Schema != nil && len(prop.Items.Schema.Properties) > 0 {
				childSchema = prop.Items.Schema
			}
			renderProperties(out, childPrefix(path, prop), childSchema)
		}
	}
}

func schemaProperty(schema *apiextensionsv1.JSONSchemaProps, name string) *apiextensionsv1.JSONSchemaProps {
	prop, ok := schema.Properties[name]
	if !ok {
		return nil
	}
	return &prop
}

func shouldRecurse(schema apiextensionsv1.JSONSchemaProps) bool {
	if len(schema.Properties) > 0 {
		return true
	}
	return schema.Items != nil && schema.Items.Schema != nil && len(schema.Items.Schema.Properties) > 0
}

func childPrefix(path string, schema apiextensionsv1.JSONSchemaProps) string {
	if schema.Items != nil && schema.Items.Schema != nil && len(schema.Items.Schema.Properties) > 0 {
		return path + "[]"
	}
	return path
}

func schemaType(schema apiextensionsv1.JSONSchemaProps) string {
	if schema.Type == "array" && schema.Items != nil && schema.Items.Schema != nil {
		return "[]" + schemaType(*schema.Items.Schema)
	}
	if schema.Type == "object" && schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil {
		return "map[string]" + schemaType(*schema.AdditionalProperties.Schema)
	}
	if schema.Format != "" {
		return schema.Type + "/" + schema.Format
	}
	if schema.Type != "" {
		return schema.Type
	}
	if schema.XPreserveUnknownFields != nil && *schema.XPreserveUnknownFields {
		return "object"
	}
	return "any"
}

func defaultValue(schema apiextensionsv1.JSONSchemaProps) string {
	if schema.Default == nil {
		return ""
	}
	return strings.TrimSpace(string(schema.Default.Raw))
}

func enumValues(schema apiextensionsv1.JSONSchemaProps) string {
	if len(schema.Enum) == 0 {
		return ""
	}
	values := make([]string, 0, len(schema.Enum))
	for _, value := range schema.Enum {
		values = append(values, strings.TrimSpace(string(value.Raw)))
	}
	return strings.Join(values, ", ")
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func markdownEscape(value string) string {
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "|", `\|`)
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}
