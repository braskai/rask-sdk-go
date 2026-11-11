// Command genresources writes the typed resource layer from the contract: one method per operation, grouped by
// resource and named after its operationId, delegating to the oapi-codegen client in package api. It also writes the
// table the acceptance suite uses to reach every method by operationId, so a new operation cannot miss the suite.
//
// Usage: genresources -spec api/openapi.json -out resources.gen.go -test-out acceptance_dispatch_gen_test.go
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

type param struct {
	Name     string `json:"name"`
	In       string `json:"in"`
	Required bool   `json:"required"`
	Ref      string `json:"$ref"`
	Schema   struct {
		Type string `json:"type"`
	} `json:"schema"`
}

type media struct {
	Schema struct {
		Ref        string `json:"$ref"`
		Properties struct {
			Data struct {
				Items struct {
					Ref string `json:"$ref"`
				} `json:"items"`
			} `json:"data"`
		} `json:"properties"`
	} `json:"schema"`
}

type response struct {
	Ref     string           `json:"$ref"`
	Content map[string]media `json:"content"`
}

type rawOperation struct {
	OperationID string              `json:"operationId"`
	Summary     string              `json:"summary"`
	Tags        []string            `json:"tags"`
	Deprecated  bool                `json:"deprecated"`
	Parameters  []param             `json:"parameters"`
	Responses   map[string]response `json:"responses"`
	RequestBody *struct {
		Required bool             `json:"required"`
		Content  map[string]media `json:"content"`
	} `json:"requestBody"`
}

type spec struct {
	Paths      map[string]map[string]json.RawMessage `json:"paths"`
	Components struct {
		Parameters map[string]param `json:"parameters"`
		Schemas    map[string]struct {
			Properties map[string]struct {
				Items struct {
					Ref string `json:"$ref"`
				} `json:"items"`
			} `json:"properties"`
		} `json:"schemas"`
		Responses map[string]response `json:"responses"`
	} `json:"components"`
}

// pathArg is one path parameter of an operation, in path order.
type pathArg struct {
	wire   string // project_id
	goName string // projectID
	goType string // string or int32
}

// op is one operation with everything the templates below need.
type op struct {
	id, name, service, method, path, summary string
	deprecated                               bool
	pathArgs                                 []pathArg
	hasKey                                   bool
	ifMatch                                  string // none, optional, required
	hasParamsType                            bool   // oapi-codegen generated <Name>Params
	exposedParams                            bool   // query parameters or If-None-Match: the caller passes the struct
	requiredParams                           bool   // a required query parameter: by value, not by optional pointer
	bodyType                                 string
	bodyOptional                             bool
	resultType                               string // empty for 204
	itemType                                 string // paged lists only
	listType                                 string
}

var pathParamRe = regexp.MustCompile(`\{(\w+)\}`)

func exportName(id string) string {
	return string(unicode.ToUpper(rune(id[0]))) + id[1:]
}

// goArgName turns project_id into projectID and version_number into versionNumber.
func goArgName(wire string) string {
	parts := strings.Split(wire, "_")
	var b strings.Builder
	for i, part := range parts {
		switch {
		case i == 0:
			b.WriteString(part)
		case part == "id":
			b.WriteString("ID")
		default:
			b.WriteString(exportName(part))
		}
	}
	return b.String()
}

func schemaName(ref string) string {
	return ref[strings.LastIndex(ref, "/")+1:]
}

// serviceOf groups an operation by resource: the contract's tag, except that folders are a resource of their own
// although the contract tags them Projects.
func serviceOf(o rawOperation, path string) string {
	if strings.HasPrefix(path, "/folders") {
		return "Folders"
	}
	var b strings.Builder
	for _, word := range strings.Fields(o.Tags[0]) {
		b.WriteString(exportName(word))
	}
	return b.String()
}

func load(path string) ([]op, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s spec
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	var ops []op
	for urlPath, item := range s.Paths {
		for method, body := range item {
			switch method {
			case "get", "post", "put", "patch", "delete":
			default:
				continue
			}
			var o rawOperation
			if err := json.Unmarshal(body, &o); err != nil {
				return nil, fmt.Errorf("%s %s: %w", method, urlPath, err)
			}
			built, err := buildOp(&s, o, method, urlPath)
			if err != nil {
				return nil, err
			}
			ops = append(ops, built)
		}
	}
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].service != ops[j].service {
			return ops[i].service < ops[j].service
		}
		return ops[i].name < ops[j].name
	})
	return ops, nil
}

func buildOp(s *spec, o rawOperation, method, urlPath string) (op, error) {
	out := op{
		id:         o.OperationID,
		name:       exportName(o.OperationID),
		service:    serviceOf(o, urlPath),
		method:     strings.ToUpper(method),
		path:       urlPath,
		summary:    o.Summary,
		deprecated: o.Deprecated,
		ifMatch:    "none",
	}
	pathParams := map[string]param{}
	for _, p := range o.Parameters {
		if p.Ref != "" {
			p = s.Components.Parameters[schemaName(p.Ref)]
		}
		switch {
		case p.In == "path":
			pathParams[p.Name] = p
		case p.In == "query":
			out.hasParamsType = true
			out.exposedParams = true
			out.requiredParams = out.requiredParams || p.Required
		case p.Name == "Idempotency-Key":
			out.hasKey = true
			out.hasParamsType = true
		case p.Name == "If-Match":
			out.hasParamsType = true
			out.ifMatch = "optional"
			if p.Required {
				out.ifMatch = "required"
			}
		case p.Name == "If-None-Match":
			out.hasParamsType = true
			out.exposedParams = true
		default:
			return op{}, fmt.Errorf("%s: header %s is not handled", o.OperationID, p.Name)
		}
	}
	if out.exposedParams && (out.hasKey || out.ifMatch != "none") {
		return op{}, fmt.Errorf("%s: query parameters together with a key or If-Match are not handled", o.OperationID)
	}
	for _, m := range pathParamRe.FindAllStringSubmatch(urlPath, -1) {
		p, ok := pathParams[m[1]]
		if !ok {
			return op{}, fmt.Errorf("%s: path parameter %s is not declared", o.OperationID, m[1])
		}
		goType := "string"
		if p.Schema.Type == "integer" {
			goType = "int32"
		}
		out.pathArgs = append(out.pathArgs, pathArg{wire: p.Name, goName: goArgName(p.Name), goType: goType})
	}
	if o.RequestBody != nil {
		mt, ok := o.RequestBody.Content["application/json"]
		if !ok {
			return op{}, fmt.Errorf("%s: only JSON bodies are handled", o.OperationID)
		}
		out.bodyType = schemaName(mt.Schema.Ref)
		out.bodyOptional = !o.RequestBody.Required
	}
	for status, r := range o.Responses {
		if !strings.HasPrefix(status, "2") {
			continue
		}
		if r.Ref != "" {
			r = s.Components.Responses[schemaName(r.Ref)]
		}
		mt, ok := r.Content["application/json"]
		if !ok {
			continue
		}
		out.resultType = schemaName(mt.Schema.Ref)
	}
	if out.resultType != "" && hasCursor(s, o) {
		item := s.Components.Schemas[out.resultType].Properties["data"].Items.Ref
		if item == "" {
			return op{}, fmt.Errorf("%s: a cursor but no data items", o.OperationID)
		}
		out.itemType = schemaName(item)
		out.listType = out.resultType
	}
	return out, nil
}

func hasCursor(s *spec, o rawOperation) bool {
	for _, p := range o.Parameters {
		if p.Ref != "" {
			p = s.Components.Parameters[schemaName(p.Ref)]
		}
		if p.In == "query" && p.Name == "cursor" {
			return true
		}
	}
	return false
}

func (o op) result() string {
	if o.resultType == "" {
		return "NoContent"
	}
	return "api." + o.resultType
}

// signature returns the parameter list after ctx, up to but not including opts.
func (o op) signature() string {
	var parts []string
	for _, a := range o.pathArgs {
		parts = append(parts, a.goName+" "+a.goType)
	}
	if o.ifMatch == "required" {
		parts = append(parts, "ifMatch string")
	}
	if o.exposedParams {
		if o.requiredParams {
			parts = append(parts, "params api."+o.name+"Params")
		} else {
			parts = append(parts, "params *api."+o.name+"Params")
		}
	}
	if o.bodyType != "" {
		if o.bodyOptional {
			parts = append(parts, "body *api."+o.bodyType)
		} else {
			parts = append(parts, "body api."+o.bodyType)
		}
	}
	parts = append(parts, "opts ...RequestOption")
	return strings.Join(parts, ", ")
}

func (o op) lowArgs() string {
	parts := []string{"ctx"}
	for _, a := range o.pathArgs {
		parts = append(parts, a.goName)
	}
	return strings.Join(parts, ", ")
}

func (o op) lowParams() string {
	if !o.hasParamsType {
		return ""
	}
	if o.exposedParams {
		if o.requiredParams {
			return "&params"
		}
		return "params"
	}
	var fields []string
	if o.hasKey {
		fields = append(fields, "IdempotencyKey: rc.key")
	}
	switch o.ifMatch {
	case "required":
		fields = append(fields, "IfMatch: ifMatch")
	case "optional":
		fields = append(fields, "IfMatch: rc.ifMatch")
	}
	return "&api." + o.name + "Params{" + strings.Join(fields, ", ") + "}"
}

func (o op) requiredArgs() string {
	var parts []string
	for _, a := range o.pathArgs {
		if a.goType == "string" {
			parts = append(parts, fmt.Sprintf("{%q, %s}", a.wire, a.goName))
		}
	}
	if o.ifMatch == "required" {
		parts = append(parts, `{"if_match", ifMatch}`)
	}
	if len(parts) == 0 {
		return "nil"
	}
	return "[]requiredArg{" + strings.Join(parts, ", ") + "}"
}

func writeResources(ops []op) string {
	var b strings.Builder
	b.WriteString("// Code generated by internal/cmd/genresources from api/openapi.json. DO NOT EDIT.\n\n")
	b.WriteString("package rask\n\nimport (\n\t\"context\"\n\t\"iter\"\n\t\"net/http\"\n\n\t\"github.com/braskai/rask-sdk-go/v3/api\"\n)\n\n")

	services := map[string]bool{}
	for _, o := range ops {
		services[o.service] = true
	}
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)

	b.WriteString("// services are the operation groups of a Client, one field per resource.\ntype services struct {\n")
	for _, name := range names {
		fmt.Fprintf(&b, "\t%s *%sService\n", name, name)
	}
	b.WriteString("}\n\nfunc newServices(c *Client) services {\n\treturn services{\n")
	for _, name := range names {
		fmt.Fprintf(&b, "\t\t%s: &%sService{c: c},\n", name, name)
	}
	b.WriteString("\t}\n}\n\n")
	for _, name := range names {
		fmt.Fprintf(&b, "// %sService groups the %s operations.\ntype %sService struct{ c *Client }\n\n", name, name, name)
	}

	b.WriteString("var (\n")
	for _, o := range ops {
		key := "false"
		if o.hasKey {
			key = "true"
		}
		fmt.Fprintf(&b, "\top%s = &operation{id: %q, method: %q, path: %q, carriesKey: %s, ifMatch: ifMatch%s}\n",
			o.name, o.id, o.method, o.path, key, exportName(o.ifMatch))
	}
	b.WriteString(")\n\n")
	b.WriteString("// operations lists every operation of the contract.\nvar operations = []*operation{\n")
	for _, o := range ops {
		fmt.Fprintf(&b, "\top%s,\n", o.name)
	}
	b.WriteString("}\n\n")

	for _, o := range ops {
		writeMethod(&b, o)
	}
	return b.String()
}

func writeMethod(b *strings.Builder, o op) {
	summary := strings.TrimSuffix(strings.TrimSpace(o.summary), ".")
	fmt.Fprintf(b, "// %s calls %s: %s.\n//\n// %s %s\n", o.name, o.id, summary, o.method, o.path)
	if o.deprecated {
		fmt.Fprintf(b, "//\n// Deprecated: the contract marks %s as deprecated; see its Sunset header.\n", o.id)
	}
	result := o.result()
	fmt.Fprintf(b, "func (s *%sService) %s(ctx context.Context, %s) (*Response[%s], error) {\n", o.service, o.name, o.signature(), result)
	fmt.Fprintf(b, "\treturn call[%s](ctx, s.c, op%s, opts, %s, func(ctx context.Context, rc *callInfo) (*http.Response, error) {\n", result, o.name, o.requiredArgs())
	args := o.lowArgs()
	params := o.lowParams()
	switch {
	case o.bodyType != "" && o.bodyOptional:
		fmt.Fprintf(b, "\t\tif body == nil {\n\t\t\treturn s.c.low.%sWithBody(%s, %s, \"\", nil)\n\t\t}\n", o.name, args, params)
		fmt.Fprintf(b, "\t\treturn s.c.low.%s(%s, %s, *body)\n", o.name, args, params)
	case o.bodyType != "":
		if params == "" {
			fmt.Fprintf(b, "\t\treturn s.c.low.%s(%s, body)\n", o.name, args)
		} else {
			fmt.Fprintf(b, "\t\treturn s.c.low.%s(%s, %s, body)\n", o.name, args, params)
		}
	case params != "":
		fmt.Fprintf(b, "\t\treturn s.c.low.%s(%s, %s)\n", o.name, args, params)
	default:
		fmt.Fprintf(b, "\t\treturn s.c.low.%s(%s)\n", o.name, args)
	}
	b.WriteString("\t})\n}\n\n")

	if o.itemType == "" {
		return
	}
	var callArgs []string
	for _, a := range o.pathArgs {
		callArgs = append(callArgs, a.goName)
	}
	prefix := strings.Join(callArgs, ", ")
	if prefix != "" {
		prefix += ", "
	}
	copyParams := "\t\tvar p api." + o.name + "Params\n\t\tif params != nil {\n\t\t\tp = *params\n\t\t}\n"
	if o.requiredParams {
		copyParams = "\t\tp := params\n"
	}
	callParam := "&p"
	if o.requiredParams {
		callParam = "p"
	}
	marker := ""
	if o.deprecated {
		marker = fmt.Sprintf("//\n// Deprecated: the contract marks %s as deprecated; see its Sunset header.\n", o.id)
	}
	fmt.Fprintf(b, "// %sPages walks the pages of %s over next_cursor, keeping the same filters. Each page keeps its raw\n// response. Fetching stops when the loop ends.\n%s", o.name, o.name, marker)
	fmt.Fprintf(b, "func (s *%sService) %sPages(ctx context.Context, %s) iter.Seq2[*Response[api.%s], error] {\n", o.service, o.name, o.signature(), o.listType)
	fmt.Fprintf(b, "\treturn pages(ctx, func(ctx context.Context, cursor *string) (*Response[api.%s], error) {\n", o.listType)
	b.WriteString(copyParams)
	b.WriteString("\t\tif cursor != nil {\n\t\t\tp.Cursor = cursor\n\t\t}\n")
	fmt.Fprintf(b, "\t\treturn s.%s(ctx, %s%s, opts...)\n\t})\n}\n\n", o.name, prefix, callParam)

	fmt.Fprintf(b, "// %sAll yields every item of %s across pages. An error ends the iteration.\n%s", o.name, o.name, marker)
	fmt.Fprintf(b, "func (s *%sService) %sAll(ctx context.Context, %s) iter.Seq2[api.%s, error] {\n", o.service, o.name, o.signature(), o.itemType)
	var passArgs []string
	passArgs = append(passArgs, callArgs...)
	passArgs = append(passArgs, "params", "opts...")
	fmt.Fprintf(b, "\treturn items[api.%s, api.%s](s.%sPages(ctx, %s))\n}\n\n", o.listType, o.itemType, o.name, strings.Join(passArgs, ", "))
}

// writeDispatch writes the acceptance suite's way to reach each method from a scenario's JSON.
func writeDispatch(ops []op) string {
	var b strings.Builder
	b.WriteString("// Code generated by internal/cmd/genresources from api/openapi.json. DO NOT EDIT.\n\n")
	b.WriteString("package rask_test\n\nimport (\n\t\"context\"\n\t\"encoding/json\"\n\n\t\"github.com/braskai/rask-sdk-go/v3\"\n\t\"github.com/braskai/rask-sdk-go/v3/api\"\n)\n\n")
	b.WriteString("// dispatch reaches every typed method by operationId.\nvar dispatch = map[string]func(context.Context, *rask.Client, *opCall, []rask.RequestOption) (*rask.Meta, error){\n")
	for _, o := range ops {
		fmt.Fprintf(&b, "\t%q: func(ctx context.Context, c *rask.Client, in *opCall, opts []rask.RequestOption) (*rask.Meta, error) {\n", o.id)
		args := dispatchArgs(&b, o)
		fmt.Fprintf(&b, "\t\tr, err := c.%s.%s(ctx, %sopts...)\n\t\treturn metaOf(r), err\n\t},\n", o.service, o.name, args)
	}
	b.WriteString("}\n\n")
	b.WriteString("// iterators reaches every auto-paging iterator by operationId.\nvar iterators = map[string]func(context.Context, *rask.Client, *opCall, []rask.RequestOption) ([]json.RawMessage, error){\n")
	for _, o := range ops {
		if o.itemType == "" {
			continue
		}
		fmt.Fprintf(&b, "\t%q: func(ctx context.Context, c *rask.Client, in *opCall, opts []rask.RequestOption) ([]json.RawMessage, error) {\n", o.id)
		args := dispatchArgs(&b, o)
		fmt.Fprintf(&b, "\t\treturn collect(c.%s.%sAll(ctx, %sopts...))\n\t},\n", o.service, o.name, args)
	}
	b.WriteString("}\n")
	return b.String()
}

func dispatchArgs(b *strings.Builder, o op) string {
	var args strings.Builder
	for _, a := range o.pathArgs {
		if a.goType == "string" {
			fmt.Fprintf(&args, "in.pathString(%q), ", a.wire)
		} else {
			fmt.Fprintf(&args, "in.pathInt32(%q), ", a.wire)
		}
	}
	if o.ifMatch == "required" {
		args.WriteString("in.ifMatch(), ")
	}
	if o.exposedParams {
		fmt.Fprintf(b, "\t\tvar params api.%sParams\n\t\tif err := in.decodeQuery(&params); err != nil {\n\t\t\treturn nil, err\n\t\t}\n", o.name)
		if strings.Contains(o.signature(), "params *api.") {
			args.WriteString("&params, ")
		} else {
			args.WriteString("params, ")
		}
	}
	if o.bodyType != "" {
		if o.bodyOptional {
			fmt.Fprintf(b, "\t\tvar body *api.%s\n\t\tif in.hasBody() {\n\t\t\tbody = new(api.%s)\n\t\t\tif err := in.decodeBody(body); err != nil {\n\t\t\t\treturn nil, err\n\t\t\t}\n\t\t}\n", o.bodyType, o.bodyType)
		} else {
			fmt.Fprintf(b, "\t\tvar body api.%s\n\t\tif err := in.decodeBody(&body); err != nil {\n\t\t\treturn nil, err\n\t\t}\n", o.bodyType)
		}
		args.WriteString("body, ")
	}
	return args.String()
}

func writeFormatted(path, source string) error {
	formatted, err := format.Source([]byte(source))
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return os.WriteFile(path, formatted, 0o644)
}

func main() {
	specPath := flag.String("spec", "api/openapi.json", "OpenAPI contract")
	out := flag.String("out", "resources.gen.go", "resource layer output")
	testOut := flag.String("test-out", "acceptance_dispatch_gen_test.go", "acceptance dispatch output; empty to skip")
	flag.Parse()
	ops, err := load(*specPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := writeFormatted(*out, writeResources(ops)); err != nil {
		log.Fatal(err)
	}
	if *testOut != "" {
		if err := writeFormatted(*testOut, writeDispatch(ops)); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("genresources: %d operations\n", len(ops))
}
