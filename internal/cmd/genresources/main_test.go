package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const contract = "../../../api/openapi.json"

func TestEveryOperationGetsAMethod(t *testing.T) {
	ops, err := load(contract)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 82 {
		t.Fatalf("%d operations, want 82", len(ops))
	}
	source := writeResources(ops)
	for _, o := range ops {
		if !strings.Contains(source, ") "+o.name+"(ctx context.Context") {
			t.Errorf("no method for %s", o.id)
		}
	}
}

// S14: an operation with deprecated: true gets Go's marker. The contract has none yet, so mark one in a copy.
func TestDeprecatedOperationGetsTheMarker(t *testing.T) {
	raw, err := os.ReadFile(contract)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	paths := document["paths"].(map[string]any)
	paths["/me"].(map[string]any)["get"].(map[string]any)["deprecated"] = true
	paths["/projects"].(map[string]any)["get"].(map[string]any)["deprecated"] = true
	marked, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "openapi.json")
	if err := os.WriteFile(path, marked, 0o600); err != nil {
		t.Fatal(err)
	}
	ops, err := load(path)
	if err != nil {
		t.Fatal(err)
	}
	source := writeResources(ops)
	for _, method := range []string{"GetMe", "ListProjects", "ListProjectsPages", "ListProjectsAll"} {
		pattern := regexp.MustCompile(`// Deprecated: [^\n]*\nfunc \(s \*\w+\) ` + method + `\(`)
		if !pattern.MatchString(source) {
			t.Errorf("%s has no Deprecated marker", method)
		}
	}
	if count := strings.Count(source, "// Deprecated:"); count != 4 {
		t.Errorf("%d markers, want 4", count)
	}
}

func TestFoldersAreTheirOwnResource(t *testing.T) {
	ops, err := load(contract)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range ops {
		if strings.HasPrefix(o.path, "/folders") && o.service != "Folders" {
			t.Errorf("%s is in %s", o.id, o.service)
		}
		if o.service == "VoiceClones" && !strings.Contains(o.id, "VoiceClone") {
			t.Errorf("%s is in %s", o.id, o.service)
		}
	}
}
