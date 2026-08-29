package weaver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type capturedSubmitRequest struct {
	Variables struct {
		Input map[string]interface{} `json:"input"`
	} `json:"variables"`
}

func writeSubmitTestFixture(t *testing.T, slug string) {
	t.Helper()
	fixturesDir := t.TempDir()
	t.Setenv("FIXTURES_DIR", fixturesDir)
	slugDir := filepath.Join(fixturesDir, slug)
	if err := os.MkdirAll(slugDir, 0o755); err != nil {
		t.Fatalf("create fixture dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slugDir, slug+".nzb"), []byte("<nzb></nzb>"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
}

func TestSubmitOneNZBOnlySendsForceWhenRequested(t *testing.T) {
	const slug = "force-options"
	writeSubmitTestFixture(t, slug)

	var requests []capturedSubmitRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request capturedSubmitRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests = append(requests, request)
		_, _ = fmt.Fprintf(w, `{"data":{"submitNzb":{"accepted":true,"item":{"id":%d}}}}`, len(requests))
	}))
	defer server.Close()

	scenario := &Scenario{Slug: slug, Title: "Force Options", Category: "test"}
	if _, err := submitOneNZB(server.URL, scenario); err != nil {
		t.Fatalf("default submit: %v", err)
	}
	if _, err := submitOneNZBWithOptions(server.URL, scenario, submitNZBOptions{force: true}); err != nil {
		t.Fatalf("forced submit: %v", err)
	}

	if _, exists := requests[0].Variables.Input["force"]; exists {
		t.Fatalf("default submission unexpectedly sent force: %#v", requests[0].Variables.Input)
	}
	if force, ok := requests[1].Variables.Input["force"].(bool); !ok || !force {
		t.Fatalf("forced submission force = %#v, want true", requests[1].Variables.Input["force"])
	}
}

func TestSubmitSlugNTimesForcesOnlyRepeatedSubmissions(t *testing.T) {
	const slug = "restart-repeat"
	writeSubmitTestFixture(t, slug)

	var inputs []map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request capturedSubmitRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		inputs = append(inputs, request.Variables.Input)
		_, _ = fmt.Fprintf(w, `{"data":{"submitNzb":{"accepted":true,"item":{"id":%d}}}}`, len(inputs))
	}))
	defer server.Close()

	ctx := &restartCaseContext{
		weaverURL: server.URL,
		Scenarios: map[string]*Scenario{
			slug: {Slug: slug, Title: "Restart Repeat"},
		},
	}
	ids, err := ctx.submitSlugNTimes(slug, 3)
	if err != nil {
		t.Fatalf("submit slug three times: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids = %v, want three ids", ids)
	}
	for i, input := range inputs {
		force, exists := input["force"]
		if i == 0 && exists {
			t.Errorf("first submission unexpectedly sent force=%v", force)
		}
		if i > 0 && force != true {
			t.Errorf("submission %d force = %#v, want true", i+1, force)
		}
	}
}
