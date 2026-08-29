package clientadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// fakeWeaver models the public Weaver control plane the adapter depends on:
// the SPA index issues a session cookie when no login is configured, every
// GraphQL request must present it, and `job(id:)` keeps answering after the
// job has left the live queue.
type fakeWeaver struct {
	t         *testing.T
	mu        sync.Mutex
	statuses  []string
	polls     int
	submitted map[string]any
	reject    bool
}

const fakeWeaverSessionCookie = "weaver_session=nntpbench-test-session"

func (fake *fakeWeaver) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Add("Set-Cookie", fakeWeaverSessionCookie+"; Path=/; HttpOnly; SameSite=Strict")
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<!doctype html><title>weaver</title>"))
	})
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		if cookie, err := r.Cookie("weaver_session"); err != nil || "weaver_session="+cookie.Value != fakeWeaverSessionCookie {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var request struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(request.Query, "{ version }"):
			_, _ = w.Write([]byte(`{"data":{"version":"0.8.4"}}`))
		case strings.Contains(request.Query, "submitNzb"):
			fake.mu.Lock()
			input, _ := request.Variables["input"].(map[string]any)
			fake.submitted = input
			reject := fake.reject
			fake.mu.Unlock()
			if reject {
				_, _ = w.Write([]byte(`{"data":{"submitNzb":{"accepted":false,"status":"REJECTED","jobId":null,"errorCode":"DUPLICATE","message":"semantic duplicate blocked","item":null}}}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":{"submitNzb":{"accepted":true,"status":"ACCEPTED","jobId":42,"errorCode":null,"message":null,"item":{"id":42}}}}`))
		case strings.Contains(request.Query, "job(id: 42)"):
			fake.mu.Lock()
			status := fake.statuses[len(fake.statuses)-1]
			if fake.polls < len(fake.statuses) {
				status = fake.statuses[fake.polls]
			}
			fake.polls++
			fake.mu.Unlock()
			// The adapter aliases each job as jN in a batched query and as a
			// bare `job` field in the single-job wait.
			field := "job"
			if strings.Contains(request.Query, "j0: job") {
				field = "j0"
			}
			_, _ = fmt.Fprintf(w, `{"data":{%q:{"status":%q}}}`, field, status)
		default:
			fake.t.Errorf("unexpected Weaver GraphQL query: %s", request.Query)
			_, _ = w.Write([]byte(`{"errors":[{"message":"unexpected query"}]}`))
		}
	})
	return mux
}

func writeTestNZB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.nzb")
	if err := os.WriteFile(path, []byte(`<?xml version="1.0"?><nzb/>`), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestWeaverAPIUsesSessionCookieVersionAndTerminalPolling(t *testing.T) {
	fake := &fakeWeaver{t: t, statuses: []string{"QUEUED", "DOWNLOADING", "EXTRACTING", "COMPLETE"}}
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	api, err := NewAPI(benchmark.Weaver, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	version, err := api.WaitReady(ctx)
	if err != nil {
		t.Fatalf("readiness: %v", err)
	}
	if version != "0.8.4" {
		t.Fatalf("readiness version = %q, want the GraphQL version field", version)
	}
	timing, err := api.QueueWithTiming(ctx, writeTestNZB(t), "fixture-password")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if timing.JobID != "42" {
		t.Fatalf("job id = %q, want 42", timing.JobID)
	}
	if fake.submitted["nzbBase64"] == "" || fake.submitted["filename"] != "fixture.nzb" || fake.submitted["password"] != "fixture-password" {
		t.Fatalf("submitNzb input = %#v", fake.submitted)
	}
	if _, forced := fake.submitted["force"]; forced {
		t.Fatalf("plain submission must not force-accept: %#v", fake.submitted)
	}
	terminal, err := api.WaitCompleteWithObservation(ctx, timing.JobID, time.Millisecond, timing.AcceptedAt)
	if err != nil {
		t.Fatalf("terminal wait: %v", err)
	}
	if terminal.ObservedAt.Before(terminal.LowerBound) || terminal.LowerBound.Before(timing.AcceptedAt) {
		t.Fatalf("terminal observation %+v is not ordered after acceptance %v", terminal, timing.AcceptedAt)
	}
	if fake.polls < len(fake.statuses) {
		t.Fatalf("terminal state observed after %d polls, want the full %d-state lifecycle", fake.polls, len(fake.statuses))
	}
}

func TestWeaverAPIReportsTheRejectionReason(t *testing.T) {
	fake := &fakeWeaver{t: t, statuses: []string{"COMPLETE"}, reject: true}
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	api, err := NewAPI(benchmark.Weaver, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := api.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = api.Queue(ctx, writeTestNZB(t), "")
	if err == nil {
		t.Fatal("rejected submission must fail")
	}
	for _, expected := range []string{"REJECTED", "DUPLICATE", "semantic duplicate blocked"} {
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("rejection error %q lacks %q", err, expected)
		}
	}
}

func TestWeaverAPIFailsFastOnFailedJob(t *testing.T) {
	fake := &fakeWeaver{t: t, statuses: []string{"DOWNLOADING", "FAILED"}}
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	api, err := NewAPI(benchmark.Weaver, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := api.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	timing, err := api.QueueWithTiming(ctx, writeTestNZB(t), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.WaitCompleteWithObservation(ctx, timing.JobID, time.Millisecond, timing.AcceptedAt); err == nil || !strings.Contains(err.Error(), "FAILED") {
		t.Fatalf("failed job must surface its terminal status, got %v", err)
	}
}

// fakeNZBGet answers only the JSON-RPC methods the adapter uses.
type fakeNZBGet struct {
	groups  []map[string]any
	history []map[string]any
}

func (fake *fakeNZBGet) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/jsonrpc" {
			http.NotFound(w, r)
			return
		}
		if user, pass, ok := r.BasicAuth(); !ok || user != controlUsername || pass != apiKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var result any
		switch request.Method {
		case "version":
			result = "24.3"
		case "listgroups":
			result = fake.groups
		case "history":
			result = fake.history
		default:
			t.Errorf("unexpected NZBGet RPC method %q", request.Method)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"version": "1.1", "result": result})
	})
}

func TestNZBGetHistoryRecordsAreAlwaysTerminal(t *testing.T) {
	fake := &fakeNZBGet{
		groups: []map[string]any{{"NZBID": 4, "Status": "DOWNLOADING"}},
		history: []map[string]any{
			{"NZBID": 1, "Status": "SUCCESS/ALL", "ParStatus": "SUCCESS", "UnpackStatus": "SUCCESS", "MoveStatus": "SUCCESS", "ScriptStatus": "NONE", "DeleteStatus": "NONE"},
			{"NZBID": 2, "Status": "WARNING/HEALTH", "ParStatus": "NONE", "UnpackStatus": "NONE", "MoveStatus": "SUCCESS", "ScriptStatus": "NONE", "DeleteStatus": "NONE"},
			{"NZBID": 3, "Status": "FAILURE/UNPACK", "ParStatus": "SUCCESS", "UnpackStatus": "FAILURE", "MoveStatus": "NONE", "ScriptStatus": "NONE", "DeleteStatus": "NONE"},
		},
	}
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()

	api, err := NewAPI(benchmark.NZBGet, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if version, err := api.WaitReady(ctx); err != nil || version != "24.3" {
		t.Fatalf("readiness = %q, %v", version, err)
	}
	observations, err := api.product.observe(ctx, []string{"1", "2", "3", "4"})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]jobObservationState{"1": jobComplete, "2": jobFailed, "3": jobFailed, "4": jobActive}
	for id, want := range expected {
		if got := observations[id].state; got != want {
			t.Fatalf("job %s observed as %v (%q), want %v", id, got, observations[id].status, want)
		}
	}
	if _, err := api.WaitComplete(ctx, "2", time.Millisecond); err == nil || !strings.Contains(err.Error(), "WARNING/HEALTH") {
		t.Fatalf("WARNING history record must end the wait with its status, got %v", err)
	}
	if _, err := api.WaitComplete(ctx, "1", time.Millisecond); err != nil {
		t.Fatalf("clean success must complete: %v", err)
	}
}
