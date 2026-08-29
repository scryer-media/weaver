package weaver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWaitForJobCancelSettledGraphQLTimesOutWhenQueueItemRemainsActive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var payload struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode GraphQL request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(payload.Query, "queueItem"):
			_, _ = w.Write([]byte(`{
				"data": {
					"queueItem": {
						"state": "DOWNLOADING",
						"progressPercent": 12.5,
						"health": 999,
						"error": null,
						"totalBytes": 1000,
						"downloadedBytes": 125,
						"optionalRecoveryBytes": 0,
						"optionalRecoveryDownloadedBytes": 0,
						"failedBytes": 42
					},
					"historyItem": null
				}
			}`))
		default:
			_, _ = w.Write([]byte(`{"data": {}}`))
		}
	}))
	defer server.Close()

	err := waitForJobCancelSettledGraphQL(server.URL, 1234, 10*time.Millisecond, time.Millisecond)
	if err == nil {
		t.Fatal("expected active queue item to time out after cancel")
	}
	if !strings.Contains(err.Error(), "did not settle after cancel") {
		t.Fatalf("expected settle timeout error, got %v", err)
	}
	if !strings.Contains(err.Error(), "status=DOWNLOADING") {
		t.Fatalf("expected active status in error, got %v", err)
	}
}

func TestWaitForJobCancelSettledGraphQLAcceptsCancelledHistoryItem(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"data": {
				"queueItem": null,
				"historyItem": {
					"state": "CANCELLED",
					"progressPercent": 0,
					"health": 0,
					"error": null,
					"totalBytes": 1000,
					"downloadedBytes": 125,
					"failedBytes": 42
				}
			}
		}`))
	}))
	defer server.Close()

	if err := waitForJobCancelSettledGraphQL(server.URL, 1234, time.Second, time.Millisecond); err != nil {
		t.Fatalf("expected cancelled history item to settle: %v", err)
	}
}
