package benchmark

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeShaperStats answers /v1/stats with a scripted count of active
// downstream connections, one entry per read, holding the last entry.
type fakeShaperStats struct {
	mu            sync.Mutex
	activePerRead []int64
	reads         int
}

func (fake *fakeShaperStats) client(t *testing.T) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/v1/stats" || request.Method != http.MethodGet {
			t.Fatalf("unexpected control request %s %s", request.Method, request.URL.Path)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		index := fake.reads
		if index >= len(fake.activePerRead) {
			index = len(fake.activePerRead) - 1
		}
		fake.reads++
		snapshot := ShaperSnapshot{
			SchemaVersion:               4,
			Status:                      "ok",
			StartedAt:                   time.Now().UTC(),
			ActiveDownstreamConnections: fake.activePerRead[index],
			DownstreamConnections:       8,
			DownstreamBytes:             1 << 20,
			DownstreamSourceConnections: map[string]uint64{"127.0.0.1": 8},
			DownstreamSourceBytes:       map[string]uint64{"127.0.0.1": 1 << 20},
		}
		body, err := json.Marshal(snapshot)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK",
			Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
	})}
}

// A client whose connections were dropped at shutdown is still counted by the
// shaper until both relay directions drain, which on a delayed link outlives
// the client's exit. The run's closing snapshot waits that out instead of
// failing a run that finished.
func TestTheClosingSnapshotWaitsForTheLinkToDrain(t *testing.T) {
	fake := &fakeShaperStats{activePerRead: []int64{8, 8, 0}}
	snapshot, err := fetchShaperSnapshotWhenQuiet(context.Background(), fake.client(t), "http://shaper.test", time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("a link that drained still failed the run: %v", err)
	}
	if snapshot.ActiveDownstreamConnections != 0 {
		t.Fatalf("returned a snapshot carrying %d draining connections", snapshot.ActiveDownstreamConnections)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.reads != 3 {
		t.Fatalf("reads=%d, want the snapshot read until it was quiet", fake.reads)
	}
}

// A client that is still connected when the budget ends did not stop, and
// the run reports it with the count the shaper saw.
func TestAClientStillConnectedAfterTheBudgetFailsTheRun(t *testing.T) {
	fake := &fakeShaperStats{activePerRead: []int64{3}}
	started := time.Now()
	_, err := fetchShaperSnapshotWhenQuiet(context.Background(), fake.client(t), "http://shaper.test", 30*time.Millisecond, 5*time.Millisecond)
	if err == nil {
		t.Fatal("a shaper that never went quiet passed the run")
	}
	if !strings.Contains(err.Error(), "3 active downstream connections") {
		t.Fatalf("failure must name the connections the shaper still holds, got: %v", err)
	}
	if time.Since(started) < 30*time.Millisecond {
		t.Fatal("the run gave up before the budget it declares")
	}
}
