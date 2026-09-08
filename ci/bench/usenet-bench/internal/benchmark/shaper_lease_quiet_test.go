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

// fakeShaperLease answers /v1/lease the way the shaper does: POST acquires and
// returns a snapshot, DELETE releases and refuses while connections are open.
type fakeShaperLease struct {
	mu               sync.Mutex
	activePerAcquire []int64
	acquires         int
	releases         int
	held             bool
}

func (fake *fakeShaperLease) client(t *testing.T, leaseID string, started time.Time) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/v1/lease" {
			t.Fatalf("unexpected control path %s", request.URL.Path)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		var active int64
		switch request.Method {
		case http.MethodPost:
			if fake.acquires < len(fake.activePerAcquire) {
				active = fake.activePerAcquire[fake.acquires]
			}
			fake.acquires++
			if fake.held {
				return conflict("execution lease is already active"), nil
			}
			fake.held = true
		case http.MethodDelete:
			fake.releases++
			if !fake.held {
				return conflict("execution lease ID does not match the active lease"), nil
			}
			fake.held = false
		}
		snapshot := ShaperSnapshot{
			SchemaVersion:                 4,
			Status:                        "ok",
			StartedAt:                     started,
			ConfiguredEgressBitsPerSecond: 1_000_000_000,
			ConfiguredBurstBytes:          1 << 20,
			ActiveDownstreamConnections:   active,
			DownstreamSourceConnections:   map[string]uint64{},
			DownstreamSourceBytes:         map[string]uint64{},
			ExecutionLeaseID:              leaseID,
			ExecutionLeaseAcquiredAt:      &started,
			Build:                         ShaperBuildIdentity{ExecutableSHA256: strings.Repeat("a", 64)},
		}
		body, err := json.Marshal(snapshot)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK",
			Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
	})}
}

func conflict(message string) *http.Response {
	return &http.Response{StatusCode: http.StatusConflict, Status: "409 Conflict",
		Body: io.NopCloser(strings.NewReader(message)), Header: make(http.Header)}
}

func quietTestLink(t *testing.T) ServerLinkProfile {
	t.Helper()
	link, err := ResolveServerLinkProfile(Link1Gbit, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return link
}

// The previous run's client keeps redialling for a moment after it is asked to
// stop. Those connections belong to nothing that is being measured, so the run
// waits for them instead of recording a harness failure that would make the
// whole phase unpublishable.
func TestTheLeaseIsTakenAgainUntilTheShaperIsQuiet(t *testing.T) {
	leaseID := strings.Repeat("b", 64)
	fake := &fakeShaperLease{activePerAcquire: []int64{8, 8, 0}}
	snapshot, err := AcquireShaperExecutionLeaseForRun(context.Background(),
		fake.client(t, leaseID, time.Now().UTC()), "http://shaper.test", leaseID, quietTestLink(t))
	if err != nil {
		t.Fatalf("a shaper that went quiet still failed the run: %v", err)
	}
	if snapshot.ActiveDownstreamConnections != 0 {
		t.Fatalf("returned a snapshot carrying %d foreign connections", snapshot.ActiveDownstreamConnections)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.acquires != 3 || fake.releases != 2 {
		t.Fatalf("acquires=%d releases=%d, want the lease handed back after each busy snapshot", fake.acquires, fake.releases)
	}
	if !fake.held {
		t.Fatal("the run must be left holding the lease it will measure against")
	}
}

// A shaper that never goes quiet is a real condition and still fails the run,
// with no lease left stranded behind it.
func TestAShaperThatStaysBusyStillFailsTheRun(t *testing.T) {
	leaseID := strings.Repeat("c", 64)
	fake := &fakeShaperLease{}
	for range 200 {
		fake.activePerAcquire = append(fake.activePerAcquire, 8)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := AcquireShaperExecutionLeaseForRun(ctx,
		fake.client(t, leaseID, time.Now().UTC()), "http://shaper.test", leaseID, quietTestLink(t))
	if err == nil {
		t.Fatal("a shaper carrying foreign connections was accepted")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.held {
		t.Fatal("a failed acquisition must not strand the lease")
	}
}
