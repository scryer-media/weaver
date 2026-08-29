package benchmark

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestFetchAndValidateShaperSnapshot(t *testing.T) {
	started := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	leaseID := strings.Repeat("e", 64)
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/v1/stats" {
			t.Fatalf("unexpected control path %s", request.URL.Path)
		}
		payload := `{"schema_version":2,"status":"ok","started_at":"2026-08-10T12:00:00Z","configured_egress_bits_per_second":1000000000,"configured_burst_bytes":1048576,"downstream_connections":3,"active_downstream_connections":0,"downstream_bytes":42,"downstream_source_connections":{"172.18.0.2":3},"downstream_source_bytes":{"172.18.0.2":42},"execution_lease_id":"` + leaseID + `","execution_lease_acquired_at":"2026-08-10T12:00:00Z","build":{"executable_sha256":"` + strings.Repeat("a", 64) + `","version":"v1","commit":"abc","build_time":"now"}}`
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(payload)), Header: make(http.Header)}, nil
	})}
	snapshot, err := FetchShaperSnapshot(context.Background(), client, "http://shaper.test")
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.StartedAt.Equal(started) {
		t.Fatalf("started_at = %s, want %s", snapshot.StartedAt, started)
	}
	link, err := ResolveServerLinkProfile(Link1Gbit, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.ValidateFor(link); err != nil {
		t.Fatal(err)
	}
	after := snapshot
	after.DownstreamBytes += 100
	after.DownstreamSourceBytes = map[string]uint64{"172.18.0.2": 142}
	if delivered, err := ValidateShaperSnapshotPair(snapshot, after); err != nil || delivered != 100 {
		t.Fatalf("snapshot pair = (%d, %v), want 100 bytes", delivered, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestShaperAttestationRejectsMismatchAndConcurrentTraffic(t *testing.T) {
	started := time.Now()
	link, err := ResolveServerLinkProfile(Link10Gbit, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := ShaperSnapshot{
		SchemaVersion:                 2,
		Status:                        "ok",
		StartedAt:                     started,
		ConfiguredEgressBitsPerSecond: link.EgressBitsPerSecond,
		ConfiguredBurstBytes:          link.BurstBytes,
		DownstreamSourceConnections:   map[string]uint64{},
		DownstreamSourceBytes:         map[string]uint64{},
		ExecutionLeaseID:              strings.Repeat("f", 64),
		ExecutionLeaseAcquiredAt:      &started,
		Build:                         ShaperBuildIdentity{ExecutableSHA256: strings.Repeat("b", 64)},
	}
	if err := snapshot.ValidateFor(link); err != nil {
		t.Fatal(err)
	}
	snapshot.ActiveDownstreamConnections = 1
	if err := snapshot.ValidateFor(link); err == nil {
		t.Fatal("active competing shaper connection was accepted")
	}
	snapshot.ActiveDownstreamConnections = 0
	snapshot.ConfiguredEgressBitsPerSecond--
	if err := snapshot.ValidateFor(link); err == nil {
		t.Fatal("mismatched shaper rate was accepted")
	}
}

func TestShaperAttestationRejectsMultipleDownstreamSources(t *testing.T) {
	started := time.Now()
	build := ShaperBuildIdentity{ExecutableSHA256: strings.Repeat("c", 64)}
	before := ShaperSnapshot{
		SchemaVersion: 2, Status: "ok", StartedAt: started, Build: build,
		DownstreamSourceConnections: map[string]uint64{}, DownstreamSourceBytes: map[string]uint64{},
		ExecutionLeaseID: strings.Repeat("1", 64), ExecutionLeaseAcquiredAt: &started,
	}
	after := before
	after.DownstreamConnections = 2
	after.DownstreamBytes = 100
	after.DownstreamSourceConnections = map[string]uint64{"172.18.0.2": 1, "172.18.0.3": 1}
	after.DownstreamSourceBytes = map[string]uint64{"172.18.0.2": 50, "172.18.0.3": 50}
	if _, err := ValidateShaperSnapshotPair(before, after); err == nil {
		t.Fatal("shaper attestation accepted traffic from multiple downstream sources")
	}
}

func TestShaperAttestationRejectsUnattributedConnection(t *testing.T) {
	started := time.Now()
	build := ShaperBuildIdentity{ExecutableSHA256: strings.Repeat("d", 64)}
	before := ShaperSnapshot{
		SchemaVersion: 2, Status: "ok", StartedAt: started, Build: build,
		DownstreamSourceConnections: map[string]uint64{}, DownstreamSourceBytes: map[string]uint64{},
		ExecutionLeaseID: strings.Repeat("2", 64), ExecutionLeaseAcquiredAt: &started,
	}
	after := before
	after.DownstreamConnections = 2
	after.DownstreamBytes = 1
	after.DownstreamSourceConnections = map[string]uint64{"172.18.0.2": 1}
	after.DownstreamSourceBytes = map[string]uint64{"172.18.0.2": 1}
	if _, err := ValidateShaperSnapshotPair(before, after); err == nil {
		t.Fatal("shaper attestation accepted a global connection without source attribution")
	}
}
