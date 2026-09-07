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
	link, err := ResolveServerLinkProfile(Link1Gbit, 0, 0, 0)
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
	link, err := ResolveServerLinkProfile(Link10Gbit, 0, 0, 0)
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

func shapedRoundTripSnapshot(t *testing.T, rttMicros uint64) ShaperSnapshot {
	t.Helper()
	started := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	acquired := started.Add(time.Minute)
	ingress := rttMicros / 2
	egress := rttMicros - ingress
	return ShaperSnapshot{
		SchemaVersion:                 4,
		Status:                        "ok",
		StartedAt:                     started,
		ConfiguredEgressBitsPerSecond: 1_000_000_000,
		ConfiguredBurstBytes:          1 << 20,
		ConfiguredRTTMicros:           rttMicros,
		LinkShaping: &ShaperLinkShaping{
			SchemaVersion:          1,
			Interface:              "eth1",
			IngressDevice:          "ifb-nntp",
			EgressMechanism:        "netem",
			IngressMechanism:       "ifb-netem",
			RTTMicros:              rttMicros,
			EgressDelayMicros:      egress,
			IngressDelayMicros:     ingress,
			NetemLimitPackets:      125_000,
			TCPWmem:                "4096 1048576 134217728",
			TCPRmem:                "4096 1048576 134217728",
			KernelRelease:          "6.8.0",
			LiveEgressDelayMicros:  egress,
			LiveIngressDelayMicros: ingress,
		},
		DownstreamSourceConnections: map[string]uint64{},
		DownstreamSourceBytes:       map[string]uint64{},
		ExecutionLeaseID:            strings.Repeat("c", 64),
		ExecutionLeaseAcquiredAt:    &acquired,
		Build:                       ShaperBuildIdentity{ExecutableSHA256: strings.Repeat("b", 64), Version: "v1", Commit: "abc", BuildTime: "now"},
	}
}

func TestShaperAttestationRoundTripMustMatchThePlanAndLiveTC(t *testing.T) {
	link, err := ResolveServerLinkProfile(Link1Gbit, 0, 0, 250_000)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := shapedRoundTripSnapshot(t, 250_000)
	if err := snapshot.ValidateFor(link); err != nil {
		t.Fatalf("consistent 250ms attestation rejected: %v", err)
	}
	// tc prints a delay with limited precision; a reading within 1% passes.
	tolerated := shapedRoundTripSnapshot(t, 250_000)
	tolerated.LinkShaping.LiveEgressDelayMicros = 124_000
	if err := tolerated.ValidateFor(link); err != nil {
		t.Fatalf("live delay within tolerance rejected: %v", err)
	}
	after := shapedRoundTripSnapshot(t, 250_000)
	after.DownstreamConnections = 2
	after.DownstreamBytes = 10
	after.DownstreamSourceConnections = map[string]uint64{"172.18.0.2": 2}
	after.DownstreamSourceBytes = map[string]uint64{"172.18.0.2": 10}
	if delivered, err := ValidateShaperSnapshotPair(snapshot, after); err != nil || delivered != 10 {
		t.Fatalf("pair with unchanged shaping = (%d, %v), want 10 bytes", delivered, err)
	}

	cases := map[string]func(s *ShaperSnapshot){
		"plan declares a different round trip": func(s *ShaperSnapshot) { s.ConfiguredRTTMicros = 500_000; s.LinkShaping.RTTMicros = 500_000 },
		"report missing":                       func(s *ShaperSnapshot) { s.LinkShaping = nil },
		"report round trip disagrees":          func(s *ShaperSnapshot) { s.LinkShaping.RTTMicros = 500_000 },
		"split does not add up":                func(s *ShaperSnapshot) { s.LinkShaping.IngressDelayMicros = 100_000 },
		"egress qdisc gone":                    func(s *ShaperSnapshot) { s.LinkShaping.LiveEgressDelayMicros = 0 },
		"ingress qdisc drifted":                func(s *ShaperSnapshot) { s.LinkShaping.LiveIngressDelayMicros = 120_000 },
		"tc could not be read":                 func(s *ShaperSnapshot) { s.LinkShaping.LiveError = "tc: not found" },
		"degraded status":                      func(s *ShaperSnapshot) { s.Status = "degraded" },
		"unknown ingress mechanism":            func(s *ShaperSnapshot) { s.LinkShaping.IngressMechanism = "police" },
		"no ingress path but a device":         func(s *ShaperSnapshot) { s.LinkShaping.IngressMechanism = "none" },
		"netem limit unset":                    func(s *ShaperSnapshot) { s.LinkShaping.NetemLimitPackets = 0 },
		"wrong report schema":                  func(s *ShaperSnapshot) { s.LinkShaping.SchemaVersion = 2 },
	}
	for name, mutate := range cases {
		broken := shapedRoundTripSnapshot(t, 250_000)
		mutate(&broken)
		if err := broken.ValidateFor(link); err == nil {
			t.Fatalf("%s: attestation must be rejected", name)
		}
	}

	// A pre-schema-4 shaper cannot render a round trip at all.
	legacy := shapedRoundTripSnapshot(t, 0)
	legacy.SchemaVersion = 3
	legacy.LinkShaping = nil
	if err := legacy.ValidateFor(link); err == nil {
		t.Fatal("schema-3 shaper must be rejected for a plan with a round trip")
	}
	unshaped, err := ResolveServerLinkProfile(Link1Gbit, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.ValidateFor(unshaped); err != nil {
		t.Fatalf("schema-3 shaper must still serve a plan without a round trip: %v", err)
	}
	// A schema-4 shaper with no delay carries no report; one that reports a
	// path for a plan declaring none is rejected.
	plain := shapedRoundTripSnapshot(t, 0)
	plain.LinkShaping = nil
	if err := plain.ValidateFor(unshaped); err != nil {
		t.Fatalf("schema-4 shaper without a round trip rejected: %v", err)
	}
	if err := snapshot.ValidateFor(unshaped); err == nil {
		t.Fatal("delayed shaper must be rejected for a plan without a round trip")
	}

	// Carrying the whole round trip on egress (no ifb module) is a valid,
	// named layout.
	egressOnly := shapedRoundTripSnapshot(t, 250_000)
	egressOnly.LinkShaping.IngressMechanism = "none"
	egressOnly.LinkShaping.IngressDevice = ""
	egressOnly.LinkShaping.IngressDelayMicros = 0
	egressOnly.LinkShaping.LiveIngressDelayMicros = 0
	egressOnly.LinkShaping.EgressDelayMicros = 250_000
	egressOnly.LinkShaping.LiveEgressDelayMicros = 250_000
	if err := egressOnly.ValidateFor(link); err != nil {
		t.Fatalf("egress-only layout rejected: %v", err)
	}

	// The shaping contract changing mid-run invalidates the pair.
	changed := shapedRoundTripSnapshot(t, 250_000)
	changed.LinkShaping.NetemLimitPackets = 1
	if _, err := ValidateShaperSnapshotPair(snapshot, changed); err == nil {
		t.Fatal("shaping change during the run must be rejected")
	}
	rttChanged := shapedRoundTripSnapshot(t, 500_000)
	if _, err := ValidateShaperSnapshotPair(snapshot, rttChanged); err == nil {
		t.Fatal("round trip change during the run must be rejected")
	}
}

func TestFetchShaperSnapshotDecodesTheLinkShapingReport(t *testing.T) {
	payload := `{"schema_version":4,"status":"ok","started_at":"2026-09-05T12:00:00Z","configured_egress_bits_per_second":1000000000,"configured_burst_bytes":1048576,"configured_rtt_micros":250000,"link_shaping":{"schema_version":1,"interface":"eth1","ingress_device":"ifb-nntp","egress_mechanism":"netem","ingress_mechanism":"ifb-netem","rtt_micros":250000,"egress_delay_micros":125000,"ingress_delay_micros":125000,"netem_limit_packets":125000,"tcp_wmem":"4096 1048576 134217728","tcp_rmem":"4096 1048576 134217728","kernel_release":"6.8.0","live_egress_delay_micros":125000,"live_ingress_delay_micros":125000},"downstream_connections":0,"active_downstream_connections":0,"downstream_bytes":0,"downstream_source_connections":{},"downstream_source_bytes":{},"downstream_commands":{},"article_requests":0,"repeated_article_requests":0,"distinct_article_requests":0,"execution_lease_id":"` + strings.Repeat("c", 64) + `","execution_lease_acquired_at":"2026-09-05T12:01:00Z","build":{"executable_sha256":"` + strings.Repeat("b", 64) + `","version":"v1","commit":"abc","build_time":"now"}}`
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(payload)), Header: make(http.Header)}, nil
	})}
	snapshot, err := FetchShaperSnapshot(context.Background(), client, "http://shaper.test")
	if err != nil {
		t.Fatal(err)
	}
	link, err := ResolveServerLinkProfile(Link1Gbit, 0, 0, 250_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.ValidateFor(link); err != nil {
		t.Fatalf("decoded schema-4 attestation rejected: %v", err)
	}
	if snapshot.LinkShaping == nil || snapshot.LinkShaping.LiveIngressDelayMicros != 125_000 {
		t.Fatalf("link shaping report not decoded: %+v", snapshot.LinkShaping)
	}
}

// userspaceRoundTripSnapshot is the same shaped snapshot as
// shapedRoundTripSnapshot, but from a shaper on a host with no tc, where the
// proxy carries the round trip itself.
func userspaceRoundTripSnapshot(t *testing.T, rttMicros uint64) ShaperSnapshot {
	t.Helper()
	snapshot := shapedRoundTripSnapshot(t, rttMicros)
	ingress := rttMicros / 2
	egress := rttMicros - ingress
	snapshot.LinkShaping = &ShaperLinkShaping{
		SchemaVersion:          1,
		EgressMechanism:        "userspace-delay",
		IngressMechanism:       "userspace-delay",
		RTTMicros:              rttMicros,
		EgressDelayMicros:      egress,
		IngressDelayMicros:     ingress,
		HandshakeDelayMicros:   rttMicros,
		EgressQueueBytes:       12_500_000,
		IngressQueueBytes:      12_500_000,
		Platform:               "windows/amd64",
		LiveEgressDelayMicros:  egress,
		LiveIngressDelayMicros: ingress,
	}
	return snapshot
}

func TestValidateForAcceptsAUserspaceDelayedLink(t *testing.T) {
	link, err := ResolveServerLinkProfile("1gbit", 0, 0, 250_000)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := userspaceRoundTripSnapshot(t, 250_000)
	if err := snapshot.ValidateFor(link); err != nil {
		t.Fatalf("a userspace-delayed shaper was rejected: %v", err)
	}
	// The observed residency floor sits a little above the configured delay;
	// the same tolerance that covers tc's rounding covers that.
	tolerated := userspaceRoundTripSnapshot(t, 250_000)
	tolerated.LinkShaping.LiveEgressDelayMicros = 125_300
	if err := tolerated.ValidateFor(link); err != nil {
		t.Fatalf("a residency floor 300us above the delay was rejected: %v", err)
	}

	cases := map[string]func(s *ShaperSnapshot){
		"claims a shaped interface":  func(s *ShaperSnapshot) { s.LinkShaping.Interface = "eth1" },
		"claims a qdisc limit":       func(s *ShaperSnapshot) { s.LinkShaping.NetemLimitPackets = 125_000 },
		"claims kernel buffers":      func(s *ShaperSnapshot) { s.LinkShaping.TCPWmem = "4096 1048576 134217728" },
		"claims a kernel release":    func(s *ShaperSnapshot) { s.LinkShaping.KernelRelease = "6.8.0" },
		"mixes mechanisms":           func(s *ShaperSnapshot) { s.LinkShaping.IngressMechanism = "ifb-netem" },
		"charges no handshake":       func(s *ShaperSnapshot) { s.LinkShaping.HandshakeDelayMicros = 0 },
		"charges half a handshake":   func(s *ShaperSnapshot) { s.LinkShaping.HandshakeDelayMicros = 125_000 },
		"has no queue":               func(s *ShaperSnapshot) { s.LinkShaping.EgressQueueBytes = 0 },
		"names no platform":          func(s *ShaperSnapshot) { s.LinkShaping.Platform = "" },
		"delivers early":             func(s *ShaperSnapshot) { s.LinkShaping.LiveEgressDelayMicros = 0 },
		"drifted from its own delay": func(s *ShaperSnapshot) { s.LinkShaping.LiveIngressDelayMicros = 120_000 },
		"unknown mechanism":          func(s *ShaperSnapshot) { s.LinkShaping.EgressMechanism = "dummynet" },
	}
	for name, mutate := range cases {
		broken := userspaceRoundTripSnapshot(t, 250_000)
		mutate(&broken)
		if err := broken.ValidateFor(link); err == nil {
			t.Fatalf("%s: attestation must be rejected", name)
		}
	}
}

func TestValidateForKeepsNetemFreeOfUserspaceFields(t *testing.T) {
	link, err := ResolveServerLinkProfile("1gbit", 0, 0, 250_000)
	if err != nil {
		t.Fatal(err)
	}
	// A netem report that also carries a handshake charge describes two
	// mechanisms at once and is evidence for neither.
	mixed := shapedRoundTripSnapshot(t, 250_000)
	mixed.LinkShaping.HandshakeDelayMicros = 250_000
	if err := mixed.ValidateFor(link); err == nil {
		t.Fatal("a netem attestation carrying userspace fields was accepted")
	}
}
