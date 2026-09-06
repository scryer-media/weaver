package nntpshaper

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseNetemDelayMicrosReadsTCOutput(t *testing.T) {
	cases := map[string]struct {
		raw   string
		want  uint64
		found bool
	}{
		"whole milliseconds":  {"qdisc netem 1: root refcnt 19 limit 1000000 delay 125ms\n", 125_000, true},
		"fractional":          {"qdisc netem 10: parent 1:1 limit 20000 delay 1.5ms\n", 1_500, true},
		"microseconds":        {"qdisc netem 1: root limit 20000 delay 500us\n", 500, true},
		"seconds":             {"qdisc netem 1: root limit 20000 delay 1s\n", 1_000_000, true},
		"with stats":          {"qdisc netem 1: root refcnt 2 limit 20000 delay 250ms\n Sent 0 bytes 0 pkt (dropped 0, overlimits 0 requeues 0)\n backlog 0b 0p requeues 0\n", 250_000, true},
		"no netem":            {"qdisc noqueue 0: root refcnt 2\n", 0, false},
		"netem without delay": {"qdisc netem 1: root limit 1000\n", 0, false},
		"empty":               {"", 0, false},
	}
	for name, tc := range cases {
		got, found := ParseNetemDelayMicros(tc.raw)
		if got != tc.want || found != tc.found {
			t.Fatalf("%s: (%d, %v), want (%d, %v)", name, got, found, tc.want, tc.found)
		}
	}
}

func writeLinkReport(t *testing.T, mutate func(map[string]any)) string {
	t.Helper()
	report := map[string]any{
		"schema_version": 1, "interface": "eth1", "ingress_device": "ifb-nntp",
		"egress_mechanism": "netem", "ingress_mechanism": "ifb-netem",
		"rtt_micros": 250_000, "egress_delay_micros": 125_000, "ingress_delay_micros": 125_000,
		"netem_limit_packets": 125_000, "tcp_wmem": "4096 1048576 134217728", "tcp_rmem": "4096 1048576 134217728",
		"kernel_release": "6.8.0", "live_egress_delay_micros": 0, "live_ingress_delay_micros": 0,
	}
	if mutate != nil {
		mutate(report)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "link.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadLinkShapingReportChecksTheDeclaredContract(t *testing.T) {
	report, err := LoadLinkShapingReport(writeLinkReport(t, nil), 250_000)
	if err != nil {
		t.Fatal(err)
	}
	if report.Interface != "eth1" || report.EgressDelayMicros != 125_000 || report.IngressMechanism != LinkIngressIFBNetem {
		t.Fatalf("unexpected report: %+v", report)
	}
	egressOnly, err := LoadLinkShapingReport(writeLinkReport(t, func(m map[string]any) {
		m["ingress_mechanism"] = "none"
		m["ingress_device"] = ""
		m["ingress_delay_micros"] = 0
		m["egress_delay_micros"] = 250_000
	}), 250_000)
	if err != nil {
		t.Fatalf("egress-only layout rejected: %v", err)
	}
	if egressOnly.IngressMechanism != LinkIngressNone {
		t.Fatalf("unexpected egress-only report: %+v", egressOnly)
	}
	cases := map[string]func(map[string]any){
		"round trip disagrees":  func(m map[string]any) { m["rtt_micros"] = 500_000 },
		"split does not add up": func(m map[string]any) { m["ingress_delay_micros"] = 100_000 },
		"no interface":          func(m map[string]any) { m["interface"] = "" },
		"egress not netem":      func(m map[string]any) { m["egress_mechanism"] = "tbf" },
		"unknown ingress":       func(m map[string]any) { m["ingress_mechanism"] = "police" },
		"ifb without device":    func(m map[string]any) { m["ingress_device"] = "" },
		"limit unset":           func(m map[string]any) { m["netem_limit_packets"] = 0 },
		"wrong schema":          func(m map[string]any) { m["schema_version"] = 2 },
		"live field pre-filled": func(m map[string]any) { m["live_egress_delay_micros"] = 125_000 },
		"unknown field":         func(m map[string]any) { m["jitter_micros"] = 5 },
	}
	for name, mutate := range cases {
		if _, err := LoadLinkShapingReport(writeLinkReport(t, mutate), 250_000); err == nil {
			t.Fatalf("%s: report must be rejected", name)
		}
	}
	if _, err := LoadLinkShapingReport(writeLinkReport(t, nil), 500_000); err == nil {
		t.Fatal("report for a different configured round trip must be rejected")
	}
	if _, err := LoadLinkShapingReport(filepath.Join(t.TempDir(), "missing.json"), 250_000); err == nil {
		t.Fatal("missing report must be rejected")
	}
}

func TestTCLiveDelaysReadsEachShapedDevice(t *testing.T) {
	original := tcQdiscShow
	t.Cleanup(func() { tcQdiscShow = original })
	tcQdiscShow = func(device string) ([]byte, error) {
		switch device {
		case "eth1":
			return []byte("qdisc netem 1: root refcnt 19 limit 125000 delay 125ms\n"), nil
		case "ifb-nntp":
			return []byte("qdisc netem 1: root refcnt 2 limit 125000 delay 125ms\n"), nil
		}
		return []byte("Cannot find device"), errors.New("exit status 1")
	}
	report := LinkShapingReport{Interface: "eth1", IngressDevice: "ifb-nntp", IngressMechanism: LinkIngressIFBNetem}
	egress, ingress, err := TCLiveDelays(report)
	if err != nil || egress != 125_000 || ingress != 125_000 {
		t.Fatalf("live delays = (%d, %d, %v)", egress, ingress, err)
	}
	egressOnly := LinkShapingReport{Interface: "eth1", IngressMechanism: LinkIngressNone}
	egress, ingress, err = TCLiveDelays(egressOnly)
	if err != nil || egress != 125_000 || ingress != 0 {
		t.Fatalf("egress-only live delays = (%d, %d, %v)", egress, ingress, err)
	}
	if _, _, err := TCLiveDelays(LinkShapingReport{Interface: "eth9", IngressMechanism: LinkIngressNone}); err == nil {
		t.Fatal("missing device must fail the probe")
	}
	tcQdiscShow = func(string) ([]byte, error) { return []byte("qdisc noqueue 0: root refcnt 2\n"), nil }
	if _, _, err := TCLiveDelays(egressOnly); err == nil {
		t.Fatal("a device without netem must fail the probe")
	}
}

func TestSnapshotCarriesTheRoundTripWithLiveReadings(t *testing.T) {
	started := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	report, err := LoadLinkShapingReport(writeLinkReport(t, nil), 250_000)
	if err != nil {
		t.Fatal(err)
	}
	probeErr := error(nil)
	attestation := NewAttestation(AttestationConfig{
		EgressBitsPerSecond: 1_000_000_000,
		BurstBytes:          1 << 20,
		RTTMicros:           250_000,
		LinkShaping:         report,
		LiveDelays: func(r LinkShapingReport) (uint64, uint64, error) {
			if probeErr != nil {
				return 0, 0, probeErr
			}
			return r.EgressDelayMicros, r.IngressDelayMicros, nil
		},
		Build:     BuildIdentity{ExecutableSHA256: "f00d", Version: "v1", Commit: "abc", BuildTime: "now"},
		StartedAt: started,
	})
	request := httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	response := httptest.NewRecorder()
	attestation.Handler().ServeHTTP(response, request)
	body := response.Body.String()
	var snapshot Snapshot
	if err := json.NewDecoder(strings.NewReader(body)).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.SchemaVersion != 4 || snapshot.Status != "ok" || snapshot.ConfiguredRTTMicros != 250_000 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if snapshot.LinkShaping == nil || snapshot.LinkShaping.LiveEgressDelayMicros != 125_000 || snapshot.LinkShaping.LiveIngressDelayMicros != 125_000 || snapshot.LinkShaping.LiveError != "" {
		t.Fatalf("live readings missing: %+v", snapshot.LinkShaping)
	}
	if report.LiveEgressDelayMicros != 0 {
		t.Fatal("snapshot must not write live readings back into the loaded report")
	}
	if !strings.Contains(body, `"link_shaping":{`) {
		t.Fatalf("link shaping report not serialized: %s", body)
	}

	probeErr = errors.New("tc: not found")
	degraded := attestation.Snapshot()
	if degraded.Status != "degraded" || degraded.LinkShaping == nil || degraded.LinkShaping.LiveError == "" {
		t.Fatalf("a failed probe must degrade the snapshot: %+v", degraded)
	}

	plain := NewAttestation(AttestationConfig{Build: BuildIdentity{ExecutableSHA256: "f00d"}, StartedAt: started}).Snapshot()
	if plain.Status != "ok" || plain.ConfiguredRTTMicros != 0 || plain.LinkShaping != nil {
		t.Fatalf("no round trip must mean no report: %+v", plain)
	}
}
