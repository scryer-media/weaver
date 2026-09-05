package benchmark

import (
	"strings"
	"testing"
	"time"
)

// The fixtures below are the shapes `tc` and the kernel actually print. They
// are deliberately verbatim, including tc's three-significant-digit rates and
// its binary byte suffixes, because the parser exists to survive exactly that.
const egressQdiscFixture = `qdisc tbf 1: root refcnt 2 rate 1Gbit burst 1Mb lat 200ms
 Sent 5368709120 bytes 3900000 pkt (dropped 0, overlimits 12 requeues 0)
 backlog 0b 0p requeues 0
qdisc netem 10: parent 1:1 limit 1000 delay 500us
 Sent 5368709120 bytes 3900000 pkt (dropped 0, overlimits 0 requeues 0)
 backlog 0b 0p requeues 0
qdisc ingress ffff: parent ffff:fff1 ----------------
 Sent 1073741824 bytes 900000 pkt (dropped 0, overlimits 0 requeues 0)
 backlog 0b 0p requeues 0`

const ingressQdiscFixture = `qdisc tbf 1: root refcnt 2 rate 1Gbit burst 1Mb lat 200ms
 Sent 1073741824 bytes 900000 pkt (dropped 0, overlimits 5 requeues 0)
 backlog 0b 0p requeues 0
qdisc netem 10: parent 1:1 limit 1000 delay 500us
 Sent 1073741824 bytes 900000 pkt (dropped 0, overlimits 0 requeues 0)
 backlog 0b 0p requeues 0`

const ingressFilterFixture = `filter parent ffff: protocol all pref 1 u32 chain 0
filter parent ffff: protocol all pref 1 u32 chain 0 fh 800: ht divisor 1
filter parent ffff: protocol all pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 flowid :1 not_in_hw
  match 00000000/00000000 at 0
	action order 1:  police 0x1 rate 1Gbit burst 1Mb mtu 2Kb action drop overhead 0b
	ref 1 bind 1 installed 5 sec used 5 sec

	Sent 1073741824 bytes 900000 pkt (dropped 0, overlimits 0 requeues 0)
	backlog 0b 0p requeues 0`

const nfsdCountersFixture = `rc 0 12 340
fh 0 0 0 0 0
io 5368709120 1073741824
th 8 0 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000 0.000`

func testShaperReport() StorageShaperReport {
	return StorageShaperReport{
		SchemaVersion:     storageShaperReportSchema,
		Interface:         "eth0",
		IngressDevice:     "ifb-nfs",
		EgressMechanism:   storageEgressTBF,
		IngressMechanism:  storageEgressIFB,
		LinkBitsPerSecond: 1_000_000_000,
		LinkBurstBytes:    1 << 20,
		RTTMicros:         1_000,
		OneWayDelayMicros: 500,
		ExportOptions:     nfsExportOptions,
		NFSVersions:       "-2 -3 +4 +4.1 +4.2",
		KernelRelease:     "6.1.0-test",
	}
}

func testNFSProfile(t *testing.T) StorageProfile {
	t.Helper()
	profile, err := ResolveStorageProfile(StorageProfileNFSComplete, NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestParseTCOutputUsesTheUnitsTCPrints(t *testing.T) {
	entries := parseQdiscs(egressQdiscFixture)
	if len(entries) != 3 {
		t.Fatalf("expected three qdisc stanzas, got %d", len(entries))
	}
	if !entries[0].Root || entries[0].Kind != "tbf" || entries[0].RateBits != 1_000_000_000 {
		t.Fatalf("root tbf misparsed: %#v", entries[0])
	}
	if entries[0].BurstBytes != 1<<20 {
		t.Fatalf("tc prints burst in binary units; got %d", entries[0].BurstBytes)
	}
	if entries[0].SentBytes != 5_368_709_120 {
		t.Fatalf("root byte counter misparsed: %d", entries[0].SentBytes)
	}
	if entries[1].Kind != "netem" || entries[1].DelayMicros != 500 {
		t.Fatalf("netem delay misparsed: %#v", entries[1])
	}
	if entries[2].Kind != "ingress" || entries[2].SentBytes != 1_073_741_824 {
		t.Fatalf("ingress qdisc misparsed: %#v", entries[2])
	}

	rate, burst := parseTCPolice(ingressFilterFixture)
	if rate != 1_000_000_000 || burst != 1<<20 {
		t.Fatalf("policing filter misparsed: rate %d burst %d", rate, burst)
	}

	// tc prints 2.5 Gbit/s with three significant digits, not as a whole
	// number of bits, which is why the comparison is a tolerance and not an
	// equality.
	if got := parseTaggedRate(strings.Fields("rate 2.5Gbit"), "rate"); got != 2_500_000_000 {
		t.Fatalf("2.5Gbit misparsed as %d", got)
	}
	if got := parseTaggedRate(strings.Fields("rate 100Mbit"), "rate"); got != 100_000_000 {
		t.Fatalf("100Mbit misparsed as %d", got)
	}
	if got := parseTaggedDuration(strings.Fields("delay 1.0ms"), "delay"); got != 1_000 {
		t.Fatalf("1.0ms misparsed as %dus", got)
	}
}

// capturedQdiscFixture and capturedPoliceFixture are verbatim output from
// iproute2 6.1 in a container, kept because that build prints a byte count
// where the documented examples print `1Mb`, and rounds a 500us netem delay to
// 499us. Both are why the assertions are tolerances over parsed numbers rather
// than string comparisons.
const capturedQdiscFixture = `qdisc tbf 1: root refcnt 19 rate 1Gbit burst 1048500b lat 200ms
 Sent 12345678 bytes 900 pkt (dropped 0, overlimits 0 requeues 0)
 backlog 0b 0p requeues 0
qdisc netem 10: parent 1:1 limit 1000 delay 499us
 Sent 12345678 bytes 900 pkt (dropped 0, overlimits 0 requeues 0)
 backlog 0b 0p requeues 0
qdisc ingress ffff: parent ffff:fff1 ----------------
 Sent 87654321 bytes 700 pkt (dropped 0, overlimits 0 requeues 0)
 backlog 0b 0p requeues 0`

const capturedPoliceFixture = `filter protocol all pref 1 u32 chain 0
filter protocol all pref 1 u32 chain 0 fh 800: ht divisor 1
filter protocol all pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 *flowid :1 not_in_hw (rule hit 0 success 0)
  match 00000000/00000000 at 0 (success 0 )
 police 0x1 rate 1Gbit burst 1048375b mtu 2Kb action drop overhead 0b
	ref 1 bind 1

 Sent 87654321 bytes 700 pkts (dropped 0, overlimits 0)`

func TestParseAcceptsRealIproute2Output(t *testing.T) {
	profile := testNFSProfile(t)
	report := testShaperReport()
	report.IngressMechanism = storageEgressPolice
	report.IngressDevice = ""
	counters, err := buildStorageCounters(time.Now().UTC(), report, capturedQdiscFixture, "", capturedPoliceFixture, nfsdCountersFixture)
	if err != nil {
		t.Fatal(err)
	}
	if counters.EgressRateBits != 1_000_000_000 || counters.IngressRateBits != 1_000_000_000 {
		t.Fatalf("captured tc output should yield the shaped rate in both directions: %#v", counters)
	}
	if counters.EgressBytes != 12_345_678 || counters.IngressBytes != 87_654_321 {
		t.Fatalf("captured byte counters misparsed: %#v", counters)
	}
	// A 500us netem delay comes back as 499us on this build; the assertion has
	// to survive that without loosening into meaninglessness.
	if counters.EgressDelayMicros != 499 {
		t.Fatalf("captured netem delay misparsed: %d", counters.EgressDelayMicros)
	}
	if err := counters.validateCountersFor(profile, report); err != nil {
		t.Fatalf("real tc output for a correctly shaped link should validate: %v", err)
	}
}

func TestParseNFSDIORequiresAnIOLine(t *testing.T) {
	read, written, err := parseNFSDIO(nfsdCountersFixture)
	if err != nil {
		t.Fatal(err)
	}
	if read != 5_368_709_120 || written != 1_073_741_824 {
		t.Fatalf("nfsd io misparsed: read %d written %d", read, written)
	}
	if _, _, err := parseNFSDIO("rc 0 0 0\nfh 0 0 0 0 0"); err == nil {
		t.Fatal("nfsd counters without an io line should be rejected")
	}
}

func TestBuildStorageCountersSelectsTheReportedMechanism(t *testing.T) {
	report := testShaperReport()
	counters, err := buildStorageCounters(time.Now().UTC(), report, egressQdiscFixture, ingressQdiscFixture, "", nfsdCountersFixture)
	if err != nil {
		t.Fatal(err)
	}
	if counters.EgressBytes != 5_368_709_120 || counters.IngressBytes != 1_073_741_824 {
		t.Fatalf("ifb counters misparsed: %#v", counters)
	}
	if counters.EgressDelayMicros != 500 || counters.IngressDelayMicros != 500 {
		t.Fatalf("ifb path must observe the fixed delay in both directions: %#v", counters)
	}
	if counters.EgressQdiscRaw == "" || counters.NFSDCountersRaw == "" {
		t.Fatal("raw command output must be retained beside the parsed values")
	}

	policed := report
	policed.IngressMechanism = storageEgressPolice
	policed.IngressDevice = ""
	counters, err = buildStorageCounters(time.Now().UTC(), policed, egressQdiscFixture, "", ingressFilterFixture, nfsdCountersFixture)
	if err != nil {
		t.Fatal(err)
	}
	if counters.IngressRateBits != 1_000_000_000 || counters.IngressBytes != 1_073_741_824 {
		t.Fatalf("policed counters misparsed: %#v", counters)
	}
	if counters.IngressDelayMicros != 0 {
		t.Fatal("policing cannot delay a packet and must not claim to")
	}

	unknown := report
	unknown.IngressMechanism = "guess"
	if _, err := buildStorageCounters(time.Now().UTC(), unknown, egressQdiscFixture, "", "", nfsdCountersFixture); err == nil {
		t.Fatal("an unrecognised client-to-server mechanism should not produce counters")
	}
}

func TestShaperReportMustMatchThePlannedProfile(t *testing.T) {
	profile := testNFSProfile(t)
	if err := testShaperReport().validateFor(profile); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*StorageShaperReport){
		"wrong schema":   func(r *StorageShaperReport) { r.SchemaVersion = 2 },
		"wrong rate":     func(r *StorageShaperReport) { r.LinkBitsPerSecond = 100_000_000 },
		"wrong burst":    func(r *StorageShaperReport) { r.LinkBurstBytes = 1 << 10 },
		"wrong rtt":      func(r *StorageShaperReport) { r.RTTMicros = 2_000 },
		"wrong one way":  func(r *StorageShaperReport) { r.OneWayDelayMicros = 1_000 },
		"wrong export":   func(r *StorageShaperReport) { r.ExportOptions = "rw" },
		"wrong egress":   func(r *StorageShaperReport) { r.EgressMechanism = "htb" },
		"wrong ingress":  func(r *StorageShaperReport) { r.IngressMechanism = "none" },
		"no interface":   func(r *StorageShaperReport) { r.Interface = "" },
		"no kernel":      func(r *StorageShaperReport) { r.KernelRelease = "" },
		"unnamed ifb":    func(r *StorageShaperReport) { r.IngressDevice = "" },
		"unlimited link": func(r *StorageShaperReport) { r.LinkBitsPerSecond = 0 },
	} {
		report := testShaperReport()
		mutate(&report)
		if err := report.validateFor(profile); err == nil {
			t.Fatalf("%s should not validate against the planned profile", name)
		}
	}
}

func TestDecodeStorageShaperReportRejectsUnknownFields(t *testing.T) {
	if _, err := decodeStorageShaperReport(`{"schema_version":1,"surprise":true}`); err == nil {
		t.Fatal("an unknown field in the container's report should fail closed")
	}
}

func testAttestation(t *testing.T) StorageAttestation {
	t.Helper()
	profile := testNFSProfile(t)
	report := testShaperReport()
	before, err := buildStorageCounters(time.Now().UTC().Add(-time.Minute), report,
		strings.ReplaceAll(egressQdiscFixture, "5368709120", "1000"),
		strings.ReplaceAll(ingressQdiscFixture, "1073741824", "500"), "",
		"io 1000 500")
	if err != nil {
		t.Fatal(err)
	}
	after, err := buildStorageCounters(time.Now().UTC(), report, egressQdiscFixture, ingressQdiscFixture, "", nfsdCountersFixture)
	if err != nil {
		t.Fatal(err)
	}
	egress, ingress, read, written, err := storageDeltas(before, after)
	if err != nil {
		t.Fatal(err)
	}
	return StorageAttestation{
		SchemaVersion:        1,
		Profile:              profile,
		Container:            "bench-nfs",
		Network:              "bench_storage",
		ServerAddress:        "172.31.0.2",
		ExportDevice:         ":/run-0001-abcdef12/complete",
		ExportOptionsRaw:     "/export <world>(sync,wdelay,hide,no_subtree_check,fsid=0,sec=sys,rw,insecure,no_root_squash,no_all_squash)",
		ClientMountLine:      "172.31.0.2:/run-0001-abcdef12/complete /mnt/complete nfs4 rw,relatime,vers=4.1,rsize=1048576,wsize=1048576 0 0",
		HelperImage:          DefaultNFSImage,
		LeaseID:              strings.Repeat("a1b2c3d4", 8),
		LeaseAcquiredAt:      time.Now().UTC().Add(-time.Minute),
		Shaper:               report,
		Before:               before,
		After:                after,
		EgressBytes:          egress,
		IngressBytes:         ingress,
		ServerReadBytes:      read,
		ServerWrittenBytes:   written,
		VerificationStrategy: storageVerificationStrategy,
		CPUAccountingCaveat:  storageCPUCaveat,
	}
}

func TestStorageAttestationAcceptsAShapedRun(t *testing.T) {
	attestation := testAttestation(t)
	if err := attestation.Validate(); err != nil {
		t.Fatal(err)
	}
	summary := attestation.Summary()
	for _, expected := range []string{"nfs-complete", "nas-1gbit", "ifb-tbf+netem"} {
		if !strings.Contains(summary, expected) {
			t.Fatalf("attestation summary %q lacks %q", summary, expected)
		}
	}
}

func TestStorageAttestationRejectsUnprovenRuns(t *testing.T) {
	for name, mutate := range map[string]func(*StorageAttestation){
		"unshaped qdisc": func(a *StorageAttestation) {
			a.After.EgressBytes = a.Before.EgressBytes
			a.EgressBytes = 0
		},
		"no client traffic": func(a *StorageAttestation) {
			a.After.IngressBytes = a.Before.IngressBytes
			a.IngressBytes = 0
		},
		"trivial server io": func(a *StorageAttestation) {
			a.After.ServerReadBytes = a.Before.ServerReadBytes + 16
			a.After.ServerWrittenBytes = a.Before.ServerWrittenBytes
			a.ServerReadBytes = 16
			a.ServerWrittenBytes = 0
		},
		"restated deltas": func(a *StorageAttestation) { a.EgressBytes = 1 },
		"counters ran backwards": func(a *StorageAttestation) {
			a.Before.ServerReadBytes = a.After.ServerReadBytes + 1
		},
		"shaper changed mid run": func(a *StorageAttestation) { a.After.EgressRateBits = 100_000_000 },
		"no lease":               func(a *StorageAttestation) { a.LeaseID = "short" },
		"no lease time":          func(a *StorageAttestation) { a.LeaseAcquiredAt = time.Time{} },
		"no server identity":     func(a *StorageAttestation) { a.ServerAddress = "" },
		"not nfsv4":              func(a *StorageAttestation) { a.ClientMountLine = strings.ReplaceAll(a.ClientMountLine, "nfs4", "nfs") },
		"not 4.1": func(a *StorageAttestation) {
			a.ClientMountLine = strings.ReplaceAll(a.ClientMountLine, "vers=4.1", "vers=4.0")
		},
		"no export evidence":     func(a *StorageAttestation) { a.ExportOptionsRaw = "/export <world>(rw)" },
		"no strategy":            func(a *StorageAttestation) { a.VerificationStrategy = "trust me" },
		"no cpu caveat":          func(a *StorageAttestation) { a.CPUAccountingCaveat = "" },
		"local profile":          func(a *StorageAttestation) { a.Profile = DefaultStorageProfile() },
		"wrong schema":           func(a *StorageAttestation) { a.SchemaVersion = 2 },
		"snapshots out of order": func(a *StorageAttestation) { a.Before.CapturedAt = a.After.CapturedAt.Add(time.Minute) },
	} {
		attestation := testAttestation(t)
		mutate(&attestation)
		if err := attestation.Validate(); err == nil {
			t.Fatalf("%s should not be publishable storage evidence", name)
		}
	}
}

func TestStorageCountersMustMatchTheDeclaredShape(t *testing.T) {
	profile := testNFSProfile(t)
	report := testShaperReport()
	counters, err := buildStorageCounters(time.Now().UTC(), report, egressQdiscFixture, ingressQdiscFixture, "", nfsdCountersFixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := counters.validateCountersFor(profile, report); err != nil {
		t.Fatal(err)
	}
	unshaped := counters
	unshaped.EgressRateBits = 0
	if err := unshaped.validateCountersFor(profile, report); err == nil {
		t.Fatal("an unshaped server-to-client path should be rejected")
	}
	// 1% is the tolerance for tc's three-significant-digit printing; 5% is a
	// different link.
	near := counters
	near.EgressRateBits = 1_005_000_000
	if err := near.validateCountersFor(profile, report); err != nil {
		t.Fatalf("a rate inside tc's print rounding should be accepted: %v", err)
	}
	far := counters
	far.EgressRateBits = 1_050_000_000
	if err := far.validateCountersFor(profile, report); err == nil {
		t.Fatal("a rate outside the print tolerance should be rejected")
	}
	policed := report
	policed.IngressMechanism = storageEgressPolice
	delayed := counters
	if err := delayed.validateCountersFor(profile, policed); err == nil {
		t.Fatal("a policed path that claims a delay should be rejected")
	}
}
