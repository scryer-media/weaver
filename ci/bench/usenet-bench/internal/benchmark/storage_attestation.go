package benchmark

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// storageShaperReportSchema is the contract between the NFS container's
// entrypoint and this controller. Bump it in both places together.
const storageShaperReportSchema = 1

const (
	storageEgressIFB    = "ifb-tbf+netem"
	storageEgressPolice = "ingress-police"
	storageEgressTBF    = "tbf+netem"
)

// minimumAttestedStorageBytes is the smallest byte delta that still proves the
// client's storage traffic crossed the shaped link. A run below it means the
// mount was satisfied somewhere other than the export and is rejected rather
// than published.
const minimumAttestedStorageBytes = 1 << 20

// storageVerificationStrategy names how output on the export is verified.
// Reading the export from the controller would need a host-side root mount, so
// the harness runs its own verifier inside a pinned helper container. That
// helper takes the NFS server's export volume with `--volumes-from` rather than
// mounting the export over NFS, so verification reads the run's output from the
// server's own filesystem and never re-crosses the shaped link. NFS
// close-to-open semantics make that view complete: the harness reads only after
// the client has reported terminal status and closed its files, which flushes
// its writes to the server, and a local reader sees the server's page cache, so
// no `sync` is required. The logic is the harness binary, not the product's.
const storageVerificationStrategy = "helper_container_server_side_harness_verify_output"

// storageCPUCaveat is copied into every NFS result. NFS client work runs in the
// host kernel on behalf of the mount, not inside the client container's
// cgroup, so the cgroup CPU counter under-reports these lanes.
const storageCPUCaveat = "cpu_time excludes NFS client kernel time for the nfs storage profiles"

// StorageShaperReport is written by the NFS container entrypoint after it has
// applied its queueing disciplines. It states what the container configured and
// which client-to-server mechanism it could actually use; the controller
// cross-checks every value against live tc output before accepting a run.
type StorageShaperReport struct {
	SchemaVersion     int    `json:"schema_version"`
	Interface         string `json:"interface"`
	IngressDevice     string `json:"ingress_device"`
	EgressMechanism   string `json:"egress_mechanism"`
	IngressMechanism  string `json:"ingress_mechanism"`
	LinkBitsPerSecond uint64 `json:"link_bits_per_second"`
	LinkBurstBytes    uint64 `json:"link_burst_bytes"`
	RTTMicros         uint64 `json:"rtt_micros"`
	OneWayDelayMicros uint64 `json:"one_way_delay_micros"`
	ExportOptions     string `json:"export_options"`
	NFSVersions       string `json:"nfs_versions"`
	KernelRelease     string `json:"kernel_release"`
}

// StorageCounters is one boundary snapshot of the NFS server container. The
// raw command output is retained beside the parsed values so a reviewer never
// has to trust this parser.
type StorageCounters struct {
	CapturedAt         time.Time `json:"captured_at"`
	EgressQdiscRaw     string    `json:"egress_qdisc_raw"`
	IngressQdiscRaw    string    `json:"ingress_qdisc_raw"`
	IngressFilterRaw   string    `json:"ingress_filter_raw"`
	NFSDCountersRaw    string    `json:"nfsd_counters_raw"`
	EgressBytes        uint64    `json:"egress_bytes"`
	IngressBytes       uint64    `json:"ingress_bytes"`
	ServerReadBytes    uint64    `json:"server_read_bytes"`
	ServerWrittenBytes uint64    `json:"server_written_bytes"`
	EgressRateBits     uint64    `json:"egress_rate_bits_per_second"`
	EgressDelayMicros  uint64    `json:"egress_delay_micros"`
	IngressRateBits    uint64    `json:"ingress_rate_bits_per_second"`
	IngressDelayMicros uint64    `json:"ingress_delay_micros"`
}

// StorageAttestation is the storage counterpart of the NNTP shaper snapshots.
// It proves that a shaped export existed, that this run held it exclusively,
// and that the client's storage bytes actually crossed the shaped link.
type StorageAttestation struct {
	SchemaVersion        int                 `json:"schema_version"`
	Profile              StorageProfile      `json:"profile"`
	Container            string              `json:"container"`
	Network              string              `json:"network"`
	ServerAddress        string              `json:"server_address"`
	ExportDevice         string              `json:"export_device"`
	ExportOptionsRaw     string              `json:"export_options_raw"`
	ClientMountLine      string              `json:"client_mount_line"`
	HelperImage          string              `json:"helper_image"`
	LeaseID              string              `json:"lease_id"`
	LeaseAcquiredAt      time.Time           `json:"lease_acquired_at"`
	Shaper               StorageShaperReport `json:"shaper"`
	Before               StorageCounters     `json:"before"`
	After                StorageCounters     `json:"after"`
	EgressBytes          uint64              `json:"egress_bytes"`
	IngressBytes         uint64              `json:"ingress_bytes"`
	ServerReadBytes      uint64              `json:"server_read_bytes"`
	ServerWrittenBytes   uint64              `json:"server_written_bytes"`
	VerificationStrategy string              `json:"verification_strategy"`
	CPUAccountingCaveat  string              `json:"cpu_accounting_caveat"`
}

// Summary is the one-line run output. It is deliberately short enough to read
// in a scrolling suite log and complete enough to spot an unshaped run.
func (a StorageAttestation) Summary() string {
	return fmt.Sprintf(
		"storage %s (%s) %d bit/s burst %d B rtt %dus egress=%s ingress=%s bytes out/in %d/%d nfsd r/w %d/%d",
		a.Profile.ID, a.Profile.NFSLinkID, a.Profile.LinkBitsPerSecond, a.Profile.LinkBurstBytes, a.Profile.RTTMicros,
		a.Shaper.EgressMechanism, a.Shaper.IngressMechanism,
		a.EgressBytes, a.IngressBytes, a.ServerReadBytes, a.ServerWrittenBytes,
	)
}

// rateTolerance is the fraction by which live tc output may differ from the
// declared profile. tc prints rates with three significant digits, so an exact
// string comparison would reject correct configurations.
const rateTolerance = 0.01

func (r StorageShaperReport) validateFor(profile StorageProfile) error {
	if r.SchemaVersion != storageShaperReportSchema {
		return fmt.Errorf("NFS shaper report has unsupported schema %d", r.SchemaVersion)
	}
	if r.LinkBitsPerSecond != profile.LinkBitsPerSecond || r.LinkBurstBytes != profile.LinkBurstBytes || r.RTTMicros != profile.RTTMicros {
		return fmt.Errorf("NFS shaper reports %d bit/s burst %d rtt %dus, plan declares %d/%d/%dus",
			r.LinkBitsPerSecond, r.LinkBurstBytes, r.RTTMicros, profile.LinkBitsPerSecond, profile.LinkBurstBytes, profile.RTTMicros)
	}
	if r.OneWayDelayMicros != profile.OneWayDelayMicros() {
		return fmt.Errorf("NFS shaper reports a %dus one-way delay, plan declares %dus", r.OneWayDelayMicros, profile.OneWayDelayMicros())
	}
	if r.ExportOptions != profile.ExportOptions {
		return fmt.Errorf("NFS export options %q do not match the declared %q", r.ExportOptions, profile.ExportOptions)
	}
	if r.EgressMechanism != storageEgressTBF {
		return fmt.Errorf("NFS shaper used unsupported server-to-client mechanism %q", r.EgressMechanism)
	}
	if r.IngressMechanism != storageEgressIFB && r.IngressMechanism != storageEgressPolice {
		return fmt.Errorf("NFS shaper used unsupported client-to-server mechanism %q", r.IngressMechanism)
	}
	if strings.TrimSpace(r.Interface) == "" || strings.TrimSpace(r.KernelRelease) == "" {
		return fmt.Errorf("NFS shaper report lacks an interface or host kernel release")
	}
	if r.IngressMechanism == storageEgressIFB && strings.TrimSpace(r.IngressDevice) == "" {
		return fmt.Errorf("NFS shaper claims an ifb redirect without naming the device")
	}
	return nil
}

// validateCountersFor asserts that what tc reports right now is what the plan
// declared. Both directions are checked; the fixed delay is only asserted for
// the ifb mechanism, because ingress policing cannot delay a packet.
func (c StorageCounters) validateCountersFor(profile StorageProfile, report StorageShaperReport) error {
	if err := withinRate("server-to-client", c.EgressRateBits, profile.LinkBitsPerSecond); err != nil {
		return err
	}
	if err := withinRate("client-to-server", c.IngressRateBits, profile.LinkBitsPerSecond); err != nil {
		return err
	}
	if err := withinDelay("server-to-client", c.EgressDelayMicros, profile.OneWayDelayMicros()); err != nil {
		return err
	}
	if report.IngressMechanism == storageEgressIFB {
		if err := withinDelay("client-to-server", c.IngressDelayMicros, profile.OneWayDelayMicros()); err != nil {
			return err
		}
	} else if c.IngressDelayMicros != 0 {
		return fmt.Errorf("policed client-to-server path unexpectedly reports a %dus delay", c.IngressDelayMicros)
	}
	if c.CapturedAt.IsZero() {
		return fmt.Errorf("storage counters lack a capture time")
	}
	return nil
}

func withinRate(direction string, observed, declared uint64) error {
	if declared == 0 {
		return fmt.Errorf("storage profile declares no %s rate", direction)
	}
	if observed == 0 {
		return fmt.Errorf("tc reports no %s rate limit; the NFS link is unshaped", direction)
	}
	difference := float64(observed) - float64(declared)
	if difference < 0 {
		difference = -difference
	}
	if difference/float64(declared) > rateTolerance {
		return fmt.Errorf("tc reports a %s rate of %d bit/s, plan declares %d bit/s", direction, observed, declared)
	}
	return nil
}

func withinDelay(direction string, observed, declared uint64) error {
	if declared == 0 {
		return fmt.Errorf("storage profile declares no %s delay", direction)
	}
	difference := int64(observed) - int64(declared)
	if difference < 0 {
		difference = -difference
	}
	// tc rounds a sub-millisecond delay to the scheduler's resolution, so
	// allow one microsecond of print rounding on top of the rate tolerance.
	if float64(difference) > float64(declared)*rateTolerance+1 {
		return fmt.Errorf("tc reports a %s delay of %dus, plan declares %dus", direction, observed, declared)
	}
	return nil
}

// Validate accepts a completed attestation. It is called both when the run
// finishes and again by the summarizer, so a published artifact can never
// contain storage evidence that would not be accepted today.
func (a StorageAttestation) Validate() error {
	if a.SchemaVersion != 1 {
		return fmt.Errorf("storage attestation has unsupported schema version %d", a.SchemaVersion)
	}
	if err := a.Profile.Validate(); err != nil {
		return err
	}
	if !a.Profile.usesNFS() {
		return fmt.Errorf("storage attestation describes non-NFS profile %q", a.Profile.ID)
	}
	if err := a.Shaper.validateFor(a.Profile); err != nil {
		return err
	}
	if err := a.Before.validateCountersFor(a.Profile, a.Shaper); err != nil {
		return fmt.Errorf("storage attestation before snapshot: %w", err)
	}
	if err := a.After.validateCountersFor(a.Profile, a.Shaper); err != nil {
		return fmt.Errorf("storage attestation after snapshot: %w", err)
	}
	if len(a.LeaseID) != 64 || strings.Trim(a.LeaseID, "0123456789abcdef") != "" || a.LeaseAcquiredAt.IsZero() {
		return fmt.Errorf("storage attestation lacks an exclusive NFS execution lease")
	}
	if strings.TrimSpace(a.Container) == "" || strings.TrimSpace(a.Network) == "" || strings.TrimSpace(a.ServerAddress) == "" || strings.TrimSpace(a.ExportDevice) == "" {
		return fmt.Errorf("storage attestation lacks NFS server identity")
	}
	if !strings.Contains(a.ClientMountLine, "nfs4") {
		return fmt.Errorf("storage attestation has no NFSv4 client mount evidence")
	}
	if !strings.Contains(a.ClientMountLine, "vers=4.1") {
		return fmt.Errorf("storage attestation client mount did not negotiate NFS 4.1: %q", a.ClientMountLine)
	}
	if !strings.Contains(a.ExportOptionsRaw, "no_root_squash") {
		return fmt.Errorf("storage attestation lacks server-side export evidence")
	}
	if a.VerificationStrategy != storageVerificationStrategy || a.CPUAccountingCaveat != storageCPUCaveat {
		return fmt.Errorf("storage attestation lacks its verification strategy or CPU accounting caveat")
	}
	egress, ingress, read, written, err := storageDeltas(a.Before, a.After)
	if err != nil {
		return err
	}
	if egress != a.EgressBytes || ingress != a.IngressBytes || read != a.ServerReadBytes || written != a.ServerWrittenBytes {
		return fmt.Errorf("storage attestation byte deltas do not match its own snapshots")
	}
	if egress == 0 || ingress == 0 {
		return fmt.Errorf("the NFS qdiscs saw no traffic during the measured run (out %d bytes, in %d bytes); the client mount bypassed the shaper", egress, ingress)
	}
	if read+written < minimumAttestedStorageBytes {
		return fmt.Errorf("the NFS server moved only %d bytes during the measured run, want at least %d", read+written, minimumAttestedStorageBytes)
	}
	return nil
}

func storageDeltas(before, after StorageCounters) (egress, ingress, read, written uint64, err error) {
	if after.CapturedAt.Before(before.CapturedAt) {
		return 0, 0, 0, 0, fmt.Errorf("storage attestation snapshots are out of order")
	}
	for _, pair := range []struct {
		name          string
		before, after uint64
		target        *uint64
	}{
		{"server-to-client qdisc", before.EgressBytes, after.EgressBytes, &egress},
		{"client-to-server qdisc", before.IngressBytes, after.IngressBytes, &ingress},
		{"NFS server read", before.ServerReadBytes, after.ServerReadBytes, &read},
		{"NFS server written", before.ServerWrittenBytes, after.ServerWrittenBytes, &written},
	} {
		if pair.after < pair.before {
			return 0, 0, 0, 0, fmt.Errorf("%s counters moved backwards during the measured run", pair.name)
		}
		*pair.target = pair.after - pair.before
	}
	if before.EgressRateBits != after.EgressRateBits || before.IngressRateBits != after.IngressRateBits ||
		before.EgressDelayMicros != after.EgressDelayMicros || before.IngressDelayMicros != after.IngressDelayMicros {
		return 0, 0, 0, 0, fmt.Errorf("the NFS shaper configuration changed during the measured run")
	}
	return egress, ingress, read, written, nil
}

func decodeStorageShaperReport(raw string) (StorageShaperReport, error) {
	var report StorageShaperReport
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return StorageShaperReport{}, fmt.Errorf("decode NFS shaper report: %w", err)
	}
	return report, nil
}

// qdiscEntry is one parsed `tc -s qdisc show` stanza.
type qdiscEntry struct {
	Kind        string
	Root        bool
	SentBytes   uint64
	RateBits    uint64
	DelayMicros uint64
	BurstBytes  uint64
}

func parseQdiscs(raw string) []qdiscEntry {
	var entries []qdiscEntry
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "qdisc "):
			fields := strings.Fields(trimmed)
			entry := qdiscEntry{Root: strings.Contains(trimmed, " root ")}
			if len(fields) > 1 {
				entry.Kind = fields[1]
			}
			entry.RateBits = parseTaggedRate(fields, "rate")
			entry.BurstBytes = parseTaggedSize(fields, "burst")
			entry.DelayMicros = parseTaggedDuration(fields, "delay")
			entries = append(entries, entry)
		case strings.HasPrefix(trimmed, "Sent ") && len(entries) > 0:
			fields := strings.Fields(trimmed)
			if len(fields) > 1 {
				if value, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					entries[len(entries)-1].SentBytes = value
				}
			}
		}
	}
	return entries
}

// parseTCPolice reads the rate and burst out of `tc -s filter show` output for
// the ingress policing fallback, where no qdisc carries the limit.
func parseTCPolice(raw string) (rateBits, burstBytes uint64) {
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if !containsField(fields, "police") {
			continue
		}
		if rate := parseTaggedRate(fields, "rate"); rate > 0 {
			rateBits = rate
		}
		if burst := parseTaggedSize(fields, "burst"); burst > 0 {
			burstBytes = burst
		}
	}
	return rateBits, burstBytes
}

func containsField(fields []string, name string) bool {
	for _, field := range fields {
		if field == name {
			return true
		}
	}
	return false
}

func taggedValue(fields []string, name string) string {
	for index, field := range fields {
		if field == name && index+1 < len(fields) {
			return fields[index+1]
		}
	}
	return ""
}

// parseTaggedRate reads a tc rate such as `1Gbit`, `100Mbit` or `2500Mbit`.
// tc prints rates with SI multipliers.
func parseTaggedRate(fields []string, name string) uint64 {
	value := taggedValue(fields, name)
	if value == "" {
		return 0
	}
	multipliers := []struct {
		suffix     string
		multiplier float64
	}{
		{"Tbit", 1e12}, {"Gbit", 1e9}, {"Mbit", 1e6}, {"Kbit", 1e3}, {"kbit", 1e3}, {"bit", 1},
	}
	for _, candidate := range multipliers {
		if !strings.HasSuffix(value, candidate.suffix) {
			continue
		}
		number, err := strconv.ParseFloat(strings.TrimSuffix(value, candidate.suffix), 64)
		if err != nil || number <= 0 {
			return 0
		}
		return uint64(number*candidate.multiplier + 0.5)
	}
	return 0
}

// parseTaggedSize reads a tc byte size such as `1Mb`, `128Kb` or `4096b`.
// tc prints sizes with binary multipliers.
func parseTaggedSize(fields []string, name string) uint64 {
	value := taggedValue(fields, name)
	if value == "" {
		return 0
	}
	multipliers := []struct {
		suffix     string
		multiplier float64
	}{
		{"Gb", 1 << 30}, {"Mb", 1 << 20}, {"Kb", 1 << 10}, {"b", 1},
	}
	for _, candidate := range multipliers {
		if !strings.HasSuffix(value, candidate.suffix) {
			continue
		}
		number, err := strconv.ParseFloat(strings.TrimSuffix(value, candidate.suffix), 64)
		if err != nil || number <= 0 {
			return 0
		}
		return uint64(number*candidate.multiplier + 0.5)
	}
	return 0
}

// parseTaggedDuration reads a netem delay such as `500us`, `1.0ms` or `1s`.
func parseTaggedDuration(fields []string, name string) uint64 {
	value := taggedValue(fields, name)
	if value == "" {
		return 0
	}
	multipliers := []struct {
		suffix     string
		multiplier float64
	}{
		{"us", 1}, {"ms", 1_000}, {"s", 1_000_000},
	}
	for _, candidate := range multipliers {
		if !strings.HasSuffix(value, candidate.suffix) {
			continue
		}
		number, err := strconv.ParseFloat(strings.TrimSuffix(value, candidate.suffix), 64)
		if err != nil || number < 0 {
			return 0
		}
		return uint64(number*candidate.multiplier + 0.5)
	}
	return 0
}

// parseNFSDIO reads the `io <read> <written>` line of /proc/net/rpc/nfsd.
func parseNFSDIO(raw string) (read, written uint64, err error) {
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "io" {
			continue
		}
		read, err = strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse NFS server read bytes: %w", err)
		}
		written, err = strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse NFS server written bytes: %w", err)
		}
		return read, written, nil
	}
	return 0, 0, fmt.Errorf("NFS server counters contain no io line")
}

// buildStorageCounters turns one boundary's raw command output into parsed,
// asserted values. It never guesses: a missing rate stays zero and the
// attestation's validation rejects the run.
func buildStorageCounters(capturedAt time.Time, report StorageShaperReport, egressRaw, ingressRaw, filterRaw, nfsdRaw string) (StorageCounters, error) {
	counters := StorageCounters{
		CapturedAt:       capturedAt,
		EgressQdiscRaw:   egressRaw,
		IngressQdiscRaw:  ingressRaw,
		IngressFilterRaw: filterRaw,
		NFSDCountersRaw:  nfsdRaw,
	}
	for _, entry := range parseQdiscs(egressRaw) {
		if entry.Root && entry.RateBits > 0 {
			counters.EgressRateBits = entry.RateBits
			counters.EgressBytes = entry.SentBytes
		}
		if entry.Kind == "netem" && entry.DelayMicros > 0 && counters.EgressDelayMicros == 0 {
			counters.EgressDelayMicros = entry.DelayMicros
		}
	}
	switch report.IngressMechanism {
	case storageEgressIFB:
		for _, entry := range parseQdiscs(ingressRaw) {
			if entry.Root && entry.RateBits > 0 {
				counters.IngressRateBits = entry.RateBits
				counters.IngressBytes = entry.SentBytes
			}
			if entry.Kind == "netem" && entry.DelayMicros > 0 && counters.IngressDelayMicros == 0 {
				counters.IngressDelayMicros = entry.DelayMicros
			}
		}
	case storageEgressPolice:
		rate, _ := parseTCPolice(filterRaw)
		counters.IngressRateBits = rate
		for _, entry := range parseQdiscs(egressRaw) {
			if entry.Kind == "ingress" {
				counters.IngressBytes = entry.SentBytes
			}
		}
	default:
		return StorageCounters{}, fmt.Errorf("unsupported client-to-server shaper mechanism %q", report.IngressMechanism)
	}
	read, written, err := parseNFSDIO(nfsdRaw)
	if err != nil {
		return StorageCounters{}, err
	}
	counters.ServerReadBytes = read
	counters.ServerWrittenBytes = written
	return counters, nil
}
