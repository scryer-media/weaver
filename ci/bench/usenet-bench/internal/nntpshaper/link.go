package nntpshaper

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const linkShapingReportSchemaVersion = 1

// Link shaping mechanisms. On Linux the container entrypoint renders the round
// trip with tc: egress is always netem, and ingress is netem on an ifb mirror
// when the host kernel offers one, otherwise absent with the whole round trip
// carried by egress. On a host with no tc -- Windows, macOS -- the proxy
// carries the delay itself and both directions report the userspace mechanism.
// The two never mix in one report and never aggregate into one result.
const (
	LinkEgressNetem     = "netem"
	LinkIngressIFBNetem = "ifb-netem"
	LinkIngressNone     = "none"
	LinkDelayUserspace  = "userspace-delay"
)

// LinkShapingReport is what the shaper container's entrypoint wrote after it
// configured the fixed round trip with tc, extended at snapshot time with the
// delays tc reports right now. The declared fields are the contract the
// controller compares against the plan; the live fields are the evidence
// that the contract still holds while a run is measured.
type LinkShapingReport struct {
	SchemaVersion      int    `json:"schema_version"`
	Interface          string `json:"interface"`
	IngressDevice      string `json:"ingress_device"`
	EgressMechanism    string `json:"egress_mechanism"`
	IngressMechanism   string `json:"ingress_mechanism"`
	RTTMicros          uint64 `json:"rtt_micros"`
	EgressDelayMicros  uint64 `json:"egress_delay_micros"`
	IngressDelayMicros uint64 `json:"ingress_delay_micros"`
	NetemLimitPackets  uint64 `json:"netem_limit_packets"`
	TCPWmem            string `json:"tcp_wmem"`
	TCPRmem            string `json:"tcp_rmem"`
	KernelRelease      string `json:"kernel_release"`
	// The userspace mechanism adds these and leaves every netem-only field
	// above empty. HandshakeDelayMicros is the whole round trip charged once
	// when a connection opens, which is the TCP handshake a proxy cannot
	// delay by relaying bytes. The queue sizes bound the bytes each direction
	// holds in flight and are the userspace analogue of the netem packet limit.
	HandshakeDelayMicros uint64 `json:"handshake_delay_micros,omitempty"`
	EgressQueueBytes     uint64 `json:"egress_queue_bytes,omitempty"`
	IngressQueueBytes    uint64 `json:"ingress_queue_bytes,omitempty"`
	Platform             string `json:"platform,omitempty"`
	// Live fields are filled per snapshot from `tc qdisc show`, never copied
	// from the declared values, so a qdisc that was removed or replaced after
	// startup shows up as a mismatch rather than a stale promise.
	LiveEgressDelayMicros  uint64 `json:"live_egress_delay_micros"`
	LiveIngressDelayMicros uint64 `json:"live_ingress_delay_micros"`
	LiveError              string `json:"live_error,omitempty"`
}

// LoadLinkShapingReport reads the entrypoint's report and checks that it
// describes the round trip the process was told to expect. A configured
// round trip without a coherent report is a startup failure: the proxy must
// not serve a run whose latency it cannot attest.
func LoadLinkShapingReport(path string, rttMicros uint64) (*LinkShapingReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read link shaping report %s: %w", path, err)
	}
	var report LinkShapingReport
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return nil, fmt.Errorf("decode link shaping report %s: %w", path, err)
	}
	if err := report.validateDeclared(rttMicros); err != nil {
		return nil, fmt.Errorf("link shaping report %s: %w", path, err)
	}
	return &report, nil
}

func (r LinkShapingReport) validateDeclared(rttMicros uint64) error {
	if r.SchemaVersion != linkShapingReportSchemaVersion {
		return fmt.Errorf("schema version %d, want %d", r.SchemaVersion, linkShapingReportSchemaVersion)
	}
	if r.RTTMicros != rttMicros {
		return fmt.Errorf("declares a %dus round trip, process configured for %dus", r.RTTMicros, rttMicros)
	}
	switch r.EgressMechanism {
	case LinkEgressNetem:
		if err := r.validateNetem(); err != nil {
			return err
		}
	case LinkDelayUserspace:
		if err := r.validateUserspace(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("egress mechanism %q, want %q or %q", r.EgressMechanism, LinkEgressNetem, LinkDelayUserspace)
	}
	if r.EgressDelayMicros == 0 || r.EgressDelayMicros+r.IngressDelayMicros != r.RTTMicros {
		return fmt.Errorf("egress %dus + ingress %dus does not make up the %dus round trip", r.EgressDelayMicros, r.IngressDelayMicros, r.RTTMicros)
	}
	if r.LiveEgressDelayMicros != 0 || r.LiveIngressDelayMicros != 0 || r.LiveError != "" {
		return fmt.Errorf("live fields must be empty in the entrypoint report")
	}
	return nil
}

func (r LinkShapingReport) validateNetem() error {
	if r.Interface == "" {
		return fmt.Errorf("names no shaped interface")
	}
	switch r.IngressMechanism {
	case LinkIngressIFBNetem:
		if r.IngressDevice == "" || r.IngressDelayMicros == 0 {
			return fmt.Errorf("ifb ingress path lacks a device or a delay")
		}
	case LinkIngressNone:
		if r.IngressDevice != "" || r.IngressDelayMicros != 0 {
			return fmt.Errorf("ingress path reports none but names a device or a delay")
		}
	default:
		return fmt.Errorf("unknown ingress mechanism %q", r.IngressMechanism)
	}
	if r.NetemLimitPackets == 0 {
		return fmt.Errorf("netem queue limit is unset")
	}
	if r.HandshakeDelayMicros != 0 || r.EgressQueueBytes != 0 || r.IngressQueueBytes != 0 || r.Platform != "" {
		return fmt.Errorf("netem path carries userspace delay fields")
	}
	return nil
}

// validateUserspace holds the userspace mechanism to the shape only it can
// have. Every netem field must be empty: a report that names an interface or a
// qdisc limit was written for a mechanism this process does not run, and
// serving it would present tc evidence for a delay tc never applied.
func (r LinkShapingReport) validateUserspace() error {
	if r.IngressMechanism != LinkDelayUserspace {
		return fmt.Errorf("ingress mechanism %q, want %q", r.IngressMechanism, LinkDelayUserspace)
	}
	if r.Interface != "" || r.IngressDevice != "" {
		return fmt.Errorf("userspace delay names a shaped device (%q/%q); it shapes no device", r.Interface, r.IngressDevice)
	}
	if r.NetemLimitPackets != 0 || r.TCPWmem != "" || r.TCPRmem != "" || r.KernelRelease != "" {
		return fmt.Errorf("userspace delay carries netem or kernel fields")
	}
	if r.IngressDelayMicros == 0 {
		return fmt.Errorf("userspace delay has no client-to-server delay")
	}
	if r.HandshakeDelayMicros != r.RTTMicros {
		return fmt.Errorf("handshake delay %dus does not match the %dus round trip", r.HandshakeDelayMicros, r.RTTMicros)
	}
	if r.EgressQueueBytes == 0 || r.IngressQueueBytes == 0 {
		return fmt.Errorf("userspace delay has an unset queue size (egress %d, ingress %d bytes)", r.EgressQueueBytes, r.IngressQueueBytes)
	}
	if r.Platform == "" {
		return fmt.Errorf("userspace delay names no platform")
	}
	return nil
}

// LiveDelayProbe reads back the netem delays tc reports for a shaped path.
type LiveDelayProbe func(report LinkShapingReport) (egressMicros, ingressMicros uint64, err error)

// tcQdiscShow is the command that backs the default probe; tests replace it.
var tcQdiscShow = func(device string) ([]byte, error) {
	return exec.Command("tc", "qdisc", "show", "dev", device).CombinedOutput()
}

// TCLiveDelays is the default probe: one `tc qdisc show` per shaped device,
// parsed for the netem delay.
func TCLiveDelays(report LinkShapingReport) (uint64, uint64, error) {
	egress, err := netemDelayMicros(report.Interface)
	if err != nil {
		return 0, 0, err
	}
	var ingress uint64
	if report.IngressMechanism == LinkIngressIFBNetem {
		ingress, err = netemDelayMicros(report.IngressDevice)
		if err != nil {
			return 0, 0, err
		}
	}
	return egress, ingress, nil
}

func netemDelayMicros(device string) (uint64, error) {
	output, err := tcQdiscShow(device)
	if err != nil {
		return 0, fmt.Errorf("tc qdisc show dev %s: %w (%s)", device, err, strings.TrimSpace(string(output)))
	}
	delay, ok := ParseNetemDelayMicros(string(output))
	if !ok {
		return 0, fmt.Errorf("tc reports no netem qdisc on %s: %s", device, strings.TrimSpace(string(output)))
	}
	return delay, nil
}

// ParseNetemDelayMicros finds the first `qdisc netem` line in `tc qdisc show`
// output and returns its fixed delay. tc prints the delay with a unit suffix
// (`500us`, `125ms`, `1.5ms`, `1s`).
func ParseNetemDelayMicros(raw string) (uint64, bool) {
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "qdisc" || fields[1] != "netem" {
			continue
		}
		for index := 2; index+1 < len(fields); index++ {
			if fields[index] != "delay" {
				continue
			}
			if micros, ok := parseTCDuration(fields[index+1]); ok {
				return micros, true
			}
			return 0, false
		}
		return 0, false
	}
	return 0, false
}

func parseTCDuration(value string) (uint64, bool) {
	for _, unit := range []struct {
		suffix     string
		multiplier float64
	}{{"us", 1}, {"ms", 1_000}, {"s", 1_000_000}} {
		if !strings.HasSuffix(value, unit.suffix) {
			continue
		}
		number, err := strconv.ParseFloat(strings.TrimSuffix(value, unit.suffix), 64)
		if err != nil || number < 0 {
			return 0, false
		}
		return uint64(number*unit.multiplier + 0.5), true
	}
	return 0, false
}
