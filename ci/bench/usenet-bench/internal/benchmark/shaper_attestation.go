package benchmark

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ShaperBuildIdentity struct {
	ExecutableSHA256 string `json:"executable_sha256"`
	ImageIdentity    string `json:"image_identity,omitempty"`
	Version          string `json:"version"`
	Commit           string `json:"commit"`
	BuildTime        string `json:"build_time"`
}

// ShaperLinkShaping mirrors the shaper's link shaping report (attestation
// schema 4): how the container rendered the plan's fixed round trip with tc
// and, per snapshot, what tc reports for those qdiscs right now.
type ShaperLinkShaping struct {
	SchemaVersion          int    `json:"schema_version"`
	Interface              string `json:"interface"`
	IngressDevice          string `json:"ingress_device"`
	EgressMechanism        string `json:"egress_mechanism"`
	IngressMechanism       string `json:"ingress_mechanism"`
	RTTMicros              uint64 `json:"rtt_micros"`
	EgressDelayMicros      uint64 `json:"egress_delay_micros"`
	IngressDelayMicros     uint64 `json:"ingress_delay_micros"`
	NetemLimitPackets      uint64 `json:"netem_limit_packets"`
	TCPWmem                string `json:"tcp_wmem"`
	TCPRmem                string `json:"tcp_rmem"`
	KernelRelease          string `json:"kernel_release"`
	HandshakeDelayMicros   uint64 `json:"handshake_delay_micros,omitempty"`
	EgressQueueBytes       uint64 `json:"egress_queue_bytes,omitempty"`
	IngressQueueBytes      uint64 `json:"ingress_queue_bytes,omitempty"`
	Platform               string `json:"platform,omitempty"`
	LiveEgressDelayMicros  uint64 `json:"live_egress_delay_micros"`
	LiveIngressDelayMicros uint64 `json:"live_ingress_delay_micros"`
	LiveError              string `json:"live_error,omitempty"`
}

const (
	shaperLinkShapingSchemaVersion = 1
	shaperEgressNetem              = "netem"
	shaperIngressIFBNetem          = "ifb-netem"
	shaperIngressNone              = "none"
	// A host with no tc -- Windows, macOS -- has the shaper carry the round
	// trip in the proxy itself. It is a different mechanism with a different
	// fidelity, not a variant of netem: it cannot delay the client's own TCP
	// handshake, only pay that round trip back before the greeting. Results
	// from the two are never comparable, which the execution target already
	// keeps apart, and the mechanism is recorded in every run artifact.
	shaperDelayUserspace = "userspace-delay"
)

// declared strips the per-snapshot live fields so two snapshots' contracts
// can be compared for identity.
func (l ShaperLinkShaping) declared() ShaperLinkShaping {
	l.LiveEgressDelayMicros, l.LiveIngressDelayMicros, l.LiveError = 0, 0, ""
	return l
}

// validateFor checks the report against the plan's round trip and the live
// qdisc readings against the report. tc prints a delay with limited
// precision, so the live comparison allows the larger of 1% and 100us.
func (l ShaperLinkShaping) validateFor(link ServerLinkProfile) error {
	if l.SchemaVersion != shaperLinkShapingSchemaVersion {
		return fmt.Errorf("shaper link shaping report has schema %d, want %d", l.SchemaVersion, shaperLinkShapingSchemaVersion)
	}
	if l.RTTMicros != link.RTTMicros {
		return fmt.Errorf("shaper link shaping report declares a %dus round trip, plan declares %dus", l.RTTMicros, link.RTTMicros)
	}
	switch l.EgressMechanism {
	case shaperEgressNetem:
		if err := l.validateNetem(); err != nil {
			return err
		}
	case shaperDelayUserspace:
		if err := l.validateUserspace(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("shaper reports unknown egress mechanism %q", l.EgressMechanism)
	}
	if l.EgressDelayMicros == 0 || l.EgressDelayMicros+l.IngressDelayMicros != l.RTTMicros {
		return fmt.Errorf("shaper egress %dus + ingress %dus does not make up the %dus round trip", l.EgressDelayMicros, l.IngressDelayMicros, l.RTTMicros)
	}
	if l.LiveError != "" {
		return fmt.Errorf("shaper could not read its qdiscs back: %s", l.LiveError)
	}
	if l.EgressMechanism == shaperDelayUserspace {
		if err := residencyWithinTolerance("server-to-client", l.LiveEgressDelayMicros, l.EgressDelayMicros); err != nil {
			return err
		}
		return residencyWithinTolerance("client-to-server", l.LiveIngressDelayMicros, l.IngressDelayMicros)
	}
	if !delayWithinTolerance(l.LiveEgressDelayMicros, l.EgressDelayMicros) {
		return fmt.Errorf("tc reports a server-to-client delay of %dus, shaper declares %dus", l.LiveEgressDelayMicros, l.EgressDelayMicros)
	}
	if !delayWithinTolerance(l.LiveIngressDelayMicros, l.IngressDelayMicros) {
		return fmt.Errorf("tc reports a client-to-server delay of %dus, shaper declares %dus", l.LiveIngressDelayMicros, l.IngressDelayMicros)
	}
	return nil
}

// residencyWithinTolerance checks a userspace delay line's observed floor,
// which is a different kind of reading from a qdisc's configured delay and
// needs a different tolerance. A chunk can never leave early -- the line sleeps
// until its release instant -- so anything short is the delay not being applied
// and fails outright. Late is ordinary: a timer wakes when the host's scheduler
// gets to it. The bound is loose enough for that jitter and tight enough that a
// floor this far out over a whole run means the queue, not the link, is setting
// the pace.
func residencyWithinTolerance(direction string, observed, declared uint64) error {
	if observed+clockSlopMicros < declared {
		return fmt.Errorf("shaper delivered %s bytes after %dus, below the %dus it declares", direction, observed, declared)
	}
	late := uint64(0)
	if observed > declared {
		late = observed - declared
	}
	allowed := declared / 20
	if allowed < maxResidencyOvershootMicros {
		allowed = maxResidencyOvershootMicros
	}
	if late > allowed {
		return fmt.Errorf("shaper's lowest observed %s delay is %dus, %dus above the %dus it declares", direction, observed, late, declared)
	}
	return nil
}

const (
	clockSlopMicros             = 100
	maxResidencyOvershootMicros = 5_000
)

func (l ShaperLinkShaping) validateNetem() error {
	if l.Interface == "" {
		return fmt.Errorf("shaper link shaping report lacks a netem egress path")
	}
	switch l.IngressMechanism {
	case shaperIngressIFBNetem:
		if l.IngressDevice == "" || l.IngressDelayMicros == 0 {
			return fmt.Errorf("shaper ifb ingress path lacks a device or a delay")
		}
	case shaperIngressNone:
		if l.IngressDevice != "" || l.IngressDelayMicros != 0 {
			return fmt.Errorf("shaper reports no ingress path but names a device or a delay")
		}
	default:
		return fmt.Errorf("shaper reports unknown ingress mechanism %q", l.IngressMechanism)
	}
	if l.NetemLimitPackets == 0 {
		return fmt.Errorf("shaper netem queue limit is unset")
	}
	if l.HandshakeDelayMicros != 0 || l.EgressQueueBytes != 0 || l.IngressQueueBytes != 0 || l.Platform != "" {
		return fmt.Errorf("shaper netem report carries userspace delay fields")
	}
	return nil
}

// validateUserspace holds the userspace mechanism to the only shape it can
// have. Every netem field must be empty: a report naming an interface or a
// qdisc limit was written for a mechanism the process does not run, and
// accepting it would credit tc evidence to a delay tc never applied.
func (l ShaperLinkShaping) validateUserspace() error {
	if l.IngressMechanism != shaperDelayUserspace {
		return fmt.Errorf("shaper reports a %q egress path with a %q ingress path", l.EgressMechanism, l.IngressMechanism)
	}
	if l.Interface != "" || l.IngressDevice != "" || l.NetemLimitPackets != 0 || l.TCPWmem != "" || l.TCPRmem != "" || l.KernelRelease != "" {
		return fmt.Errorf("shaper userspace delay report carries netem or kernel evidence")
	}
	if l.IngressDelayMicros == 0 {
		return fmt.Errorf("shaper userspace delay has no client-to-server delay")
	}
	if l.HandshakeDelayMicros != l.RTTMicros {
		return fmt.Errorf("shaper charges a %dus handshake for a %dus round trip", l.HandshakeDelayMicros, l.RTTMicros)
	}
	if l.EgressQueueBytes == 0 || l.IngressQueueBytes == 0 {
		return fmt.Errorf("shaper userspace delay has an unset queue size (egress %d, ingress %d bytes)", l.EgressQueueBytes, l.IngressQueueBytes)
	}
	if l.Platform == "" {
		return fmt.Errorf("shaper userspace delay names no platform")
	}
	return nil
}

func delayWithinTolerance(observed, declared uint64) bool {
	tolerance := declared / 100
	if tolerance < 100 {
		tolerance = 100
	}
	if observed > declared {
		return observed-declared <= tolerance
	}
	return declared-observed <= tolerance
}

type ShaperSnapshot struct {
	SchemaVersion                 int       `json:"schema_version"`
	Status                        string    `json:"status"`
	StartedAt                     time.Time `json:"started_at"`
	ConfiguredEgressBitsPerSecond uint64    `json:"configured_egress_bits_per_second"`
	ConfiguredBurstBytes          uint64    `json:"configured_burst_bytes"`
	// The fixed round trip (attestation schema 4). A schema-2 or -3 shaper
	// leaves both empty and can only serve a plan that declares none.
	ConfiguredRTTMicros         uint64             `json:"configured_rtt_micros,omitempty"`
	LinkShaping                 *ShaperLinkShaping `json:"link_shaping,omitempty"`
	DownstreamConnections       uint64             `json:"downstream_connections"`
	ActiveDownstreamConnections int64              `json:"active_downstream_connections"`
	DownstreamBytes             uint64             `json:"downstream_bytes"`
	DownstreamSourceConnections map[string]uint64  `json:"downstream_source_connections"`
	DownstreamSourceBytes       map[string]uint64  `json:"downstream_source_bytes"`
	// The command census (attestation schema 3). A schema-2 shaper leaves them
	// zero and the artifact carries no census.
	DownstreamCommands       map[string]uint64   `json:"downstream_commands,omitempty"`
	ArticleRequests          uint64              `json:"article_requests,omitempty"`
	RepeatedArticleRequests  uint64              `json:"repeated_article_requests,omitempty"`
	DistinctArticleRequests  uint64              `json:"distinct_article_requests,omitempty"`
	ExecutionLeaseID         string              `json:"execution_lease_id,omitempty"`
	ExecutionLeaseAcquiredAt *time.Time          `json:"execution_lease_acquired_at,omitempty"`
	Build                    ShaperBuildIdentity `json:"build"`
}

func NewShaperExecutionLeaseID() (string, error) {
	id, err := newExecutionLeaseID()
	if err != nil {
		return "", fmt.Errorf("generate shaper execution lease ID: %w", err)
	}
	return id, nil
}

// newExecutionLeaseID is the shared identity for every exclusive benchmark
// resource lease. A lease is only meaningful if a second run cannot guess it.
func newExecutionLeaseID() (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func AcquireShaperExecutionLease(ctx context.Context, client *http.Client, controlURL, leaseID string) (ShaperSnapshot, error) {
	return mutateShaperExecutionLease(ctx, client, controlURL, leaseID, http.MethodPost)
}

// shaperQuietBudget bounds how long a run waits for the shaper to be free of
// the previous run's connections, and how often it looks.
const (
	shaperQuietBudget   = 15 * time.Second
	shaperQuietInterval = 250 * time.Millisecond
)

// AcquireShaperExecutionLeaseForRun takes the lease for one measured run and
// will not hand back a lease the previous run's client is still connected to.
//
// A client process outlives the adapter that launched it by a moment: the
// harness asks it to stop, but its sockets are closed by the operating system
// afterwards, and a frozen client's children are reaped later still. The
// previous run's release already waits for the shaper to reach zero active
// downstream connections, so the shaper is quiet at that instant -- but a
// client that is still alive keeps redialling, and the first dial to land
// after a new lease exists is counted against the new run. Failing the suite
// there is not a measurement of anything: it throws away a run that had not
// started, and `summarize` refuses a whole artifact root that contains one, so
// a race in another product's shutdown discards an entire measured phase. The
// lease is therefore released and taken again until the shaper is quiet, and
// only a shaper that stays busy for the whole budget fails the run.
//
// On success the lease is held and the caller owns releasing it. On failure no
// lease is held.
func AcquireShaperExecutionLeaseForRun(ctx context.Context, client *http.Client, controlURL, leaseID string, link ServerLinkProfile) (ShaperSnapshot, error) {
	deadline := time.Now().Add(shaperQuietBudget)
	for {
		snapshot, err := AcquireShaperExecutionLease(ctx, client, controlURL, leaseID)
		if err == nil {
			if snapshot.ActiveDownstreamConnections == 0 {
				validateErr := snapshot.ValidateFor(link)
				if validateErr == nil {
					return snapshot, nil
				}
				return ShaperSnapshot{}, handBackShaperExecutionLease(client, controlURL, leaseID, validateErr)
			}
			// Hand the lease back rather than measuring against it, so the
			// stray connections drain instead of being attributed here. The
			// shaper refuses a release while they are still open, so this is
			// also what waits for them.
			err = fmt.Errorf("shaper has %d active downstream connections outside the measured run", snapshot.ActiveDownstreamConnections)
			if releaseErr := releaseShaperExecutionLeaseAfterRun(client, controlURL, leaseID); releaseErr != nil {
				err = fmt.Errorf("%w (the lease could not be handed back: %v)", err, releaseErr)
			}
		}
		if time.Now().After(deadline) {
			return ShaperSnapshot{}, fmt.Errorf("shaper did not become quiet within %s: %w", shaperQuietBudget, err)
		}
		select {
		case <-ctx.Done():
			return ShaperSnapshot{}, ctx.Err()
		case <-time.After(shaperQuietInterval):
		}
	}
}

// handBackShaperExecutionLease releases a lease the run will not use, so a
// fatal condition does not also strand the lease for every suite behind it.
func handBackShaperExecutionLease(client *http.Client, controlURL, leaseID string, cause error) error {
	if err := releaseShaperExecutionLeaseAfterRun(client, controlURL, leaseID); err != nil {
		return fmt.Errorf("%w (the lease could not be handed back: %v)", cause, err)
	}
	return cause
}

func ReleaseShaperExecutionLease(ctx context.Context, client *http.Client, controlURL, leaseID string) error {
	_, err := mutateShaperExecutionLease(ctx, client, controlURL, leaseID, http.MethodDelete)
	return err
}

func releaseShaperExecutionLeaseAfterRun(client *http.Client, controlURL, leaseID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var lastErr error
	for {
		if err := ReleaseShaperExecutionLease(ctx, client, controlURL, leaseID); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last release error: %v)", ctx.Err(), lastErr)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func mutateShaperExecutionLease(ctx context.Context, client *http.Client, controlURL, leaseID, method string) (ShaperSnapshot, error) {
	if err := ValidateShaperControlURL(controlURL); err != nil {
		return ShaperSnapshot{}, err
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	payload, err := json.Marshal(struct {
		LeaseID string `json:"lease_id"`
	}{LeaseID: leaseID})
	if err != nil {
		return ShaperSnapshot{}, err
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(controlURL, "/")+"/v1/lease", bytes.NewReader(payload))
	if err != nil {
		return ShaperSnapshot{}, fmt.Errorf("create shaper lease request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return ShaperSnapshot{}, fmt.Errorf("mutate shaper execution lease: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ShaperSnapshot{}, fmt.Errorf("mutate shaper execution lease: HTTP %s", response.Status)
	}
	var snapshot ShaperSnapshot
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return ShaperSnapshot{}, fmt.Errorf("decode shaper lease response: %w", err)
	}
	return snapshot, nil
}

func ValidateShaperControlURL(value string) error {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("parse shaper control URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("shaper control URL must use http or https")
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("shaper control URL must contain only scheme, host, and optional base path")
	}
	return nil
}

func FetchShaperSnapshot(ctx context.Context, client *http.Client, controlURL string) (ShaperSnapshot, error) {
	if err := ValidateShaperControlURL(controlURL); err != nil {
		return ShaperSnapshot{}, err
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(controlURL, "/")+"/v1/stats", nil)
	if err != nil {
		return ShaperSnapshot{}, fmt.Errorf("create shaper attestation request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return ShaperSnapshot{}, fmt.Errorf("fetch shaper attestation: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ShaperSnapshot{}, fmt.Errorf("fetch shaper attestation: HTTP %s", response.Status)
	}
	var snapshot ShaperSnapshot
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return ShaperSnapshot{}, fmt.Errorf("decode shaper attestation: %w", err)
	}
	return snapshot, nil
}

// ShaperArticleCensus is what one measured run asked the server for, from the
// shaper's count of the client's own command lines: how many article requests
// (ARTICLE/BODY/HEAD/STAT) it sent, how many named a distinct message-id, and
// how many repeated a message-id already requested in the same run. Downstream
// bytes above the NZB's article bytes with zero repeats is a client reading
// more than it asked for; with repeats it is the client asking twice.
type ShaperArticleCensus struct {
	ArticleRequests         uint64 `json:"article_requests"`
	DistinctArticleRequests uint64 `json:"distinct_article_requests"`
	RepeatedArticleRequests uint64 `json:"repeated_article_requests"`
}

// HasArticleCensus reports whether the shaper that produced this snapshot
// counted commands at all.
func (s ShaperSnapshot) HasArticleCensus() bool {
	return s.SchemaVersion >= 3
}

// ShaperArticleCensusFor brackets one run between a lease-acquisition snapshot
// and the post-run snapshot. It returns nil for a schema-2 shaper. The lease
// acquisition reset the distinct set, so the after snapshot's distinct count
// is the run's own.
func ShaperArticleCensusFor(before, after ShaperSnapshot) (*ShaperArticleCensus, error) {
	if !before.HasArticleCensus() || !after.HasArticleCensus() {
		return nil, nil
	}
	if before.DistinctArticleRequests != 0 {
		return nil, fmt.Errorf("shaper lease snapshot already counts %d distinct articles; the census was not reset for this run", before.DistinctArticleRequests)
	}
	if after.ArticleRequests < before.ArticleRequests || after.RepeatedArticleRequests < before.RepeatedArticleRequests {
		return nil, fmt.Errorf("shaper command census moved backwards during the measured run")
	}
	census := &ShaperArticleCensus{
		ArticleRequests:         after.ArticleRequests - before.ArticleRequests,
		DistinctArticleRequests: after.DistinctArticleRequests,
		RepeatedArticleRequests: after.RepeatedArticleRequests - before.RepeatedArticleRequests,
	}
	if census.DistinctArticleRequests+census.RepeatedArticleRequests > census.ArticleRequests {
		return nil, fmt.Errorf("shaper command census is inconsistent: %d distinct + %d repeated exceeds %d article requests", census.DistinctArticleRequests, census.RepeatedArticleRequests, census.ArticleRequests)
	}
	return census, nil
}

func (s ShaperSnapshot) ValidateFor(link ServerLinkProfile) error {
	if (s.SchemaVersion < 2 || s.SchemaVersion > 4) || s.Status != "ok" || s.StartedAt.IsZero() {
		return fmt.Errorf("shaper attestation has unsupported schema, status, or start time")
	}
	if s.ConfiguredEgressBitsPerSecond != link.EgressBitsPerSecond || s.ConfiguredBurstBytes != link.BurstBytes {
		return fmt.Errorf("shaper attestation rate/burst %d/%d does not match plan %d/%d", s.ConfiguredEgressBitsPerSecond, s.ConfiguredBurstBytes, link.EgressBitsPerSecond, link.BurstBytes)
	}
	if err := s.validateRoundTripFor(link); err != nil {
		return err
	}
	if len(s.Build.ExecutableSHA256) != 64 || strings.Trim(s.Build.ExecutableSHA256, "0123456789abcdef") != "" {
		return fmt.Errorf("shaper attestation lacks a lowercase executable SHA-256")
	}
	if s.ActiveDownstreamConnections != 0 {
		return fmt.Errorf("shaper has %d active downstream connections outside the measured run", s.ActiveDownstreamConnections)
	}
	if s.DownstreamSourceConnections == nil || s.DownstreamSourceBytes == nil {
		return fmt.Errorf("shaper attestation lacks source-attributed counters")
	}
	if len(s.ExecutionLeaseID) != 64 || strings.Trim(s.ExecutionLeaseID, "0123456789abcdef") != "" || s.ExecutionLeaseAcquiredAt == nil || s.ExecutionLeaseAcquiredAt.IsZero() {
		return fmt.Errorf("shaper attestation lacks an active immutable execution lease")
	}
	return nil
}

// validateRoundTripFor checks the plan's fixed round trip against what the
// shaper attests. A pre-schema-4 shaper cannot add delay, so it is accepted
// only for a plan that declares none; a schema-4 shaper must carry a report
// whose declared split and live qdisc readings both match the plan.
func (s ShaperSnapshot) validateRoundTripFor(link ServerLinkProfile) error {
	if s.SchemaVersion < 4 {
		if link.RTTMicros != 0 {
			return fmt.Errorf("shaper attestation schema %d cannot render the plan's %dus round trip; rebuild the shaper image", s.SchemaVersion, link.RTTMicros)
		}
		return nil
	}
	if s.ConfiguredRTTMicros != link.RTTMicros {
		return fmt.Errorf("shaper attestation round trip %dus does not match plan %dus", s.ConfiguredRTTMicros, link.RTTMicros)
	}
	if link.RTTMicros == 0 {
		if s.LinkShaping != nil {
			return fmt.Errorf("shaper reports a link shaping path for a plan that declares no round trip")
		}
		return nil
	}
	if s.LinkShaping == nil {
		return fmt.Errorf("shaper attestation lacks the link shaping report for its %dus round trip", link.RTTMicros)
	}
	return s.LinkShaping.validateFor(link)
}

func ValidateShaperSnapshotPair(before, after ShaperSnapshot) (uint64, error) {
	if before.SchemaVersion != after.SchemaVersion || !before.StartedAt.Equal(after.StartedAt) || before.ConfiguredEgressBitsPerSecond != after.ConfiguredEgressBitsPerSecond || before.ConfiguredBurstBytes != after.ConfiguredBurstBytes || before.ConfiguredRTTMicros != after.ConfiguredRTTMicros || before.Build != after.Build || before.ExecutionLeaseID != after.ExecutionLeaseID || before.ExecutionLeaseAcquiredAt == nil || after.ExecutionLeaseAcquiredAt == nil || !before.ExecutionLeaseAcquiredAt.Equal(*after.ExecutionLeaseAcquiredAt) {
		return 0, fmt.Errorf("shaper identity or configuration changed during the measured run")
	}
	if (before.LinkShaping == nil) != (after.LinkShaping == nil) || (before.LinkShaping != nil && before.LinkShaping.declared() != after.LinkShaping.declared()) {
		return 0, fmt.Errorf("shaper link shaping changed during the measured run")
	}
	if after.DownstreamConnections < before.DownstreamConnections || after.DownstreamBytes < before.DownstreamBytes {
		return 0, fmt.Errorf("shaper counters moved backwards during the measured run")
	}
	if after.ActiveDownstreamConnections != 0 {
		return 0, fmt.Errorf("shaper still has %d active downstream connections after the measured run", after.ActiveDownstreamConnections)
	}
	delivered := after.DownstreamBytes - before.DownstreamBytes
	changedSources := make(map[string]bool)
	var attributedConnections uint64
	for source, beforeCount := range before.DownstreamSourceConnections {
		afterCount, ok := after.DownstreamSourceConnections[source]
		if !ok || afterCount < beforeCount {
			return 0, fmt.Errorf("shaper source connection counters moved backwards during the measured run")
		}
		if afterCount > beforeCount {
			changedSources[source] = true
			attributedConnections += afterCount - beforeCount
		}
	}
	for source, afterCount := range after.DownstreamSourceConnections {
		if _, existed := before.DownstreamSourceConnections[source]; !existed && afterCount > 0 {
			changedSources[source] = true
			attributedConnections += afterCount
		}
	}
	if attributedConnections != after.DownstreamConnections-before.DownstreamConnections {
		return 0, fmt.Errorf("shaper global and source-attributed connection deltas disagree")
	}
	var attributedBytes uint64
	for source, beforeBytes := range before.DownstreamSourceBytes {
		afterBytes, ok := after.DownstreamSourceBytes[source]
		if !ok || afterBytes < beforeBytes {
			return 0, fmt.Errorf("shaper source byte counters moved backwards during the measured run")
		}
		if afterBytes > beforeBytes {
			changedSources[source] = true
			attributedBytes += afterBytes - beforeBytes
		}
	}
	for source, afterBytes := range after.DownstreamSourceBytes {
		if _, existed := before.DownstreamSourceBytes[source]; existed {
			continue
		}
		if afterBytes > 0 {
			changedSources[source] = true
			attributedBytes += afterBytes
		}
	}
	if delivered != attributedBytes {
		return 0, fmt.Errorf("shaper global and source-attributed byte deltas disagree")
	}
	if delivered > 0 && len(changedSources) != 1 {
		return 0, fmt.Errorf("shaper observed %d downstream sources during the measured run, want exactly one", len(changedSources))
	}
	return delivered, nil
}
