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

type ShaperSnapshot struct {
	SchemaVersion                 int                 `json:"schema_version"`
	Status                        string              `json:"status"`
	StartedAt                     time.Time           `json:"started_at"`
	ConfiguredEgressBitsPerSecond uint64              `json:"configured_egress_bits_per_second"`
	ConfiguredBurstBytes          uint64              `json:"configured_burst_bytes"`
	DownstreamConnections         uint64              `json:"downstream_connections"`
	ActiveDownstreamConnections   int64               `json:"active_downstream_connections"`
	DownstreamBytes               uint64              `json:"downstream_bytes"`
	DownstreamSourceConnections   map[string]uint64   `json:"downstream_source_connections"`
	DownstreamSourceBytes         map[string]uint64   `json:"downstream_source_bytes"`
	ExecutionLeaseID              string              `json:"execution_lease_id,omitempty"`
	ExecutionLeaseAcquiredAt      *time.Time          `json:"execution_lease_acquired_at,omitempty"`
	Build                         ShaperBuildIdentity `json:"build"`
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

func ReleaseShaperExecutionLease(ctx context.Context, client *http.Client, controlURL, leaseID string) error {
	_, err := mutateShaperExecutionLease(ctx, client, controlURL, leaseID, http.MethodDelete)
	return err
}

func releaseShaperExecutionLeaseAfterRun(controlURL, leaseID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var lastErr error
	for {
		if err := ReleaseShaperExecutionLease(ctx, nil, controlURL, leaseID); err == nil {
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

func (s ShaperSnapshot) ValidateFor(link ServerLinkProfile) error {
	if s.SchemaVersion != 2 || s.Status != "ok" || s.StartedAt.IsZero() {
		return fmt.Errorf("shaper attestation has unsupported schema, status, or start time")
	}
	if s.ConfiguredEgressBitsPerSecond != link.EgressBitsPerSecond || s.ConfiguredBurstBytes != link.BurstBytes {
		return fmt.Errorf("shaper attestation rate/burst %d/%d does not match plan %d/%d", s.ConfiguredEgressBitsPerSecond, s.ConfiguredBurstBytes, link.EgressBitsPerSecond, link.BurstBytes)
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

func ValidateShaperSnapshotPair(before, after ShaperSnapshot) (uint64, error) {
	if before.SchemaVersion != after.SchemaVersion || !before.StartedAt.Equal(after.StartedAt) || before.ConfiguredEgressBitsPerSecond != after.ConfiguredEgressBitsPerSecond || before.ConfiguredBurstBytes != after.ConfiguredBurstBytes || before.Build != after.Build || before.ExecutionLeaseID != after.ExecutionLeaseID || before.ExecutionLeaseAcquiredAt == nil || after.ExecutionLeaseAcquiredAt == nil || !before.ExecutionLeaseAcquiredAt.Equal(*after.ExecutionLeaseAcquiredAt) {
		return 0, fmt.Errorf("shaper identity or configuration changed during the measured run")
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
