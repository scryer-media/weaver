package nntpshaper

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const attestationSchemaVersion = 3

// BuildIdentity carries the immutable executable digest and optional build and
// image metadata emitted with every control response.
type BuildIdentity struct {
	ExecutableSHA256 string `json:"executable_sha256"`
	ImageIdentity    string `json:"image_identity,omitempty"`
	Version          string `json:"version"`
	Commit           string `json:"commit"`
	BuildTime        string `json:"build_time"`
}

// AttestationConfig describes the static benchmark-link contract.
type AttestationConfig struct {
	EgressBitsPerSecond uint64
	BurstBytes          uint64
	Build               BuildIdentity
	StartedAt           time.Time
}

// Snapshot is the versioned control-plane record for a shaper process.
type Snapshot struct {
	SchemaVersion                 int               `json:"schema_version"`
	Status                        string            `json:"status"`
	StartedAt                     time.Time         `json:"started_at"`
	ConfiguredEgressBitsPerSecond uint64            `json:"configured_egress_bits_per_second"`
	ConfiguredBurstBytes          uint64            `json:"configured_burst_bytes"`
	DownstreamConnections         uint64            `json:"downstream_connections"`
	ActiveDownstreamConnections   int64             `json:"active_downstream_connections"`
	DownstreamBytes               uint64            `json:"downstream_bytes"`
	DownstreamSourceConnections   map[string]uint64 `json:"downstream_source_connections"`
	DownstreamSourceBytes         map[string]uint64 `json:"downstream_source_bytes"`
	// DownstreamCommands tallies every command line clients sent upstream, by
	// verb, since the process started.
	DownstreamCommands map[string]uint64 `json:"downstream_commands"`
	// ArticleRequests counts ARTICLE/BODY/HEAD/STAT commands since the process
	// started; RepeatedArticleRequests counts those whose message-id had
	// already been requested during the same execution lease, and
	// DistinctArticleRequests is the number of message-ids seen during the
	// current lease. Both cumulative counters are monotonic, so a before/after
	// pair brackets one run the way the byte counters do; the distinct count
	// resets when a lease is acquired.
	ArticleRequests          uint64        `json:"article_requests"`
	RepeatedArticleRequests  uint64        `json:"repeated_article_requests"`
	DistinctArticleRequests  uint64        `json:"distinct_article_requests"`
	ExecutionLeaseID         string        `json:"execution_lease_id,omitempty"`
	ExecutionLeaseAcquiredAt *time.Time    `json:"execution_lease_acquired_at,omitempty"`
	Build                    BuildIdentity `json:"build"`
}

// Attestation tracks downstream delivery with atomics and serves immutable
// configuration plus live counters on the shaper control plane.
type Attestation struct {
	config AttestationConfig

	downstreamConnections       atomic.Uint64
	activeDownstreamConnections atomic.Int64
	downstreamBytes             atomic.Uint64
	sourcesMu                   sync.Mutex
	downstreamSourceConnections map[string]uint64
	downstreamSourceBytes       map[string]uint64
	commandsMu                  sync.Mutex
	downstreamCommands          map[string]uint64
	articleRequests             uint64
	repeatedArticleRequests     uint64
	leaseArticles               map[string]struct{}
	executionLeaseID            string
	executionLeaseAcquiredAt    time.Time
	executionLeaseSource        string
}

// CurrentExecutableSHA256 returns the SHA-256 digest of the running shaper
// binary. It is computed once at process startup and cannot be changed by an
// operator after the process has started.
func CurrentExecutableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	return FileSHA256(path)
}

// FileSHA256 returns a lowercase SHA-256 hex digest for a regular file.
func FileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func NewAttestation(config AttestationConfig) *Attestation {
	if config.StartedAt.IsZero() {
		config.StartedAt = time.Now().UTC()
	} else {
		config.StartedAt = config.StartedAt.UTC()
	}
	return &Attestation{
		config:                      config,
		downstreamSourceConnections: make(map[string]uint64),
		downstreamSourceBytes:       make(map[string]uint64),
		downstreamCommands:          make(map[string]uint64),
		leaseArticles:               make(map[string]struct{}),
	}
}

// OpenDownstream records an accepted client connection and returns an idempotent
// release function that must run when that connection is finished.
func (a *Attestation) OpenDownstream(source string) (func(), error) {
	a.sourcesMu.Lock()
	if a.executionLeaseID == "" {
		a.sourcesMu.Unlock()
		return nil, fmt.Errorf("no active benchmark execution lease")
	}
	if a.executionLeaseSource == "" {
		a.executionLeaseSource = source
	} else if a.executionLeaseSource != source {
		a.sourcesMu.Unlock()
		return nil, fmt.Errorf("execution lease belongs to downstream source %s", a.executionLeaseSource)
	}
	a.downstreamSourceConnections[source]++
	a.downstreamConnections.Add(1)
	a.activeDownstreamConnections.Add(1)
	a.sourcesMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { a.activeDownstreamConnections.Add(-1) })
	}, nil
}

func (a *Attestation) AcquireExecutionLease(leaseID string) error {
	leaseID = strings.TrimSpace(leaseID)
	if len(leaseID) != 64 || strings.Trim(leaseID, "0123456789abcdef") != "" {
		return fmt.Errorf("execution lease ID must be a lowercase 64-character hexadecimal token")
	}
	a.sourcesMu.Lock()
	defer a.sourcesMu.Unlock()
	if a.executionLeaseID != "" {
		return fmt.Errorf("execution lease %s is already active", a.executionLeaseID)
	}
	if a.activeDownstreamConnections.Load() != 0 {
		return fmt.Errorf("cannot acquire execution lease with active downstream connections")
	}
	a.executionLeaseID = leaseID
	a.executionLeaseAcquiredAt = time.Now().UTC()
	a.executionLeaseSource = ""
	a.commandsMu.Lock()
	a.leaseArticles = make(map[string]struct{})
	a.commandsMu.Unlock()
	return nil
}

func (a *Attestation) ReleaseExecutionLease(leaseID string) error {
	a.sourcesMu.Lock()
	defer a.sourcesMu.Unlock()
	if a.executionLeaseID == "" || a.executionLeaseID != strings.TrimSpace(leaseID) {
		return fmt.Errorf("execution lease ID does not match the active lease")
	}
	if a.activeDownstreamConnections.Load() != 0 {
		return fmt.Errorf("cannot release execution lease with active downstream connections")
	}
	a.executionLeaseID = ""
	a.executionLeaseAcquiredAt = time.Time{}
	a.executionLeaseSource = ""
	return nil
}

// AddDownstreamBytes records bytes successfully written to a downstream client.
func (a *Attestation) AddDownstreamBytes(source string, n int) {
	if n > 0 {
		a.downstreamBytes.Add(uint64(n))
		a.sourcesMu.Lock()
		a.downstreamSourceBytes[source] += uint64(n)
		a.sourcesMu.Unlock()
	}
}

// ObserveCommand records one command line a client sent upstream. Article
// requests are keyed by message-id within the current execution lease so a
// repeat — the same article asked for twice in one measured run — is counted
// wherever the second request arrives, on the same connection or another.
func (a *Attestation) ObserveCommand(verb, argument string) {
	a.commandsMu.Lock()
	defer a.commandsMu.Unlock()
	a.downstreamCommands[verb]++
	if !articleVerbs[verb] {
		return
	}
	a.articleRequests++
	id := normalizeMessageID(argument)
	if id == "" {
		return
	}
	if _, seen := a.leaseArticles[id]; seen {
		a.repeatedArticleRequests++
		return
	}
	a.leaseArticles[id] = struct{}{}
}

func (a *Attestation) Snapshot() Snapshot {
	a.commandsMu.Lock()
	commands := cloneCounters(a.downstreamCommands)
	articleRequests := a.articleRequests
	repeatedArticleRequests := a.repeatedArticleRequests
	distinctArticles := uint64(len(a.leaseArticles))
	a.commandsMu.Unlock()
	a.sourcesMu.Lock()
	sourceConnections := cloneCounters(a.downstreamSourceConnections)
	sourceBytes := cloneCounters(a.downstreamSourceBytes)
	leaseID := a.executionLeaseID
	leaseAcquiredAt := a.executionLeaseAcquiredAt
	a.sourcesMu.Unlock()
	var leaseAcquiredAtPointer *time.Time
	if !leaseAcquiredAt.IsZero() {
		leaseAcquiredAtPointer = &leaseAcquiredAt
	}
	return Snapshot{
		SchemaVersion:                 attestationSchemaVersion,
		Status:                        "ok",
		StartedAt:                     a.config.StartedAt,
		ConfiguredEgressBitsPerSecond: a.config.EgressBitsPerSecond,
		ConfiguredBurstBytes:          a.config.BurstBytes,
		DownstreamConnections:         a.downstreamConnections.Load(),
		ActiveDownstreamConnections:   a.activeDownstreamConnections.Load(),
		DownstreamBytes:               a.downstreamBytes.Load(),
		DownstreamSourceConnections:   sourceConnections,
		DownstreamSourceBytes:         sourceBytes,
		DownstreamCommands:            commands,
		ArticleRequests:               articleRequests,
		RepeatedArticleRequests:       repeatedArticleRequests,
		DistinctArticleRequests:       distinctArticles,
		ExecutionLeaseID:              leaseID,
		ExecutionLeaseAcquiredAt:      leaseAcquiredAtPointer,
		Build:                         a.config.Build,
	}
}

func cloneCounters(source map[string]uint64) map[string]uint64 {
	clone := make(map[string]uint64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

// Handler exposes stable JSON endpoints for liveness and benchmark evidence.
func (a *Attestation) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", a.serveSnapshot)
	mux.HandleFunc("/v1/stats", a.serveSnapshot)
	mux.HandleFunc("/v1/lease", a.serveLease)
	return mux
}

func (a *Attestation) serveLease(writer http.ResponseWriter, request *http.Request) {
	var payload struct {
		LeaseID string `json:"lease_id"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		http.Error(writer, "invalid lease request", http.StatusBadRequest)
		return
	}
	var err error
	switch request.Method {
	case http.MethodPost:
		err = a.AcquireExecutionLease(payload.LeaseID)
	case http.MethodDelete:
		err = a.ReleaseExecutionLease(payload.LeaseID)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusConflict)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(a.Snapshot())
}

func (a *Attestation) serveSnapshot(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(a.Snapshot())
}
