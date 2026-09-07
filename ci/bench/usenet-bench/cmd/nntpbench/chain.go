// chain drives a whole benchmark session from one declarative description:
// the phases to measure, the link conditions each is measured under, and the
// summaries to produce afterwards. It replaces the shell orchestration these
// series used to be driven by, so one session description runs unchanged on
// Linux, macOS and Windows.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/rawstack"
)

// ChainSchemaVersion is the only chain configuration schema this build reads.
// A session description is an operator artifact that outlives the session it
// drove, so it declares its version rather than being recognized by shape.
const ChainSchemaVersion = 1

// chainLockName is created in the log directory for the lifetime of a session.
// Two chains sharing one benchmark host would interleave their shaper
// reconfigurations and silently mismeasure every phase after the first, so the
// second refuses to start rather than producing plausible wrong numbers.
const chainLockName = "nntpbench-chain.lock"

// defaultPhaseSettle is the pause between a finished phase and the next one.
// Client containers release ports and flush their writes on their own schedule
// after the controller returns, and a phase starting into that tail measures
// the previous phase's cleanup.
const defaultPhaseSettle = 10 * time.Second

// defaultShaperSettle is the pause after the shaper is recreated. Its queueing
// discipline is installed asynchronously, and measuring through a
// half-configured shaper records conditions the plan never asked for.
const defaultShaperSettle = 5 * time.Second

// ChainConfig describes a benchmark session: the execution context every phase
// shares, the preconditions the host must satisfy before any measurement is
// taken, and the ordered phases themselves.
type ChainConfig struct {
	SchemaVersion int    `json:"schema_version"`
	Name          string `json:"name"`

	// Stack selects how the server side runs. "docker" drives the Compose
	// stack and is the default. "raw" starts the NNTP server and the shaper as
	// local processes, which is the only way to measure on a host that cannot
	// have the containerized stack: Windows has no netem, and on an ARM Mac
	// every container runs inside a Linux virtual machine whose scheduling and
	// networking are the very things this benchmark measures.
	Stack string         `json:"stack,omitempty"`
	Raw   *ChainRawStack `json:"raw,omitempty"`

	// ComposeFile and ComposeProject locate the NNTP server and shaper stack.
	// The chain only ever recreates the shaper: recreating the server would
	// discard the seeded article store every phase reads.
	ComposeFile    string `json:"compose_file,omitempty"`
	ComposeProject string `json:"compose_project,omitempty"`
	// PasswordFile is passed to Compose as NNTP_BENCH_PASSWORD_FILE and to each
	// phase as --password-file.
	PasswordFile string `json:"password_file,omitempty"`
	// ShaperService is the Compose service carrying the link shaping, and
	// ServerService is the one the run CA is copied out of.
	ShaperService string `json:"shaper_service,omitempty"`
	ServerService string `json:"server_service,omitempty"`
	// ServerEnvDir receives the generated link environment file for each
	// distinct set of conditions. Defaults to the config file's directory.
	ServerEnvDir string `json:"server_env_dir,omitempty"`

	Adapters         string `json:"adapters"`
	Target           string `json:"target,omitempty"`
	NNTPHost         string `json:"nntp_host,omitempty"`
	NNTPPort         string `json:"nntp_port,omitempty"`
	NNTPTLSPort      string `json:"nntp_tls_port,omitempty"`
	Username         string `json:"username,omitempty"`
	CAFile           string `json:"ca_file,omitempty"`
	ShaperControlURL string `json:"shaper_control_url,omitempty"`
	Connections      int    `json:"connections,omitempty"`
	Timeout          string `json:"timeout,omitempty"`

	// ArtifactsDir is the parent of every phase's artifact directory, and
	// LogDir receives phase logs, summaries and the session record. Both
	// default to the config file's directory.
	ArtifactsDir string `json:"artifacts_dir,omitempty"`
	LogDir       string `json:"log_dir,omitempty"`

	// PhaseSettle overrides the pause between phases.
	PhaseSettle string `json:"phase_settle,omitempty"`

	// RestoreServerLink and RestoreServerRTT return the shaper to a known state
	// when the session ends, so a finished chain never leaves a rate limit or a
	// latency injection behind for the next operator to discover the hard way.
	RestoreServerLink string `json:"restore_server_link,omitempty"`
	RestoreServerRTT  string `json:"restore_server_rtt,omitempty"`

	// FixtureSets names corpora a plan spec can refer to, so the several
	// phases that measure one corpus declare it once and cannot drift apart.
	FixtureSets map[string][]string `json:"fixture_sets,omitempty"`

	Require ChainRequirements `json:"require"`
	Phases  []ChainPhase      `json:"phases"`
}

// ChainRawStack describes a server side made of local processes. Only the two
// directories are required: the ports have defaults above 1024 so the stack
// needs no privileges on any host, and the delay queue is derived from the
// link unless the link is unlimited, which has no bandwidth-delay product to
// derive from.
type ChainRawStack struct {
	BinDir  string `json:"bin_dir"`
	DataDir string `json:"data_dir"`
	CertDir string `json:"cert_dir,omitempty"`
	Host    string `json:"host,omitempty"`
	// Pipelining advertises RFC 4644 PIPELINING as commercial providers do.
	// It defaults to on: a server that stays silent benches every client one
	// article per round trip and hides the difference latency is there to show.
	Pipelining *bool `json:"pipelining,omitempty"`

	UpstreamPlaintextPort int    `json:"upstream_plaintext_port,omitempty"`
	UpstreamTLSPort       int    `json:"upstream_tls_port,omitempty"`
	PlaintextPort         int    `json:"plaintext_port,omitempty"`
	TLSPort               int    `json:"tls_port,omitempty"`
	ControlPort           int    `json:"control_port,omitempty"`
	DelayQueueBytes       uint64 `json:"delay_queue_bytes,omitempty"`
	StartTimeout          string `json:"start_timeout,omitempty"`
}

// ChainRequirements are host preconditions checked once, before the first
// phase runs. Each exists because the failure it catches would otherwise be
// discovered only after hours of measurement had already been spent.
type ChainRequirements struct {
	// ClientVersion asserts that a client adapter's pinned image is the build
	// the operator meant to measure.
	ClientVersion *ChainClientVersion `json:"client_version,omitempty"`
	// ServerPipeliningContainer asserts the named container advertises
	// PIPELINING, by checking its configured environment.
	ServerPipeliningContainer string `json:"server_pipelining_container,omitempty"`
	// FixtureRoots must all exist and be directories.
	FixtureRoots []string `json:"fixture_roots,omitempty"`
	// AbsentProcesses must not be running: another tenant on the benchmark host
	// invalidates every timing the session would record.
	AbsentProcesses []string `json:"absent_processes,omitempty"`
}

// ChainClientVersion names the binary inside a client image, which is not
// always that image's own entrypoint, and the version strings that satisfy the
// check.
type ChainClientVersion struct {
	Client     string   `json:"client"`
	Entrypoint string   `json:"entrypoint,omitempty"`
	Args       []string `json:"args,omitempty"`
	// Accept lists substrings, any one of which satisfies the check.
	Accept []string `json:"accept"`
	// RejectImage lists image substrings that fail outright, so a known stale
	// pin is reported as the stale pin it is rather than as a wrong version.
	RejectImage []string `json:"reject_image,omitempty"`
}

// ChainPhase is one measured plan run, together with the link conditions it
// must be measured under and the summaries to produce from it.
type ChainPhase struct {
	Name string `json:"name"`
	// Mode selects the execution command: sequential, queue or
	// queue-transition.
	Mode string `json:"mode"`
	// Plan names the saved plan this phase runs. PlanSpec generates it when it
	// is absent, so a whole session travels as one configuration plus a corpus
	// rather than as a configuration plus a pile of plans made by hand on
	// whichever machine happened to have the tooling.
	PlanSpec     *ChainPlanSpec `json:"plan_spec,omitempty"`
	Plan         string         `json:"plan"`
	FixturesRoot string         `json:"fixtures_root"`
	// Artifacts is resolved under ArtifactsDir unless it is absolute.
	Artifacts string `json:"artifacts"`

	// ServerLink and ServerRTT are this phase's link conditions. The shaper is
	// reconfigured only when they differ from the phase before, so consecutive
	// phases sharing conditions do not pay for a needless restart.
	ServerLink       string `json:"server_link,omitempty"`
	ServerRTT        string `json:"server_rtt,omitempty"`
	ServerEgressBPS  uint64 `json:"server_egress_bps,omitempty"`
	ServerBurstBytes uint64 `json:"server_burst_bytes,omitempty"`

	// NFS carries the shaped-storage wiring a storage phase needs.
	NFS *ChainNFS `json:"nfs,omitempty"`

	// SummarizeBaselines lists baseline clients to summarize this phase
	// against; QueueDrainSummary asks for the drain summary instead.
	SummarizeBaselines []string `json:"summarize_baselines,omitempty"`
	QueueDrainSummary  bool     `json:"queue_drain_summary,omitempty"`
	SummarizeCandidate string   `json:"summarize_candidate,omitempty"`
	MinimumBlocks      int      `json:"minimum_blocks,omitempty"`
}

// ChainPlanSpec is everything a phase's plan is built from. The link
// conditions are not repeated here: a plan records the link it was built for,
// and it is the phase that declares those, so the two can never disagree.
type ChainPlanSpec struct {
	// Fixtures lists the corpus explicitly, FixtureSet names one the
	// configuration declares, and Corpus reads one from a declared corpus
	// file. Exactly one of the three is used.
	Fixtures   []string `json:"fixtures,omitempty"`
	FixtureSet string   `json:"fixture_set,omitempty"`
	Corpus     string   `json:"corpus,omitempty"`
	// ExcludeFixtures drops named fixtures from whichever of those was used.
	// Every id must be present, so a typo is refused rather than silently
	// keeping the fixture it was meant to drop.
	ExcludeFixtures []string `json:"exclude_fixtures,omitempty"`

	Clients           []string `json:"clients,omitempty"`
	ArchiveToolchains []string `json:"archive_toolchains,omitempty"`
	Transports        []string `json:"transports,omitempty"`
	Targets           []string `json:"targets,omitempty"`
	Profile           string   `json:"profile"`
	Repetitions       int      `json:"repetitions"`
	Seed              int64    `json:"seed"`

	StorageProfile string `json:"storage_profile,omitempty"`
	NFSLink        string `json:"nfs_link,omitempty"`

	// ExcludeClients removes one client from one fixture's lanes, with the
	// reason recorded in the plan and carried into the summary.
	ExcludeClients []ChainClientExclusion `json:"exclude_clients,omitempty"`
}

// ChainClientExclusion records a client that deterministically cannot finish a
// fixture. The reason travels into the plan and out through the summary, so an
// absent result is always explained.
type ChainClientExclusion struct {
	Client    string `json:"client"`
	FixtureID string `json:"fixture_id"`
	Reason    string `json:"reason"`
}

// ChainNFS carries the shaped-NFS wiring a storage phase needs.
type ChainNFS struct {
	Container    string `json:"container"`
	Network      string `json:"network,omitempty"`
	HelperImage  string `json:"helper_image,omitempty"`
	VerifyBinary string `json:"verify_binary,omitempty"`
}

// ChainPhaseResult records what a phase did, in a form that outlives the run.
type ChainPhaseResult struct {
	Name       string    `json:"name"`
	Mode       string    `json:"mode"`
	ServerLink string    `json:"server_link"`
	ServerRTT  string    `json:"server_rtt"`
	StartedAt  time.Time `json:"started_at"`
	EndedAt    time.Time `json:"ended_at"`
	ExitCode   int       `json:"exit_code"`
	Verdict    string    `json:"verdict"`
	Suites     int       `json:"suites"`
	Artifacts  string    `json:"artifacts"`
	LogPath    string    `json:"log_path"`
	Summaries  []string  `json:"summaries,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// ChainResult is the machine-readable record of a whole session.
type ChainResult struct {
	SchemaVersion int                `json:"schema_version"`
	Name          string             `json:"name"`
	Host          string             `json:"host"`
	Platform      string             `json:"platform"`
	StartedAt     time.Time          `json:"started_at"`
	EndedAt       time.Time          `json:"ended_at"`
	Phases        []ChainPhaseResult `json:"phases"`
	Completed     bool               `json:"completed"`
}

// Phase verdicts. A client that did not finish is a measured outcome; a
// harness failure means the phase produced nothing to measure.
const (
	chainVerdictClean              = "clean"
	chainVerdictClientDidNotFinish = "client-did-not-finish"
	chainVerdictHarnessFailure     = "harness-failure"
)

func chain(args []string) error {
	flags := flag.NewFlagSet("chain", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var configPath, only string
	var dryRun, skipSummaries bool
	flags.StringVar(&configPath, "config", "", "chain configuration JSON")
	flags.BoolVar(&dryRun, "dry-run", false, "validate the configuration and host preconditions, then stop without measuring")
	flags.StringVar(&only, "only", "", "comma-separated phase names to run instead of every declared phase")
	flags.BoolVar(&skipSummaries, "skip-summaries", false, "measure every phase but produce no summaries")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if configPath == "" {
		return fmt.Errorf("--config is required")
	}
	config, err := loadChainConfig(configPath)
	if err != nil {
		return err
	}
	phases, err := selectChainPhases(config.Phases, only)
	if err != nil {
		return err
	}
	for _, dir := range []string{config.ArtifactsDir, config.LogDir, config.ServerEnvDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}

	logFile, err := os.OpenFile(filepath.Join(config.LogDir, chainLogName(config.Name)),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open the chain log: %w", err)
	}
	defer logFile.Close()
	log := chainLogger(io.MultiWriter(os.Stdout, logFile))

	release, err := acquireChainLock(config.LogDir)
	if err != nil {
		return err
	}
	defer release()

	platform := runtime.GOOS + "/" + runtime.GOARCH
	log("chain %s: %d phase(s) on a %s stack, %s", config.Name, len(phases), config.Stack, platform)
	if err := checkChainRequirements(config, log); err != nil {
		return err
	}
	// The stack is built before the dry run returns, so a raw session's
	// binaries, article store and ports are checked without starting anything.
	stack, err := newChainStack(&config, log)
	if err != nil {
		return err
	}
	defer stack.stop(log)
	if err := stack.preflight(log); err != nil {
		return err
	}
	plans, err := buildChainPlans(phases, config.Target, dryRun, log)
	if err != nil {
		return err
	}
	if err := checkChainPhaseFixtures(phases, plans, log); err != nil {
		return err
	}
	if err := checkChainArtifactRoots(phases); err != nil {
		return err
	}
	if dryRun {
		log("dry run: the configuration and every precondition are satisfied; nothing measured")
		return nil
	}

	host, _ := os.Hostname()
	result := ChainResult{
		SchemaVersion: ChainSchemaVersion, Name: config.Name,
		Host: host, Platform: platform, StartedAt: time.Now().UTC(),
	}
	resultPath := filepath.Join(config.LogDir, chainResultName(config.Name))
	settle, err := chainDuration(config.PhaseSettle, defaultPhaseSettle)
	if err != nil {
		return err
	}

	// The shaper's state at entry is unknown, so the first phase always
	// configures it rather than trusting whatever a previous session left.
	applied := ""
	for index, phase := range phases {
		if key := phase.shaperKey(); key != applied {
			if err := stack.apply(config, phase, log); err != nil {
				return err
			}
			applied = key
		}
		result.Phases = append(result.Phases, runChainPhase(config, phase, log))
		// The record is rewritten after every phase, so an interrupted session
		// still leaves an accurate account of what it measured.
		if err := writeChainResult(resultPath, result); err != nil {
			log("WARNING: could not write the chain result: %v", err)
		}
		if index+1 < len(phases) {
			time.Sleep(settle)
		}
	}

	// Summaries run only after every measurement is finished. Summarizing is
	// CPU- and memory-hungry, and running one beside a phase would charge that
	// phase for work which is not part of what it measures.
	if !skipSummaries {
		for index := range result.Phases {
			summarizeChainPhase(config, phases[index], &result.Phases[index], log)
		}
		if err := writeChainResult(resultPath, result); err != nil {
			log("WARNING: could not write the chain result: %v", err)
		}
	}

	if config.RestoreServerLink != "" {
		log("restoring the shaper to %s / %s", config.RestoreServerLink, chainRTTLabel(config.RestoreServerRTT))
		restore := ChainPhase{Name: "restore", ServerLink: config.RestoreServerLink, ServerRTT: config.RestoreServerRTT}
		if err := stack.apply(config, restore, log); err != nil {
			log("WARNING: could not restore the shaper: %v", err)
		}
	}
	result.EndedAt = time.Now().UTC()
	result.Completed = true
	if err := writeChainResult(resultPath, result); err != nil {
		return err
	}
	log("CHAIN-DONE %s", resultPath)
	return chainHarnessFailure(result)
}

// chainHarnessFailure reports the phases that measured nothing. A chain runs
// every phase whatever happens -- a long session must not be abandoned because
// one phase failed, and the record carries each verdict -- but exiting zero
// after measuring nothing tells a wrapper script the session succeeded. A
// client that did not finish is a recorded result and not this.
func chainHarnessFailure(result ChainResult) error {
	var failed []string
	for _, phase := range result.Phases {
		if phase.Verdict == chainVerdictHarnessFailure {
			failed = append(failed, fmt.Sprintf("%s (rc=%d)", phase.Name, phase.ExitCode))
		}
	}
	if len(failed) == 0 {
		return nil
	}
	return fmt.Errorf("%d of %d phases measured nothing: %s",
		len(failed), len(result.Phases), strings.Join(failed, ", "))
}

// loadChainConfig reads, validates and normalizes a session description. Every
// relative path is resolved against the configuration file's own directory, so
// a chain and its plans move between machines as one unit.
func loadChainConfig(path string) (ChainConfig, error) {
	var config ChainConfig
	file, err := os.Open(path)
	if err != nil {
		return config, fmt.Errorf("read the chain config: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	// An unknown field is far more likely to be a misspelled key the operator
	// meant to set than a deliberate annotation, and silently ignoring one
	// would measure something other than what was asked for.
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return config, fmt.Errorf("parse the chain config: %w", err)
	}
	if config.SchemaVersion != ChainSchemaVersion {
		return config, fmt.Errorf("chain config schema_version %d is not supported; this build reads %d",
			config.SchemaVersion, ChainSchemaVersion)
	}
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return config, err
	}
	if config.Name == "" {
		config.Name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	if config.Stack == "" {
		config.Stack = ChainStackDocker
	}
	if config.ComposeProject == "" {
		config.ComposeProject = "nntp-bench"
	}
	if config.ShaperService == "" {
		config.ShaperService = "nntp-shaper"
	}
	if config.ServerService == "" {
		config.ServerService = "nntp"
	}
	if config.ArtifactsDir == "" {
		config.ArtifactsDir = base
	}
	if config.LogDir == "" {
		config.LogDir = config.ArtifactsDir
	}
	if config.ServerEnvDir == "" {
		config.ServerEnvDir = base
	}
	for _, field := range []*string{
		&config.ComposeFile, &config.PasswordFile, &config.Adapters, &config.CAFile,
		&config.ArtifactsDir, &config.LogDir, &config.ServerEnvDir,
	} {
		*field = resolveChainPath(base, *field)
	}
	if config.Stack == ChainStackRaw && config.Raw != nil {
		if config.Raw.CertDir == "" {
			config.Raw.CertDir = filepath.Join(base, "certs")
		}
		for _, field := range []*string{&config.Raw.BinDir, &config.Raw.DataDir, &config.Raw.CertDir} {
			*field = resolveChainPath(base, *field)
		}
	}
	for i := range config.Phases {
		phase := &config.Phases[i]
		phase.Plan = resolveChainPath(base, phase.Plan)
		phase.FixturesRoot = resolveChainPath(base, phase.FixturesRoot)
		phase.Artifacts = resolveChainPath(config.ArtifactsDir, phase.Artifacts)
		if phase.NFS != nil {
			phase.NFS.VerifyBinary = resolveChainPath(base, phase.NFS.VerifyBinary)
		}
	}
	for i := range config.Require.FixtureRoots {
		config.Require.FixtureRoots[i] = resolveChainPath(base, config.Require.FixtureRoots[i])
	}
	if err := resolveChainFixtureSets(&config, base); err != nil {
		return config, err
	}
	return config, validateChainConfig(config)
}

// resolveChainFixtureSets replaces each plan spec's named set with the corpus
// the configuration declares for it, so everything downstream reads one field.
func resolveChainFixtureSets(config *ChainConfig, base string) error {
	for i := range config.Phases {
		spec := config.Phases[i].PlanSpec
		if spec == nil {
			continue
		}
		spec.Corpus = resolveChainPath(base, spec.Corpus)
		if spec.FixtureSet == "" {
			continue
		}
		if len(spec.Fixtures) > 0 {
			return fmt.Errorf("phase %s: plan_spec sets both fixtures and fixture_set", config.Phases[i].Name)
		}
		set, ok := config.FixtureSets[spec.FixtureSet]
		if !ok {
			return fmt.Errorf("phase %s: plan_spec names fixture set %q, which the configuration does not declare",
				config.Phases[i].Name, spec.FixtureSet)
		}
		if len(set) == 0 {
			return fmt.Errorf("fixture set %q is empty", spec.FixtureSet)
		}
		spec.Fixtures = append([]string(nil), set...)
	}
	return nil
}

// resolveChainPath anchors a relative path to base, leaving empty and absolute
// values untouched.
func resolveChainPath(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}

func validateChainConfig(config ChainConfig) error {
	if len(config.Phases) == 0 {
		return fmt.Errorf("the chain config declares no phases")
	}
	if err := validateChainStack(config); err != nil {
		return err
	}
	if config.Adapters == "" {
		return fmt.Errorf("adapters is required")
	}
	if _, err := chainDuration(config.PhaseSettle, defaultPhaseSettle); err != nil {
		return fmt.Errorf("phase_settle: %w", err)
	}
	if _, err := chainDuration(config.Timeout, 0); err != nil {
		return fmt.Errorf("timeout: %w", err)
	}
	seen := make(map[string]bool, len(config.Phases))
	artifacts := make(map[string]string, len(config.Phases))
	for _, phase := range config.Phases {
		switch {
		case phase.Name == "":
			return fmt.Errorf("every phase needs a name")
		case seen[phase.Name]:
			return fmt.Errorf("phase %q is declared twice; phase names become log and summary paths and must be unique", phase.Name)
		case phase.Plan == "":
			return fmt.Errorf("phase %s: plan is required", phase.Name)
		case phase.FixturesRoot == "":
			return fmt.Errorf("phase %s: fixtures_root is required", phase.Name)
		case phase.Artifacts == "":
			return fmt.Errorf("phase %s: artifacts is required", phase.Name)
		}
		seen[phase.Name] = true
		// Two phases writing one artifact root would interleave their suites
		// and make the summary of either meaningless.
		if other, clash := artifacts[phase.Artifacts]; clash {
			return fmt.Errorf("phases %s and %s both write %s; every phase needs its own artifact root",
				other, phase.Name, phase.Artifacts)
		}
		artifacts[phase.Artifacts] = phase.Name
		switch phase.Mode {
		case "sequential", "queue", "queue-transition":
		default:
			return fmt.Errorf("phase %s: mode %q is not one of sequential, queue, queue-transition", phase.Name, phase.Mode)
		}
		if _, err := phase.linkProfile(); err != nil {
			return fmt.Errorf("phase %s: %w", phase.Name, err)
		}
		if phase.NFS != nil && phase.NFS.Container == "" {
			return fmt.Errorf("phase %s: nfs.container is required when nfs is set", phase.Name)
		}
		if phase.NFS != nil && config.Stack == ChainStackRaw {
			return fmt.Errorf("phase %s: an NFS storage profile needs the throttled export container, which a raw stack does not run; native lanes are local-storage only", phase.Name)
		}
	}
	if config.RestoreServerLink != "" {
		restore := ChainPhase{ServerLink: config.RestoreServerLink, ServerRTT: config.RestoreServerRTT}
		if _, err := restore.linkProfile(); err != nil {
			return fmt.Errorf("restore_server_link: %w", err)
		}
	}
	return nil
}

// Stack kinds.
const (
	ChainStackDocker = "docker"
	ChainStackRaw    = "raw"
)

// validateChainStack holds each stack kind to the fields that mean something
// for it. A raw stack carrying Compose settings, or a Docker requirement that
// inspects containers, describes a host arrangement the session will not have.
func validateChainStack(config ChainConfig) error {
	switch config.Stack {
	case ChainStackDocker:
		if config.ComposeFile == "" {
			return fmt.Errorf("compose_file is required for a docker stack")
		}
		if config.Raw != nil {
			return fmt.Errorf("raw is set on a docker stack")
		}
		return nil
	case ChainStackRaw:
	default:
		return fmt.Errorf("stack %q is not one of %s, %s", config.Stack, ChainStackDocker, ChainStackRaw)
	}
	if config.Raw == nil {
		return fmt.Errorf("raw is required for a raw stack")
	}
	if config.ComposeFile != "" {
		return fmt.Errorf("compose_file is set on a raw stack, which runs no containers")
	}
	if config.Raw.BinDir == "" || config.Raw.DataDir == "" {
		return fmt.Errorf("raw needs bin_dir and data_dir")
	}
	if _, err := chainDuration(config.Raw.StartTimeout, rawstack.DefaultStartTimeout); err != nil {
		return fmt.Errorf("raw.start_timeout: %w", err)
	}
	if config.Require.ClientVersion != nil {
		return fmt.Errorf("require.client_version inspects a client's Docker image, which a raw stack does not have; pin the native client in the adapter catalog instead")
	}
	if config.Require.ServerPipeliningContainer != "" {
		return fmt.Errorf("require.server_pipelining_container inspects a container a raw stack does not run; raw.pipelining sets it directly")
	}
	// Every execution target that is not docker-linux names an operating
	// system, and there is no native Linux target to record a raw Linux run
	// under. Recording one as docker-linux would file it beside results from a
	// different packaging boundary.
	if _, err := rawExecutionTarget(); err != nil {
		return err
	}
	return nil
}

// rawExecutionTarget is the execution target a raw stack's host records under.
func rawExecutionTarget() (benchmark.ExecutionTarget, error) {
	switch runtime.GOOS {
	case "darwin":
		return benchmark.MacOSNative, nil
	case "windows":
		return benchmark.WindowsNative, nil
	default:
		return "", fmt.Errorf("a raw stack has no execution target on %s; the benchmark records native runs as %s or %s only",
			runtime.GOOS, benchmark.MacOSNative, benchmark.WindowsNative)
	}
}

// selectChainPhases narrows a session to the named phases, preserving the
// declared order so a partial rerun still measures in a comparable sequence.
func selectChainPhases(phases []ChainPhase, only string) ([]ChainPhase, error) {
	if strings.TrimSpace(only) == "" {
		return phases, nil
	}
	wanted := make(map[string]bool)
	for _, name := range strings.Split(only, ",") {
		if name = strings.TrimSpace(name); name != "" {
			wanted[name] = true
		}
	}
	var selected []ChainPhase
	for _, phase := range phases {
		if wanted[phase.Name] {
			selected = append(selected, phase)
			delete(wanted, phase.Name)
		}
	}
	if len(wanted) > 0 {
		missing := make([]string, 0, len(wanted))
		for name := range wanted {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return nil, fmt.Errorf("--only names phases this chain does not declare: %s", strings.Join(missing, ", "))
	}
	return selected, nil
}

// linkProfile resolves the phase's link conditions through the same validator
// the server-env command uses, so a session cannot describe a link the shaper
// would refuse to render.
func (p ChainPhase) linkProfile() (benchmark.ServerLinkProfile, error) {
	profile := p.ServerLink
	if profile == "" {
		profile = benchmark.LinkUnlimited
	}
	rtt, err := parseChainRTT(p.ServerRTT)
	if err != nil {
		return benchmark.ServerLinkProfile{}, err
	}
	return benchmark.ResolveServerLinkProfile(profile, p.ServerEgressBPS, p.ServerBurstBytes, serverRTTMicros(rtt))
}

// parseChainRTT reads a phase's declared round trip. An empty value means no
// added latency, so a phase that does not care about latency need not say so.
func parseChainRTT(value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	rtt, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("server_rtt %q is not a duration: %w", value, err)
	}
	if rtt < 0 {
		return 0, fmt.Errorf("server_rtt %q is negative", value)
	}
	return rtt, nil
}

func chainDuration(value string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%q is not a duration: %w", value, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%q is negative", value)
	}
	return parsed, nil
}

// shaperKey identifies a phase's link conditions, so consecutive phases that
// share conditions do not restart the shaper between them.
func (p ChainPhase) shaperKey() string {
	return fmt.Sprintf("%s|%s|%d|%d", p.ServerLink, p.ServerRTT, p.ServerEgressBPS, p.ServerBurstBytes)
}

func chainRTTLabel(value string) string {
	if strings.TrimSpace(value) == "" {
		return "0s"
	}
	return value
}

// applyChainShaper writes the phase's link environment file, recreates the
// shaper from it, and refreshes the CA the phases present to the server. The
// server itself is never recreated: it holds the seeded article store.
// chainStack is the server side a session measures through. The phase loop
// only ever asks for a link, so the two arrangements -- containers under
// Compose, or local processes -- stay interchangeable from its point of view.
type chainStack interface {
	// preflight reports host conditions that only the host can answer, before
	// anything is started or measured.
	preflight(log func(string, ...any)) error
	apply(config ChainConfig, phase ChainPhase, log func(string, ...any)) error
	stop(log func(string, ...any))
}

// chainRawStackConfig is the one place a chain description becomes a raw stack
// configuration. Preflight settles its check from here too, so a host that
// passes a check is the host the session will actually run on.
func chainRawStackConfig(config ChainConfig) (rawstack.Config, error) {
	if config.Raw == nil {
		return rawstack.Config{}, fmt.Errorf("chain %s declares no raw stack", config.Name)
	}
	timeout, err := chainDuration(config.Raw.StartTimeout, rawstack.DefaultStartTimeout)
	if err != nil {
		return rawstack.Config{}, err
	}
	pipelining := true
	if config.Raw.Pipelining != nil {
		pipelining = *config.Raw.Pipelining
	}
	return rawstack.Config{
		BinDir:                config.Raw.BinDir,
		DataDir:               config.Raw.DataDir,
		CertDir:               config.Raw.CertDir,
		LogDir:                config.LogDir,
		Username:              defaultChainString(config.Username, "fixture-user"),
		PasswordFile:          config.PasswordFile,
		Pipelining:            pipelining,
		Host:                  config.Raw.Host,
		UpstreamPlaintextPort: config.Raw.UpstreamPlaintextPort,
		UpstreamTLSPort:       config.Raw.UpstreamTLSPort,
		PlaintextPort:         config.Raw.PlaintextPort,
		TLSPort:               config.Raw.TLSPort,
		ControlPort:           config.Raw.ControlPort,
		DelayQueueBytes:       config.Raw.DelayQueueBytes,
		StartTimeout:          timeout,
	}, nil
}

// newChainStack builds the session's stack and, for a raw one, settles every
// value the phases need to reach it: the ports, the host, the CA the server
// will generate and the shaper's control plane. Deriving them here rather than
// asking the operator to restate them in the config keeps the run arguments
// and the running processes from ever describing different endpoints.
func newChainStack(config *ChainConfig, log func(string, ...any)) (chainStack, error) {
	if config.Stack != ChainStackRaw {
		return dockerChainStack{}, nil
	}
	target, err := rawExecutionTarget()
	if err != nil {
		return nil, err
	}
	settings, err := chainRawStackConfig(*config)
	if err != nil {
		return nil, err
	}
	stack, err := rawstack.New(settings)
	if err != nil {
		return nil, err
	}
	config.Target = defaultChainString(config.Target, string(target))
	config.Username = defaultChainString(config.Username, stack.Username())
	config.NNTPHost = defaultChainString(config.NNTPHost, stack.Host())
	config.NNTPPort = defaultChainString(config.NNTPPort, stack.PlaintextPort())
	config.NNTPTLSPort = defaultChainString(config.NNTPTLSPort, stack.TLSPort())
	config.CAFile = defaultChainString(config.CAFile, stack.CAFile())
	config.ShaperControlURL = defaultChainString(config.ShaperControlURL, stack.ControlURL())
	log("raw stack: %s:%s plaintext, %s TLS, control %s, target %s",
		config.NNTPHost, config.NNTPPort, config.NNTPTLSPort, config.ShaperControlURL, config.Target)
	return &rawChainStack{stack: stack}, nil
}

func defaultChainString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

type dockerChainStack struct{}

// The Compose stack's own preconditions are checked against the containers it
// runs, in checkChainRequirements; there is nothing further to ask the host.
func (dockerChainStack) preflight(func(string, ...any)) error { return nil }

func (dockerChainStack) apply(config ChainConfig, phase ChainPhase, log func(string, ...any)) error {
	return applyChainShaper(config, phase, log)
}

func (dockerChainStack) stop(func(string, ...any)) {}

type rawChainStack struct {
	stack   *rawstack.Stack
	started bool
}

// apply starts the stack for the first link and replaces the shaper for every
// later one. The server is never restarted: reopening the article store
// between phases would charge one phase for another's cold cache.
// preflight asks the host whether the stack's ports are actually free. It is
// the one condition a configuration cannot settle, and an occupied port
// otherwise surfaces as a failed start after the plans are already built.
func (r *rawChainStack) preflight(log func(string, ...any)) error {
	failed := rawstack.FailedChecks(r.stack.Preflight())
	if len(failed) == 0 {
		log("precondition ok: the raw stack's binaries, article store and ports are all available")
		return nil
	}
	reasons := make([]string, 0, len(failed))
	for _, check := range failed {
		reasons = append(reasons, check.Reason)
	}
	return fmt.Errorf("the raw stack cannot start on this host:\n  %s", strings.Join(reasons, "\n  "))
}

func (r *rawChainStack) apply(config ChainConfig, phase ChainPhase, log func(string, ...any)) error {
	profile, err := phase.linkProfile()
	if err != nil {
		return err
	}
	// The link file is written for a raw session too. It is the record of the
	// conditions a run was measured under, and it is what a later reader
	// compares against, whether or not a container ever read it.
	envPath := filepath.Join(config.ServerEnvDir, chainServerEnvName(phase))
	if err := writeChainServerEnv(envPath, profile); err != nil {
		return err
	}
	ctx := context.Background()
	if !r.started {
		log("raw stack -> link %s, rtt %s (%s)", profile.ID, chainRTTLabel(phase.ServerRTT), filepath.Base(envPath))
		if err := r.stack.Start(ctx, profile); err != nil {
			return err
		}
		r.started = true
		return nil
	}
	log("shaper -> link %s, rtt %s (%s)", profile.ID, chainRTTLabel(phase.ServerRTT), filepath.Base(envPath))
	return r.stack.Reshape(ctx, profile)
}

func (r *rawChainStack) stop(log func(string, ...any)) {
	if !r.started {
		return
	}
	if err := r.stack.Stop(); err != nil {
		log("WARNING: could not stop the raw stack: %v", err)
	}
}

func applyChainShaper(config ChainConfig, phase ChainPhase, log func(string, ...any)) error {
	profile, err := phase.linkProfile()
	if err != nil {
		return err
	}
	envPath := filepath.Join(config.ServerEnvDir, chainServerEnvName(phase))
	if err := writeChainServerEnv(envPath, profile); err != nil {
		return err
	}
	log("shaper -> link %s, rtt %s (%s)", profile.ID, chainRTTLabel(phase.ServerRTT), filepath.Base(envPath))
	if err := composeChain(config, envPath, "up", "-d", config.ShaperService); err != nil {
		return err
	}
	if config.CAFile != "" {
		if err := composeChain(config, envPath, "cp", config.ServerService+":/certs/ca.pem", config.CAFile); err != nil {
			return fmt.Errorf("refresh the server CA: %w", err)
		}
	}
	time.Sleep(defaultShaperSettle)
	return nil
}

// writeChainServerEnv produces the link environment file, or confirms that an
// existing file already describes exactly these conditions. The writer refuses
// to overwrite, because a link file is the record of the conditions some past
// run was measured under; a session needing different conditions gets its own
// file, and one that matches reuses it.
func writeChainServerEnv(path string, profile benchmark.ServerLinkProfile) error {
	err := benchmark.WriteServerLinkEnvironment(path, profile)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("write the link environment: %w", err)
	}
	existing, readErr := os.ReadFile(path)
	if readErr != nil {
		return fmt.Errorf("read the existing link environment %s: %w", path, readErr)
	}
	expected, expectErr := renderChainServerEnv(profile)
	if expectErr != nil {
		return expectErr
	}
	if string(existing) != expected {
		return fmt.Errorf("%s already exists and does not describe link %s at %s; move it aside, or point server_env_dir somewhere else",
			path, profile.ID, profile.RTT())
	}
	return nil
}

// renderChainServerEnv produces the exact bytes the environment writer would
// write, by writing them, so the comparison above can never drift from the
// real format.
func renderChainServerEnv(profile benchmark.ServerLinkProfile) (string, error) {
	dir, err := os.MkdirTemp("", "nntpbench-link")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "link.env")
	if err := benchmark.WriteServerLinkEnvironment(path, profile); err != nil {
		return "", err
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func composeChain(config ChainConfig, envPath string, args ...string) error {
	full := append([]string{
		"compose", "-p", config.ComposeProject,
		"--env-file", envPath, "-f", config.ComposeFile,
	}, args...)
	command := exec.Command("docker", full...)
	command.Env = os.Environ()
	if config.PasswordFile != "" {
		command.Env = append(command.Env, "NNTP_BENCH_PASSWORD_FILE="+config.PasswordFile)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

// chainServerEnvName derives a stable name from the link conditions, so two
// phases measured under the same conditions share one environment file.
func chainServerEnvName(phase ChainPhase) string {
	link := phase.ServerLink
	if link == "" {
		link = benchmark.LinkUnlimited
	}
	rtt := strings.TrimSpace(phase.ServerRTT)
	if rtt == "" {
		rtt = "0s"
	}
	if link == benchmark.LinkCustom {
		return fmt.Sprintf("server-custom-%d-%d-rtt%s.env", phase.ServerEgressBPS, phase.ServerBurstBytes, sanitizeChainName(rtt))
	}
	return fmt.Sprintf("server-%s-rtt%s.env", link, sanitizeChainName(rtt))
}

func chainLogName(name string) string      { return "chain-" + sanitizeChainName(name) + ".log" }
func chainResultName(name string) string   { return "chain-" + sanitizeChainName(name) + "-result.json" }
func chainPhaseLogName(name string) string { return "run-" + sanitizeChainName(name) + ".log" }

// sanitizeChainName keeps generated file names portable: Windows rejects
// several characters that are perfectly legal in a phase name.
func sanitizeChainName(name string) string {
	mapped := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-', r == '_', r == '.':
			return r
		default:
			return '-'
		}
	}, name)
	if mapped == "" {
		return "chain"
	}
	return mapped
}

// runChainPhase measures one phase by re-executing this same binary. Running
// the phase out of process keeps a phase that exhausts memory while rendering
// its artifacts from taking the session down with it, gives every phase an
// exit status of its own, and makes each phase reproducible by hand from the
// command line the log records.
func runChainPhase(config ChainConfig, phase ChainPhase, log func(string, ...any)) ChainPhaseResult {
	result := ChainPhaseResult{
		Name: phase.Name, Mode: phase.Mode,
		ServerLink: phase.ServerLink, ServerRTT: chainRTTLabel(phase.ServerRTT),
		Artifacts: phase.Artifacts, StartedAt: time.Now().UTC(),
	}
	// A raw session starts no containers, so there are none to clean up and
	// nothing to ask a Docker daemon that may not be installed at all.
	if config.Stack != ChainStackRaw {
		removeStrayRunContainers(log)
	}
	releaseShaperLease(config.ShaperControlURL, log)

	logPath := filepath.Join(config.LogDir, chainPhaseLogName(phase.Name))
	result.LogPath = logPath
	args := chainPhaseArgs(config, phase)
	log("STARTING-%s: nntpbench %s", phase.Name, strings.Join(args, " "))
	code, err := runChainSelf(args, logPath)
	result.EndedAt = time.Now().UTC()
	result.ExitCode = code
	if err != nil {
		result.Error = err.Error()
	}
	switch code {
	case 0:
		result.Verdict = chainVerdictClean
	case benchmark.ExitStatusClientDidNotFinish:
		result.Verdict = chainVerdictClientDidNotFinish
	default:
		result.Verdict = chainVerdictHarnessFailure
	}
	result.Suites = countChainSuites(phase.Artifacts)
	log("%s-EXITED rc=%d %s suites=%d after %s", phase.Name, code, result.Verdict,
		result.Suites, result.EndedAt.Sub(result.StartedAt).Round(time.Second))
	return result
}

// chainPhaseArgs builds a phase's command line. It mirrors what an operator
// would type, so a failed phase can be reproduced from the log without
// reconstructing anything. The optional flags are emitted in a fixed order:
// two identical phases must not produce command lines that differ only in
// their arrangement.
func chainPhaseArgs(config ChainConfig, phase ChainPhase) []string {
	args := []string{
		phase.Mode,
		"--plan", phase.Plan,
		"--artifacts", phase.Artifacts,
		"--adapters", config.Adapters,
		"--fixtures-root", phase.FixturesRoot,
	}
	optional := map[string]string{
		"--target":             config.Target,
		"--nntp-host":          config.NNTPHost,
		"--nntp-port":          config.NNTPPort,
		"--nntp-tls-port":      config.NNTPTLSPort,
		"--shaper-control-url": config.ShaperControlURL,
		"--tls-ca-file":        config.CAFile,
		"--username":           config.Username,
		"--password-file":      config.PasswordFile,
		"--timeout":            config.Timeout,
	}
	if config.Connections > 0 {
		optional["--connections"] = fmt.Sprint(config.Connections)
	}
	if phase.NFS != nil {
		optional["--nfs-container"] = phase.NFS.Container
		optional["--nfs-network"] = phase.NFS.Network
		optional["--nfs-helper-image"] = phase.NFS.HelperImage
		optional["--nfs-verify-binary"] = phase.NFS.VerifyBinary
	}
	names := make([]string, 0, len(optional))
	for name, value := range optional {
		if value != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		args = append(args, name, optional[name])
	}
	return args
}

// summarizeChainPhase produces the phase's declared summaries. A summary that
// cannot be produced is logged and survived: the measurements are already on
// disk, and the summarize command can still read them afterwards.
func summarizeChainPhase(config ChainConfig, phase ChainPhase, result *ChainPhaseResult, log func(string, ...any)) {
	candidate := phase.SummarizeCandidate
	if candidate == "" {
		candidate = "weaver"
	}
	minimum := phase.MinimumBlocks
	if minimum <= 0 {
		minimum = 3
	}
	for _, baseline := range phase.SummarizeBaselines {
		name := fmt.Sprintf("summary-%s-vs-%s", sanitizeChainName(phase.Name), sanitizeChainName(baseline))
		args := []string{
			"summarize",
			"--artifacts", phase.Artifacts,
			"--baseline", baseline,
			"--candidate", candidate,
			"--minimum-blocks", fmt.Sprint(minimum),
		}
		if path, ok := runChainSummary(config, name, args, log); ok {
			result.Summaries = append(result.Summaries, path)
		}
	}
	if phase.QueueDrainSummary {
		name := fmt.Sprintf("summary-%s-queue-drain", sanitizeChainName(phase.Name))
		args := []string{"summarize", "--mode", "queue-drain", "--artifacts", phase.Artifacts}
		if path, ok := runChainSummary(config, name, args, log); ok {
			result.Summaries = append(result.Summaries, path)
		}
	}
}

func runChainSummary(config ChainConfig, name string, args []string, log func(string, ...any)) (string, bool) {
	outPath := filepath.Join(config.LogDir, name+".json")
	errPath := filepath.Join(config.LogDir, name+".err")
	code, err := runChainSelfSplit(args, outPath, errPath)
	if code != 0 || err != nil {
		log("%s FAILED rc=%d; see %s", name, code, filepath.Base(errPath))
		return "", false
	}
	log("%s ok", name)
	return outPath, true
}

// runChainSelf runs this binary again, sending both output streams to one log.
func runChainSelf(args []string, logPath string) (int, error) {
	file, err := os.Create(logPath)
	if err != nil {
		return -1, fmt.Errorf("create the phase log: %w", err)
	}
	defer file.Close()
	return runChainCommand(args, file, file)
}

// runChainSelfSplit runs this binary again, keeping the streams apart so a
// summary's JSON is never contaminated by its own diagnostics.
func runChainSelfSplit(args []string, outPath, errPath string) (int, error) {
	out, err := os.Create(outPath)
	if err != nil {
		return -1, err
	}
	defer out.Close()
	errFile, err := os.Create(errPath)
	if err != nil {
		return -1, err
	}
	defer errFile.Close()
	return runChainCommand(args, out, errFile)
}

func runChainCommand(args []string, stdout, stderr io.Writer) (int, error) {
	self, err := os.Executable()
	if err != nil {
		return -1, fmt.Errorf("locate this executable: %w", err)
	}
	command := exec.Command(self, args...)
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode(), nil
		}
		return -1, err
	}
	return 0, nil
}

// countChainSuites reports how many suite directories a phase produced, which
// is the cheapest honest signal that a phase did real work even when it exited
// with a failure.
func countChainSuites(artifacts string) int {
	entries, err := os.ReadDir(artifacts)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// The execution commands name suite directories <mode>-NNNN.
		if index := strings.LastIndex(entry.Name(), "-"); index > 0 && isChainDigits(entry.Name()[index+1:]) {
			count++
		}
	}
	return count
}

func isChainDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// removeStrayRunContainers clears client containers a killed phase left
// behind. A stray container holding the client's ports fails the next phase
// for a reason that has nothing to do with what that phase measures.
func removeStrayRunContainers(log func(string, ...any)) {
	output, err := exec.Command("docker", "ps", "-a", "--format", "{{.Names}}").Output()
	if err != nil {
		return
	}
	for _, name := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if name = strings.TrimSpace(name); name == "" || !strings.Contains(name, "nntpbench-run") {
			continue
		}
		log("removing the stray run container %s", name)
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}
}

// releaseShaperLease drops an execution lease a killed phase never returned.
// The shaper refuses concurrent measurement, so a stale lease would block the
// next phase for its whole timeout.
func releaseShaperLease(controlURL string, log func(string, ...any)) {
	if controlURL == "" {
		return
	}
	base := strings.TrimSuffix(controlURL, "/")
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Get(base + "/v1/stats")
	if err != nil {
		return
	}
	defer response.Body.Close()
	var stats struct {
		LeaseID string `json:"execution_lease_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&stats); err != nil || stats.LeaseID == "" {
		return
	}
	log("releasing the stale shaper lease %s", stats.LeaseID)
	payload, err := json.Marshal(map[string]string{"lease_id": stats.LeaseID})
	if err != nil {
		return
	}
	request, err := http.NewRequest(http.MethodDelete, base+"/v1/lease", strings.NewReader(string(payload)))
	if err != nil {
		return
	}
	request.Header.Set("content-type", "application/json")
	if response, err := client.Do(request); err == nil {
		response.Body.Close()
	}
}

// checkChainRequirements verifies the host before any measurement is taken.
func checkChainRequirements(config ChainConfig, log func(string, ...any)) error {
	for _, root := range config.Require.FixtureRoots {
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("the required fixture root %s is missing", root)
		}
	}
	for _, name := range config.Require.AbsentProcesses {
		running, err := processRunning(name)
		if err != nil {
			return fmt.Errorf("check whether %s is running: %w", name, err)
		}
		if running {
			return fmt.Errorf("%s is running on this host; every timing this session recorded would be contaminated by it", name)
		}
	}
	if container := config.Require.ServerPipeliningContainer; container != "" {
		ok, err := containerHasEnv(container, "NNTP_PIPELINING=1")
		if err != nil {
			return fmt.Errorf("inspect %s: %w", container, err)
		}
		if !ok {
			return fmt.Errorf("the NNTP service %s does not advertise PIPELINING; recreate it from the current compose file before measuring", container)
		}
		log("precondition ok: %s advertises PIPELINING", container)
	}
	if want := config.Require.ClientVersion; want != nil {
		image, err := adapterClientImage(config.Adapters, want.Client)
		if err != nil {
			return err
		}
		for _, reject := range want.RejectImage {
			if strings.Contains(image, reject) {
				return fmt.Errorf("the %s adapter still pins %s; pin the intended build before measuring", want.Client, image)
			}
		}
		reported, err := clientImageVersion(image, want.Entrypoint, want.Args)
		if err != nil {
			return fmt.Errorf("read the %s image version: %w", want.Client, err)
		}
		matched := len(want.Accept) == 0
		for _, accept := range want.Accept {
			if strings.Contains(reported, accept) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("the pinned %s image reports %q, which is none of %s; pin the intended build before measuring",
				want.Client, reported, strings.Join(want.Accept, ", "))
		}
		log("precondition ok: %s reports %q", want.Client, reported)
	}
	return nil
}

// checkChainArtifactRoots refuses to measure into a directory that already
// holds suites. A summary reads every suite under its root, so a rerun that
// reused a root would pool this session's measurements with the previous
// one's and report the mixture as a single result.
func checkChainArtifactRoots(phases []ChainPhase) error {
	for _, phase := range phases {
		if suites := countChainSuites(phase.Artifacts); suites > 0 {
			return fmt.Errorf("phase %s would measure into %s, which already holds %d suite(s); point it at a new artifact root so this session is not pooled with the last one",
				phase.Name, phase.Artifacts, suites)
		}
	}
	return nil
}

// buildChainPlans writes any plan a phase declared a spec for and does not
// already have. Building is deterministic in the spec's own seed, so a plan
// generated on one machine is byte-for-byte the plan generated on the next;
// an existing plan is left exactly as it is, because it is the record of what
// a past session measured.
// buildChainPlans builds every phase plan the chain will run. The target is
// the chain's own, because a plan that does not carry it is a plan this chain
// cannot run: the phase would start and be refused before its first suite.
func buildChainPlans(phases []ChainPhase, target string, dryRun bool, log func(string, ...any)) (map[string]benchmark.Plan, error) {
	built := make(map[string]benchmark.Plan)
	for _, phase := range phases {
		if phase.PlanSpec == nil {
			continue
		}
		if _, err := os.Stat(phase.Plan); err == nil {
			log("phase %s reuses the plan already at %s", phase.Name, filepath.Base(phase.Plan))
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("phase %s: %w", phase.Name, err)
		}
		plan, err := buildChainPlan(phase, target)
		if err != nil {
			return nil, fmt.Errorf("phase %s: %w", phase.Name, err)
		}
		built[phase.Name] = plan
		if dryRun {
			log("phase %s: would build %s from %d fixtures x %d repetitions = %d runs",
				phase.Name, filepath.Base(phase.Plan), len(plan.FixtureIDs), plan.Repetitions, len(plan.Runs))
			continue
		}
		if err := os.MkdirAll(filepath.Dir(phase.Plan), 0o755); err != nil {
			return nil, err
		}
		if err := benchmark.WritePlan(phase.Plan, plan); err != nil {
			return nil, fmt.Errorf("phase %s: %w", phase.Name, err)
		}
		log("phase %s: built %s from %d fixtures x %d repetitions = %d runs",
			phase.Name, filepath.Base(phase.Plan), len(plan.FixtureIDs), plan.Repetitions, len(plan.Runs))
	}
	return built, nil
}

// chainPlanTargets settles which execution targets a phase plan carries. A
// chain that names its target builds for it, so a native chain does not build
// a Docker plan and then be refused by its own first phase. A spec may still
// name targets outright -- a plan for several hosts is built once -- but it has
// to include the one this chain runs, and saying so here beats discovering it
// when the phase exits non-zero with nothing measured.
func chainPlanTargets(specified []string, target string) ([]benchmark.ExecutionTarget, error) {
	if len(specified) == 0 {
		specified = []string{defaultChainString(target, "docker-linux")}
	}
	targets, err := parseExecutionTargets(strings.Join(specified, ","))
	if err != nil {
		return nil, err
	}
	if target == "" {
		return targets, nil
	}
	for _, candidate := range targets {
		if string(candidate) == target {
			return targets, nil
		}
	}
	return nil, fmt.Errorf("plan_spec.targets %s does not include %q, the target this chain runs",
		strings.Join(specified, ","), target)
}

func buildChainPlan(phase ChainPhase, target string) (benchmark.Plan, error) {
	spec := phase.PlanSpec
	if spec.Profile == "" {
		return benchmark.Plan{}, fmt.Errorf("plan_spec.profile is required; a stock and an equivalent-throughput plan are separate plans")
	}
	fixtureIDs := append([]string(nil), spec.Fixtures...)
	if len(fixtureIDs) == 0 {
		if spec.Corpus == "" {
			return benchmark.Plan{}, fmt.Errorf("plan_spec needs either fixtures or corpus")
		}
		corpus, err := fixture.LoadCorpus(spec.Corpus)
		if err != nil {
			return benchmark.Plan{}, err
		}
		fixtureIDs = corpus.FixtureIDs
	}
	fixtureIDs, err := excludeFixtures(fixtureIDs, spec.ExcludeFixtures)
	if err != nil {
		return benchmark.Plan{}, err
	}
	clients, err := parseClients(strings.Join(defaultChainList(spec.Clients, "weaver", "sabnzbd", "nzbget"), ","))
	if err != nil {
		return benchmark.Plan{}, err
	}
	toolchains, err := parseArchiveToolchains(strings.Join(defaultChainList(spec.ArchiveToolchains, "vanilla"), ","))
	if err != nil {
		return benchmark.Plan{}, err
	}
	transports, err := parseTransports(strings.Join(defaultChainList(spec.Transports, "plaintext", "tls"), ","))
	if err != nil {
		return benchmark.Plan{}, err
	}
	targets, err := chainPlanTargets(spec.Targets, target)
	if err != nil {
		return benchmark.Plan{}, err
	}
	link, err := phase.linkProfile()
	if err != nil {
		return benchmark.Plan{}, err
	}
	storageID := spec.StorageProfile
	if storageID == "" {
		storageID = benchmark.StorageProfileLocal
	}
	storage, err := resolveStoragePlanProfile(storageID, spec.NFSLink)
	if err != nil {
		return benchmark.Plan{}, err
	}
	exclusions := make([]benchmark.ClientExclusion, 0, len(spec.ExcludeClients))
	for _, exclusion := range spec.ExcludeClients {
		if exclusion.Client == "" || exclusion.FixtureID == "" || exclusion.Reason == "" {
			return benchmark.Plan{}, fmt.Errorf("every exclude_clients entry needs a client, a fixture_id and a reason")
		}
		exclusions = append(exclusions, benchmark.ClientExclusion{
			Client:    benchmark.Client(exclusion.Client),
			FixtureID: exclusion.FixtureID,
			Reason:    exclusion.Reason,
		})
	}
	return benchmark.BuildPlan(benchmark.PlanOptions{
		FixtureIDs:        fixtureIDs,
		Clients:           clients,
		ArchiveToolchains: toolchains,
		Transports:        transports,
		Targets:           targets,
		Profile:           spec.Profile,
		ServerLink:        link,
		StorageProfile:    storage,
		Repetitions:       spec.Repetitions,
		Seed:              spec.Seed,
		ClientExclusions:  exclusions,
	})
}

func defaultChainList(values []string, fallback ...string) []string {
	if len(values) == 0 {
		return fallback
	}
	return values
}

// checkChainPhaseFixtures reads each phase's plan and confirms that the corpus
// it names can actually produce the result the phase is for. Both failures it
// catches are otherwise invisible until far too late: an undersized fixture
// fails its suite hours into the run, and an unclassified one runs to
// completion and then summarizes to nothing.
func checkChainPhaseFixtures(phases []ChainPhase, built map[string]benchmark.Plan, log func(string, ...any)) error {
	for _, phase := range phases {
		plan, ok := built[phase.Name]
		if !ok {
			loaded, err := benchmark.LoadPlan(phase.Plan)
			if err != nil {
				return fmt.Errorf("phase %s: %w", phase.Name, err)
			}
			plan = loaded
		}
		var missing, undersized, unclassified []string
		for _, id := range plan.FixtureIDs {
			manifest, err := fixture.LoadGeneratedManifest(
				filepath.Join(phase.FixturesRoot, id, "fixture-manifest.json"))
			if err != nil {
				missing = append(missing, id)
				continue
			}
			if err := manifest.ValidatePostedSize(); err != nil {
				undersized = append(undersized,
					fmt.Sprintf("%s (%.1f MiB)", id, float64(manifest.PostedBytes())/(1<<20)))
			}
			// A summary aggregates by fixture class, so a paired summary of
			// unclassified fixtures is empty however well the run went. A
			// phase that asks only for a drain summary does not use classes.
			if len(phase.SummarizeBaselines) > 0 && !manifest.Case.Class.Valid() {
				unclassified = append(unclassified, id)
			}
		}
		for _, problem := range []struct {
			fixtures []string
			message  string
		}{
			{missing, "have no fixture manifest under " + phase.FixturesRoot},
			{undersized, fmt.Sprintf("post less than the %d MiB floor and would fail their suites",
				fixture.MinimumPostedBytes>>20)},
			{unclassified, "declare no headline or breadth class, so this phase would summarize to nothing"},
		} {
			if len(problem.fixtures) == 0 {
				continue
			}
			sort.Strings(problem.fixtures)
			return fmt.Errorf("phase %s: %d of %d fixtures %s:\n  %s",
				phase.Name, len(problem.fixtures), len(plan.FixtureIDs), problem.message,
				strings.Join(problem.fixtures, "\n  "))
		}
		log("precondition ok: phase %s names %d usable fixtures", phase.Name, len(plan.FixtureIDs))
	}
	return nil
}

// processRunning reports whether a process whose command line contains name is
// running, using each platform's own process listing.
func processRunning(name string) (bool, error) {
	var command *exec.Cmd
	if runtime.GOOS == "windows" {
		command = exec.Command("tasklist", "/fo", "csv", "/nh")
	} else {
		command = exec.Command("ps", "-A", "-o", "args=")
	}
	output, err := command.Output()
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		if strings.Contains(line, name) {
			return true, nil
		}
	}
	return false, nil
}

// containerHasEnv reports whether a container's configured environment carries
// an exact NAME=VALUE entry.
func containerHasEnv(container, want string) (bool, error) {
	output, err := exec.Command("docker", "inspect", container,
		"--format", "{{range .Config.Env}}{{println .}}{{end}}").Output()
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		if strings.TrimSpace(line) == want {
			return true, nil
		}
	}
	return false, nil
}

// adapterClientImage reads one client's pinned image out of the adapter
// catalog, so the version check tests the image the phases will actually run
// rather than whatever tag happens to be present locally.
func adapterClientImage(adaptersPath, client string) (string, error) {
	contents, err := os.ReadFile(adaptersPath)
	if err != nil {
		return "", fmt.Errorf("read the adapter catalog: %w", err)
	}
	var catalog struct {
		Adapters []struct {
			Client      string            `json:"client"`
			Environment map[string]string `json:"environment"`
		} `json:"adapters"`
	}
	if err := json.Unmarshal(contents, &catalog); err != nil {
		return "", fmt.Errorf("parse the adapter catalog: %w", err)
	}
	for _, adapter := range catalog.Adapters {
		if adapter.Client == client {
			if image := adapter.Environment["CLIENT_IMAGE"]; image != "" {
				return image, nil
			}
			return "", fmt.Errorf("the %s adapter declares no CLIENT_IMAGE", client)
		}
	}
	return "", fmt.Errorf("the adapter catalog declares no client %q", client)
}

// clientImageVersion runs a client image's own binary to ask what it is. The
// binary is named explicitly because a client image's entrypoint is usually a
// service launcher rather than the versioned executable.
func clientImageVersion(image, entrypoint string, args []string) (string, error) {
	full := []string{"run", "--rm"}
	if entrypoint != "" {
		full = append(full, "--entrypoint", entrypoint)
	}
	full = append(full, image)
	if len(args) == 0 {
		args = []string{"--version"}
	}
	full = append(full, args...)
	output, err := exec.Command("docker", full...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	line := strings.TrimSpace(string(output))
	if index := strings.IndexAny(line, "\r\n"); index >= 0 {
		line = strings.TrimSpace(line[:index])
	}
	return line, nil
}

// acquireChainLock keeps two sessions from sharing one benchmark host. The
// lock records who holds it, so a stale file names the session that left it.
func acquireChainLock(dir string) (func(), error) {
	path := filepath.Join(dir, chainLockName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			detail := ""
			if held, readErr := os.ReadFile(path); readErr == nil {
				detail = " held by " + strings.TrimSpace(string(held))
			}
			return nil, fmt.Errorf("another chain is running on this host%s; wait for it, or remove %s if it is stale", detail, path)
		}
		return nil, fmt.Errorf("acquire the chain lock: %w", err)
	}
	host, _ := os.Hostname()
	fmt.Fprintf(file, "pid %d on %s since %s", os.Getpid(), host, time.Now().UTC().Format(time.RFC3339))
	file.Close()
	return func() { os.Remove(path) }, nil
}

func writeChainResult(path string, result ChainResult) error {
	contents, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(contents, '\n'), 0o644)
}

// chainLogger timestamps every line in UTC, so logs from machines in different
// time zones can be laid side by side.
func chainLogger(out io.Writer) func(string, ...any) {
	return func(format string, args ...any) {
		fmt.Fprintf(out, "=== %s %s\n", time.Now().UTC().Format(time.RFC3339), fmt.Sprintf(format, args...))
	}
}
