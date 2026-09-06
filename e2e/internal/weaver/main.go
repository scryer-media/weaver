// weaver-e2e is the CLI tool for the Weaver end-to-end test environment.
//
// It orchestrates Nyuu (real usenet poster) for article posting and NZB generation.
//
// Subcommands:
//
//	seed <fixture-dir>   Post fixture via Nyuu and generate its NZB
//	seed-all             Seed all fixtures from testdata/
//	verify               STAT articles in NNTP
//	status               Show health of all services
//	scenarios            List all available test scenarios
package weaver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Scenario is the JSON manifest for a pre-built test fixture.
type Scenario struct {
	Slug                               string                     `json:"slug"`
	Title                              string                     `json:"title"`
	Description                        string                     `json:"description"`
	Category                           string                     `json:"category"`
	ExpectedOutcome                    string                     `json:"expected_outcome"`
	Password                           string                     `json:"password,omitempty"`
	SegmentSize                        int                        `json:"segment_size,omitempty"`
	NZBSegmentNumbers                  []int                      `json:"nzb_segment_numbers,omitempty"`
	NZBSegmentNumberStart              int                        `json:"nzb_segment_number_start,omitempty"`
	NZBSegmentNumberStep               int                        `json:"nzb_segment_number_step,omitempty"`
	NZBSubjectFilenameOverrides        map[string]string          `json:"nzb_subject_filename_overrides,omitempty"`
	SkipArticlesPct                    int                        `json:"skip_articles_pct,omitempty"`
	DeleteFirstMessageIDs              int                        `json:"deleteFirstMessageIDs,omitempty"`
	DeleteFirstProbeSampleHits         int                        `json:"deleteFirstProbeSampleHits,omitempty"`
	PrimaryDeleteFirstMessageIDs       int                        `json:"primaryDeleteFirstMessageIDs,omitempty"`
	SharedAssets                       []string                   `json:"sharedAssets,omitempty"`
	FixtureAssets                      []string                   `json:"fixtureAssets,omitempty"`
	BackupFixtureAssets                []string                   `json:"backupFixtureAssets,omitempty"`
	DeleteSubjectContains              []string                   `json:"deleteSubjectContains,omitempty"`
	DeleteSubjectTailArticles          int                        `json:"deleteSubjectTailArticles,omitempty"`
	DeleteSegmentNumbers               []int                      `json:"deleteSegmentNumbers,omitempty"`
	DeleteSegmentSubjectContains       []string                   `json:"deleteSegmentSubjectContains,omitempty"`
	PrimaryDeleteSubjectContains       []string                   `json:"primaryDeleteSubjectContains,omitempty"`
	PrimaryChaosConfig                 string                     `json:"primaryChaosConfig,omitempty"`
	BackupUnavailableUntilFileComplete string                     `json:"backupUnavailableUntilFileComplete,omitempty"`
	RequiredJobEvents                  []string                   `json:"requiredJobEvents,omitempty"`
	ForbiddenJobEvents                 []string                   `json:"forbiddenJobEvents,omitempty"`
	MaxJobEventCounts                  map[string]int             `json:"maxJobEventCounts,omitempty"`
	ExpectedOutputBLAKE3               map[string]string          `json:"expectedOutputBLAKE3,omitempty"`
	ForbiddenOutputPaths               []string                   `json:"forbiddenOutputPaths,omitempty"`
	RuntimeAssertions                  *ScenarioRuntimeAssertions `json:"runtimeAssertions,omitempty"`
}

type ScenarioRuntimeAssertions struct {
	FileIdentityRewrite *ScenarioFileIdentityRewriteAssertion `json:"fileIdentityRewrite,omitempty"`
	Par2CleanSettlement *ScenarioPar2CleanSettlementAssertion `json:"par2CleanSettlement,omitempty"`
	DirectStore         *ScenarioDirectStoreAssertion         `json:"directStore,omitempty"`
	DirectUnpack        *ScenarioDirectUnpackAssertion        `json:"directUnpack,omitempty"`
	QueueLiveness       *ScenarioQueueLivenessAssertion       `json:"queueLiveness,omitempty"`
	HealthProbe         *ScenarioHealthProbeAssertion         `json:"healthProbe,omitempty"`
}

// ScenarioHealthProbeAssertion pins how the health probe behaved on a job that
// still completed. A job can finish with the right bytes after the probe sat
// on its soft timeout for every article it could not sample, so the outcome
// alone never sees that stall; the probe's own log lines do.
type ScenarioHealthProbeAssertion struct {
	// RequireActivated demands the probe actually ran for this job — the
	// fixture's damage must be enough to cross the activation threshold, or
	// the rest of the assertion is vacuous.
	RequireActivated bool `json:"requireActivated,omitempty"`
	// ForbidInconclusive fails the job if any probe round ended inconclusive:
	// a confirmation batch that hit its transport deadline instead of getting
	// an answer, which on a healthy server is the probe waiting on a lane it
	// should have been handed.
	ForbidInconclusive bool `json:"forbidInconclusive,omitempty"`
}

type ScenarioFileIdentityRewriteAssertion struct {
	RequiredCurrentFilenames     []string `json:"requiredCurrentFilenames,omitempty"`
	ForbiddenCurrentFilenames    []string `json:"forbiddenCurrentFilenames,omitempty"`
	RequiredClassificationSource string   `json:"requiredClassificationSource,omitempty"`
}

type ScenarioPar2CleanSettlementAssertion struct {
	ExpectedSetSliceSizes        map[string]uint64   `json:"expectedSetSliceSizes,omitempty"`
	ExpectedSetVerificationModes map[string][]string `json:"expectedSetVerificationModes,omitempty"`
	VerificationReadBytes        uint64              `json:"verificationReadBytes"`
}

// ScenarioDirectStoreAssertion pins the exceptional direct-store path in a
// fixture which otherwise has byte-identical conventional output. It is only
// evaluated in a direct-store phase.
type ScenarioDirectStoreAssertion struct {
	ExpectedDemotionReason           string `json:"expectedDemotionReason"`
	RequireRoutedByteMaterialization bool   `json:"requireRoutedByteMaterialization"`
	ForbidVolumeRefetch              bool   `json:"forbidVolumeRefetch"`
}

// ScenarioDirectUnpackAssertion pins what the 7z direct-unpack chase did, read
// from weaver's own log lines — the harness has no metrics surface, and the
// chase is otherwise invisible by design: a consumed chase and a conventional
// extraction deliver byte-identical output.
//
// The positive fields are only evaluated in a phase that ran with the gate on;
// ForbidAnyActivity is only evaluated in a phase that ran with it off, so one
// scenario proves engagement where the feature is enabled and darkness where it
// is not.
type ScenarioDirectUnpackAssertion struct {
	// RequireArmed demands the set was admitted to a chase.
	RequireArmed bool `json:"requireArmed,omitempty"`
	// RequireConsumed demands its members were installed rather than the
	// archive being decoded a second time.
	RequireConsumed bool `json:"requireConsumed,omitempty"`
	// ExpectedDemotionReason demands a demotion carrying this reason.
	ExpectedDemotionReason string `json:"expectedDemotionReason,omitempty"`
	// RequireRearmAfterRestart demands the set armed at least twice, which is
	// what a restart mid-chase looks like: the pre-restart chase dies with the
	// process, and the resumed download re-arms from the persisted floor.
	RequireRearmAfterRestart bool `json:"requireRearmAfterRestart,omitempty"`
	// ForbidAnyActivity demands the feature left no trace at all.
	ForbidAnyActivity bool `json:"forbidAnyActivity,omitempty"`
}

// ScenarioQueueLivenessAssertion holds an unrelated fixture which must finish
// while this scenario's deliberately delayed post-repair PAR2 verification is
// still running.
type ScenarioQueueLivenessAssertion struct {
	ProbeSlug string `json:"probeSlug"`
}

func (s *Scenario) fileIdentityRewriteAssertion() *ScenarioFileIdentityRewriteAssertion {
	if s == nil || s.RuntimeAssertions == nil {
		return nil
	}
	return s.RuntimeAssertions.FileIdentityRewrite
}

func (s *Scenario) par2CleanSettlementAssertion() *ScenarioPar2CleanSettlementAssertion {
	if s == nil || s.RuntimeAssertions == nil {
		return nil
	}
	return s.RuntimeAssertions.Par2CleanSettlement
}

func (s *Scenario) directStoreAssertion() *ScenarioDirectStoreAssertion {
	if s == nil || s.RuntimeAssertions == nil {
		return nil
	}
	return s.RuntimeAssertions.DirectStore
}

func (s *Scenario) directUnpackAssertion() *ScenarioDirectUnpackAssertion {
	if s == nil || s.RuntimeAssertions == nil {
		return nil
	}
	return s.RuntimeAssertions.DirectUnpack
}

func (s *Scenario) queueLivenessAssertion() *ScenarioQueueLivenessAssertion {
	if s == nil || s.RuntimeAssertions == nil {
		return nil
	}
	return s.RuntimeAssertions.QueueLiveness
}

func (s *Scenario) healthProbeAssertion() *ScenarioHealthProbeAssertion {
	if s == nil || s.RuntimeAssertions == nil {
		return nil
	}
	return s.RuntimeAssertions.HealthProbe
}

type runtimePortState struct {
	NNTPPort           int `json:"nntp_port"`
	NNTPTLSPort        int `json:"nntp_tls_port"`
	NNTP2Port          int `json:"nntp2_port"`
	ToxiproxyAPIPort   int `json:"toxiproxy_api_port"`
	ToxiproxyNNTP1Port int `json:"toxiproxy_nntp1_port"`
	ToxiproxyNNTP2Port int `json:"toxiproxy_nntp2_port"`
	WeaverPort         int `json:"weaver_port"`
	PostgresPort       int `json:"postgres_port"`
	NzbgetPort         int `json:"nzbget_port"`
	SabnzbdPort        int `json:"sabnzbd_port"`
	LocalWeaverPort    int `json:"local_weaver_port"`
}

var (
	runtimePortsOnce        sync.Once
	runtimePortsErr         error
	nzbDatePattern          = regexp.MustCompile(`date="(\d+)"`)
	nzbSegmentNumberPattern = regexp.MustCompile(`(<segment\b[^>]*\snumber=")\d+(")`)
	weaverCookieJars        sync.Map
	weaverImageOnce         sync.Once
	weaverImageErr          error
	weaverBuildOnce         sync.Once
	weaverBuildErr          error
	weaverBuildPath         string
)

const stableNZBDate = "1704067200"

var cliProgramName = "weaver-e2e"

func Run(args []string, programName string) {
	if trimmed := strings.TrimSpace(programName); trimmed != "" {
		cliProgramName = trimmed
	}
	log.SetFlags(log.Ltime)
	applyDefaultNNTPCredentialEnv()

	if len(args) < 1 {
		printUsage(os.Stderr)
		os.Exit(1)
	}

	if args[0] != "scenarios" && args[0] != "full" && args[0] != "release-gate" {
		ensureRuntimePortEnv()
	}

	switch args[0] {
	case "seed":
		if len(args) < 2 {
			log.Fatalf("usage: %s seed <fixture-dir>", cliProgramName)
		}
		cmdSeed(args[1])
	case "seed-all":
		cmdSeedAll()
	case "functional":
		cmdFunctional()
	case "verify":
		cmdVerify()
	case "status":
		cmdStatus()
	case "scenarios":
		cmdScenarios()
	case "submit":
		if len(args) < 2 {
			log.Fatalf("usage: %s submit <fixture-slug>", cliProgramName)
		}
		cmdSubmit(args[1])
	case "full":
		cmdFull()
	case "release-gate":
		cmdWeaverReleaseGate(args[1:])
	case "release-console":
		cmdWeaverReleaseConsole(args[1:])
	case "release-flow":
		cmdWeaverReleaseFlow(args[1:])
	case "release-finalize":
		cmdWeaverReleaseFinalize(args[1:])
	case "test-all":
		cmdTestAll()
	case "test":
		if len(args) < 2 {
			log.Fatalf("usage: %s test <slug> [slug...]", cliProgramName)
		}
		cmdTest(args[1:])
	case "pgo":
		cmdPgo(args[1:])
	case "download-bench":
		cmdDownloadBench(args[1:])
	case "adaptive-dispatch":
		cmdAdaptiveDispatchTest()
	case "container-restart":
		cmdContainerRestartTest()
	case "restart-all":
		cmdRestartAll()
	case "restart-test":
		cmdRestartTest(args[1:])
	case "chaos":
		if len(args) < 2 {
			log.Fatalf("usage: %s chaos <config>  (e.g. 'drop_conn=10,slow_body=50' or 'off')", cliProgramName)
		}
		cmdChaos(strings.Join(args[1:], " "))
	case "chaos-test":
		cmdChaosTest()
	case "tcp-chaos":
		cmdTcpChaosTest()
	case "tls-test":
		cmdTlsTest()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		printUsage(os.Stderr)
		os.Exit(1)
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `Usage: %s <command> [args]

Commands:
  seed <fixture-dir>    Post fixture via Nyuu, register NZB with indexer
  seed-all              Seed all fixtures from testdata/
  functional            Run functional full-suite phases with the dashboard
  verify                STAT articles in NNTP server, search indexer
  status                Check health of all e2e services
  scenarios             List all available test scenarios
  full                  Seed fixtures, then run functional, chaos, Docker restart, and managed restart phases
  release-gate [flow]   Run the independent Weaver product-behavior release gate
  release-console [run] Serve the latest or selected Weaver release-gate artifacts
  test <slug> [slug...] Run specific test(s) by slug
  test-all              Submit all NZBs, poll all simultaneously
  pgo [slug...]         Run representative managed-Weaver flows for LLVM PGO data
  download-bench [slug...] Benchmark download-heavy scenarios sequentially
  adaptive-dispatch    Verify latency-aware multi-server dispatch preference
  container-restart    Restart the Docker Weaver service and verify its persisted encryption key
  restart-all           Run the restart/crash litmus suite
  restart-test [case...] Run specific restart case(s)
  chaos <config>        Configure NNTP chaos on the primary server
  chaos-test            Run the NNTP chaos suite
  tcp-chaos             Run the TCP chaos suite
  tls-test              Run the TLS NNTP suite

Environment:
  E2E_DIR              Path to the e2e repo root (auto-detected by default)
  E2E_PROJECT          Docker Compose project name for this run (default: e2e)
  FIXTURES_DIR         Path to seeded fixtures (default: <repo>/fixtures)
  TESTDATA_DIR         Path to source fixtures (default: <repo>/testdata)
  E2E_RUNTIME_PORTS_FILE  Path to the runtime port state file
  E2E_RUN_DIR          Path to local temp state for managed weaver runs
  E2E_WEAVER_DATASTORE Weaver datastore for managed local runs: sqlite|postgres (default: sqlite)
  E2E_WEAVER_RELEASE_GATE_JOBS Parallel product-flow workers (default: 8, max: 16)
  E2E_WEAVER_RELEASE_GATE_ROOT Stable root for release-gate runs and latest pointer
  E2E_WEAVER_PLAYWRIGHT_IMAGE Weaver-only Playwright image override
  E2E_VERBOSE          Stream external command output instead of summarizing it
  E2E_WEAVER_IMAGE     Override the Weaver image used by dockerized shared-stack runs
  E2E_FORCE_REBUILD_WEAVER_IMAGE  Force rebuilding the local Weaver image for e2e full
  E2E_FORCE_REBUILD_NYUU_IMAGE    Force rebuilding the Nyuu image for e2e/full seeding
  NNTP_HOST            NNTP server host (default: localhost)
  NNTP_PORT            NNTP server port (default: runtime-assigned open port)
  WEAVER_URL           Weaver GraphQL base URL (default: runtime-assigned open port)
  E2E_NZBGET_PORT      Host port for NZBGet (default: runtime-assigned open port)
  E2E_SABNZBD_PORT     Host port for SABnzbd (default: runtime-assigned open port)
  E2E_WEAVER_POSTGRES_PORT Host port for Weaver Postgres (default: runtime-assigned open port)
  WEAVER_BIN           Path to a local weaver binary for managed local runs
  E2E_WEAVER_PROFILE_DIR Directory for LLVM raw profile output from managed local runs
  NYUU_IMAGE           Docker image for Nyuu (default: e2e-nyuu)
  DOCKER_NETWORK       Docker network name (default: <project>_default)
  DOWNLOAD_BENCH_LOCAL_WEAVER Start a local weaver for download-bench (0/1)
  DOWNLOAD_BENCH_ITERATIONS   Iterations per scenario (default: 3)
  DOWNLOAD_BENCH_SAMPLE_MS    Poll/sample interval in ms (default: 250)
  DOWNLOAD_BENCH_TIMEOUT_SEC  Per-run timeout in seconds (default: 300)
  DOWNLOAD_BENCH_OUTPUT_DIR   Directory for logs, samples, and summary JSON
  DOWNLOAD_BENCH_SLUGS        Default comma-separated slugs when args omitted
  DOWNLOAD_BENCH_CONNECTIONS  Primary NNTP connections for managed local runs
  ADAPTIVE_DISPATCH_LATENCY_MS Latency injected on proxied server 1 (default: 75)
  ADAPTIVE_DISPATCH_MIN_DIRECT_PCT Minimum direct-server BODY share (default: 60)
  WEAVER_PORT          Managed local weaver host port (default: runtime-assigned open port)
  DOWNLOAD_BENCH_RUST_LOG     RUST_LOG for managed local weaver (default: warn)
  E2E_SUSPEND_TOLERANCE_SEC   Extra wall-clock gap allowed before reporting host sleep (default: 30)
  E2E_RESTART_PROFILE         Restart expectation profile: hardened|current (default: hardened)
  E2E_RESTART_ONLY_CASE       Limit restart-all to a specific restart case name
  E2E_RESTART_TIMEOUT_SEC     Per-case timeout for restart suite (default: 900)
  E2E_RESTART_KEEP_ARTIFACTS  Keep restart artifacts under E2E_RUN_DIR (default: true)
  CHAOS_ONLY_ROUND     Limit chaos-test to a specific round number
  TCP_CHAOS_ONLY_ROUND Limit tcp-chaos to a specific round number`, cliProgramName)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, value, err)
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		log.Fatalf("invalid %s=%q (expected true/false)", key, value)
		return fallback
	}
}

func mustPortInt(label, value string) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", label, value, err)
	}
	return parsed
}

func sanitizeProjectName(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "e2e"
	}

	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_':
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
				lastDash = true
			}
		default:
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}

	sanitized := strings.Trim(b.String(), "-")
	if sanitized == "" {
		return "e2e"
	}
	return sanitized
}

func composeProject() string {
	return sanitizeProjectName(env("E2E_PROJECT", "e2e"))
}

func localRunDir() string {
	if value := strings.TrimSpace(os.Getenv("E2E_RUN_DIR")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join("/tmp", "weaver-e2e-"+composeProject())
}

func localWeaverDir() string {
	return filepath.Join(localRunDir(), "weaver")
}

func localWeaverConfigPath() string {
	return filepath.Join(localWeaverDir(), "weaver.toml")
}

func localWeaverLogPath() string {
	return filepath.Join(localWeaverDir(), "weaver.log")
}

func localWeaverPIDPath() string {
	return filepath.Join(localWeaverDir(), "weaver.pid")
}

func runtimePortsStatePath() string {
	if value := strings.TrimSpace(os.Getenv("E2E_RUNTIME_PORTS_FILE")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join("/tmp", "weaver-e2e-runtime-ports-"+composeProject()+".json")
}

func runtimePortEnvKeys() []string {
	return []string{
		"E2E_NNTP_PORT",
		"E2E_NNTP_TLS_PORT",
		"E2E_NNTP2_PORT",
		"E2E_TOXIPROXY_API_PORT",
		"E2E_TOXIPROXY_NNTP1_PORT",
		"E2E_TOXIPROXY_NNTP2_PORT",
		"E2E_WEAVER_PORT",
		"E2E_WEAVER_POSTGRES_PORT",
		"E2E_NZBGET_PORT",
		"E2E_SABNZBD_PORT",
		"E2E_LOCAL_WEAVER_PORT",
	}
}

func runtimePortEnvConfigured() bool {
	for _, key := range runtimePortEnvKeys() {
		if strings.TrimSpace(os.Getenv(key)) == "" {
			return false
		}
	}
	return true
}

func ensureRuntimePortEnv() {
	runtimePortsOnce.Do(func() {
		runtimePortsErr = initRuntimePortEnv()
	})
	if runtimePortsErr != nil {
		log.Fatalf("initialize runtime host ports: %v", runtimePortsErr)
	}
}

func initRuntimePortEnv() error {
	if runtimePortEnvConfigured() {
		return nil
	}

	var (
		state runtimePortState
		err   error
	)

	if runtimeStackRunning() {
		state, err = discoverRuntimePortState()
		if err != nil {
			state, err = loadRuntimePortState(runtimePortsStatePath())
			if err != nil {
				return err
			}
		}
		if err := saveRuntimePortState(runtimePortsStatePath(), state); err != nil {
			return err
		}
	} else {
		state, err = allocateRuntimePortState()
		if err != nil {
			return err
		}
		if err := saveRuntimePortState(runtimePortsStatePath(), state); err != nil {
			return err
		}
	}

	applyRuntimePortEnv(state)
	return nil
}

func loadRuntimePortState(path string) (runtimePortState, error) {
	var state runtimePortState
	data, err := os.ReadFile(path)
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, err
	}
	if err := validateRuntimePortState(state); err != nil {
		return state, err
	}
	return state, nil
}

func saveRuntimePortState(path string, state runtimePortState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// reallocateRuntimePortsForDockerRetry picks a fresh set of host ports after
// Docker loses the race between the probe listener closing and compose binding
// its published ports. Preserve explicit NNTP aliases when a caller has
// deliberately overridden the runtime-assigned endpoint.
func reallocateRuntimePortsForDockerRetry() error {
	statePath := runtimePortsStatePath()
	previous, err := loadRuntimePortState(statePath)
	if err != nil {
		return fmt.Errorf("load current runtime ports: %w", err)
	}
	next, err := allocateRuntimePortState()
	if err != nil {
		return fmt.Errorf("allocate fresh runtime ports: %w", err)
	}
	if err := saveRuntimePortState(statePath, next); err != nil {
		return fmt.Errorf("save fresh runtime ports: %w", err)
	}

	applyRuntimePortEnvPreservingExplicitAliases(previous, next)
	return nil
}

func isDockerHostPortBindCollision(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "failed to bind host port") &&
		strings.Contains(message, "address already in use")
}

func validateRuntimePortState(state runtimePortState) error {
	ports := []int{
		state.NNTPPort,
		state.NNTPTLSPort,
		state.NNTP2Port,
		state.ToxiproxyAPIPort,
		state.ToxiproxyNNTP1Port,
		state.ToxiproxyNNTP2Port,
		state.WeaverPort,
		state.PostgresPort,
		state.NzbgetPort,
		state.SabnzbdPort,
		state.LocalWeaverPort,
	}
	for _, port := range ports {
		if port <= 0 {
			return fmt.Errorf("invalid runtime port state: found non-positive port %d", port)
		}
	}
	return nil
}

func allocateRuntimePortState() (runtimePortState, error) {
	state := runtimePortState{}
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()

	if err := reserveRuntimePortState(&state, &listeners); err != nil {
		return state, err
	}

	return state, nil
}

func allocateRuntimePortStates(count int) ([]runtimePortState, error) {
	states := make([]runtimePortState, count)
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()

	for i := range states {
		if err := reserveRuntimePortState(&states[i], &listeners); err != nil {
			return states, fmt.Errorf("reserve runtime ports for phase %d: %w", i+1, err)
		}
	}
	return states, nil
}

type runtimePortAssignment struct {
	value *int
	name  string
}

func runtimePortAssignments(state *runtimePortState) []runtimePortAssignment {
	return []runtimePortAssignment{
		{value: &state.NNTPPort, name: "NNTP"},
		{value: &state.NNTPTLSPort, name: "NNTP TLS"},
		{value: &state.NNTP2Port, name: "backup NNTP"},
		{value: &state.ToxiproxyAPIPort, name: "toxiproxy API"},
		{value: &state.ToxiproxyNNTP1Port, name: "toxiproxy NNTP1"},
		{value: &state.ToxiproxyNNTP2Port, name: "toxiproxy NNTP2"},
		{value: &state.WeaverPort, name: "docker weaver"},
		{value: &state.PostgresPort, name: "postgres"},
		{value: &state.NzbgetPort, name: "nzbget"},
		{value: &state.SabnzbdPort, name: "sabnzbd"},
		{value: &state.LocalWeaverPort, name: "local weaver"},
	}
}

func reserveRuntimePortState(state *runtimePortState, listeners *[]net.Listener) error {
	for _, assignment := range runtimePortAssignments(state) {
		listener, err := net.Listen("tcp4", "0.0.0.0:0")
		if err != nil {
			return fmt.Errorf("reserve %s port: %w", assignment.name, err)
		}
		tcpAddr, ok := listener.Addr().(*net.TCPAddr)
		if !ok {
			_ = listener.Close()
			return fmt.Errorf("reserve %s port: unexpected address %T", assignment.name, listener.Addr())
		}
		*assignment.value = tcpAddr.Port
		*listeners = append(*listeners, listener)
	}
	return nil
}

func discoverRuntimePortState() (runtimePortState, error) {
	var state runtimePortState
	var err error

	state.NNTPPort, err = inspectDockerHostPort("nntp", "119/tcp")
	if err != nil {
		return state, err
	}
	state.NNTPTLSPort, err = inspectDockerHostPort("nntp", "563/tcp")
	if err != nil {
		return state, err
	}
	if dockerContainerRunning("weaver") {
		state.WeaverPort, err = inspectDockerHostPort("weaver", "9090/tcp")
		if err != nil {
			return state, err
		}
	}
	if dockerContainerRunning("weaver-postgres") {
		state.PostgresPort, err = inspectDockerHostPort("weaver-postgres", "5432/tcp")
		if err != nil {
			return state, err
		}
	}
	if dockerContainerRunning("nzbget") {
		state.NzbgetPort, err = inspectDockerHostPort("nzbget", "6789/tcp")
		if err != nil {
			return state, err
		}
	}
	if dockerContainerRunning("sabnzbd") {
		state.SabnzbdPort, err = inspectDockerHostPort("sabnzbd", "8080/tcp")
		if err != nil {
			return state, err
		}
	}
	if dockerContainerRunning("nntp2") {
		state.NNTP2Port, err = inspectDockerHostPort("nntp2", "119/tcp")
		if err != nil {
			return state, err
		}
	}
	if dockerContainerRunning("toxiproxy") {
		state.ToxiproxyAPIPort, err = inspectDockerHostPort("toxiproxy", "8474/tcp")
		if err != nil {
			return state, err
		}
		state.ToxiproxyNNTP1Port, err = inspectDockerHostPort("toxiproxy", "3119/tcp")
		if err != nil {
			return state, err
		}
		state.ToxiproxyNNTP2Port, err = inspectDockerHostPort("toxiproxy", "4119/tcp")
		if err != nil {
			return state, err
		}
	}
	existing, loadErr := loadRuntimePortState(runtimePortsStatePath())
	if loadErr == nil && existing.LocalWeaverPort > 0 {
		state.LocalWeaverPort = existing.LocalWeaverPort
	}

	allocated, allocErr := allocateRuntimePortState()
	if allocErr != nil {
		return state, allocErr
	}
	if state.WeaverPort == 0 {
		if existing.WeaverPort > 0 {
			state.WeaverPort = existing.WeaverPort
		} else {
			state.WeaverPort = allocated.WeaverPort
		}
	}
	if state.PostgresPort == 0 {
		if existing.PostgresPort > 0 {
			state.PostgresPort = existing.PostgresPort
		} else {
			state.PostgresPort = allocated.PostgresPort
		}
	}
	if state.NNTP2Port == 0 {
		state.NNTP2Port = allocated.NNTP2Port
	}
	if state.ToxiproxyAPIPort == 0 {
		state.ToxiproxyAPIPort = allocated.ToxiproxyAPIPort
	}
	if state.ToxiproxyNNTP1Port == 0 {
		state.ToxiproxyNNTP1Port = allocated.ToxiproxyNNTP1Port
	}
	if state.ToxiproxyNNTP2Port == 0 {
		state.ToxiproxyNNTP2Port = allocated.ToxiproxyNNTP2Port
	}
	if state.NzbgetPort == 0 {
		state.NzbgetPort = allocated.NzbgetPort
	}
	if state.SabnzbdPort == 0 {
		state.SabnzbdPort = allocated.SabnzbdPort
	}
	if state.LocalWeaverPort == 0 {
		state.LocalWeaverPort = allocated.LocalWeaverPort
	}

	if err := validateRuntimePortState(state); err != nil {
		return state, err
	}
	return state, nil
}

func dockerComposeArgs(args ...string) []string {
	composeArgs := []string{"compose", "-p", composeProject()}
	if envBool(nntpSeedImageActiveEnv, false) {
		composeArgs = append(
			composeArgs,
			"-f", filepath.Join(e2eDir(), "docker-compose.yml"),
			"-f", filepath.Join(e2eDir(), "docker-compose.preseeded-nntp.yml"),
		)
	}
	return append(composeArgs, args...)
}

func dockerComposeServiceContainerID(service string) (string, error) {
	args := dockerComposeArgs("ps", "-q", service)
	cmd := exec.Command("docker", args...)
	cmd.Dir = e2eDir()
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("resolve container for service %s: %w", service, err)
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		return "", fmt.Errorf("service %s is not running", service)
	}
	return id, nil
}

func inspectDockerHostPort(serviceName, containerPort string) (int, error) {
	containerID, err := dockerComposeServiceContainerID(serviceName)
	if err != nil {
		return 0, err
	}
	cmd := exec.Command("docker", "inspect", "-f", fmt.Sprintf("{{(index (index .NetworkSettings.Ports %q) 0).HostPort}}", containerPort), containerID)
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("inspect %s %s: %w", serviceName, containerPort, err)
	}
	value := strings.TrimSpace(string(out))
	if value == "" || value == "<no value>" {
		return 0, fmt.Errorf("inspect %s %s: missing host port", serviceName, containerPort)
	}
	port, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("inspect %s %s: parse host port %q: %w", serviceName, containerPort, value, err)
	}
	return port, nil
}

func setEnv(key, value string) {
	_ = os.Setenv(key, value)
}

func applyRuntimePortEnv(state runtimePortState) {
	for key, value := range runtimePortEnvValues(state) {
		setEnv(key, value)
	}
}

func runtimePortEnvValues(state runtimePortState) map[string]string {
	return map[string]string{
		"E2E_NNTP_PORT":            strconv.Itoa(state.NNTPPort),
		"E2E_NNTP_TLS_PORT":        strconv.Itoa(state.NNTPTLSPort),
		"E2E_NNTP2_PORT":           strconv.Itoa(state.NNTP2Port),
		"E2E_TOXIPROXY_API_PORT":   strconv.Itoa(state.ToxiproxyAPIPort),
		"E2E_TOXIPROXY_NNTP1_PORT": strconv.Itoa(state.ToxiproxyNNTP1Port),
		"E2E_TOXIPROXY_NNTP2_PORT": strconv.Itoa(state.ToxiproxyNNTP2Port),
		"E2E_WEAVER_PORT":          strconv.Itoa(state.WeaverPort),
		"E2E_WEAVER_POSTGRES_PORT": strconv.Itoa(state.PostgresPort),
		"E2E_NZBGET_PORT":          strconv.Itoa(state.NzbgetPort),
		"E2E_SABNZBD_PORT":         strconv.Itoa(state.SabnzbdPort),
		"E2E_LOCAL_WEAVER_PORT":    strconv.Itoa(state.LocalWeaverPort),
		"NNTP_PORT":                strconv.Itoa(state.NNTPPort),
		"NNTP_TLS_PORT":            strconv.Itoa(state.NNTPTLSPort),
		"NNTP_BACKUP_PORT":         strconv.Itoa(state.NNTP2Port),
		"TOXIPROXY_NNTP1_PORT":     strconv.Itoa(state.ToxiproxyNNTP1Port),
		"TOXIPROXY_NNTP2_PORT":     strconv.Itoa(state.ToxiproxyNNTP2Port),
		"WEAVER_URL":               fmt.Sprintf("http://localhost:%d", state.WeaverPort),
		"NZBGET_URL":               fmt.Sprintf("http://localhost:%d", state.NzbgetPort),
		"SABNZBD_URL":              fmt.Sprintf("http://localhost:%d", state.SabnzbdPort),
		"TOXIPROXY_URL":            fmt.Sprintf("http://localhost:%d", state.ToxiproxyAPIPort),
		"WEAVER_PORT":              strconv.Itoa(state.LocalWeaverPort),
		"DOWNLOAD_BENCH_NNTP_PORT": strconv.Itoa(state.NNTPPort),
	}
}

func refreshRuntimePortEnvFromRunningStack() error {
	state, err := discoverRuntimePortState()
	if err != nil {
		return err
	}
	if err := saveRuntimePortState(runtimePortsStatePath(), state); err != nil {
		return err
	}
	applyRuntimePortEnv(state)
	return nil
}

func applyRuntimePortEnvPreservingExplicitAliases(previous, next runtimePortState) {
	previousEnv := runtimePortEnvValues(previous)
	for key, value := range runtimePortEnvValues(next) {
		if strings.HasPrefix(key, "E2E_") || os.Getenv(key) == previousEnv[key] {
			setEnv(key, value)
		}
	}
}

func runtimeStackRunning() bool {
	for _, name := range []string{
		"nntp",
		"nntp2",
		"weaver",
		"nzbget",
		"sabnzbd",
		"toxiproxy",
	} {
		if dockerContainerRunning(name) {
			return true
		}
	}
	return false
}

func dockerContainerRunning(service string) bool {
	containerID, err := dockerComposeServiceContainerID(service)
	if err != nil {
		return false
	}
	check := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", containerID)
	out, err := check.Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

func e2eVerbose() bool {
	return envBool("E2E_VERBOSE", false)
}

func suspendTolerance() time.Duration {
	return time.Duration(envInt("E2E_SUSPEND_TOLERANCE_SEC", 30)) * time.Second
}

type tailBuffer struct {
	limit int
	data  []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return len(p), nil
	}
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return len(p), nil
	}
	needed := len(b.data) + len(p) - b.limit
	if needed > 0 {
		b.data = append([]byte(nil), b.data[needed:]...)
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *tailBuffer) String() string {
	return strings.TrimSpace(string(b.data))
}

func runExternalCommand(cmd *exec.Cmd, summary string) error {
	if e2eVerbose() {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("%s: %w", summary, err)
		}
		return nil
	}

	tail := &tailBuffer{limit: 64 * 1024}
	cmd.Stdout = tail
	cmd.Stderr = tail
	if err := cmd.Run(); err != nil {
		if output := tail.String(); output != "" {
			return fmt.Errorf("%s: %w\n\nLast command output:\n%s", summary, err, output)
		}
		return fmt.Errorf("%s: %w", summary, err)
	}
	return nil
}

func sleepWithSuspendDetection(interval time.Duration, label string) error {
	start := time.Now()
	time.Sleep(interval)
	wallElapsed := time.Duration(time.Now().UnixNano() - start.UnixNano())
	if wallElapsed > interval+suspendTolerance() {
		return fmt.Errorf(
			"detected host suspend or large clock jump during %s: expected ~%s sleep, observed %s; rerun after disabling sleep",
			label,
			interval.Round(time.Second),
			wallElapsed.Round(time.Second),
		)
	}
	return nil
}

func mustSleepWithSuspendDetection(interval time.Duration, label string) {
	if err := sleepWithSuspendDetection(interval, label); err != nil {
		log.Fatal(err)
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func absolutePath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

func detectE2EDir(start string) string {
	current := absolutePath(start)
	for {
		if pathExists(filepath.Join(current, "docker-compose.yml")) &&
			(pathExists(filepath.Join(current, "cmd", "weaver-e2e", "main.go")) ||
				pathExists(filepath.Join(current, "internal", "weaver", "main.go"))) {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			return ""
		}
		current = parent
	}
}

func e2eDir() string {
	if d := env("E2E_DIR", ""); d != "" {
		return absolutePath(d)
	}

	var candidates []string
	if exe, err := os.Executable(); err == nil {
		if resolved, err := filepath.EvalSymlinks(exe); err == nil {
			exe = resolved
		}
		candidates = append(candidates, filepath.Dir(exe))
	}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, cwd)
	}

	for _, candidate := range candidates {
		if root := detectE2EDir(candidate); root != "" {
			return root
		}
	}

	log.Fatal("cannot determine e2e repo root; set E2E_DIR")
	return ""
}

func repoPath(parts ...string) string {
	all := append([]string{e2eDir()}, parts...)
	return filepath.Join(all...)
}

// weaverRepoRoot is the Weaver repository that owns this harness. The harness
// lives at <weaver-repo>/e2e, so the repo root is the parent of the e2e
// directory.
func weaverRepoRoot() string {
	return filepath.Dir(e2eDir())
}

func weaverRepoPath() string {
	if configured := strings.TrimSpace(os.Getenv("E2E_WEAVER_REPO")); configured != "" {
		return absolutePath(configured)
	}
	return weaverRepoRoot()
}

func resolveRepoPath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	for _, candidate := range []string{path, repoPath(path)} {
		if pathExists(candidate) {
			return absolutePath(candidate)
		}
	}
	return repoPath(path)
}

func fixturesDir() string {
	if d := env("FIXTURES_DIR", ""); d != "" {
		return absolutePath(d)
	}
	return repoPath("fixtures")
}

func testdataDir() string {
	if d := env("TESTDATA_DIR", ""); d != "" {
		return absolutePath(d)
	}
	return repoPath("testdata")
}

var canonicalFixtureSlugs = []string{
	// The 7z direct-unpack codec matrix: every coder chain the pinned 7-Zip
	// writes and weaver decodes, in both shapes. The split variants are the
	// ones a chase can overlap; the single .7z ones have no topology until
	// the whole file lands and assert the ordinary conventional outcome.
	"direct-unpack-aes256",
	"direct-unpack-aes256-header",
	"direct-unpack-aes256-header-split",
	"direct-unpack-aes256-repair",
	"direct-unpack-aes256-split",
	"direct-unpack-bcj-lzma2",
	"direct-unpack-bcj-lzma2-split",
	"direct-unpack-bcj2",
	"direct-unpack-bcj2-split",
	"direct-unpack-bzip2",
	"direct-unpack-bzip2-split",
	"direct-unpack-copy",
	"direct-unpack-copy-split",
	"direct-unpack-deflate",
	"direct-unpack-deflate-split",
	"direct-unpack-delta-lzma2",
	"direct-unpack-delta-lzma2-split",
	"direct-unpack-lzma",
	"direct-unpack-lzma-split",
	"direct-unpack-lzma2",
	"direct-unpack-lzma2-split",
	"direct-unpack-nonsolid",
	"direct-unpack-nonsolid-split",
	"direct-unpack-ppmd",
	"direct-unpack-ppmd-split",
	"direct-unpack-repair",
	"direct-unpack-repair-unvouched",
	"direct-unpack-solid",
	"direct-unpack-solid-split",
	"7z-encrypted",
	"brotli-single",
	"bzip2-single",
	"deflate-single",
	// Direct-store routing writes a RAR set's members straight to their
	// destinations and never materialises the source volumes. Its output is
	// byte-identical to the conventional path, so these fixtures prove nothing
	// by their bytes alone — what they prove is that the sets weaver *should*
	// route direct still do, asserted from weaver's own counters after the run
	// (see assertDirectStoreEngagement). Each is store-method and non-solid;
	// archives direct-store is right to refuse are the rest of this corpus.
	"direct-store-encrypted",
	"direct-store-encrypted-par2-repair",
	"direct-store-hp",
	"direct-store-hp-par2-repair",
	"direct-store-multi-member",
	"direct-store-multivolume",
	"direct-store-par2-repair",
	"direct-store-par2-withheld-volume",
	"direct-store-post-repair-queue-liveness",
	"direct-store-quick-open",
	"direct-store-rar4",
	"direct-store-rar4-encrypted",
	"direct-store-rar4-encrypted-par2-repair",
	"direct-store-single",
	"empty-rar",
	"gzip-corrupted",
	"gzip-single",
	"health-failure",
	"large-segments",
	"mixed-archive",
	"nested-3deep",
	"nested-5deep",
	"nested-obfuscated-split-7z",
	"nested-rar",
	"nested-xz-rar",
	"obfuscated-rar",
	"obfuscated-rar-retry-7z",
	"obfuscated-rar-split-topology",
	"obfuscated-split-7z",
	"obfuscated-rar-unknown-numeric",
	"par2-direct-late-malformed-chain-rebind",
	"par2-opaque-magic-rebind",
	"par2-obfuscated-rar-repair",
	"par2-obfuscated-rar-rewrite",
	"par2-optional-prefix-hole",
	"par2-rar-placement-normalization",
	"par2-rar-placement-normalization-multi-swap",
	"par2-heavy-damage",
	"par2-heavy-damage-a",
	"par2-heavy-damage-b",
	"par2-heavy-damage-c",
	"par2-insufficient",
	"par2-multi-grid-late-discovery",
	"par2-multi-grid-overlap-clean",
	"par2-multi-set-archives",
	"par2-multi-set-archives-clean",
	"par2-multi-set-archives-insufficient",
	"par2-direct-repair",
	"par2-ignorable-deficit",
	"par2-partial-volume",
	"par2-two-sets",
	"par2-rar-placement-stripped-recovery",
	"par2-7z-repair",
	"par2-split-7z-withheld-part",
	"par2-multivolume",
	"par2-rar4",
	"par2-repair",
	"par2-small-repair",
	"par2-small-repair-a",
	"par2-small-repair-b",
	"par2-small-repair-c",
	"par2-small-repair-d",
	"rar4-corrupted",
	"rar4-encrypted",
	"rar4-member-encrypted",
	"rar4-multi-member",
	"rar4-multi-member-encrypted",
	"rar4-multivolume",
	"rar4-multivolume-encrypted",
	"rar4-recovery-volume-light",
	"rar4-single",
	"rar4-solid",
	"rar5-colliding-member-paths",
	"rar5-corrupted",
	"rar5-encrypted",
	"rar5-filename-dedupe",
	"rar5-filename-normalization",
	"rar5-hp-encrypted",
	"rar5-hp-recovery-volume-heavy",
	"rar5-multi-member",
	"rar5-multi-member-encrypted",
	"rar5-multivolume",
	"rar5-multivolume-missing-tail",
	"rar5-multivolume-encrypted",
	"rar5-no-password-meta",
	"rar5-recovery-volume-heavy",
	"rar5-recovery-volume-insufficient",
	"rar5-recovery-volume-light",
	"rar5-single",
	"rar5-solid",
	"rar5-solid-encrypted",
	"rar5-solid-encrypted-missing-middle-par2",
	"rar5-solid-multi-member",
	"rar5-solid-multivolume",
	"rar5-wrong-password",
	"single-7z",
	"single-7z-corrupted",
	"single-mkv",
	"multiserver-primary-missing-direct",
	"multiserver-primary-corrupt-direct",
	"single-mkv-sparse-nzb",
	"multiserver-backup-par2-repair",
	"split-plain-mkv",
	"split-plain-par2",
	"split-7z",
	"split-7z-corrupted",
	"split-7z-encrypted",
	"split-xz",
	"tar-archive",
	"tar-bzip2-archive",
	"tar-corrupted",
	"tar-gzip-archive",
	"targz-archive",
	"targz-corrupted",
	"tbz2-archive",
	"tgz-archive",
	"unicode-filenames",
	// uuencode. Nyuu cannot post these — see internal/weaver/uu_seed.go — so
	// their articles come from the corpus pre-encoded and the seeder posts
	// them itself.
	"uu-release",
	"uu-mixed-yenc",
	"uu-preamble-tail",
	"uu-missing-middle",
	"zip-corrupted",
	"zip-encrypted",
	"zip-unencrypted",
	"xz-text",
	"xz-video",
	"zstd-single",
}

// Keep NNTP protocol chaos to two four-job batches per round. This covers a
// raw payload, both RAR generations, encryption, repair, split input,
// obfuscation, and large BODY transfers without multiplying retry-heavy work.
var chaosFixtureSlugs = []string{
	"single-mkv",
	"rar5-multivolume",
	"rar4-encrypted",
	"7z-encrypted",
	"par2-repair",
	"split-7z",
	"obfuscated-rar",
	"large-segments",
}

// TCP transport chaos uses the same compact representative workload as NNTP
// protocol chaos. It retains both archive generations, encryption, repair,
// split input, obfuscation, and large BODY transfers.
var tcpChaosFixtureSlugs = []string{
	"single-mkv",
	"rar5-multivolume",
	"rar4-encrypted",
	"7z-encrypted",
	"par2-repair",
	"split-7z",
	"obfuscated-rar",
	"large-segments",
}

const chaosStatProbeSlug = "stat-health-probe"

var chaosSeedFixtureSlugs = append(
	append([]string(nil), chaosFixtureSlugs...),
	chaosStatProbeSlug,
)

var restartFixtureSlugs = []string{
	"direct-store-par2-alias-restart",
	"direct-unpack-restart",
	"par2-heavy-damage",
	"par2-heavy-damage-a",
	"par2-small-repair",
	"rar5-multi-member",
}

func fixtureSlugsForSeedProfile(profile string) []string {
	switch strings.TrimSpace(strings.ToLower(profile)) {
	case "", "functional", "canonical":
		return append([]string(nil), canonicalFixtureSlugs...)
	case "chaos":
		return append([]string(nil), chaosSeedFixtureSlugs...)
	case "tcp-chaos":
		return append([]string(nil), tcpChaosFixtureSlugs...)
	case "restart":
		return append([]string(nil), restartFixtureSlugs...)
	default:
		log.Fatalf("invalid E2E_SEED_PROFILE=%q (expected functional|chaos|tcp-chaos|restart)", profile)
		return nil
	}
}

func fixtureDirsForSlugs(slugs []string) []string {
	dirs := make([]string, 0, len(slugs))
	for _, slug := range slugs {
		dirs = append(dirs, filepath.Join(testdataDir(), slug))
	}
	return dirs
}

func loadScenariosForSlugs(slugs []string) []*Scenario {
	scenarios := make([]*Scenario, 0, len(slugs))
	for _, slug := range slugs {
		scenario, err := loadScenario(filepath.Join(testdataDir(), slug))
		if err != nil {
			log.Fatalf("load scenario %q: %v", slug, err)
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios
}

func loadCanonicalScenarios() []*Scenario {
	return loadScenariosForSlugs(canonicalFixtureSlugs)
}

func nntpHost() string { return env("NNTP_HOST", "localhost") }

func nntpPort() string {
	if value := strings.TrimSpace(os.Getenv("NNTP_PORT")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return os.Getenv("E2E_NNTP_PORT")
}

func nntpTLSPort() string {
	if value := strings.TrimSpace(os.Getenv("NNTP_TLS_PORT")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return os.Getenv("E2E_NNTP_TLS_PORT")
}

func backupNntpPort() string {
	if value := strings.TrimSpace(os.Getenv("NNTP_BACKUP_PORT")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return os.Getenv("E2E_NNTP2_PORT")
}

func toxiproxyNntp1Port() string {
	if value := strings.TrimSpace(os.Getenv("TOXIPROXY_NNTP1_PORT")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return os.Getenv("E2E_TOXIPROXY_NNTP1_PORT")
}

func toxiproxyNntp2Port() string {
	if value := strings.TrimSpace(os.Getenv("TOXIPROXY_NNTP2_PORT")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return os.Getenv("E2E_TOXIPROXY_NNTP2_PORT")
}

func nyuuImage() string      { return env("NYUU_IMAGE", "e2e-nyuu") }
func nyuuBackupHost() string { return env("E2E_NYUU_BACKUP_HOST", "nntp2") }
func nyuuBackupPort() string { return env("E2E_NYUU_BACKUP_PORT", "119") }
func loadScenario(dir string) (*Scenario, error) {
	data, err := os.ReadFile(filepath.Join(dir, "scenario.json"))
	if err != nil {
		return nil, err
	}
	var s Scenario
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func readNZBDateAttributes(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	matches := nzbDatePattern.FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		return nil, nil
	}

	dates := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		dates = append(dates, string(match[1]))
	}
	return dates, nil
}

func normalizeNZBDateAttributes(path string, preferredDates []string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	indices := nzbDatePattern.FindAllSubmatchIndex(data, -1)
	if len(indices) == 0 {
		return nil
	}

	dates := preferredDates
	if len(dates) != len(indices) {
		dates = make([]string, len(indices))
		for i := range dates {
			dates[i] = stableNZBDate
		}
	}

	var normalized bytes.Buffer
	last := 0
	for i, idx := range indices {
		if len(idx) < 4 {
			continue
		}
		normalized.Write(data[last:idx[2]])
		normalized.WriteString(dates[i])
		last = idx[3]
	}
	normalized.Write(data[last:])

	return os.WriteFile(path, normalized.Bytes(), 0o644)
}

// dataFiles returns all non-JSON files in a fixture directory.
func dataFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		files = append(files, e.Name())
	}
	return files, nil
}

func seedStageRoot() string {
	return filepath.Join(fixturesDir(), ".seed-stage")
}

func nyuuFixturePath(relPath string) string {
	return "/work/fixtures/" + strings.TrimPrefix(filepath.ToSlash(relPath), "/")
}

func nyuuContainerPathForHost(hostPath string) (string, error) {
	rel, err := filepath.Rel(fixturesDir(), hostPath)
	if err != nil {
		return "", err
	}
	rel = filepath.Clean(rel)
	if rel == "." || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path %s is outside fixtures dir %s", hostPath, fixturesDir())
	}
	return nyuuFixturePath(rel), nil
}

type stagedSeedFile struct {
	source string
	name   string
	size   int64
}

func prepareFixtureStaging(absDir string, scenario *Scenario) (string, []string, int64, func(), error) {
	return prepareFixtureStagingWithInputs(
		absDir,
		scenario.Slug,
		true,
		scenario.SharedAssets,
		scenario.FixtureAssets,
	)
}

func prepareBackupFixtureStaging(absDir string, scenario *Scenario) (string, []string, int64, func(), error) {
	return prepareFixtureStagingWithInputs(
		absDir,
		scenario.Slug+"-backup",
		false,
		nil,
		scenario.BackupFixtureAssets,
	)
}

func prepareFixtureStagingWithInputs(
	absDir string,
	stagePrefix string,
	includeLocalFiles bool,
	sharedAssets []string,
	fixtureAssets []string,
) (string, []string, int64, func(), error) {
	staged, err := collectFixtureStagingFiles(
		absDir,
		includeLocalFiles,
		filepath.Join(testdataDir(), "shared"),
		sharedAssets,
		testdataDir(),
		fixtureAssets,
	)
	if err != nil {
		return "", nil, 0, nil, err
	}
	if len(staged) == 0 {
		return "", nil, 0, nil, fmt.Errorf("fixture %s has no staged files", absDir)
	}

	stageRoot := seedStageRoot()
	if err := os.MkdirAll(stageRoot, 0o755); err != nil {
		return "", nil, 0, nil, err
	}

	stageDir, err := os.MkdirTemp(stageRoot, stagePrefix+"-")
	if err != nil {
		return "", nil, 0, nil, err
	}

	cleanup := func() {
		_ = os.RemoveAll(stageDir)
	}

	var (
		names      []string
		totalBytes int64
	)

	sort.Slice(staged, func(i, j int) bool {
		return staged[i].name < staged[j].name
	})

	for _, file := range staged {
		dest := filepath.Join(stageDir, file.name)
		if err := copyFile(file.source, dest); err != nil {
			cleanup()
			return "", nil, 0, nil, err
		}
		names = append(names, file.name)
		totalBytes += file.size
	}

	return stageDir, names, totalBytes, cleanup, nil
}

func collectFixtureStagingFiles(
	absDir string,
	includeLocalFiles bool,
	sharedRoot string,
	sharedAssets []string,
	fixtureRoot string,
	fixtureAssets []string,
) ([]stagedSeedFile, error) {
	localFiles := []string(nil)
	if includeLocalFiles {
		files, err := dataFiles(absDir)
		if err != nil {
			return nil, err
		}
		localFiles = files
	}

	var staged []stagedSeedFile
	seenNames := make(map[string]string)
	addSource := func(source, name string) error {
		if prior, exists := seenNames[name]; exists {
			return fmt.Errorf("duplicate staged filename %q from %s and %s", name, prior, source)
		}
		info, statErr := os.Stat(source)
		if statErr != nil {
			return statErr
		}
		seenNames[name] = source
		staged = append(staged, stagedSeedFile{source: source, name: name, size: info.Size()})
		return nil
	}

	for _, name := range localFiles {
		source := filepath.Join(absDir, name)
		if err := addSource(source, name); err != nil {
			return nil, err
		}
	}

	for _, asset := range sharedAssets {
		cleanAsset, stagedName, err := parseSharedAssetSpec(asset)
		if err != nil {
			return nil, err
		}
		source := filepath.Join(sharedRoot, cleanAsset)
		if err := addSource(source, stagedName); err != nil {
			return nil, err
		}
	}

	for _, asset := range fixtureAssets {
		cleanAsset, stagedName, err := parseFixtureAssetSpec(asset)
		if err != nil {
			return nil, err
		}
		source := filepath.Join(fixtureRoot, cleanAsset)
		if err := addSource(source, stagedName); err != nil {
			return nil, err
		}
	}

	return staged, nil
}

func parseSharedAssetSpec(asset string) (string, string, error) {
	return parseStagedAssetSpec(asset, "shared asset")
}

func parseFixtureAssetSpec(asset string) (string, string, error) {
	return parseStagedAssetSpec(asset, "fixture asset")
}

func parseStagedAssetSpec(asset string, label string) (string, string, error) {
	parts := strings.SplitN(asset, "::", 2)
	source := filepath.Clean(strings.TrimSpace(parts[0]))
	if source == "" || source == "." {
		return "", "", fmt.Errorf("invalid %s path %q", label, asset)
	}
	if strings.HasPrefix(source, "..") || filepath.IsAbs(source) {
		return "", "", fmt.Errorf("invalid %s path %q", label, asset)
	}

	stagedName := filepath.Base(source)
	if len(parts) == 2 {
		stagedName = filepath.Clean(strings.TrimSpace(parts[1]))
		if stagedName == "" || stagedName == "." || stagedName == ".." || filepath.IsAbs(stagedName) || strings.HasPrefix(stagedName, "..") {
			return "", "", fmt.Errorf("invalid %s staged filename %q", label, asset)
		}
	}

	return source, stagedName, nil
}

func copyFile(source, dest string) error {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	dst, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return dst.Close()
}

func ensureSeedingInfrastructure() {
	if err := ensureSeedingInfrastructureErr(); err != nil {
		log.Fatal(err)
	}
}

func ensureSeedingInfrastructureErr() error {
	if err := ensureNyuuImageBuilt(); err != nil {
		return fmt.Errorf("build nyuu image: %w", err)
	}
	if err := dockerComposeUp("nntp", "nyuu"); err != nil {
		return fmt.Errorf("start seeding infrastructure: %w", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		return fmt.Errorf("refresh runtime ports after starting seeding infrastructure: %w", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	if err := ensureNntpChaosOff(); err != nil {
		return fmt.Errorf("reset NNTP chaos before seeding: %w", err)
	}
	return nil
}

func ensureStandardDockerInfrastructure() {
	if err := applyNntpSeedImageCacheForProfile(os.Getenv("E2E_SEED_PROFILE")); err != nil {
		log.Fatalf("prepare pre-seeded NNTP images: %v", err)
	}
	services := []string{"nntp", "nntp2"}
	if weaverUsesPostgresDatastore() {
		services = append(services, "weaver-postgres")
	}
	if err := dockerComposeUp(services...); err != nil {
		log.Fatalf("start standard infrastructure: %v", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		log.Fatalf("refresh runtime ports after starting standard infrastructure: %v", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForTCP("localhost:"+backupNntpPort(), 30*time.Second)
	if weaverUsesPostgresDatastore() {
		if err := waitForWeaverPostgresReady(30 * time.Second); err != nil {
			log.Fatalf("wait for Weaver Postgres: %v", err)
		}
	}
	if err := ensureStandardManagedWeaver(); err != nil {
		log.Fatalf("start managed local weaver: %v", err)
	}
}

// --- scenarios ---

func cmdScenarios() {
	fmt.Printf("%-25s %-5s %-22s %s\n", "SLUG", "CAT", "OUTCOME", "DESCRIPTION")
	fmt.Println(strings.Repeat("-", 100))
	for _, s := range loadCanonicalScenarios() {
		fmt.Printf("%-25s %-5s %-22s %s\n", s.Slug, s.Category, s.ExpectedOutcome, s.Description)
	}
}

// --- status ---

func cmdStatus() {
	fmt.Println("Service status:")
	checkTCP("NNTP", nntpHost()+":"+nntpPort())
	checkHTTP("Weaver", graphqlURL(defaultWeaverURL()))
}

func cmdFull() {
	runParallelFullSuite()
}

func cmdFunctional() {
	runFunctionalFullSuite()
}

func checkTCP(name, addr string) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("  %-10s %-35s DOWN\n", name, addr)
		return
	}
	conn.Close()
	fmt.Printf("  %-10s %-35s UP\n", name, addr)
}

func checkHTTP(name, url string) {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(url)
	if err != nil {
		fmt.Printf("  %-10s %-35s DOWN\n", name, url)
		return
	}
	resp.Body.Close()
	fmt.Printf("  %-10s %-35s UP (%d)\n", name, url, resp.StatusCode)
}
