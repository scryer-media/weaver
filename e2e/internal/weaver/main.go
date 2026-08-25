// weaver-e2e is the CLI tool for the Weaver end-to-end test environment.
//
// It orchestrates Nyuu (real usenet poster) for article posting and NZB generation,
// and talks to the newznab indexer to register releases.
//
// Subcommands:
//
//	seed <fixture-dir>   Post fixture via Nyuu, register NZB with indexer
//	seed-all             Seed all fixtures from testdata/
//	verify               STAT articles in NNTP, search releases in indexer
//	status               Show health of all services
//	scenarios            List all available test scenarios
package weaver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Scenario is the JSON manifest for a pre-built test fixture.
type Scenario struct {
	Slug                               string                     `json:"slug"`
	Title                              string                     `json:"title"`
	IndexerTitle                       string                     `json:"indexerTitle,omitempty"`
	Description                        string                     `json:"description"`
	Category                           string                     `json:"category"`
	ExpectedOutcome                    string                     `json:"expected_outcome"`
	Password                           string                     `json:"password,omitempty"`
	SegmentSize                        int                        `json:"segment_size,omitempty"`
	NZBSegmentNumbers                  []int                      `json:"nzb_segment_numbers,omitempty"`
	NZBSegmentNumberStart              int                        `json:"nzb_segment_number_start,omitempty"`
	NZBSegmentNumberStep               int                        `json:"nzb_segment_number_step,omitempty"`
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
	NewznabAttributes                  map[string]string          `json:"newznabAttributes,omitempty"`
	RuntimeAssertions                  *ScenarioRuntimeAssertions `json:"runtimeAssertions,omitempty"`
}

type ScenarioRuntimeAssertions struct {
	FileIdentityRewrite *ScenarioFileIdentityRewriteAssertion `json:"fileIdentityRewrite,omitempty"`
	Par2CleanSettlement *ScenarioPar2CleanSettlementAssertion `json:"par2CleanSettlement,omitempty"`
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

type runtimePortState struct {
	NNTPPort           int `json:"nntp_port"`
	NNTPTLSPort        int `json:"nntp_tls_port"`
	NNTP2Port          int `json:"nntp2_port"`
	ToxiproxyAPIPort   int `json:"toxiproxy_api_port"`
	ToxiproxyNNTP1Port int `json:"toxiproxy_nntp1_port"`
	ToxiproxyNNTP2Port int `json:"toxiproxy_nntp2_port"`
	NewznabPort        int `json:"newznab_port"`
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
  NEWZNAB_URL          Newznab indexer URL (default: runtime-assigned open port)
  NEWZNAB_API_KEY      Newznab API key (default: test-e2e-key)
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
		"E2E_NEWZNAB_PORT",
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
		state.NewznabPort,
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
		{value: &state.NewznabPort, name: "newznab"},
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
	state.NewznabPort, err = inspectDockerHostPort("newznab", "8088/tcp")
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
		"E2E_NEWZNAB_PORT":         strconv.Itoa(state.NewznabPort),
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
		"NEWZNAB_URL":              fmt.Sprintf("http://localhost:%d", state.NewznabPort),
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
		"newznab",
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
	"direct-store-multi-member",
	"direct-store-multivolume",
	"direct-store-par2-repair",
	"direct-store-rar4",
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
	"par2-obfuscated-rar-repair",
	"par2-obfuscated-rar-rewrite",
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
	"rar5-corrupted",
	"rar5-encrypted",
	"rar5-filename-dedupe",
	"rar5-filename-normalization",
	"rar5-hp-encrypted",
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

func newznabURL() string {
	if value := strings.TrimSpace(os.Getenv("NEWZNAB_URL")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return fmt.Sprintf("http://localhost:%s", os.Getenv("E2E_NEWZNAB_PORT"))
}

func newznabAPIKey() string  { return env("NEWZNAB_API_KEY", "test-e2e-key") }
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
	if err := dockerComposeUp("nntp", "newznab", "nyuu"); err != nil {
		return fmt.Errorf("start seeding infrastructure: %w", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		return fmt.Errorf("refresh runtime ports after starting seeding infrastructure: %w", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForHTTP(newznabURL()+"/admin/health", 30*time.Second)
	if err := ensureNntpChaosOff(); err != nil {
		return fmt.Errorf("reset NNTP chaos before seeding: %w", err)
	}
	return nil
}

func ensureStandardDockerInfrastructure() {
	if err := applyNntpSeedImageCacheForProfile(os.Getenv("E2E_SEED_PROFILE")); err != nil {
		log.Fatalf("prepare pre-seeded NNTP images: %v", err)
	}
	services := []string{"nntp", "newznab", "nntp2"}
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
	waitForHTTP(newznabURL()+"/admin/health", 30*time.Second)
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

// --- seed ---

func cmdSeed(dir string) {
	absDir := resolveRepoPath(dir)
	// Fixtures before infrastructure: a missing payload should fail (or be
	// fetched or generated) before any container is started for it.
	ensureFixtureDir(absDir)
	ensureSeedingInfrastructure()
	scenario, err := loadScenario(absDir)
	if err != nil {
		log.Fatal(err)
	}
	if err := seedFixture(absDir); err != nil {
		log.Fatal(err)
	}
	if scenarioNeedsBackupServerState(scenario) {
		if err := ensureBackupNntpReady(); err != nil {
			log.Fatal(err)
		}
		if scenarioHasBackupFixtureOverride(scenario) {
			if err := seedScenarioBackupOverride(absDir, scenario); err != nil {
				log.Fatal(err)
			}
		}
		if err := applyPrimarySeedMutations(scenario); err != nil {
			log.Fatal(err)
		}
	}
}

func seedFixture(dir string) error {
	return seedFixtureWithRetry(dir, envInt("E2E_SEED_RETRIES", 3))
}

func seedFixtureWithRetry(dir string, attempts int) error {
	absDir := resolveRepoPath(dir)
	ensureFixtureDir(absDir)

	scenario, err := loadScenario(absDir)
	if err != nil {
		return fmt.Errorf("load scenario from %s: %w", absDir, err)
	}

	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		lastErr = seedScenarioRelease(absDir, scenario)
		if lastErr == nil {
			return nil
		}
		if attempt == attempts || !isTransientSeedError(lastErr) {
			return lastErr
		}

		log.Printf("[%s] transient seed failure on attempt %d/%d: %v", scenario.Slug, attempt, attempts, lastErr)
		waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	}

	return lastErr
}

func isTransientSeedError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	transientMarkers := []string{
		"NNTP connection failed",
		"connect ECONNREFUSED",
		"connect: connection refused",
		"connect: EOF",
		"read greeting",
		"read response",
		"write command",
		"unexpected greeting",
		"empty response",
	}
	for _, marker := range transientMarkers {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func seedScenarioRelease(absDir string, scenario *Scenario) error {
	logSeed := func(format string, args ...interface{}) {
		log.Printf("[%s] %s", scenario.Slug, fmt.Sprintf(format, args...))
	}

	// A uu scenario's articles are already encoded; they are posted from the
	// corpus rather than handed to nyuu, which only speaks yEnc. A scenario
	// may have both kinds, one kind, or — for a pure uu release — nothing at
	// all for nyuu to stage.
	uuPlan, err := loadUUPlan(absDir)
	if err != nil {
		return fmt.Errorf("load uu posting plan for %s: %w", scenario.Slug, err)
	}
	stagesForNyuu, err := scenarioStagesPostableFiles(absDir, scenario)
	if err != nil {
		return fmt.Errorf("inspect staged files in %s: %w", absDir, err)
	}
	if !stagesForNyuu && uuPlan == nil {
		return fmt.Errorf("fixture %s has no staged files and no uu posting plan", absDir)
	}

	var (
		stageDir     string
		files        []string
		totalBytes   int64
		cleanupStage = func() {}
	)
	if stagesForNyuu {
		stageDir, files, totalBytes, cleanupStage, err = prepareFixtureStaging(absDir, scenario)
		if err != nil {
			return fmt.Errorf("prepare staged files in %s: %w", absDir, err)
		}
	}
	defer cleanupStage()

	uuFileCount := 0
	if uuPlan != nil {
		uuFileCount = len(uuPlan.Files)
	}
	logSeed("seeding %d yEnc file(s) and %d uuencoded file(s), title=%s", len(files), uuFileCount, scenario.Title)

	if err := purgeSeededArticles(scenario.Slug); err != nil {
		return fmt.Errorf("purge existing articles for %s: %w", scenario.Slug, err)
	}

	nzbDir := filepath.Join(fixturesDir(), scenario.Slug)
	if err := os.MkdirAll(nzbDir, 0o755); err != nil {
		return fmt.Errorf("create generated NZB dir for %s: %w", scenario.Slug, err)
	}
	nzbPath := filepath.Join(nzbDir, scenario.Slug+".nzb")
	existingDates, err := readNZBDateAttributes(nzbPath)
	if err != nil {
		return fmt.Errorf("read existing NZB dates for %s: %w", scenario.Slug, err)
	}
	if err := os.Remove(nzbPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("reset generated NZB for %s: %w", scenario.Slug, err)
	}
	if stagesForNyuu {
		stageDirInNyuu, err := nyuuContainerPathForHost(stageDir)
		if err != nil {
			return fmt.Errorf("resolve nyuu stage path for %s: %w", scenario.Slug, err)
		}
		nzbPathInNyuu, err := nyuuContainerPathForHost(nzbPath)
		if err != nil {
			return fmt.Errorf("resolve nyuu nzb path for %s: %w", scenario.Slug, err)
		}

		if err := runNyuuPost(stageDirInNyuu, files, scenario, "127.0.0.1", "119", nzbPathInNyuu); err != nil {
			return fmt.Errorf("nyuu failed for %s: %w", scenario.Slug, err)
		}
	}

	if uuPlan != nil {
		elements, uuBytes, err := seedUUArticles(nntpHost(), nntpPort(), scenario, absDir, uuPlan)
		if err != nil {
			return fmt.Errorf("post uu articles for %s: %w", scenario.Slug, err)
		}
		totalBytes += uuBytes
		if stagesForNyuu {
			err = spliceUUFilesIntoNZB(nzbPath, elements)
		} else {
			err = writeUUNZB(nzbPath, scenario, elements)
		}
		if err != nil {
			return fmt.Errorf("write uu NZB entries for %s: %w", scenario.Slug, err)
		}
	}

	if err := normalizeNZBDateAttributes(nzbPath, existingDates); err != nil {
		return fmt.Errorf("normalize NZB dates for %s: %w", scenario.Slug, err)
	}

	nzbData, err := os.ReadFile(nzbPath)
	if err != nil {
		return fmt.Errorf("read NZB for %s: %w", scenario.Slug, err)
	}
	segmentNumbers, err := scenarioNZBSegmentNumbers(nzbData, scenario)
	if err != nil {
		return fmt.Errorf("build NZB segment numbers for %s: %w", scenario.Slug, err)
	}
	if len(segmentNumbers) > 0 {
		nzbData, err = rewriteNZBSegmentNumbers(nzbData, segmentNumbers)
		if err != nil {
			return fmt.Errorf("rewrite NZB segment numbers for %s: %w", scenario.Slug, err)
		}
		if err := os.WriteFile(nzbPath, nzbData, 0o644); err != nil {
			return fmt.Errorf("persist rewritten NZB for %s: %w", scenario.Slug, err)
		}
	}

	if len(scenario.DeleteSubjectContains) > 0 {
		messageIDs, err := extractMessageIDsBySubjectContains(nzbData, scenario.DeleteSubjectContains, scenario.DeleteSubjectTailArticles)
		if err != nil {
			return fmt.Errorf("extract targeted delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no NZB segments matched deleteSubjectContains=%v for %s",
				scenario.DeleteSubjectContains,
				scenario.Slug,
			)
		}
		logSeed(
			"deleting %d article(s) matching subject filters %v...",
			len(messageIDs),
			scenario.DeleteSubjectContains,
		)
		if err := deleteArticlesByMessageID(messageIDs); err != nil {
			return fmt.Errorf("delete targeted articles for %s: %w", scenario.Slug, err)
		}
	}

	if len(scenario.DeleteSegmentNumbers) > 0 {
		messageIDs, err := extractMessageIDsBySegmentNumbers(nzbData, segmentDeleteNeedles(scenario), scenario.DeleteSegmentNumbers)
		if err != nil {
			return fmt.Errorf("extract segment-number delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no NZB segments matched deleteSegmentNumbers=%v for %s",
				scenario.DeleteSegmentNumbers,
				scenario.Slug,
			)
		}
		logSeed("deleting %d article(s) at segment number(s) %v...", len(messageIDs), scenario.DeleteSegmentNumbers)
		if err := deleteArticlesByMessageID(messageIDs); err != nil {
			return fmt.Errorf("delete segment-number articles for %s: %w", scenario.Slug, err)
		}
	}

	if scenario.DeleteFirstProbeSampleHits > 0 {
		messageIDs, err := extractFirstProbeSampleMessageIDs(nzbData, scenario.DeleteFirstProbeSampleHits)
		if err != nil {
			return fmt.Errorf("extract probe-sample delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no probe-sample message ids available for %s (requested %d)",
				scenario.Slug,
				scenario.DeleteFirstProbeSampleHits,
			)
		}
		logSeed("deleting %d first-round probe sample article(s)...", len(messageIDs))
		if err := deleteArticlesByMessageID(messageIDs); err != nil {
			return fmt.Errorf("delete probe-sample articles for %s: %w", scenario.Slug, err)
		}
	}

	if scenario.DeleteFirstMessageIDs > 0 {
		messageIDs, err := extractFirstMessageIDs(nzbData, scenario.DeleteFirstMessageIDs)
		if err != nil {
			return fmt.Errorf("extract first-message delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no leading message ids available for %s (requested %d)",
				scenario.Slug,
				scenario.DeleteFirstMessageIDs,
			)
		}
		logSeed("deleting %d leading article(s)...", len(messageIDs))
		if err := deleteArticlesByMessageID(messageIDs); err != nil {
			return fmt.Errorf("delete leading articles for %s: %w", scenario.Slug, err)
		}
	}

	// For health-failure scenarios, delete a percentage of articles from NNTP
	// to simulate missing segments.
	if scenario.SkipArticlesPct > 0 {
		logSeed("deleting %d%% of articles...", scenario.SkipArticlesPct)
		if err := deleteArticles(scenario.Slug, scenario.SkipArticlesPct); err != nil {
			logSeed("WARNING: delete articles failed: %v", err)
		}
	}

	logSeed("NZB generated: %d bytes", len(nzbData))

	// Compute total size from data files
	// Register release with indexer
	if err := registerRelease(scenario, nzbData, totalBytes); err != nil {
		return fmt.Errorf("register release for %s: %w", scenario.Slug, err)
	}

	logSeed("done: guid=e2e-%s", scenario.Slug)
	return nil
}

func scenarioHasBackupFixtureOverride(scenario *Scenario) bool {
	return scenario != nil && len(scenario.BackupFixtureAssets) > 0
}

func scenarioNeedsBackupServerState(scenario *Scenario) bool {
	if scenario == nil {
		return false
	}
	return scenarioHasBackupFixtureOverride(scenario) ||
		scenario.PrimaryDeleteFirstMessageIDs > 0 ||
		len(scenario.PrimaryDeleteSubjectContains) > 0 ||
		strings.TrimSpace(scenario.PrimaryChaosConfig) != "" ||
		strings.TrimSpace(scenario.BackupUnavailableUntilFileComplete) != ""
}

// segmentDeleteNeedles narrows which files the segment-number deletion may
// reach. It is its own field because deleteSubjectContains deletes whole files
// wherever it is set: a scenario that wants one interior article of one named
// file — and every other article of that file kept — cannot express that by
// combining the two. With no needles of its own it falls back to the whole-file
// filter, which is the shape the scenarios that predate this field rely on.
func segmentDeleteNeedles(scenario *Scenario) []string {
	if scenario == nil {
		return nil
	}
	if len(scenario.DeleteSegmentSubjectContains) > 0 {
		return scenario.DeleteSegmentSubjectContains
	}
	return scenario.DeleteSubjectContains
}

func scenarioUsesExclusiveNntpState(scenario *Scenario) bool {
	return scenario != nil && (strings.TrimSpace(scenario.PrimaryChaosConfig) != "" ||
		strings.TrimSpace(scenario.BackupUnavailableUntilFileComplete) != "")
}

func applyBackupFixtureOverridesForSlugs(slugs []string) error {
	for _, slug := range slugs {
		absDir := filepath.Join(testdataDir(), slug)
		scenario, err := loadScenario(absDir)
		if err != nil {
			return fmt.Errorf("load scenario from %s: %w", absDir, err)
		}
		if !scenarioHasBackupFixtureOverride(scenario) {
			continue
		}
		if err := seedScenarioBackupOverride(absDir, scenario); err != nil {
			return err
		}
	}
	return nil
}

func seedScenarioBackupOverride(absDir string, scenario *Scenario) error {
	logSeed := func(format string, args ...interface{}) {
		log.Printf("[%s] %s", scenario.Slug, fmt.Sprintf(format, args...))
	}

	stageDir, files, _, cleanupStage, err := prepareBackupFixtureStaging(absDir, scenario)
	if err != nil {
		return fmt.Errorf("prepare backup staged files in %s: %w", absDir, err)
	}
	defer cleanupStage()

	if err := deleteArticlesAt(nntpHost(), backupNntpPort(), scenario.Slug, 100); err != nil {
		return fmt.Errorf("purge backup articles for %s: %w", scenario.Slug, err)
	}

	nzbPath := filepath.Join(stageDir, scenario.Slug+"-backup.nzb")
	nzbPathInNyuu, err := nyuuContainerPathForHost(nzbPath)
	if err != nil {
		return fmt.Errorf("resolve backup nyuu nzb path for %s: %w", scenario.Slug, err)
	}
	stageDirInNyuu, err := nyuuContainerPathForHost(stageDir)
	if err != nil {
		return fmt.Errorf("resolve backup nyuu stage path for %s: %w", scenario.Slug, err)
	}

	logSeed("posting backup override (%d file(s))...", len(files))
	if err := runNyuuPost(stageDirInNyuu, files, scenario, nyuuBackupHost(), nyuuBackupPort(), nzbPathInNyuu); err != nil {
		return fmt.Errorf("post backup override for %s: %w", scenario.Slug, err)
	}

	logSeed("backup override ready")
	return nil
}

func applyPrimarySeedMutationsForSlugs(slugs []string) error {
	for _, slug := range slugs {
		absDir := filepath.Join(testdataDir(), slug)
		scenario, err := loadScenario(absDir)
		if err != nil {
			return fmt.Errorf("load scenario from %s: %w", absDir, err)
		}
		if err := applyPrimarySeedMutations(scenario); err != nil {
			return err
		}
	}
	return nil
}

func applyPrimarySeedMutations(scenario *Scenario) error {
	if scenario == nil {
		return nil
	}
	if scenario.PrimaryDeleteFirstMessageIDs <= 0 && len(scenario.PrimaryDeleteSubjectContains) == 0 {
		return nil
	}

	nzbPath := filepath.Join(fixturesDir(), scenario.Slug, scenario.Slug+".nzb")
	nzbData, err := os.ReadFile(nzbPath)
	if err != nil {
		return fmt.Errorf("read NZB for primary-only seed mutations on %s: %w", scenario.Slug, err)
	}

	logSeed := func(format string, args ...interface{}) {
		log.Printf("[%s] %s", scenario.Slug, fmt.Sprintf(format, args...))
	}

	if scenario.PrimaryDeleteFirstMessageIDs > 0 {
		messageIDs, err := extractFirstMessageIDs(nzbData, scenario.PrimaryDeleteFirstMessageIDs)
		if err != nil {
			return fmt.Errorf("extract primary-only leading delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no primary-only leading message ids available for %s (requested %d)",
				scenario.Slug,
				scenario.PrimaryDeleteFirstMessageIDs,
			)
		}
		logSeed("deleting %d leading article(s) from primary only...", len(messageIDs))
		if err := deleteArticlesByMessageIDOnServers(messageIDs, true, false); err != nil {
			return fmt.Errorf("delete primary-only leading articles for %s: %w", scenario.Slug, err)
		}
	}

	if len(scenario.PrimaryDeleteSubjectContains) > 0 {
		messageIDs, err := extractMessageIDsBySubjectContains(nzbData, scenario.PrimaryDeleteSubjectContains, 0)
		if err != nil {
			return fmt.Errorf("extract primary-only subject delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no NZB segments matched primaryDeleteSubjectContains=%v for %s",
				scenario.PrimaryDeleteSubjectContains,
				scenario.Slug,
			)
		}
		logSeed(
			"deleting %d article(s) from primary only matching subject filters %v...",
			len(messageIDs),
			scenario.PrimaryDeleteSubjectContains,
		)
		if err := deleteArticlesByMessageIDOnServers(messageIDs, true, false); err != nil {
			return fmt.Errorf("delete primary-only subject articles for %s: %w", scenario.Slug, err)
		}
	}

	return nil
}

func runNyuuPost(
	stageDirInNyuu string,
	files []string,
	scenario *Scenario,
	host string,
	port string,
	nzbPathInNyuu string,
) error {
	nyuuArgs := append(
		dockerComposeArgs(
			"exec",
			"-T",
			"nyuu",
			"nyuu",
		),
		"-h", host, "-P", port, "--ssl=false",
		"-u", nntpUsername(),
		"-p", nntpPassword(),
		"-n", "1",
		"-g", "alt.binaries.test",
		"-f", "e2e-test@example.invalid",
		"--keep-message-id",
		"--message-id", fmt.Sprintf("e2e-%s-{0filenum}-{0part}@e2e-test", scenario.Slug),
		"-o", nzbPathInNyuu,
		"-O",
		"--check-connections", "0",
		"--skip-errors", "post-reject",
	)

	if scenario.Password != "" {
		nyuuArgs = append(nyuuArgs, "--nzb-password", scenario.Password)
	}

	nyuuArgs = append(nyuuArgs, "-M", "name="+scenario.Title)
	nyuuArgs = append(nyuuArgs, "-M", "category="+scenario.Category)

	segSize := "750K"
	if scenario.SegmentSize > 0 {
		segSize = fmt.Sprintf("%d", scenario.SegmentSize)
	}
	nyuuArgs = append(nyuuArgs, "-a", segSize)

	for _, f := range files {
		nyuuArgs = append(nyuuArgs, strings.TrimRight(stageDirInNyuu, "/")+"/"+f)
	}

	cmd := exec.Command("docker", nyuuArgs...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "nyuu post")
}

func registerRelease(s *Scenario, nzbXML []byte, sizeBytes int64) error {
	attributes := map[string]string{
		"category": s.Category,
		"size":     fmt.Sprintf("%d", sizeBytes),
	}
	for key, value := range s.NewznabAttributes {
		attributes[key] = value
	}
	if s.Password != "" {
		attributes["password"] = s.Password
	}

	payload := map[string]interface{}{
		"guid":       fmt.Sprintf("e2e-%s", s.Slug),
		"title":      scenarioIndexerTitle(s),
		"nzb_xml":    nzbXML, // Go json.Marshal base64-encodes []byte
		"size_bytes": sizeBytes,
		"attributes": attributes,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	waitForHTTP(newznabURL()+"/admin/health", 30*time.Second)

	client := &http.Client{Timeout: 5 * time.Second}
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		resp, err := client.Post(newznabURL()+"/admin/releases", "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusCreated {
			resp.Body.Close()
			return nil
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastErr = fmt.Errorf("indexer returned %d: %s", resp.StatusCode, respBody)
		time.Sleep(1 * time.Second)
	}
	return lastErr
}

func scenarioIndexerTitle(s *Scenario) string {
	if s == nil {
		return ""
	}
	if title := strings.TrimSpace(s.IndexerTitle); title != "" {
		return title
	}
	return strings.TrimSpace(s.Title)
}

// --- seed-all ---

func seedAllForProfile(profile string) {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		profile = "functional"
	}
	slugs := fixtureSlugsForSeedProfile(profile)
	dirs := fixtureDirsForSlugs(slugs)

	if len(dirs) == 0 {
		log.Fatalf("no fixtures configured for seed profile %q", profile)
	}

	ensureFixtureProfiles(profile)
	var seedImages nntpSeedImageSet
	if nntpSeedImageCacheEnabled() {
		var err error
		seedImages, err = nntpSeedImageSetForProfile(profile, slugs)
		if err != nil {
			log.Fatalf("fingerprint pre-seeded NNTP images for profile %s: %v", profile, err)
		}
		if seedImages.ready() {
			emitProgressEvent(progressEvent{Kind: "seed_total", Total: len(dirs), Detail: "fixtures (pre-seeded NNTP images)"})
			if err := restoreSeedImageCache(seedImages, slugs); err != nil {
				emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
				log.Fatalf("restore pre-seeded NNTP images for profile %s: %v", profile, err)
			}
			for index, slug := range slugs {
				emitProgressEvent(progressEvent{
					Kind:    "seed_progress",
					Current: index + 1,
					Total:   len(dirs),
					Status:  "pass",
					Detail:  slug + " (pre-seeded image)",
				})
			}
			emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "pass"})
			return
		}
	}
	ensureSeedingInfrastructure()
	emitProgressEvent(progressEvent{Kind: "seed_total", Total: len(dirs), Detail: "fixtures"})

	workerCount := envInt("E2E_SEED_JOBS", 4)
	if workerCount < 1 {
		log.Fatalf("invalid E2E_SEED_JOBS=%d (expected >= 1)", workerCount)
	}
	if workerCount > len(dirs) {
		workerCount = len(dirs)
	}

	log.Printf("seeding %d fixtures with %d worker(s) for profile=%s...", len(dirs), workerCount, profile)

	type seedJob struct {
		index int
		dir   string
	}
	type seedResult struct {
		index int
		name  string
		err   error
	}

	jobs := make(chan seedJob)
	results := make(chan seedResult, len(dirs))
	var wg sync.WaitGroup

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				name := filepath.Base(job.dir)
				log.Printf("[%d/%d] %s queued", job.index+1, len(dirs), name)

				var resultErr error
				func() {
					defer func() {
						if r := recover(); r != nil {
							resultErr = fmt.Errorf("panic while seeding %s: %v", name, r)
						}
					}()
					resultErr = seedFixture(job.dir)
				}()

				results <- seedResult{index: job.index, name: name, err: resultErr}
			}
		}()
	}

	go func() {
		for i, dir := range dirs {
			jobs <- seedJob{index: i, dir: dir}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	passed, failed := 0, 0
	var failedNames []string
	for result := range results {
		if result.err != nil {
			failed++
			failedNames = append(failedNames, result.name)
			log.Printf("[%d/%d] %s FAILED: %v", result.index+1, len(dirs), result.name, result.err)
			emitProgressEvent(progressEvent{
				Kind:    "seed_progress",
				Current: passed + failed,
				Total:   len(dirs),
				Status:  "fail",
				Detail:  result.name,
			})
			continue
		}
		passed++
		log.Printf("[%d/%d] %s PASS", result.index+1, len(dirs), result.name)
		emitProgressEvent(progressEvent{
			Kind:    "seed_progress",
			Current: passed + failed,
			Total:   len(dirs),
			Status:  "pass",
			Detail:  result.name,
		})
	}

	log.Printf("seed-all complete: %d passed, %d failed out of %d", passed, failed, len(dirs))
	if len(failedNames) > 0 {
		sort.Strings(failedNames)
		log.Printf("seed failures: %s", strings.Join(failedNames, ", "))
	}
	if failed > 0 {
		emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
		os.Exit(1)
	}
	if err := ensureBackupNntpReady(); err != nil {
		emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
		log.Fatalf("prepare backup NNTP after seed-all: %v", err)
	}
	if err := applyBackupFixtureOverridesForSlugs(slugs); err != nil {
		emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
		log.Fatalf("apply backup fixture overrides after seed-all: %v", err)
	}
	if err := applyPrimarySeedMutationsForSlugs(slugs); err != nil {
		emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
		log.Fatalf("apply primary-only fixture mutations after seed-all: %v", err)
	}
	emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "pass"})
	if nntpSeedImageCaptureEnabled() {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := captureSeedImageCache(ctx, seedImages, slugs, nntpSeedCacheCaptureConfig{
			Project:   composeProject(),
			StageRoot: os.TempDir(),
			LockRoot:  os.TempDir(),
			OwnerPID:  os.Getpid(),
		}); err != nil {
			// The completed seed remains valid for this phase. The image cache is
			// a local acceleration layer, so a disk or Docker image-build problem
			// must not turn a correctly seeded E2E phase into a false failure.
			log.Printf("warning: pre-seed NNTP image cache unavailable for profile=%s: %v", profile, err)
		}
	}
}

func cmdSeedAll() {
	seedAllForProfile(env("E2E_SEED_PROFILE", "functional"))
}

// syncArticlesToBackup streams all articles from nntp to nntp2 through Docker.
// It is a no-op when nntp2 is not running.
func syncArticlesToBackup() error {
	if !dockerContainerRunning("nntp2") {
		return nil
	}

	sourceID, err := dockerComposeServiceContainerID("nntp")
	if err != nil {
		return fmt.Errorf("resolve primary NNTP container: %w", err)
	}
	backupID, err := dockerComposeServiceContainerID("nntp2")
	if err != nil {
		return fmt.Errorf("resolve backup NNTP container: %w", err)
	}

	log.Printf("syncing articles to backup NNTP server (nntp2)...")
	if err := streamArticlesToBackup(sourceID, backupID); err != nil {
		return err
	}
	// Tell nntp2 to reload its index
	resp := sendNntpCommandTo(nntpHost(), backupNntpPort(), "RELOAD")
	if resp != "" {
		log.Printf("  nntp2 reload: %s", resp)
	}
	log.Printf("  backup NNTP server synced")
	return nil
}

// streamArticlesToBackup avoids materializing the complete fixture corpus on
// the host. The previous docker cp -> temporary directory -> docker cp path
// doubled disk I/O and left both functional datastores contending for it.
func streamArticlesToBackup(sourceID, backupID string) error {
	source, destination := articleSyncCommands(sourceID, backupID)

	var sourceStderr, destinationStderr bytes.Buffer
	source.Stderr = &sourceStderr
	destination.Stderr = &destinationStderr
	archive, err := source.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open primary NNTP article stream: %w", err)
	}
	destination.Stdin = archive
	if err := destination.Start(); err != nil {
		return fmt.Errorf("start backup NNTP article stream: %w", err)
	}
	if err := source.Start(); err != nil {
		_ = destination.Process.Kill()
		_ = destination.Wait()
		return fmt.Errorf("start primary NNTP article stream: %w", err)
	}

	if err := source.Wait(); err != nil {
		_ = destination.Wait()
		return fmt.Errorf("stream primary NNTP articles: %w: %s", err, strings.TrimSpace(sourceStderr.String()))
	}
	if err := destination.Wait(); err != nil {
		return fmt.Errorf("extract backup NNTP articles: %w: %s", err, strings.TrimSpace(destinationStderr.String()))
	}
	return nil
}

// articleSyncCommands streams Docker's archive protocol directly between
// containers. Unlike `docker exec ... tar`, this does not require tar inside
// the intentionally minimal e2e-nntp image.
func articleSyncCommands(sourceID, backupID string) (*exec.Cmd, *exec.Cmd) {
	source := exec.Command("docker", "cp", sourceID+":/data/articles/.", "-")
	destination := exec.Command("docker", "cp", "-", backupID+":/data/articles")
	return source, destination
}

// --- verify ---

func cmdVerify() {
	// Collect NZBs and check articles exist via NNTP STAT
	addr := nntpHost() + ":" + nntpPort()
	log.Printf("verifying fixtures against NNTP at %s...", addr)

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		log.Fatalf("nntp connect: %v", err)
	}
	defer conn.Close()

	// Read greeting
	buf := make([]byte, 512)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, _ := conn.Read(buf)
	greeting := string(buf[:n])
	if !strings.HasPrefix(greeting, "200") {
		log.Fatalf("unexpected greeting: %s", greeting)
	}

	totalChecked, totalFound := 0, 0
	for _, slug := range canonicalFixtureSlugs {
		nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
		nzbData, err := os.ReadFile(nzbPath)
		if err != nil {
			continue
		}

		// Extract message-IDs from the NZB (simple regex, not a full parser)
		msgIDs := extractMessageIDs(string(nzbData))
		missing := 0
		for _, msgID := range msgIDs {
			totalChecked++
			// Send STAT
			fmt.Fprintf(conn, "STAT <%s>\r\n", msgID)
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				log.Printf("  read error: %v", err)
				continue
			}
			resp := string(buf[:n])
			if strings.HasPrefix(resp, "223") {
				totalFound++
			} else {
				missing++
			}
		}

		if missing > 0 {
			log.Printf("  %s: %d/%d articles missing", slug, missing, len(msgIDs))
		} else {
			log.Printf("  %s: OK (%d articles)", slug, len(msgIDs))
		}
	}

	// Verify releases in indexer
	log.Printf("verifying releases in indexer at %s...", newznabURL())
	for _, slug := range canonicalFixtureSlugs {
		s, err := loadScenario(filepath.Join(testdataDir(), slug))
		if err != nil {
			log.Fatalf("load canonical scenario %q: %v", slug, err)
		}

		guid := fmt.Sprintf("e2e-%s", s.Slug)
		url := fmt.Sprintf("%s/api?t=search&q=%s&apikey=%s", newznabURL(), s.Title, newznabAPIKey())
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("  %s: search error: %v", guid, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			log.Printf("  %s: indexer OK", guid)
		} else {
			log.Printf("  %s: indexer returned %d", guid, resp.StatusCode)
		}
	}

	log.Printf("NNTP: %d/%d articles verified", totalFound, totalChecked)
}

// extractMessageIDs pulls message-IDs from NZB XML (simple string scan).
func extractMessageIDs(nzb string) []string {
	var ids []string
	const segmentStart = "<segment"
	rest := nzb
	for {
		idx := strings.Index(rest, segmentStart)
		if idx < 0 {
			break
		}
		rest = rest[idx+len(segmentStart):]
		// `<segment` is also a prefix of the `<segments>` container. Accept
		// only an actual segment element so the first article in every file is
		// not accidentally folded into its parent container tag.
		if len(rest) > 0 && rest[0] != '>' && rest[0] != ' ' && rest[0] != '\t' && rest[0] != '\r' && rest[0] != '\n' {
			continue
		}
		start := strings.Index(rest, ">")
		if start < 0 {
			break
		}
		rest = rest[start+1:]
		end := strings.Index(rest, "</segment>")
		if end < 0 {
			break
		}
		msgID := strings.TrimSpace(rest[:end])
		if msgID != "" {
			ids = append(ids, msgID)
		}
		rest = rest[end:]
	}
	return ids
}

// extractMessageIDsBySubjectContains collects the article ids of every NZB file
// whose subject matches one of the needles.
//
// `tailArticles` bounds it to the LAST n articles of each matching file, which
// is the difference between two very different kinds of damage. Deleting a
// whole volume makes it *missing*, and a missing volume is a different product
// path from a volume with *holes* in it. Deleting from the tail — never the
// head — is what keeps the hole in payload bytes: every RAR volume carries its
// own signature and headers in its first article, and a volume whose header is
// gone cannot be mapped at all. Zero or negative means every article, the
// original whole-file behaviour.
func extractMessageIDsBySubjectContains(nzbData []byte, needles []string, tailArticles int) ([]string, error) {
	type nzbSegment struct {
		MessageID string `xml:",chardata"`
	}
	type nzbFile struct {
		Subject  string       `xml:"subject,attr"`
		Segments []nzbSegment `xml:"segments>segment"`
	}
	type nzbDoc struct {
		Files []nzbFile `xml:"file"`
	}

	var doc nzbDoc
	if err := xml.Unmarshal(nzbData, &doc); err != nil {
		return nil, err
	}

	lowerNeedles := make([]string, 0, len(needles))
	for _, needle := range needles {
		needle = strings.ToLower(strings.TrimSpace(needle))
		if needle != "" {
			lowerNeedles = append(lowerNeedles, needle)
		}
	}
	if len(lowerNeedles) == 0 {
		return nil, nil
	}

	var ids []string
	for _, file := range doc.Files {
		subject := strings.ToLower(file.Subject)
		matched := false
		for _, needle := range lowerNeedles {
			if strings.Contains(subject, needle) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		segments := file.Segments
		if tailArticles > 0 && len(segments) > tailArticles {
			segments = segments[len(segments)-tailArticles:]
		}
		for _, segment := range segments {
			msgID := strings.TrimSpace(segment.MessageID)
			msgID = strings.TrimPrefix(msgID, "<")
			msgID = strings.TrimSuffix(msgID, ">")
			if msgID != "" {
				ids = append(ids, msgID)
			}
		}
	}

	return ids, nil
}

// extractMessageIDsBySegmentNumbers selects articles by their position within
// a file, which is what an interior-hole scenario needs: deleteSubjectContains
// picks whole files and deleteSubjectTailArticles only reaches the tail, so
// neither can name a segment in the middle. The subject needles stay optional
// and narrow which files are considered; with none, every file is.
func extractMessageIDsBySegmentNumbers(nzbData []byte, needles []string, numbers []int) ([]string, error) {
	type nzbSegment struct {
		Number    int    `xml:"number,attr"`
		MessageID string `xml:",chardata"`
	}
	type nzbFile struct {
		Subject  string       `xml:"subject,attr"`
		Segments []nzbSegment `xml:"segments>segment"`
	}
	type nzbDoc struct {
		Files []nzbFile `xml:"file"`
	}

	var doc nzbDoc
	if err := xml.Unmarshal(nzbData, &doc); err != nil {
		return nil, err
	}

	wanted := make(map[int]struct{}, len(numbers))
	for _, number := range numbers {
		wanted[number] = struct{}{}
	}
	if len(wanted) == 0 {
		return nil, nil
	}

	lowerNeedles := make([]string, 0, len(needles))
	for _, needle := range needles {
		if needle = strings.ToLower(strings.TrimSpace(needle)); needle != "" {
			lowerNeedles = append(lowerNeedles, needle)
		}
	}

	var ids []string
	for _, file := range doc.Files {
		if len(lowerNeedles) > 0 {
			subject := strings.ToLower(file.Subject)
			matched := false
			for _, needle := range lowerNeedles {
				if strings.Contains(subject, needle) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		for _, segment := range file.Segments {
			if _, ok := wanted[segment.Number]; !ok {
				continue
			}
			msgID := strings.TrimSpace(segment.MessageID)
			msgID = strings.TrimPrefix(msgID, "<")
			msgID = strings.TrimSuffix(msgID, ">")
			if msgID != "" {
				ids = append(ids, msgID)
			}
		}
	}

	return ids, nil
}

func extractAllMessageIDsFromNZB(nzbData []byte) ([]string, error) {
	type nzbSegment struct {
		MessageID string `xml:",chardata"`
	}
	type nzbFile struct {
		Segments []nzbSegment `xml:"segments>segment"`
	}
	type nzbDoc struct {
		Files []nzbFile `xml:"file"`
	}

	var doc nzbDoc
	if err := xml.Unmarshal(nzbData, &doc); err != nil {
		return nil, err
	}

	var ids []string
	for _, file := range doc.Files {
		for _, segment := range file.Segments {
			msgID := strings.TrimSpace(segment.MessageID)
			msgID = strings.TrimPrefix(msgID, "<")
			msgID = strings.TrimSuffix(msgID, ">")
			if msgID != "" {
				ids = append(ids, msgID)
			}
		}
	}
	return ids, nil
}

func rewriteNZBSegmentNumbers(nzbData []byte, numbers []int) ([]byte, error) {
	if len(numbers) == 0 {
		return nzbData, nil
	}

	matches := nzbSegmentNumberPattern.FindAllSubmatchIndex(nzbData, -1)
	if len(matches) != len(numbers) {
		return nil, fmt.Errorf(
			"segment number override count mismatch: got %d override(s) for %d segment(s)",
			len(numbers),
			len(matches),
		)
	}

	out := make([]byte, 0, len(nzbData)+len(numbers)*2)
	last := 0
	for index, match := range matches {
		number := numbers[index]
		if number <= 0 {
			return nil, fmt.Errorf("segment numbers must be positive, got %d at index %d", number, index)
		}
		out = append(out, nzbData[last:match[3]]...)
		out = strconv.AppendInt(out, int64(number), 10)
		out = append(out, nzbData[match[4]:match[5]]...)
		last = match[1]
	}
	out = append(out, nzbData[last:]...)
	return out, nil
}

func scenarioNZBSegmentNumbers(nzbData []byte, scenario *Scenario) ([]int, error) {
	if scenario == nil {
		return nil, nil
	}
	if len(scenario.NZBSegmentNumbers) > 0 {
		if scenario.NZBSegmentNumberStart != 0 || scenario.NZBSegmentNumberStep != 0 {
			return nil, fmt.Errorf("cannot combine explicit nzb_segment_numbers with nzb_segment_number_start/step")
		}
		return scenario.NZBSegmentNumbers, nil
	}
	if scenario.NZBSegmentNumberStart == 0 && scenario.NZBSegmentNumberStep == 0 {
		return nil, nil
	}
	if scenario.NZBSegmentNumberStep <= 0 {
		return nil, fmt.Errorf("nzb_segment_number_step must be positive")
	}

	segmentCount := len(nzbSegmentNumberPattern.FindAllSubmatchIndex(nzbData, -1))
	if segmentCount == 0 {
		return nil, fmt.Errorf("generated NZB contained no segments")
	}

	start := scenario.NZBSegmentNumberStart
	if start <= 0 {
		start = 1
	}

	numbers := make([]int, segmentCount)
	current := start
	for index := range numbers {
		numbers[index] = current
		current += scenario.NZBSegmentNumberStep
	}
	return numbers, nil
}

func healthProbeSampleIndices(totalSegs, probeRound int) []int {
	if totalSegs == 0 {
		return nil
	}

	probeCount := totalSegs * 8 / 100
	if probeCount < 10 {
		probeCount = 10
	}
	if probeCount > totalSegs {
		probeCount = totalSegs
	}

	stride := totalSegs / probeCount
	if stride < 1 {
		stride = 1
	}

	offset := 0
	if stride > 1 {
		offset = probeRound % stride
	}

	indices := make([]int, 0, probeCount)
	for i := offset; i < totalSegs; i += stride {
		indices = append(indices, i)
	}
	return indices
}

func extractFirstProbeSampleMessageIDs(nzbData []byte, count int) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}

	ids, err := extractAllMessageIDsFromNZB(nzbData)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	probeIndices := healthProbeSampleIndices(len(ids), 0)
	if len(probeIndices) == 0 {
		return nil, nil
	}
	if count > len(probeIndices) {
		count = len(probeIndices)
	}

	selected := make([]string, 0, count)
	for _, idx := range probeIndices[:count] {
		selected = append(selected, ids[idx])
	}
	return selected, nil
}

func extractFirstMessageIDs(nzbData []byte, count int) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}

	ids, err := extractAllMessageIDsFromNZB(nzbData)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	if count > len(ids) {
		count = len(ids)
	}

	return append([]string(nil), ids[:count]...), nil
}

func deleteArticlesByMessageID(messageIDs []string) error {
	return deleteArticlesByMessageIDOnServers(messageIDs, true, true)
}

func deleteArticlesByMessageIDOnServers(messageIDs []string, includePrimary, includeBackup bool) error {
	if includePrimary {
		if err := deleteArticleIDsAt(nntpHost(), nntpPort(), messageIDs); err != nil {
			return err
		}
	}
	if includeBackup && backupNntpRunning() {
		if err := deleteArticleIDsAt(nntpHost(), backupNntpPort(), messageIDs); err != nil {
			return err
		}
	}
	return nil
}

func deleteArticleIDsAt(host, port string, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	greeting, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read greeting: %w", err)
	}
	if !strings.HasPrefix(greeting, "200") {
		return fmt.Errorf("unexpected greeting: %s", strings.TrimSpace(greeting))
	}
	if err := authenticateNNTPConnection(conn, r, addr); err != nil {
		return err
	}

	for _, messageID := range messageIDs {
		cmd := fmt.Sprintf("DELETEID <%s>\r\n", messageID)
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte(cmd)); err != nil {
			return fmt.Errorf("write DELETEID for <%s>: %w", messageID, err)
		}

		conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		resp, err := r.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read DELETEID response for <%s>: %w", messageID, err)
		}
		resp = strings.TrimSpace(resp)
		if !strings.HasPrefix(resp, "290") {
			return fmt.Errorf("DELETEID failed for <%s>: %s", messageID, resp)
		}
	}

	_, _ = conn.Write([]byte("QUIT\r\n"))
	return nil
}

// --- submit ---

func cmdSubmit(slug string) {
	// Find the NZB
	nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
	nzbData, err := os.ReadFile(nzbPath)
	if err != nil {
		log.Fatalf("read NZB %s: %v", nzbPath, err)
	}

	// Load scenario for metadata
	scenarioPath := filepath.Join(testdataDir(), slug, "scenario.json")
	scenario, err := loadScenario(filepath.Dir(scenarioPath))
	if err != nil {
		log.Fatalf("load scenario: %v", err)
	}

	weaverURL := defaultWeaverURL()
	prepareStandardTestRun(weaverURL, false)

	// Base64-encode the NZB
	nzbB64 := base64.StdEncoding.EncodeToString(nzbData)

	// Build GraphQL mutation
	query := `mutation($input: SubmitNzbInput!) {
		submitNzb(input: $input) {
			accepted
			item {
				id
				name
				state
			}
		}
	}`

	input := map[string]interface{}{
		"nzbBase64": nzbB64,
		"filename":  scenario.Title + ".nzb",
		"category":  scenario.Category,
	}
	if scenario.Password != "" {
		input["password"] = scenario.Password
	}

	payload := map[string]interface{}{
		"query":     query,
		"variables": map[string]interface{}{"input": input},
	}

	body, _ := json.Marshal(payload)
	log.Printf("submitting NZB to weaver (%s, %d bytes)...", slug, len(nzbData))

	resp, err := postGraphQL(weaverURL, body)
	if err != nil {
		log.Fatalf("submit to weaver: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	log.Printf("weaver response (%d): %s", resp.StatusCode, string(respBody))

	// Parse response to get job ID
	var gqlResp struct {
		Data struct {
			SubmitNzb struct {
				Accepted bool `json:"accepted"`
				Item     struct {
					ID    int    `json:"id"`
					Name  string `json:"name"`
					State string `json:"state"`
				} `json:"item"`
			} `json:"submitNzb"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(respBody, &gqlResp); err != nil {
		log.Fatalf("parse response: %v", err)
	}
	if len(gqlResp.Errors) > 0 {
		log.Fatalf("GraphQL error: %s", gqlResp.Errors[0].Message)
	}

	jobID := gqlResp.Data.SubmitNzb.Item.ID
	log.Printf("job created: id=%d name=%s state=%s accepted=%t", jobID, gqlResp.Data.SubmitNzb.Item.Name, gqlResp.Data.SubmitNzb.Item.State, gqlResp.Data.SubmitNzb.Accepted)

	// Poll job status until terminal
	log.Printf("polling job %d...", jobID)
	for {
		mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("job %d polling", jobID))
		job, err := fetchFacadeItemSnapshot(weaverURL, jobID)
		if err != nil {
			log.Printf("  poll error: %v", err)
			continue
		}
		if !job.Found {
			log.Printf("  poll warning: item %d not found yet", jobID)
			continue
		}

		log.Printf("  status=%s progress=%.1f%% health=%.1f%%",
			job.Status, job.ProgressPercent, float64(job.Health)/10)

		switch job.Status {
		case "COMPLETE":
			log.Printf("job %d completed successfully!", jobID)
			return
		case "FAILED":
			errMsg := job.Error
			if errMsg == "" {
				errMsg = "unknown"
			}
			log.Printf("job %d FAILED: %s", jobID, errMsg)
			if scenario.ExpectedOutcome == "health_failure" {
				log.Printf("(failure was expected for this scenario)")
				return
			}
			os.Exit(1)
		}
	}
}

// --- test-all ---

type testJob struct {
	slug                               string
	scenario                           *Scenario
	jobID                              int
	status                             string // terminal status or "submit_error"/"skip"
	errMsg                             string
	fileIdentityRewriteObserved        bool
	fileIdentityRewriteLastObservation fileIdentityRewriteObservation
	fileIdentityRewriteLastQueryError  string
}

type chaosRoundJobArtifact struct {
	Slug            string  `json:"slug"`
	JobID           int     `json:"job_id"`
	Status          string  `json:"status"`
	Found           bool    `json:"found,omitempty"`
	Error           string  `json:"error,omitempty"`
	ProgressPercent float64 `json:"progress_percent,omitempty"`
	Health          int     `json:"health,omitempty"`
}

var functionalRewritePollInterval = 250 * time.Millisecond
var functionalNormalPollInterval = 2 * time.Second

const (
	functionalFastStatusPollBatchSize = 16
	functionalRegularBatchSize        = 8
)

func countResolvedTestJobs(jobs []testJob) int {
	resolved := 0
	for _, job := range jobs {
		if job.status != "" && job.status != "queued_exclusive" && job.status != "queued_regular" {
			resolved++
		}
	}
	return resolved
}

func observeRuntimeFileIdentityRewrite(job *testJob, dbPath string, queryContext string) {
	if !testJobNeedsRuntimeFileIdentityRewrite(job) {
		return
	}
	observer, err := openFileIdentityRewriteObserver(dbPath)
	if err != nil {
		job.fileIdentityRewriteLastQueryError = err.Error()
		log.Printf("  %s: runtime file-identity rewrite query error%s: %v", job.slug, queryContext, err)
		return
	}
	defer observer.Close()

	observeRuntimeFileIdentityRewriteWithObserver(job, observer, queryContext)
}

func observeRuntimeFileIdentityRewriteWithObserver(
	job *testJob,
	observer *fileIdentityRewriteObserver,
	queryContext string,
) {
	assertion := job.scenario.fileIdentityRewriteAssertion()
	if assertion == nil {
		return
	}
	observation, observeErr := observer.Observe(job.jobID, assertion)
	if observeErr != nil {
		job.fileIdentityRewriteLastQueryError = observeErr.Error()
		log.Printf("  %s: runtime file-identity rewrite query error%s: %v", job.slug, queryContext, observeErr)
	} else {
		job.fileIdentityRewriteLastObservation = observation
		job.fileIdentityRewriteLastQueryError = ""
		job.fileIdentityRewriteObserved = observation.Observed
		if observation.Observed {
			log.Printf("  %s: observed runtime file-identity rewrite", job.slug)
		}
	}
}

func testJobNeedsRuntimeFileIdentityRewrite(job *testJob) bool {
	if job == nil || job.status != "" || job.fileIdentityRewriteObserved {
		return false
	}
	return job.scenario != nil && job.scenario.fileIdentityRewriteAssertion() != nil
}

func functionalHasPendingFileIdentityRewrite(jobs []testJob) bool {
	for i := range jobs {
		if testJobNeedsRuntimeFileIdentityRewrite(&jobs[i]) {
			return true
		}
	}
	return false
}

func functionalRegularBatches(jobs []testJob, batchSize int) [][]int {
	if batchSize <= 0 {
		return nil
	}

	regular := make([]int, 0, len(jobs))
	for i := range jobs {
		if jobs[i].status == "queued_regular" {
			regular = append(regular, i)
		}
	}

	batches := make([][]int, 0, (len(regular)+batchSize-1)/batchSize)
	for start := 0; start < len(regular); start += batchSize {
		end := min(start+batchSize, len(regular))
		batches = append(batches, regular[start:end])
	}
	return batches
}

func functionalStatusPollIndexes(jobs []testJob, fastRewritePolling bool, cursor *int) []int {
	pending := make([]int, 0, len(jobs))
	for i := range jobs {
		if jobs[i].status == "" {
			pending = append(pending, i)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if !fastRewritePolling || len(pending) <= functionalFastStatusPollBatchSize {
		if cursor != nil {
			*cursor = 0
		}
		return pending
	}

	start := 0
	if cursor != nil && *cursor > 0 {
		start = *cursor % len(pending)
	}
	count := functionalFastStatusPollBatchSize
	indexes := make([]int, 0, count)
	for offset := 0; offset < count; offset++ {
		indexes = append(indexes, pending[(start+offset)%len(pending)])
	}
	if cursor != nil {
		*cursor = (start + count) % len(pending)
	}
	return indexes
}

func finalizeTestJobFromSnapshot(job *testJob, dbPath string, snapshot facadeItemSnapshot, queryContext string) {
	assertion := job.scenario.fileIdentityRewriteAssertion()
	observeRuntimeFileIdentityRewrite(job, dbPath, queryContext)

	if overrideStatus, overrideErrMsg, overridden := applyRuntimeFileIdentityRewriteTerminalCheck(
		snapshot.Status,
		assertion,
		job.fileIdentityRewriteObserved,
		job.fileIdentityRewriteLastObservation,
		job.fileIdentityRewriteLastQueryError,
	); overridden {
		job.status = overrideStatus
		job.errMsg = overrideErrMsg
	} else {
		job.status, job.errMsg = applyTerminalStateCheck(dbPath, job.jobID, job.slug, snapshot.Status)
		if job.errMsg == "" {
			job.errMsg = snapshot.Error
		}
	}
}

func waitForActiveFileComplete(dbPath string, jobID int, filename string, timeout time.Duration) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	query := rebindWeaverSQL(
		datastore,
		`SELECT COUNT(*) FROM active_files WHERE job_id = ? AND filename = ?`,
	)
	deadline := time.Now().Add(timeout)
	for {
		var count int
		err := db.QueryRow(query, jobID, filename).Scan(&count)
		if err == nil && count > 0 {
			return nil
		}
		if err != nil && !isTransientSQLiteBusy(err) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("job %d did not complete file %q within %s", jobID, filename, timeout)
		}
		mustSleepWithSuspendDetection(50*time.Millisecond, fmt.Sprintf("job %d file-completion gate", jobID))
	}
}

func runExclusiveFunctionalJob(weaverURL, dbPath string, job *testJob) {
	config := strings.TrimSpace(job.scenario.PrimaryChaosConfig)
	backupGateFilename := strings.TrimSpace(job.scenario.BackupUnavailableUntilFileComplete)
	var releaseBackupGate func() error
	if err := ensureNntpChaosOff(); err != nil {
		job.status = "setup_error"
		job.errMsg = err.Error()
		return
	}
	defer func() {
		if releaseBackupGate != nil {
			if err := releaseBackupGate(); err != nil {
				log.Printf("warning: release backup gate after exclusive scenario %s: %v", job.slug, err)
			}
		}
		if err := ensureNntpChaosOff(); err != nil {
			log.Printf("warning: reset NNTP chaos after exclusive scenario %s: %v", job.slug, err)
		}
	}()

	if config != "" {
		if err := setNntpChaosOnServer(nntpHost(), nntpPort(), config); err != nil {
			job.status = "setup_error"
			job.errMsg = err.Error()
			return
		}
		log.Printf("  %s: primary NNTP chaos enabled: %s", job.slug, config)
	}
	if backupGateFilename != "" {
		if !backupNntpRunning() {
			job.status = "setup_error"
			job.errMsg = "backup-unavailable gate requires the backup NNTP server"
			return
		}
		if len(job.scenario.PrimaryDeleteSubjectContains) == 0 {
			job.status = "setup_error"
			job.errMsg = "backup-unavailable gate requires primaryDeleteSubjectContains"
			return
		}
		// Refuse new sessions and make any pooled session re-authenticate on
		// BODY, so earlier scenarios cannot leave a connection around the gate.
		release, err := holdNntpChaosOnServer(
			nntpHost(),
			backupNntpPort(),
			"greet_400=100,reauth_body=100",
		)
		if err != nil {
			job.status = "setup_error"
			job.errMsg = err.Error()
			return
		}
		releaseBackupGate = release
		log.Printf("  %s: backup NNTP unavailable until %s completes", job.slug, backupGateFilename)
	}

	jobID, err := submitOneNZB(weaverURL, job.scenario)
	if err != nil {
		job.status = "submit_error"
		job.errMsg = err.Error()
		return
	}
	job.jobID = jobID
	log.Printf("  %s: submitted exclusive job=%d", job.slug, jobID)
	if backupGateFilename != "" {
		if err := waitForActiveFileComplete(dbPath, jobID, backupGateFilename, 60*time.Second); err != nil {
			job.status = "setup_error"
			job.errMsg = err.Error()
			return
		}
		release := releaseBackupGate
		releaseBackupGate = nil
		if err := release(); err != nil {
			job.status = "setup_error"
			job.errMsg = err.Error()
			return
		}
		log.Printf("  %s: backup NNTP released after %s completed", job.slug, backupGateFilename)
	}

	deadline := time.Now().Add(180 * time.Second)
	firstPoll := true
	for time.Now().Before(deadline) {
		if !firstPoll {
			mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("exclusive test %s polling", job.slug))
		}
		firstPoll = false

		snapshot, err := fetchFacadeItemSnapshot(weaverURL, job.jobID)
		if err != nil || !snapshot.Found || !facadeTerminalStatus(snapshot.Status) {
			continue
		}
		finalizeTestJobFromSnapshot(job, dbPath, snapshot, " during exclusive polling")
		log.Printf(
			"  %s: %s (health=%.1f%% err=%s)",
			job.slug,
			job.status,
			float64(snapshot.Health)/10,
			job.errMsg,
		)
		return
	}

	reconciled := reconcileTerminalSnapshots(
		weaverURL,
		[]int{job.jobID},
		15*time.Second,
		fmt.Sprintf("exclusive test %s final reconciliation", job.slug),
	)
	if snapshot, ok := reconciled[job.jobID]; ok {
		finalizeTestJobFromSnapshot(job, dbPath, snapshot, " during exclusive reconciliation")
		log.Printf(
			"  %s: %s after reconciliation (health=%.1f%% err=%s)",
			job.slug,
			job.status,
			float64(snapshot.Health)/10,
			job.errMsg,
		)
		return
	}

	job.status = "timeout"
	job.errMsg = "exclusive scenario timed out after 180s"
	log.Printf("  %s: TIMEOUT after 180s", job.slug)
}

func cmdTest(targets []string) {
	// Validate all targets have NZBs
	var slugs []string
	for _, slug := range targets {
		nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
		if _, err := os.Stat(nzbPath); err != nil {
			log.Fatalf("no NZB for %q — run '%s seed %s' first", slug, cliProgramName, filepath.Join(testdataDir(), slug))
		}
		slugs = append(slugs, slug)
	}
	runTests(slugs)
}

func cmdTestAll() {
	slugs := append([]string(nil), canonicalFixtureSlugs...)

	if len(slugs) == 0 {
		log.Fatal("no canonical fixtures configured")
	}
	runTests(slugs)
}

func runTests(slugs []string) {
	ensureStandardDockerInfrastructure()
	weaverURL := defaultWeaverURL()
	prepareStandardTestRun(weaverURL, true)
	if err := installFileIdentityRewriteObserver(localWeaverDBPath()); err != nil {
		log.Fatalf("install file identity rewrite observer: %v", err)
	}
	emitProgressEvent(progressEvent{Kind: "phase_total", Total: len(slugs), Detail: "functional fixtures"})

	log.Printf("submitting %d fixtures to weaver at %s...", len(slugs), weaverURL)

	jobs := make([]testJob, 0, len(slugs))
	for i, slug := range slugs {
		scenarioDir := filepath.Join(testdataDir(), slug)
		scenario, err := loadScenario(scenarioDir)
		if err != nil {
			scenario = &Scenario{Slug: slug, ExpectedOutcome: "success"}
		}

		job := testJob{slug: slug, scenario: scenario}
		if scenarioUsesExclusiveNntpState(scenario) {
			job.status = "queued_exclusive"
			log.Printf("[%d/%d] %s — queued exclusive NNTP scenario", i+1, len(slugs), slug)
			jobs = append(jobs, job)
			continue
		}

		job.status = "queued_regular"
		log.Printf("[%d/%d] %s — queued regular scenario", i+1, len(slugs), slug)
		jobs = append(jobs, job)
	}

	dbPath := localWeaverDBPath()
	regularBatches := functionalRegularBatches(jobs, functionalRegularBatchSize)
	weaverDiedMidRun := false
	for batchNumber, batchIndexes := range regularBatches {
		log.Printf(
			"submitting regular functional batch %d/%d (%d jobs)",
			batchNumber+1,
			len(regularBatches),
			len(batchIndexes),
		)
		for _, i := range batchIndexes {
			jobs[i].status = ""
			jobID, err := submitOneNZB(weaverURL, jobs[i].scenario)
			if err != nil {
				log.Printf("[%d/%d] %s — submit error: %v", i+1, len(jobs), jobs[i].slug, err)
				jobs[i].status = "submit_error"
				jobs[i].errMsg = err.Error()
				emitProgressEvent(progressEvent{
					Kind:    "phase_progress",
					Current: countResolvedTestJobs(jobs),
					Total:   len(jobs),
					Status:  "submit_error",
					Detail:  jobs[i].slug,
				})
				if died, waitErr := managedWeaverDied(); died {
					weaverDiedMidRun = true
					log.Printf("FATAL: managed weaver exited during batch submission (%v)", waitErr)
					log.Printf("weaver died here:\n%s", managedWeaverDeathReport())
					break
				}
				continue
			}

			jobs[i].jobID = jobID
			log.Printf("[%d/%d] %s — submitted job=%d", i+1, len(jobs), jobs[i].slug, jobID)
		}

		pending := 0
		queuedExclusive := 0
		for _, j := range jobs {
			if j.status == "" {
				pending++
			}
			if j.status == "queued_exclusive" {
				queuedExclusive++
			}
		}
		log.Printf(
			"regular batch %d/%d submissions complete (%d pending, %d queued exclusive, %d already resolved)",
			batchNumber+1,
			len(regularBatches),
			pending,
			queuedExclusive,
			countResolvedTestJobs(jobs),
		)

		completionTimeout := functionalCompletionTimeout()
		deadline := time.Now().Add(completionTimeout)
		log.Printf("waiting up to %s for regular functional jobs", completionTimeout)
		firstPoll := true
		statusPollCursor := 0
		for pending > 0 && time.Now().Before(deadline) {
			// Fail fast on a dead server. Polling on would score every remaining
			// job as `timeout` — a label that describes the corpse, not the cause —
			// and burn the whole completion budget doing it.
			if died, waitErr := managedWeaverDied(); died {
				weaverDiedMidRun = true
				log.Printf(
					"FATAL: managed weaver exited mid-run (%v) with %d job(s) still pending; "+
						"abandoning the wait — these are not timeouts, there is no server to answer them",
					waitErr, pending,
				)
				log.Printf("weaver died here:\n%s", managedWeaverDeathReport())
				break
			}
			if !firstPoll {
				mustSleepWithSuspendDetection(functionalPollInterval(jobs), "functional test polling")
			}
			firstPoll = false

			fastRewritePolling := functionalHasPendingFileIdentityRewrite(jobs)
			if fastRewritePolling {
				observer, err := openFileIdentityRewriteObserver(dbPath)
				if err != nil {
					for i := range jobs {
						if testJobNeedsRuntimeFileIdentityRewrite(&jobs[i]) {
							jobs[i].fileIdentityRewriteLastQueryError = err.Error()
						}
					}
					log.Printf("  runtime file-identity rewrite query error: %v", err)
				} else {
					for i := range jobs {
						if testJobNeedsRuntimeFileIdentityRewrite(&jobs[i]) {
							observeRuntimeFileIdentityRewriteWithObserver(&jobs[i], observer, "")
						}
					}
					_ = observer.Close()
				}
			}

			for _, i := range functionalStatusPollIndexes(jobs, fastRewritePolling, &statusPollCursor) {
				snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobs[i].jobID)
				if err != nil {
					continue
				}
				if !snapshot.Found {
					continue
				}
				s := snapshot.Status

				if s == "COMPLETE" || s == "FAILED" {
					finalizeTestJobFromSnapshot(&jobs[i], dbPath, snapshot, "")
					pending--
					resolved := countResolvedTestJobs(jobs)
					log.Printf("  %s: %s (health=%.1f%% err=%s) [%d pending]",
						jobs[i].slug, jobs[i].status, float64(snapshot.Health)/10, jobs[i].errMsg, pending)
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: resolved,
						Total:   len(jobs),
						Status:  strings.ToLower(jobs[i].status),
						Detail:  jobs[i].slug,
					})
				}
			}
		}

		// Mark remaining as timeout
		remainingIDs := make([]int, 0, pending)
		for _, job := range jobs {
			if job.status == "" && job.jobID > 0 {
				remainingIDs = append(remainingIDs, job.jobID)
			}
		}
		if weaverDiedMidRun && len(remainingIDs) > 0 {
			log.Printf(
				"skipping final reconciliation for %d job(s): weaver is gone, so the snapshot query can only re-learn that",
				len(remainingIDs),
			)
		} else if len(remainingIDs) > 0 {
			log.Printf("reconciling %d unresolved functional job(s) before timeout scoring", len(remainingIDs))
			reconciled := reconcileTerminalSnapshots(weaverURL, remainingIDs, 15*time.Second, "functional test final reconciliation")
			for i := range jobs {
				if jobs[i].status != "" {
					continue
				}
				snapshot, ok := reconciled[jobs[i].jobID]
				if !ok {
					continue
				}
				finalizeTestJobFromSnapshot(&jobs[i], dbPath, snapshot, " during reconciliation")
				pending--
				resolved := countResolvedTestJobs(jobs)
				log.Printf("  %s: %s after reconciliation (health=%.1f%% err=%s) [%d pending]",
					jobs[i].slug, jobs[i].status, float64(snapshot.Health)/10, jobs[i].errMsg, pending)
				emitProgressEvent(progressEvent{
					Kind:    "phase_progress",
					Current: resolved,
					Total:   len(jobs),
					Status:  strings.ToLower(jobs[i].status),
					Detail:  jobs[i].slug,
				})
			}
		}

		timedOutRegularJobs := 0
		for i := range jobs {
			if jobs[i].status == "" {
				jobs[i].status = "timeout"
				timedOutRegularJobs++
				pending--
				if weaverDiedMidRun {
					// Keep the status string — scoring and reconciliation elsewhere
					// key off "timeout" — but do not let the report imply the
					// scenario was given its full budget and failed to finish.
					jobs[i].errMsg = "weaver exited mid-run; scenario never had a server to complete against"
					log.Printf("  %s: NOT RUN (weaver exited mid-run)", jobs[i].slug)
				} else {
					log.Printf("  %s: TIMEOUT after %s", jobs[i].slug, completionTimeout)
				}
				emitProgressEvent(progressEvent{
					Kind:    "phase_progress",
					Current: countResolvedTestJobs(jobs),
					Total:   len(jobs),
					Status:  "timeout",
					Detail:  jobs[i].slug,
				})
			}
		}
		if weaverDiedMidRun && timedOutRegularJobs > 0 {
			// This was once the dominant cost of a crashed run: each job's
			// cancel-settle spends 15s waiting for a reply that cannot come, so
			// dozens of unrun scenarios turned into a ~17-minute tail. There is
			// nothing to cancel on a process that has exited.
			log.Printf(
				"skipping cancel/settle for %d job(s): weaver is not running, so there is nothing to cancel",
				timedOutRegularJobs,
			)
		} else if timedOutRegularJobs > 0 {
			log.Printf("canceling %d timed out regular job(s) before the next batch", timedOutRegularJobs)
			for _, i := range batchIndexes {
				j := &jobs[i]
				if j.status != "timeout" || j.jobID == 0 {
					continue
				}
				if err := cancelJobGraphQL(weaverURL, j.jobID); err != nil {
					log.Printf("  WARNING: cancel timed out job %s (%d): %v", j.slug, j.jobID, err)
				}
				if err := waitForJobCancelSettledGraphQL(
					weaverURL,
					j.jobID,
					weaverCancelSettleTimeout,
					weaverCancelSettlePollInterval,
				); err != nil {
					log.Printf("  queue snapshot after failed cancel settle: %s", describeJobsGraphQL(weaverURL))
					log.Printf("  WARNING: timed out job %s (%d) did not settle before exclusive scenarios: %v", j.slug, j.jobID, err)
				}
			}
		}
		if weaverDiedMidRun {
			break
		}
	}

	if weaverDiedMidRun {
		for i := range jobs {
			if jobs[i].status != "queued_regular" {
				continue
			}
			jobs[i].status = "timeout"
			jobs[i].errMsg = "weaver exited before this batch was submitted"
			log.Printf("  %s: NOT RUN (weaver exited before submission)", jobs[i].slug)
			emitProgressEvent(progressEvent{
				Kind:    "phase_progress",
				Current: countResolvedTestJobs(jobs),
				Total:   len(jobs),
				Status:  "timeout",
				Detail:  jobs[i].slug,
			})
		}
	}

	for i := range jobs {
		if jobs[i].status != "queued_exclusive" {
			continue
		}
		if weaverDiedMidRun {
			jobs[i].status = "timeout"
			jobs[i].errMsg = "weaver exited before the exclusive scenario ran"
			log.Printf("  %s: NOT RUN (weaver exited before exclusive scenario)", jobs[i].slug)
			emitProgressEvent(progressEvent{
				Kind:    "phase_progress",
				Current: countResolvedTestJobs(jobs),
				Total:   len(jobs),
				Status:  "timeout",
				Detail:  jobs[i].slug,
			})
			continue
		}
		log.Printf("running exclusive NNTP scenario %s...", jobs[i].slug)
		runExclusiveFunctionalJob(weaverURL, dbPath, &jobs[i])
		emitProgressEvent(progressEvent{
			Kind:    "phase_progress",
			Current: countResolvedTestJobs(jobs),
			Total:   len(jobs),
			Status:  strings.ToLower(jobs[i].status),
			Detail:  jobs[i].slug,
		})
	}

	// Summary
	fmt.Println()
	fmt.Printf("%-25s %-22s %-12s %s\n", "FIXTURE", "EXPECTED", "ACTUAL", "RESULT")
	fmt.Println(strings.Repeat("-", 70))
	passCount, failCount := 0, 0
	for _, j := range jobs {
		passed := false
		switch j.scenario.ExpectedOutcome {
		case "success", "repair_then_success":
			passed = j.status == "COMPLETE"
		case "health_failure", "encryption_unsupported", "repair_failure", "extraction_failure":
			passed = j.status == "FAILED"
		case "nested_depth_exceeded":
			// Completes but output is still an archive (not the final media)
			passed = j.status == "COMPLETE"
		default:
			passed = j.status == "COMPLETE"
		}

		label := "PASS"
		if !passed {
			label = "FAIL"
			failCount++
		} else {
			passCount++
		}
		fmt.Printf("%-25s %-22s %-12s %s\n", j.slug, j.scenario.ExpectedOutcome, j.status, label)
	}
	fmt.Println(strings.Repeat("-", 70))
	fmt.Printf("Total: %d passed, %d failed out of %d\n", passCount, failCount, len(jobs))

	if err := assertDirectStoreEngagement(weaverURL); err != nil {
		fmt.Printf("DIRECT-STORE ASSERTION FAILED: %v\n", err)
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(jobs), Total: len(jobs), Status: "fail"})
		os.Exit(1)
	}

	if failCount > 0 {
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(jobs), Total: len(jobs), Status: "fail"})
		os.Exit(1)
	}

	emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(jobs), Total: len(jobs), Status: "pass"})
	stopManagedWeaverAfterProfileCollection()
}

// directStoreCounters is weaver's lifetime view of direct-store routing.
type directStoreCounters struct {
	Admitted            int64 `json:"directSetsAdmitted"`
	Demoted             int64 `json:"directSetsDemoted"`
	FinalizedDirect     int64 `json:"directSetsFinalizedDirect"`
	RepairedWhileDirect int64 `json:"directSetsRepairedWhileDirect"`
}

func fetchDirectStoreCounters(weaverURL string) (directStoreCounters, error) {
	var counters directStoreCounters
	payload, _ := json.Marshal(map[string]interface{}{
		"query": `query {
			metrics {
				directSetsAdmitted
				directSetsDemoted
				directSetsFinalizedDirect
				directSetsRepairedWhileDirect
			}
		}`,
	})
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := postGraphQLWithClient(client, weaverURL, payload)
	if err != nil {
		return counters, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return counters, fmt.Errorf("metrics query returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var gqlResp struct {
		Data struct {
			Metrics directStoreCounters `json:"metrics"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return counters, fmt.Errorf("decode metrics response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return counters, fmt.Errorf("metrics query error: %s", gqlResp.Errors[0].Message)
	}
	return gqlResp.Data.Metrics, nil
}

// assertDirectStoreEngagement checks that direct-store routing actually carried
// the corpus when the phase asked for it.
//
// This is the assertion the scenario files cannot express. Direct-store emits
// the same `SegmentCommitted`/`FileComplete` events as the conventional path
// and produces byte-identical output, so a run that silently demoted every set
// — or never engaged the gate at all — passes every `expectedOutputBLAKE3` and
// every event assertion in the corpus. Until these counters existed, a green
// `functional-direct` phase meant "no wrong bytes", not "direct-store worked".
//
// Deliberately not asserted: `Demoted == 0`. The functional corpus contains
// archives direct-store is *right* to refuse — compressed and solid members,
// for two — so a zero demotion count would be a bug, not a success. What must
// hold is that the gate engaged and that at least one set rode it all the way
// to completion without ever writing a source volume, which is the only
// externally observable proof the feature did its job.
//
// A no-op when the phase did not enable direct-store: the conventional phases
// legitimately report zeroes, and asserting there would fail them all.
func assertDirectStoreEngagement(weaverURL string) error {
	if !directStoreEnabledForPhase() {
		return nil
	}

	counters, err := fetchDirectStoreCounters(weaverURL)
	if err != nil {
		return fmt.Errorf("could not read direct-store counters: %w", err)
	}

	fmt.Printf(
		"direct-store: admitted=%d finalized_direct=%d demoted=%d repaired_while_direct=%d\n",
		counters.Admitted, counters.FinalizedDirect, counters.Demoted, counters.RepairedWhileDirect,
	)

	if counters.Admitted == 0 {
		return fmt.Errorf(
			"WEAVER_RAR_DIRECT_STORE is on but weaver admitted 0 archive sets — "+
				"the gate never engaged, so this phase proved nothing about direct routing "+
				"(demoted=%d finalized_direct=%d)",
			counters.Demoted, counters.FinalizedDirect,
		)
	}
	if counters.FinalizedDirect == 0 {
		return fmt.Errorf(
			"weaver admitted %d direct set(s) but finalized 0 of them directly — every set "+
				"fell back to writing source volumes, which is the silent-demotion failure this "+
				"phase exists to catch (demoted=%d)",
			counters.Admitted, counters.Demoted,
		)
	}
	if counters.FinalizedDirect < directStoreArchiveSetCount {
		return fmt.Errorf(
			"only %d set(s) finalized direct, but the corpus carries %d archive sets built to route "+
				"direct end to end — at least one of them demoted; check the weaver log for "+
				"`direct-store set demoted` and its reason",
			counters.FinalizedDirect, directStoreArchiveSetCount,
		)
	}
	// The corpus carries `direct-store-par2-repair`, a damaged set whose whole
	// point is to be repaired in place without leaving direct routing. The
	// finalized-direct floor above cannot see the difference between "repaired
	// while direct" and "was never damaged at all" — a fixture regression that
	// stops producing damage would pass every other check while the repair path
	// silently went unexercised. The counter is the only witness that the
	// repair actually ran, which is why a zero here fails the phase.
	if counters.RepairedWhileDirect < 1 {
		return fmt.Errorf(
			"repaired_while_direct=0, but the corpus carries a damaged direct fixture that must "+
				"be repaired in place — either its set demoted for damage the recovery could have "+
				"covered, or the fixture no longer produces damage; check the weaver log for "+
				"`staying direct while the targeted recovery arrives` (admitted=%d finalized_direct=%d demoted=%d)",
			counters.Admitted, counters.FinalizedDirect, counters.Demoted,
		)
	}
	return assertNoUnexpectedDirectDemotions()
}

// Demotion reasons that are direct-store *working*: the set carried something
// it is designed to refuse, and refusing is the correct outcome. The functional
// corpus is full of these — compressed, solid and encrypted RAR sets that exist
// to test the conventional path — so their presence says nothing about whether
// direct routing is healthy.
// Archive sets in the canonical corpus built to route direct end to end. Kept
// as a count rather than a list because the counters report finalized sets in
// aggregate; par2-multi-set-archives contributes two independent sets.
const directStoreArchiveSetCount = 8

// Demotion reasons that mean direct-store REFUSED an archive it is designed not
// to carry. Refusal is the correct outcome and says nothing about health.
//
// Taken from the product's own `DemotionReason::metric()` strings. Every other
// reason in that enum is a failure — a set weaver admitted and then could not
// carry — and fails the phase below.
var byDesignDirectRefusals = map[string]bool{
	"member_compressed":        true,
	"member_encrypted":         true,
	"member_solid":             true,
	"member_directory":         true,
	"member_redirection":       true,
	"member_no_checksum":       true,
	"member_malformed_chain":   true,
	"member_blake2_only":       true,
	"tolerance_budget":         true,
	"unsupported_format":       true,
	"encrypted_facts_disagree": true,
}

// Jobs whose fixtures carry deliberately damaged bytes, where a checksum
// demotion is the product working rather than failing.
//
// The general corpus exists to exercise the CONVENTIONAL path, and several of
// its archives are corrupt on purpose. With direct-store on for every functional
// run those sets are admitted first, detect their own damage and demote —
// correctly. Without this exemption the phase would fail on fixtures whose whole
// point is being broken. Matched against the submitted job name.
var jobsAllowedToDemoteOnDamage = []string{
	"Corrupted", "PAR2", "MissingMiddle", "Damaged", "WrongPass",
}

// isDamageDemotion reports whether a reason means "the bytes were wrong", which
// a deliberately corrupt fixture is entitled to produce.
func isDamageDemotion(reason string) bool {
	switch reason {
	case "member_checksum_mismatch", "part_checksum_mismatch", "volume_crc_mismatch",
		"par2_damaged", "par2_unbindable":
		return true
	}
	return false
}

func jobIsAllowedToDemoteOnDamage(jobName string) bool {
	for _, needle := range jobsAllowedToDemoteOnDamage {
		if strings.Contains(jobName, needle) {
			return true
		}
	}
	return false
}

// directStoreJobNames maps job id -> submitted job name so a demotion can be
// attributed to the fixture that caused it. Without the attribution the check
// sees only aggregate reasons and cannot tell a corrupt-on-purpose fixture from
// a healthy set that failed.
func directStoreJobNames(log string) map[string]string {
	names := map[string]string{}
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, "submitted NZB job") {
			continue
		}
		clean := ansiEscape.ReplaceAllString(line, "")
		id := directLogJobID(clean)
		if id == "" {
			continue
		}
		start := strings.Index(clean, "name=")
		if start < 0 {
			continue
		}
		name := clean[start+len("name="):]
		if cut := strings.Index(name, " category="); cut >= 0 {
			name = name[:cut]
		}
		names[id] = strings.TrimSpace(name)
	}
	return names
}

func directLogJobID(line string) string {
	clean := ansiEscape.ReplaceAllString(line, "")
	start := strings.Index(clean, "job_id=")
	if start < 0 {
		return ""
	}
	rest := clean[start+len("job_id="):]
	end := strings.IndexAny(rest, " \t")
	if end < 0 {
		return strings.TrimSpace(rest)
	}
	return rest[:end]
}

// assertNoUnexpectedDirectDemotions fails the phase on any demotion that is
// neither a by-design refusal nor a deliberately damaged fixture.
//
// The counters alone cannot express this: `directSetsDemoted` sums correct
// refusals and genuine failures together, and this corpus produces dozens of the
// former every run. The reason lives only in weaver's log, so that is where this
// reads it.
func assertNoUnexpectedDirectDemotions() error {
	raw, err := os.ReadFile(localWeaverLogPath())
	if err != nil {
		return fmt.Errorf("could not read the weaver log to check demotion reasons: %w", err)
	}
	log := string(raw)
	jobNames := directStoreJobNames(log)

	unexpected := map[string]int{}
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, "direct-store set demoted") {
			continue
		}
		reason := directDemotionReason(line)
		if reason == "" || byDesignDirectRefusals[reason] ||
			// Header-encrypted refusals carry a per-format suffix
			// (`header_encrypted_rar4`, `..._unkeyable`), so match the family.
			strings.HasPrefix(reason, "header_encrypted") {
			continue
		}
		job := jobNames[directLogJobID(line)]
		if isDamageDemotion(reason) && jobIsAllowedToDemoteOnDamage(job) {
			continue
		}
		unexpected[fmt.Sprintf("%s (job %s)", reason, job)]++
	}
	if len(unexpected) == 0 {
		return nil
	}
	reasons := make([]string, 0, len(unexpected))
	for reason, count := range unexpected {
		reasons = append(reasons, fmt.Sprintf("%s x%d", reason, count))
	}
	sort.Strings(reasons)
	return fmt.Errorf(
		"direct-store demoted set(s) for reason(s) that are neither by-design refusals nor "+
			"deliberate fixture damage: %s — a set was admitted and then could not be carried",
		strings.Join(reasons, ", "),
	)
}

// ansiEscape matches the SGR sequences weaver's tracing writer emits around
// every field name and separator.
var ansiEscape = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

// directDemotionReason pulls the reason out of a `direct-store set demoted ...
// reason="member_compressed"` log line.
//
// Stripping ANSI first is the whole subtlety of reading that log. Weaver writes
// it with colour, so the raw bytes are
// `reason<ESC>[0m<ESC>[2m=<ESC>[0m"member_compressed"` and a search for the
// literal `reason="` matches nothing at all. Parsing without stripping made
// this check silently pass a run that had demoted three sets for failure
// reasons — it reported success at precisely the moment it had something to
// report, which is the worst way for an assertion to be wrong.
func directDemotionReason(line string) string {
	line = ansiEscape.ReplaceAllString(line, "")
	const key = "reason="
	start := strings.Index(line, key)
	if start < 0 {
		return ""
	}
	rest := strings.TrimPrefix(line[start+len(key):], `"`)
	end := strings.IndexAny(rest, "\" \t")
	if end < 0 {
		return strings.TrimSpace(rest)
	}
	return rest[:end]
}

// directStoreEnabledForPhase reports whether this harness process was launched
// with direct-store on. The phase definition passes `WEAVER_RAR_DIRECT_STORE`
// through `extraEnv`, which reaches both weaver and this process, so the
// harness can read the same switch the product read.
func directStoreEnabledForPhase() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("WEAVER_RAR_DIRECT_STORE"))) {
	case "1", "true", "on", "yes":
		return true
	default:
		return false
	}
}

func functionalPollInterval(jobs []testJob) time.Duration {
	if functionalHasPendingFileIdentityRewrite(jobs) {
		return functionalRewritePollInterval
	}
	return functionalNormalPollInterval
}

func functionalCompletionTimeout() time.Duration {
	if weaverUsesPostgresDatastore() {
		return 8 * time.Minute
	}
	return 180 * time.Second
}

type downloadBenchSnapshot struct {
	Job struct {
		Status                          string  `json:"status"`
		Progress                        float64 `json:"progress"`
		Health                          int     `json:"health"`
		TotalBytes                      uint64  `json:"totalBytes"`
		DownloadedBytes                 uint64  `json:"downloadedBytes"`
		OptionalRecoveryBytes           uint64  `json:"optionalRecoveryBytes"`
		OptionalRecoveryDownloadedBytes uint64  `json:"optionalRecoveryDownloadedBytes"`
		FailedBytes                     uint64  `json:"failedBytes"`
		Error                           *string `json:"error"`
	} `json:"job"`
	Metrics struct {
		CurrentDownloadSpeed uint64  `json:"currentDownloadSpeed"`
		ArticlesPerSec       float64 `json:"articlesPerSec"`
		DecodeRateMbps       float64 `json:"decodeRateMbps"`
		BytesDownloaded      uint64  `json:"bytesDownloaded"`
		BytesDecoded         uint64  `json:"bytesDecoded"`
		BytesCommitted       uint64  `json:"bytesCommitted"`
		SegmentsDownloaded   uint64  `json:"segmentsDownloaded"`
		SegmentsDecoded      uint64  `json:"segmentsDecoded"`
		SegmentsCommitted    uint64  `json:"segmentsCommitted"`
	} `json:"metrics"`
}

type downloadBenchSample struct {
	ElapsedMs                       int64   `json:"elapsed_ms"`
	Status                          string  `json:"status"`
	Progress                        float64 `json:"progress"`
	Health                          int     `json:"health"`
	TotalBytes                      uint64  `json:"total_bytes"`
	DownloadedBytes                 uint64  `json:"downloaded_bytes"`
	OptionalRecoveryBytes           uint64  `json:"optional_recovery_bytes"`
	OptionalRecoveryDownloadedBytes uint64  `json:"optional_recovery_downloaded_bytes"`
	FailedBytes                     uint64  `json:"failed_bytes"`
	CurrentDownloadSpeed            uint64  `json:"current_download_speed"`
	ArticlesPerSec                  float64 `json:"articles_per_sec"`
	DecodeRateMbps                  float64 `json:"decode_rate_mbps"`
	BytesDownloaded                 uint64  `json:"bytes_downloaded_total"`
	BytesDecoded                    uint64  `json:"bytes_decoded_total"`
	BytesCommitted                  uint64  `json:"bytes_committed_total"`
	SegmentsDownloaded              uint64  `json:"segments_downloaded_total"`
	SegmentsDecoded                 uint64  `json:"segments_decoded_total"`
	SegmentsCommitted               uint64  `json:"segments_committed_total"`
	Error                           string  `json:"error,omitempty"`
}

type downloadBenchRun struct {
	Scenario               string   `json:"scenario"`
	Iteration              int      `json:"iteration"`
	JobID                  int      `json:"job_id"`
	Status                 string   `json:"status"`
	Error                  string   `json:"error,omitempty"`
	TotalBytes             uint64   `json:"total_bytes"`
	DownloadedBytes        uint64   `json:"downloaded_bytes"`
	FailedBytes            uint64   `json:"failed_bytes"`
	LowestHealth           int      `json:"lowest_health"`
	DurationMs             int64    `json:"duration_ms"`
	TimeToFirstByteMs      *int64   `json:"time_to_first_byte_ms,omitempty"`
	TimeToAllBytesMs       *int64   `json:"time_to_all_bytes_ms,omitempty"`
	AvgEndToEndBytesPerSec float64  `json:"avg_end_to_end_bytes_per_sec"`
	AvgActiveBytesPerSec   float64  `json:"avg_active_bytes_per_sec"`
	PeakDownloadSpeed      uint64   `json:"peak_download_speed"`
	PeakArticlesPerSec     float64  `json:"peak_articles_per_sec"`
	PeakDecodeRateMbps     float64  `json:"peak_decode_rate_mbps"`
	SampleCount            int      `json:"sample_count"`
	SampleFile             string   `json:"sample_file,omitempty"`
	StatusesSeen           []string `json:"statuses_seen,omitempty"`
}

type downloadBenchAggregate struct {
	Scenario               string  `json:"scenario"`
	Runs                   int     `json:"runs"`
	Successes              int     `json:"successes"`
	AvgDurationMs          float64 `json:"avg_duration_ms"`
	AvgTimeToFirstByteMs   float64 `json:"avg_time_to_first_byte_ms"`
	AvgTimeToAllBytesMs    float64 `json:"avg_time_to_all_bytes_ms"`
	AvgEndToEndBytesPerSec float64 `json:"avg_end_to_end_bytes_per_sec"`
	AvgActiveBytesPerSec   float64 `json:"avg_active_bytes_per_sec"`
	AvgPeakDownloadSpeed   float64 `json:"avg_peak_download_speed"`
	AvgPeakArticlesPerSec  float64 `json:"avg_peak_articles_per_sec"`
	AvgPeakDecodeRateMbps  float64 `json:"avg_peak_decode_rate_mbps"`
	LowestObservedHealth   int     `json:"lowest_observed_health"`
}

type downloadBenchSummary struct {
	GeneratedAt        string                   `json:"generated_at"`
	ManagedLocalWeaver bool                     `json:"managed_local_weaver"`
	WeaverURL          string                   `json:"weaver_url"`
	OutputDir          string                   `json:"output_dir"`
	Iterations         int                      `json:"iterations"`
	SampleIntervalMs   int                      `json:"sample_interval_ms"`
	TimeoutSec         int                      `json:"timeout_sec"`
	Runs               []downloadBenchRun       `json:"runs"`
	Aggregates         []downloadBenchAggregate `json:"aggregates"`
}

type managedWeaverSession struct {
	URL     string
	LogPath string
	PID     int
	cmd     *exec.Cmd
	logFile *os.File
}

func cmdDownloadBench(args []string) {
	slugs := args
	if len(slugs) == 0 {
		envSlugs := strings.TrimSpace(env("DOWNLOAD_BENCH_SLUGS", ""))
		if envSlugs != "" {
			for _, slug := range strings.Split(envSlugs, ",") {
				slug = strings.TrimSpace(slug)
				if slug != "" {
					slugs = append(slugs, slug)
				}
			}
		}
	}
	if len(slugs) == 0 {
		slugs = []string{"single-mkv", "large-segments"}
	}

	iterations := envInt("DOWNLOAD_BENCH_ITERATIONS", 3)
	sampleIntervalMs := envInt("DOWNLOAD_BENCH_SAMPLE_MS", 250)
	timeoutSec := envInt("DOWNLOAD_BENCH_TIMEOUT_SEC", 300)
	managedLocalWeaver := envBool("DOWNLOAD_BENCH_LOCAL_WEAVER", false)
	outputDir := downloadBenchOutputDir()
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create download bench output dir: %v", err)
	}

	var (
		weaverURL string
		session   *managedWeaverSession
	)
	if managedLocalWeaver {
		var err error
		session, err = startManagedDownloadBenchWeaver(outputDir)
		if err != nil {
			log.Fatalf("start managed weaver: %v", err)
		}
		defer session.Close()
		weaverURL = session.URL
		log.Printf("managed weaver ready: pid=%d url=%s log=%s", session.PID, session.URL, session.LogPath)
	} else {
		weaverURL = defaultWeaverURL()
	}

	prepareStandardTestRun(weaverURL, true)

	scenarios := make([]*Scenario, 0, len(slugs))
	for _, slug := range slugs {
		scenarioPath := filepath.Join(testdataDir(), slug)
		scenario, err := loadScenario(scenarioPath)
		if err != nil {
			log.Fatalf("load scenario %s: %v", slug, err)
		}
		nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
		if _, err := os.Stat(nzbPath); err != nil {
			log.Fatalf("no NZB for %q — run '%s seed %s' first", slug, cliProgramName, scenarioPath)
		}
		scenarios = append(scenarios, scenario)
	}

	sampleInterval := time.Duration(sampleIntervalMs) * time.Millisecond
	timeout := time.Duration(timeoutSec) * time.Second
	summary := downloadBenchSummary{
		GeneratedAt:        time.Now().Format(time.RFC3339),
		ManagedLocalWeaver: managedLocalWeaver,
		WeaverURL:          weaverURL,
		OutputDir:          outputDir,
		Iterations:         iterations,
		SampleIntervalMs:   sampleIntervalMs,
		TimeoutSec:         timeoutSec,
	}

	for _, scenario := range scenarios {
		for iteration := 1; iteration <= iterations; iteration++ {
			prepareStandardTestRun(weaverURL, true)
			run := runDownloadBenchIteration(weaverURL, scenario, iteration, outputDir, sampleInterval, timeout)
			summary.Runs = append(summary.Runs, run)
			log.Printf(
				"download-bench %s #%d: status=%s duration=%s first_byte=%s all_bytes=%s avg_active=%s peak=%s",
				run.Scenario,
				run.Iteration,
				run.Status,
				formatMilliseconds(run.DurationMs),
				formatOptionalMilliseconds(run.TimeToFirstByteMs),
				formatOptionalMilliseconds(run.TimeToAllBytesMs),
				formatBytesPerSecond(run.AvgActiveBytesPerSec),
				formatBytesPerSecond(float64(run.PeakDownloadSpeed)),
			)
		}
	}

	summary.Aggregates = buildDownloadBenchAggregates(summary.Runs)
	summaryPath := filepath.Join(outputDir, "summary.json")
	summaryJSON, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		log.Fatalf("marshal download bench summary: %v", err)
	}
	if err := os.WriteFile(summaryPath, summaryJSON, 0o644); err != nil {
		log.Fatalf("write download bench summary: %v", err)
	}

	printDownloadBenchSummary(summary)
	log.Printf("download bench artifacts written to %s", outputDir)

	for _, run := range summary.Runs {
		if run.Status != "COMPLETE" {
			os.Exit(1)
		}
	}
}

func cmdPgo(args []string) {
	profileDir := weaverProfileDir()
	outputDir := weaverPgoOutputDir()
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		log.Fatalf("create Weaver PGO profile dir: %v", err)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create Weaver PGO output dir: %v", err)
	}
	beforeCount, err := countProfrawFiles(profileDir)
	if err != nil {
		log.Fatalf("scan Weaver PGO profile dir: %v", err)
	}

	if strings.TrimSpace(os.Getenv("E2E_WEAVER_PROFILE_DIR")) == "" {
		setEnv("E2E_WEAVER_PROFILE_DIR", profileDir)
	}
	if strings.TrimSpace(os.Getenv("DOWNLOAD_BENCH_LOCAL_WEAVER")) == "" {
		setEnv("DOWNLOAD_BENCH_LOCAL_WEAVER", "1")
	}
	if strings.TrimSpace(os.Getenv("DOWNLOAD_BENCH_ITERATIONS")) == "" {
		setEnv("DOWNLOAD_BENCH_ITERATIONS", "2")
	}
	if strings.TrimSpace(os.Getenv("DOWNLOAD_BENCH_OUTPUT_DIR")) == "" {
		setEnv("DOWNLOAD_BENCH_OUTPUT_DIR", filepath.Join(outputDir, "download-bench"))
	}

	log.Printf("collecting Weaver LLVM profiles into %s", profileDir)
	log.Printf("PGO run artifacts: %s", outputDir)
	log.Printf("managed Weaver binary: %s", findWeaverBin())

	log.Printf("phase 1/3: baseline suite")
	cmdTestAll()

	log.Printf("phase 2/3: TLS subset")
	cmdTlsTest()

	log.Printf("phase 3/3: download hotpath benchmark")
	cmdDownloadBench(args)

	afterCount, err := countProfrawFiles(profileDir)
	if err != nil {
		log.Fatalf("scan Weaver PGO profile dir after run: %v", err)
	}
	if afterCount <= beforeCount {
		log.Fatalf("no new .profraw files were written under %s; rerun with an instrumented Weaver binary", profileDir)
	}

	log.Printf(
		"collected %d new LLVM raw profile(s) (%d total) under %s",
		afterCount-beforeCount,
		afterCount,
		profileDir,
	)
}

func downloadBenchOutputDir() string {
	if value := strings.TrimSpace(env("DOWNLOAD_BENCH_OUTPUT_DIR", "")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join(os.TempDir(), "e2e-download-bench-"+time.Now().Format("20060102-150405"))
}

func startManagedDownloadBenchWeaver(outputDir string) (*managedWeaverSession, error) {
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForHTTP(newznabURL()+"/admin/health", 30*time.Second)

	weaverBin := env("WEAVER_BIN", findWeaverBin())
	weaverPort := localWeaverPort()
	configPath := filepath.Join(outputDir, "weaver.toml")
	logPath := filepath.Join(outputDir, "weaver.log")
	pidPath := filepath.Join(outputDir, "weaver.pid")

	killWeaver()
	cleanWeaverState()
	writeDownloadBenchWeaverConfig(
		configPath,
		envInt("DOWNLOAD_BENCH_NNTP_PORT", mustPortInt("DOWNLOAD_BENCH_NNTP_PORT", nntpPort())),
		envInt("DOWNLOAD_BENCH_CONNECTIONS", 8),
	)

	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("create weaver log: %w", err)
	}

	cmd := exec.Command(weaverBin, "--config", configPath, "serve", "--port", weaverPort)
	cmd.Env = managedWeaverEnv(os.Environ(), outputDir, env("DOWNLOAD_BENCH_RUST_LOG", "warn"))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("start weaver: %w", err)
	}

	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644); err != nil {
		log.Printf("warning: write weaver pid file: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(localWeaverPIDPath()), 0o755); err == nil {
		if err := os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644); err != nil {
			log.Printf("warning: write local weaver pid file: %v", err)
		}
	}

	url := fmt.Sprintf("http://localhost:%s", weaverPort)
	waitForGraphQL(graphqlURL(url), 20*time.Second)

	return &managedWeaverSession{
		URL:     url,
		LogPath: logPath,
		PID:     cmd.Process.Pid,
		cmd:     cmd,
		logFile: logFile,
	}, nil
}

func (s *managedWeaverSession) Close() {
	if s == nil {
		return
	}
	if s.cmd != nil && s.cmd.Process != nil {
		stopManagedWeaverCommand(s.cmd, 30*time.Second)
	}
	_ = os.Remove(localWeaverPIDPath())
	if s.logFile != nil {
		_ = s.logFile.Close()
	}
}

func writeDownloadBenchWeaverConfig(path string, nntpPort, connections int) {
	root := localWeaverDir()
	os.MkdirAll(filepath.Dir(path), 0o755)
	os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	config := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true
max_retries = 3

[[servers]]
id = 1
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = %d
active = true
priority = 0

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "series"
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), nntpPort, connections)
	_ = os.WriteFile(path, []byte(config), 0o644)
}

func managedWeaverEnv(base []string, runRoot, rustLog string) []string {
	if strings.TrimSpace(runRoot) == "" {
		runRoot = localRunDir()
	}
	homeDir := filepath.Join(runRoot, ".weaver-home")
	cacheDir := filepath.Join(homeDir, ".cache")
	configDir := filepath.Join(homeDir, ".config")
	dataDir := filepath.Join(homeDir, ".local", "share")
	_ = os.MkdirAll(cacheDir, 0o755)
	_ = os.MkdirAll(configDir, 0o755)
	_ = os.MkdirAll(dataDir, 0o755)

	env := append([]string(nil), base...)
	env = append(env,
		"HOME="+homeDir,
		"XDG_CACHE_HOME="+cacheDir,
		"XDG_CONFIG_HOME="+configDir,
		"XDG_DATA_HOME="+dataDir,
		"WEAVER_FORCE_KEY_FILE=1",
		// The managed server is reached only through its loopback port by the
		// E2E client. Trusting loopback keeps it loginless for those tests
		// without weakening non-local browser administration.
		"WEAVER_TRUSTED_CIDRS=127.0.0.1/32,::1/128",
	)
	if weaverUsesPostgresDatastore() {
		env = appendOrReplaceEnv(env, "WEAVER_DATABASE_URL", weaverPostgresURL())
	}
	if strings.TrimSpace(rustLog) != "" {
		env = append(env, "RUST_LOG="+rustLog)
	}
	if profileDir := strings.TrimSpace(os.Getenv("E2E_WEAVER_PROFILE_DIR")); profileDir != "" {
		profileDir = absolutePath(profileDir)
		if err := os.MkdirAll(profileDir, 0o755); err != nil {
			log.Fatalf("create managed Weaver profile dir: %v", err)
		}
		env = append(env, "LLVM_PROFILE_FILE="+filepath.Join(profileDir, managedWeaverProfileStem(runRoot)+"-%m-%p.profraw"))
	}
	return env
}

func weaverProfileDir() string {
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_PROFILE_DIR")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join(localRunDir(), "pgo", "profraw")
}

func weaverPgoOutputDir() string {
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_PGO_OUTPUT_DIR")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join(localRunDir(), "pgo")
}

func countProfrawFiles(dir string) (int, error) {
	matches, err := filepath.Glob(filepath.Join(absolutePath(dir), "*.profraw"))
	if err != nil {
		return 0, err
	}
	return len(matches), nil
}

func managedWeaverProfileStem(runRoot string) string {
	stem := strings.TrimSpace(strings.ToLower(filepath.Base(runRoot)))
	if stem == "" || stem == "." || stem == string(filepath.Separator) {
		return "weaver"
	}
	replacer := strings.NewReplacer(
		"/", "-",
		"\\", "-",
		" ", "-",
		".", "-",
		":", "-",
		"@", "-",
		"%", "-",
		"+", "-",
		"=", "-",
	)
	stem = strings.Trim(replacer.Replace(stem), "-_")
	if stem == "" {
		return "weaver"
	}
	return stem
}

func runDownloadBenchIteration(
	weaverURL string,
	scenario *Scenario,
	iteration int,
	outputDir string,
	sampleInterval time.Duration,
	timeout time.Duration,
) downloadBenchRun {
	run := downloadBenchRun{
		Scenario:     scenario.Slug,
		Iteration:    iteration,
		Status:       "SUBMIT_ERROR",
		LowestHealth: 1000,
		StatusesSeen: make([]string, 0, 8),
	}

	started := time.Now()
	jobID, err := submitOneNZB(weaverURL, scenario)
	if err != nil {
		run.Error = err.Error()
		return run
	}
	run.JobID = jobID

	samplePath := filepath.Join(outputDir, fmt.Sprintf("%s-run%d-samples.json", scenario.Slug, iteration))
	deadline := started.Add(timeout)
	samples := make([]downloadBenchSample, 0, 256)
	statusSeen := map[string]bool{}
	var firstByteMs *int64
	var allBytesMs *int64

	for {
		snapshot, err := queryDownloadBenchSnapshot(weaverURL, jobID)
		if err == nil {
			elapsedMs := time.Since(started).Milliseconds()
			sample := snapshotToDownloadBenchSample(elapsedMs, snapshot)
			samples = append(samples, sample)

			if !statusSeen[sample.Status] {
				statusSeen[sample.Status] = true
				run.StatusesSeen = append(run.StatusesSeen, sample.Status)
			}
			if sample.Health < run.LowestHealth {
				run.LowestHealth = sample.Health
			}
			if sample.CurrentDownloadSpeed > run.PeakDownloadSpeed {
				run.PeakDownloadSpeed = sample.CurrentDownloadSpeed
			}
			if sample.ArticlesPerSec > run.PeakArticlesPerSec {
				run.PeakArticlesPerSec = sample.ArticlesPerSec
			}
			if sample.DecodeRateMbps > run.PeakDecodeRateMbps {
				run.PeakDecodeRateMbps = sample.DecodeRateMbps
			}

			processedBytes := processedDownloadBytes(sample.DownloadedBytes, sample.FailedBytes)
			if firstByteMs == nil && processedBytes > 0 {
				value := elapsedMs
				firstByteMs = &value
			}
			if allBytesMs == nil && (sample.Progress >= 0.999 || sample.Status == "COMPLETE" || sample.Status == "FAILED") {
				value := elapsedMs
				allBytesMs = &value
			}

			if sample.Status == "COMPLETE" || sample.Status == "FAILED" {
				run.Status, run.Error = applyTerminalStateCheck(localWeaverDBPath(), jobID, scenario.Slug, sample.Status)
				if run.Error == "" {
					run.Error = sample.Error
				}
				run.TotalBytes = sample.TotalBytes
				run.DownloadedBytes = sample.DownloadedBytes
				run.FailedBytes = sample.FailedBytes
				break
			}
		}

		if time.Now().After(deadline) {
			run.Status = "TIMEOUT"
			run.Error = fmt.Sprintf("timed out after %s", timeout)
			break
		}
		time.Sleep(sampleInterval)
	}

	run.SampleCount = len(samples)
	run.SampleFile = samplePath
	run.DurationMs = time.Since(started).Milliseconds()
	run.TimeToFirstByteMs = firstByteMs
	run.TimeToAllBytesMs = allBytesMs

	if len(samples) > 0 {
		last := samples[len(samples)-1]
		run.TotalBytes = last.TotalBytes
		run.DownloadedBytes = last.DownloadedBytes
		run.FailedBytes = last.FailedBytes
		if last.Health < run.LowestHealth {
			run.LowestHealth = last.Health
		}
		run.AvgEndToEndBytesPerSec = bytesPerSecond(processedDownloadBytes(last.DownloadedBytes, last.FailedBytes), run.DurationMs)
		if allBytesMs != nil {
			activeMs := *allBytesMs
			if firstByteMs != nil && *allBytesMs > *firstByteMs {
				activeMs = *allBytesMs - *firstByteMs
			}
			run.AvgActiveBytesPerSec = bytesPerSecond(processedDownloadBytes(last.DownloadedBytes, last.FailedBytes), activeMs)
		}
	}

	sampleJSON, err := json.MarshalIndent(samples, "", "  ")
	if err != nil {
		log.Printf("warning: marshal download bench samples for %s #%d: %v", scenario.Slug, iteration, err)
	} else if err := os.WriteFile(samplePath, sampleJSON, 0o644); err != nil {
		log.Printf("warning: write download bench samples for %s #%d: %v", scenario.Slug, iteration, err)
	}

	return run
}

func queryDownloadBenchSnapshot(weaverURL string, jobID int) (downloadBenchSnapshot, error) {
	var result downloadBenchSnapshot
	item, err := fetchFacadeItemSnapshot(weaverURL, jobID)
	if err != nil {
		return result, err
	}
	if !item.Found {
		return result, fmt.Errorf("item %d not found", jobID)
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"query": `query {
			metrics {
				currentDownloadSpeed
				articlesPerSec
				decodeRateMbps
				bytesDownloaded
				bytesDecoded
				bytesCommitted
				segmentsDownloaded
				segmentsDecoded
				segmentsCommitted
			}
		}`,
	})
	resp, err := postGraphQL(weaverURL, payload)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	var gqlResp struct {
		Data struct {
			Metrics struct {
				CurrentDownloadSpeed uint64  `json:"currentDownloadSpeed"`
				ArticlesPerSec       float64 `json:"articlesPerSec"`
				DecodeRateMbps       float64 `json:"decodeRateMbps"`
				BytesDownloaded      uint64  `json:"bytesDownloaded"`
				BytesDecoded         uint64  `json:"bytesDecoded"`
				BytesCommitted       uint64  `json:"bytesCommitted"`
				SegmentsDownloaded   uint64  `json:"segmentsDownloaded"`
				SegmentsDecoded      uint64  `json:"segmentsDecoded"`
				SegmentsCommitted    uint64  `json:"segmentsCommitted"`
			} `json:"metrics"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return result, err
	}
	if len(gqlResp.Errors) > 0 {
		return result, fmt.Errorf("%s", gqlResp.Errors[0].Message)
	}

	result.Job.Status = item.Status
	result.Job.Progress = item.ProgressPercent
	result.Job.Health = item.Health
	result.Job.TotalBytes = item.TotalBytes
	result.Job.DownloadedBytes = item.DownloadedBytes
	result.Job.OptionalRecoveryBytes = item.OptionalRecoveryBytes
	result.Job.OptionalRecoveryDownloadedBytes = item.OptionalRecoveryDownloadedBytes
	result.Job.FailedBytes = item.FailedBytes
	if item.Error != "" {
		errMsg := item.Error
		result.Job.Error = &errMsg
	}
	result.Metrics = gqlResp.Data.Metrics
	return result, nil
}

func snapshotToDownloadBenchSample(elapsedMs int64, snapshot downloadBenchSnapshot) downloadBenchSample {
	sample := downloadBenchSample{
		ElapsedMs:                       elapsedMs,
		Status:                          snapshot.Job.Status,
		Progress:                        snapshot.Job.Progress,
		Health:                          snapshot.Job.Health,
		TotalBytes:                      snapshot.Job.TotalBytes,
		DownloadedBytes:                 snapshot.Job.DownloadedBytes,
		OptionalRecoveryBytes:           snapshot.Job.OptionalRecoveryBytes,
		OptionalRecoveryDownloadedBytes: snapshot.Job.OptionalRecoveryDownloadedBytes,
		FailedBytes:                     snapshot.Job.FailedBytes,
		CurrentDownloadSpeed:            snapshot.Metrics.CurrentDownloadSpeed,
		ArticlesPerSec:                  snapshot.Metrics.ArticlesPerSec,
		DecodeRateMbps:                  snapshot.Metrics.DecodeRateMbps,
		BytesDownloaded:                 snapshot.Metrics.BytesDownloaded,
		BytesDecoded:                    snapshot.Metrics.BytesDecoded,
		BytesCommitted:                  snapshot.Metrics.BytesCommitted,
		SegmentsDownloaded:              snapshot.Metrics.SegmentsDownloaded,
		SegmentsDecoded:                 snapshot.Metrics.SegmentsDecoded,
		SegmentsCommitted:               snapshot.Metrics.SegmentsCommitted,
	}
	if snapshot.Job.Error != nil {
		sample.Error = *snapshot.Job.Error
	}
	return sample
}

func buildDownloadBenchAggregates(runs []downloadBenchRun) []downloadBenchAggregate {
	type accumulator struct {
		aggregate       downloadBenchAggregate
		durationSum     float64
		firstByteSum    float64
		firstByteCount  int
		allBytesSum     float64
		allBytesCount   int
		endToEndSum     float64
		activeSum       float64
		peakSpeedSum    float64
		peakArticlesSum float64
		peakDecodeSum   float64
	}

	byScenario := make(map[string]*accumulator)
	for _, run := range runs {
		acc := byScenario[run.Scenario]
		if acc == nil {
			acc = &accumulator{aggregate: downloadBenchAggregate{
				Scenario:             run.Scenario,
				LowestObservedHealth: 1000,
			}}
			byScenario[run.Scenario] = acc
		}

		acc.aggregate.Runs++
		if run.Status == "COMPLETE" {
			acc.aggregate.Successes++
		}
		acc.durationSum += float64(run.DurationMs)
		acc.endToEndSum += run.AvgEndToEndBytesPerSec
		acc.activeSum += run.AvgActiveBytesPerSec
		acc.peakSpeedSum += float64(run.PeakDownloadSpeed)
		acc.peakArticlesSum += run.PeakArticlesPerSec
		acc.peakDecodeSum += run.PeakDecodeRateMbps
		if run.LowestHealth < acc.aggregate.LowestObservedHealth {
			acc.aggregate.LowestObservedHealth = run.LowestHealth
		}
		if run.TimeToFirstByteMs != nil {
			acc.firstByteSum += float64(*run.TimeToFirstByteMs)
			acc.firstByteCount++
		}
		if run.TimeToAllBytesMs != nil {
			acc.allBytesSum += float64(*run.TimeToAllBytesMs)
			acc.allBytesCount++
		}
	}

	out := make([]downloadBenchAggregate, 0, len(byScenario))
	for _, scenario := range sortedKeys(byScenario) {
		acc := byScenario[scenario]
		runsCount := float64(acc.aggregate.Runs)
		acc.aggregate.AvgDurationMs = acc.durationSum / runsCount
		acc.aggregate.AvgEndToEndBytesPerSec = acc.endToEndSum / runsCount
		acc.aggregate.AvgActiveBytesPerSec = acc.activeSum / runsCount
		acc.aggregate.AvgPeakDownloadSpeed = acc.peakSpeedSum / runsCount
		acc.aggregate.AvgPeakArticlesPerSec = acc.peakArticlesSum / runsCount
		acc.aggregate.AvgPeakDecodeRateMbps = acc.peakDecodeSum / runsCount
		if acc.firstByteCount > 0 {
			acc.aggregate.AvgTimeToFirstByteMs = acc.firstByteSum / float64(acc.firstByteCount)
		}
		if acc.allBytesCount > 0 {
			acc.aggregate.AvgTimeToAllBytesMs = acc.allBytesSum / float64(acc.allBytesCount)
		}
		out = append(out, acc.aggregate)
	}
	return out
}

func sortedKeys[T any](items map[string]T) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func printDownloadBenchSummary(summary downloadBenchSummary) {
	fmt.Println()
	fmt.Printf("%-18s %-5s %-10s %-11s %-11s %-12s %-12s %-10s %-8s\n",
		"SCENARIO", "RUN", "STATUS", "TOTAL", "1ST BYTE", "ALL BYTES", "AVG ACTIVE", "PEAK", "HEALTH")
	fmt.Println(strings.Repeat("-", 112))
	for _, run := range summary.Runs {
		fmt.Printf("%-18s %-5d %-10s %-11s %-11s %-12s %-12s %-10s %-8.1f\n",
			run.Scenario,
			run.Iteration,
			run.Status,
			formatMilliseconds(run.DurationMs),
			formatOptionalMilliseconds(run.TimeToFirstByteMs),
			formatOptionalMilliseconds(run.TimeToAllBytesMs),
			formatBytesPerSecond(run.AvgActiveBytesPerSec),
			formatBytesPerSecond(float64(run.PeakDownloadSpeed)),
			float64(run.LowestHealth)/10.0,
		)
	}
	fmt.Println(strings.Repeat("-", 112))
	fmt.Println("AVERAGES")
	for _, aggregate := range summary.Aggregates {
		fmt.Printf("  %-18s runs=%d ok=%d total=%s first_byte=%s all_bytes=%s avg_active=%s peak=%s decode=%.1f health=%.1f%%\n",
			aggregate.Scenario,
			aggregate.Runs,
			aggregate.Successes,
			formatMilliseconds(int64(aggregate.AvgDurationMs)),
			formatFloatMilliseconds(aggregate.AvgTimeToFirstByteMs),
			formatFloatMilliseconds(aggregate.AvgTimeToAllBytesMs),
			formatBytesPerSecond(aggregate.AvgActiveBytesPerSec),
			formatBytesPerSecond(aggregate.AvgPeakDownloadSpeed),
			aggregate.AvgPeakDecodeRateMbps,
			float64(aggregate.LowestObservedHealth)/10.0,
		)
	}
}

func processedDownloadBytes(downloadedBytes, failedBytes uint64) uint64 {
	return downloadedBytes + failedBytes
}

func bytesPerSecond(bytes uint64, durationMs int64) float64 {
	if durationMs <= 0 {
		return 0
	}
	return float64(bytes) / (float64(durationMs) / 1000.0)
}

func formatMilliseconds(ms int64) string {
	if ms <= 0 {
		return "0ms"
	}
	return (time.Duration(ms) * time.Millisecond).Round(10 * time.Millisecond).String()
}

func formatOptionalMilliseconds(ms *int64) string {
	if ms == nil {
		return "n/a"
	}
	return formatMilliseconds(*ms)
}

func formatFloatMilliseconds(ms float64) string {
	if ms <= 0 {
		return "n/a"
	}
	return formatMilliseconds(int64(ms))
}

func formatBytesPerSecond(bytesPerSecond float64) string {
	if bytesPerSecond <= 0 {
		return "0 B/s"
	}
	units := []string{"B/s", "KiB/s", "MiB/s", "GiB/s"}
	value := bytesPerSecond
	unit := units[0]
	for i := 1; i < len(units) && value >= 1024; i++ {
		value /= 1024
		unit = units[i]
	}
	if value >= 10 {
		return fmt.Sprintf("%.0f %s", value, unit)
	}
	return fmt.Sprintf("%.1f %s", value, unit)
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
	checkHTTP("Newznab", newznabURL()+"/admin/health")
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

// --- helpers ---

func deleteArticles(slug string, pct int) error {
	return deleteArticlesAt(nntpHost(), nntpPort(), slug, pct)
}

func deleteArticlesAt(host, port, slug string, pct int) error {
	addr := host + ":" + port
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	// Read greeting
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	greeting, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read greeting: %w", err)
	}
	if !strings.HasPrefix(greeting, "200") {
		return fmt.Errorf("unexpected greeting: %s", strings.TrimSpace(greeting))
	}
	if err := authenticateNNTPConnection(conn, r, addr); err != nil {
		return err
	}

	// Send DELETE command
	prefix := fmt.Sprintf("e2e-%s", slug)
	cmd := fmt.Sprintf("DELETE %s %d\r\n", prefix, pct)
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("write command: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	resp, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	resp = strings.TrimSpace(resp)
	if !strings.HasPrefix(resp, "290") {
		return fmt.Errorf("DELETE failed: %s", resp)
	}
	log.Printf("  %s", resp)

	conn.Write([]byte("QUIT\r\n"))
	return nil
}

func seededArticleIDs(slug string) ([]string, error) {
	nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
	nzbData, err := os.ReadFile(nzbPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read existing NZB %s: %w", nzbPath, err)
	}
	return extractMessageIDs(string(nzbData)), nil
}

func purgeSeededArticlesAt(host string, port string, slug string) error {
	messageIDs, err := seededArticleIDs(slug)
	if err != nil {
		return err
	}
	return deleteArticleIDsAt(host, port, messageIDs)
}

func purgeSeededArticles(slug string) error {
	if err := purgeSeededArticlesAt(nntpHost(), nntpPort(), slug); err != nil {
		return err
	}
	if backupNntpRunning() {
		if err := purgeSeededArticlesAt(nntpHost(), backupNntpPort(), slug); err != nil {
			return err
		}
	}
	return nil
}

func waitForTCP(addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		mustSleepWithSuspendDetection(time.Second, fmt.Sprintf("waiting for TCP %s", addr))
	}
	log.Fatalf("timeout waiting for %s", addr)
}

type facadeItemSnapshot struct {
	Found                           bool
	InQueue                         bool
	Status                          string
	ProgressPercent                 float64
	Health                          int
	Error                           string
	TotalBytes                      uint64
	DownloadedBytes                 uint64
	OptionalRecoveryBytes           uint64
	OptionalRecoveryDownloadedBytes uint64
	FailedBytes                     uint64
}

func normalizeFacadeState(state string) string {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "COMPLETED":
		return "COMPLETE"
	case "FAILED":
		return "FAILED"
	default:
		return strings.ToUpper(strings.TrimSpace(state))
	}
}

func facadeTerminalStatus(state string) bool {
	switch normalizeFacadeState(state) {
	case "COMPLETE", "FAILED":
		return true
	default:
		return false
	}
}

func fetchFacadeItemSnapshot(weaverURL string, jobID int) (facadeItemSnapshot, error) {
	var result facadeItemSnapshot
	payload, _ := json.Marshal(map[string]interface{}{
		"query": `query($id: Int!) {
			queueItem(id: $id) {
				id
				state
				progressPercent
				health
				error
				totalBytes
				downloadedBytes
				optionalRecoveryBytes
				optionalRecoveryDownloadedBytes
				failedBytes
			}
			historyItem(id: $id) {
				id
				state
				progressPercent
				health
				error
				totalBytes
				downloadedBytes
				failedBytes
			}
		}`,
		"variables": map[string]interface{}{"id": jobID},
	})
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := postGraphQLWithClient(client, weaverURL, payload)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return result, fmt.Errorf("facade snapshot returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var gqlResp struct {
		Data struct {
			QueueItem *struct {
				State                           string  `json:"state"`
				ProgressPercent                 float64 `json:"progressPercent"`
				Health                          int     `json:"health"`
				Error                           *string `json:"error"`
				TotalBytes                      uint64  `json:"totalBytes"`
				DownloadedBytes                 uint64  `json:"downloadedBytes"`
				OptionalRecoveryBytes           uint64  `json:"optionalRecoveryBytes"`
				OptionalRecoveryDownloadedBytes uint64  `json:"optionalRecoveryDownloadedBytes"`
				FailedBytes                     uint64  `json:"failedBytes"`
			} `json:"queueItem"`
			HistoryItem *struct {
				State           string  `json:"state"`
				ProgressPercent float64 `json:"progressPercent"`
				Health          int     `json:"health"`
				Error           *string `json:"error"`
				TotalBytes      uint64  `json:"totalBytes"`
				DownloadedBytes uint64  `json:"downloadedBytes"`
				FailedBytes     uint64  `json:"failedBytes"`
			} `json:"historyItem"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return result, err
	}
	if len(gqlResp.Errors) > 0 {
		return result, fmt.Errorf("gql: %s", gqlResp.Errors[0].Message)
	}

	if item := gqlResp.Data.QueueItem; item != nil {
		result.Found = true
		result.InQueue = true
		result.Status = normalizeFacadeState(item.State)
		result.ProgressPercent = item.ProgressPercent
		result.Health = item.Health
		result.TotalBytes = item.TotalBytes
		result.DownloadedBytes = item.DownloadedBytes
		result.OptionalRecoveryBytes = item.OptionalRecoveryBytes
		result.OptionalRecoveryDownloadedBytes = item.OptionalRecoveryDownloadedBytes
		result.FailedBytes = item.FailedBytes
		if item.Error != nil {
			result.Error = *item.Error
		}
		return result, nil
	}
	if item := gqlResp.Data.HistoryItem; item != nil {
		result.Found = true
		result.Status = normalizeFacadeState(item.State)
		result.ProgressPercent = item.ProgressPercent
		result.Health = item.Health
		result.TotalBytes = item.TotalBytes
		result.DownloadedBytes = item.DownloadedBytes
		result.FailedBytes = item.FailedBytes
		if item.Error != nil {
			result.Error = *item.Error
		}
		return result, nil
	}

	return result, nil
}

func cancelJobGraphQL(weaverURL string, jobID int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"query":     `mutation($id: Int!) { cancelQueueItem(id: $id) { success message } }`,
		"variables": map[string]interface{}{"id": jobID},
	})
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := postGraphQLWithClient(client, weaverURL, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cancel job %d returned %d: %s", jobID, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var gqlResp struct {
		Data struct {
			CancelQueueItem struct {
				Success bool    `json:"success"`
				Message *string `json:"message"`
			} `json:"cancelQueueItem"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return err
	}
	if len(gqlResp.Errors) > 0 {
		return fmt.Errorf("cancel job %d gql: %s", jobID, gqlResp.Errors[0].Message)
	}
	if !gqlResp.Data.CancelQueueItem.Success {
		message := ""
		if gqlResp.Data.CancelQueueItem.Message != nil {
			message = *gqlResp.Data.CancelQueueItem.Message
		}
		return fmt.Errorf("cancel job %d did not succeed: %s", jobID, message)
	}
	return nil
}

const (
	weaverCancelSettleTimeout      = 15 * time.Second
	weaverCancelSettlePollInterval = 500 * time.Millisecond
)

func waitForJobCancelSettledGraphQL(weaverURL string, jobID int, timeout, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = weaverCancelSettlePollInterval
	}
	deadline := time.Now().Add(timeout)
	lastState := "not checked"

	for {
		snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
		if err != nil {
			lastState = err.Error()
		} else if !snapshot.Found {
			return nil
		} else {
			status := normalizeFacadeState(snapshot.Status)
			lastState = fmt.Sprintf(
				"found=%t in_queue=%t status=%s progress=%.2f health=%d failed_bytes=%d error=%q",
				snapshot.Found,
				snapshot.InQueue,
				status,
				snapshot.ProgressPercent,
				snapshot.Health,
				snapshot.FailedBytes,
				snapshot.Error,
			)
			if cancelSettledState(status) {
				return nil
			}
		}

		if timeout <= 0 || !time.Now().Before(deadline) {
			return fmt.Errorf("job %d did not settle after cancel within %s: %s", jobID, timeout, lastState)
		}
		time.Sleep(pollInterval)
	}
}

func cancelSettledState(state string) bool {
	switch normalizeFacadeState(state) {
	case "COMPLETE", "FAILED", "CANCELLED", "CANCELED":
		return true
	default:
		return false
	}
}

func describeJobsGraphQL(weaverURL string) string {
	jobs, err := listJobsGraphQL(weaverURL)
	if err != nil {
		return fmt.Sprintf("list jobs failed: %v", err)
	}
	if len(jobs) == 0 {
		return "no queue/history jobs"
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].ID == jobs[j].ID {
			return jobs[i].Status < jobs[j].Status
		}
		return jobs[i].ID < jobs[j].ID
	})
	parts := make([]string, 0, len(jobs))
	for _, job := range jobs {
		parts = append(parts, fmt.Sprintf("%d=%s", job.ID, job.Status))
	}
	return strings.Join(parts, ", ")
}

func localWeaverLogTail(maxLines int) string {
	if maxLines <= 0 {
		maxLines = 40
	}
	path := localWeaverLogPath()
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("read %s: %v", path, err)
	}
	text := strings.TrimRight(string(data), "\n")
	if text == "" {
		return fmt.Sprintf("%s is empty", path)
	}
	lines := strings.Split(text, "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return strings.Join(lines, "\n")
}

func deleteAllHistoryGraphQL(weaverURL string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	listPayload, _ := json.Marshal(map[string]interface{}{
		"query": `query {
			historyItems(first: 1000) {
				id
			}
		}`,
	})
	resp, err := postGraphQLWithClient(client, weaverURL, listPayload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("list history returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var listResp struct {
		Data struct {
			HistoryItems []struct {
				ID int `json:"id"`
			} `json:"historyItems"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return err
	}
	if len(listResp.Errors) > 0 {
		return fmt.Errorf("list history gql: %s", listResp.Errors[0].Message)
	}
	if len(listResp.Data.HistoryItems) == 0 {
		return nil
	}
	ids := make([]int, 0, len(listResp.Data.HistoryItems))
	for _, item := range listResp.Data.HistoryItems {
		ids = append(ids, item.ID)
	}

	deletePayload, _ := json.Marshal(map[string]interface{}{
		"query": `mutation($ids: [Int!]!) {
			removeHistoryItems(ids: $ids, deleteFiles: true) {
				success
				removedIds
			}
		}`,
		"variables": map[string]interface{}{"ids": ids},
	})
	deleteResp, err := postGraphQLWithClient(client, weaverURL, deletePayload)
	if err != nil {
		return err
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != 200 {
		body, _ := io.ReadAll(deleteResp.Body)
		return fmt.Errorf("delete history returned %d: %s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
	}
	var gqlResp struct {
		Data struct {
			RemoveHistoryItems struct {
				Success bool `json:"success"`
			} `json:"removeHistoryItems"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(deleteResp.Body).Decode(&gqlResp); err != nil {
		return err
	}
	if len(gqlResp.Errors) > 0 {
		return fmt.Errorf("delete history gql: %s", gqlResp.Errors[0].Message)
	}
	if !gqlResp.Data.RemoveHistoryItems.Success {
		return fmt.Errorf("delete history did not succeed")
	}
	return nil
}

func listJobsGraphQL(weaverURL string) ([]struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}, error) {
	payload, _ := json.Marshal(map[string]interface{}{
		"query": `query {
			queueItems(first: 1000) { id state }
			historyItems(first: 1000) { id state }
		}`,
	})
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := postGraphQLWithClient(client, weaverURL, payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list jobs returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var gqlResp struct {
		Data struct {
			QueueItems []struct {
				ID    int    `json:"id"`
				State string `json:"state"`
			} `json:"queueItems"`
			HistoryItems []struct {
				ID    int    `json:"id"`
				State string `json:"state"`
			} `json:"historyItems"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return nil, fmt.Errorf("decode jobs response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return nil, fmt.Errorf("list jobs GraphQL error: %s", gqlResp.Errors[0].Message)
	}
	jobs := make([]struct {
		ID     int    `json:"id"`
		Status string `json:"status"`
	}, 0, len(gqlResp.Data.QueueItems)+len(gqlResp.Data.HistoryItems))
	for _, item := range gqlResp.Data.QueueItems {
		jobs = append(jobs, struct {
			ID     int    `json:"id"`
			Status string `json:"status"`
		}{ID: item.ID, Status: normalizeFacadeState(item.State)})
	}
	for _, item := range gqlResp.Data.HistoryItems {
		jobs = append(jobs, struct {
			ID     int    `json:"id"`
			Status string `json:"status"`
		}{ID: item.ID, Status: normalizeFacadeState(item.State)})
	}
	return jobs, nil
}

func prepareStandardTestRun(weaverURL string, clearHistory bool) {
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForHTTP(newznabURL()+"/admin/health", 30*time.Second)
	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)

	if err := ensureNntpChaosOff(); err != nil {
		log.Fatalf("reset NNTP chaos before test run: %v", err)
	}
	if !clearHistory {
		return
	}

	jobs, err := listJobsGraphQL(weaverURL)
	if err != nil {
		log.Fatalf("list weaver jobs before test run: %v", err)
	}
	for _, job := range jobs {
		if job.Status == "COMPLETE" || job.Status == "FAILED" {
			continue
		}
		if err := cancelJobGraphQL(weaverURL, job.ID); err != nil {
			log.Printf("warning: cancel stale job %d (%s): %v", job.ID, job.Status, err)
		}
	}
	if err := deleteAllHistoryGraphQL(weaverURL); err != nil {
		log.Fatalf("clear weaver history before test run: %v", err)
	}
	time.Sleep(2 * time.Second)
}

func tailFileLines(path string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return []string{fmt.Sprintf("failed to read %s: %v", path, err)}
	}
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	var trimmed []string
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			trimmed = append(trimmed, line)
		}
	}
	if len(trimmed) > limit {
		trimmed = trimmed[len(trimmed)-limit:]
	}
	return trimmed
}

func writeChaosRoundArtifacts(
	root string,
	roundNumber int,
	name string,
	config string,
	jobs []chaosRoundJobArtifact,
	weaverURL string,
) {
	type roundArtifact struct {
		Round         int                     `json:"round"`
		Name          string                  `json:"name"`
		Config        string                  `json:"config"`
		RecordedAt    string                  `json:"recorded_at"`
		Jobs          []chaosRoundJobArtifact `json:"jobs"`
		CurrentWeaver []struct {
			ID     int    `json:"id"`
			Status string `json:"status"`
		} `json:"current_weaver_jobs,omitempty"`
		WeaverLogTail []string `json:"weaver_log_tail,omitempty"`
	}

	currentJobs, err := listJobsGraphQL(weaverURL)
	if err != nil {
		log.Printf("warning: list jobs for NNTP chaos round %d artifact: %v", roundNumber, err)
	}
	enrichedJobs := append([]chaosRoundJobArtifact(nil), jobs...)
	for i := range enrichedJobs {
		if enrichedJobs[i].JobID <= 0 {
			continue
		}
		snapshot, snapshotErr := fetchFacadeItemSnapshot(weaverURL, enrichedJobs[i].JobID)
		if snapshotErr != nil {
			enrichedJobs[i].Error = fmt.Sprintf("snapshot error: %v", snapshotErr)
			continue
		}
		enrichedJobs[i].Found = snapshot.Found
		enrichedJobs[i].ProgressPercent = snapshot.ProgressPercent
		enrichedJobs[i].Health = snapshot.Health
		if snapshot.Error != "" {
			enrichedJobs[i].Error = snapshot.Error
		}
		if snapshot.Found && facadeTerminalStatus(snapshot.Status) {
			enrichedJobs[i].Status = snapshot.Status
		}
	}
	artifact := roundArtifact{
		Round:         roundNumber,
		Name:          name,
		Config:        config,
		RecordedAt:    time.Now().Format(time.RFC3339Nano),
		Jobs:          enrichedJobs,
		CurrentWeaver: currentJobs,
		WeaverLogTail: tailFileLines(localWeaverLogPath(), 200),
	}
	data, marshalErr := json.MarshalIndent(artifact, "", "  ")
	if marshalErr != nil {
		log.Printf("warning: marshal NNTP chaos round %d artifact: %v", roundNumber, marshalErr)
		return
	}
	path := filepath.Join(root, fmt.Sprintf("round-%02d.json", roundNumber))
	if writeErr := os.WriteFile(path, data, 0o644); writeErr != nil {
		log.Printf("warning: write NNTP chaos round %d artifact: %v", roundNumber, writeErr)
	}
}

func waitForGraphQL(url string, timeout time.Duration) {
	client := weaverHTTPClient(url, 3*time.Second)
	body := []byte(`{"query":"{ version }"}`)
	deadline := time.Now().Add(timeout)
	lastFailure := "not attempted"
	for time.Now().Before(deadline) {
		if err := refreshWeaverBrowserSession(client, url); err != nil {
			lastFailure = "load UI: " + err.Error()
			mustSleepWithSuspendDetection(time.Second, fmt.Sprintf("waiting for GraphQL %s", url))
			continue
		}
		resp, err := postGraphQLWithClient(client, url, body)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		lastFailure = describeGraphQLAttempt(resp, err)
		mustSleepWithSuspendDetection(time.Second, fmt.Sprintf("waiting for GraphQL %s", url))
	}
	log.Fatalf("timeout waiting for %s (last failure: %s)", url, lastFailure)
}

func describeGraphQLAttempt(resp *http.Response, err error) string {
	if err != nil {
		return err.Error()
	}
	if resp == nil {
		return "no response"
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	snippet := strings.TrimSpace(string(body))
	if snippet == "" {
		return fmt.Sprintf("status %d", resp.StatusCode)
	}
	return fmt.Sprintf("status %d: %s", resp.StatusCode, snippet)
}

func waitForHTTP(url string, timeout time.Duration) {
	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		mustSleepWithSuspendDetection(time.Second, fmt.Sprintf("waiting for HTTP %s", url))
	}
	log.Fatalf("timeout waiting for %s", url)
}

func defaultWeaverURL() string {
	if value := strings.TrimSpace(os.Getenv("WEAVER_URL")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return fmt.Sprintf("http://localhost:%s", os.Getenv("E2E_WEAVER_PORT"))
}

func localWeaverPort() string {
	if value := strings.TrimSpace(os.Getenv("WEAVER_PORT")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return os.Getenv("E2E_LOCAL_WEAVER_PORT")
}

func graphqlURL(weaverURL string) string {
	if strings.HasSuffix(weaverURL, "/graphql") {
		return weaverURL
	}
	return strings.TrimRight(weaverURL, "/") + "/graphql"
}

func weaverBaseURL(weaverURL string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(weaverURL), "/")
	return strings.TrimSuffix(trimmed, "/graphql")
}

func weaverHTTPClient(weaverURL string, timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{
		Timeout: timeout,
		Jar:     weaverCookieJar(weaverURL),
	}
}

func weaverCookieJar(weaverURL string) http.CookieJar {
	baseURL := weaverBaseURL(weaverURL)
	if jar, ok := weaverCookieJars.Load(baseURL); ok {
		if typed, ok := jar.(http.CookieJar); ok {
			return typed
		}
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		log.Fatalf("create Weaver cookie jar: %v", err)
	}
	actual, _ := weaverCookieJars.LoadOrStore(baseURL, jar)
	return actual.(http.CookieJar)
}

func ensureClientCookieJar(client *http.Client) error {
	if client.Jar != nil {
		return nil
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return err
	}
	client.Jar = jar
	return nil
}

func refreshWeaverBrowserSession(client *http.Client, weaverURL string) error {
	if client == nil {
		return fmt.Errorf("nil http client")
	}
	if err := ensureClientCookieJar(client); err != nil {
		return fmt.Errorf("create cookie jar: %w", err)
	}
	resp, err := client.Get(weaverBaseURL(weaverURL))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("load Weaver UI returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func doGraphQLPost(client *http.Client, weaverURL string, payload []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, graphqlURL(weaverURL), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}

func postGraphQL(weaverURL string, payload []byte) (*http.Response, error) {
	return postGraphQLWithClient(weaverHTTPClient(weaverURL, 10*time.Second), weaverURL, payload)
}

func postGraphQLWithClient(client *http.Client, weaverURL string, payload []byte) (*http.Response, error) {
	if client == nil {
		client = weaverHTTPClient(weaverURL, 10*time.Second)
	}

	resp, err := doGraphQLPost(client, weaverURL, payload)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if err := refreshWeaverBrowserSession(client, weaverURL); err != nil {
		return nil, fmt.Errorf("graphql unauthorized and failed to load Weaver browser session: %w", err)
	}
	return doGraphQLPost(client, weaverURL, payload)
}

// --- chaos ---

func sendNntpCommand(cmd string) string {
	return sendNntpCommandTo(nntpHost(), nntpPort(), cmd)
}

func sendNntpCommandTo(host, port, cmd string) (resp string) {
	resp, _ = sendNntpCommandToWithRetry(host, port, cmd, 1)
	return resp
}

func sendNntpCommandToWithRetry(host, port, cmd string, attempts int) (string, error) {
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		resp, err := sendNntpCommandToOnce(host, port, cmd)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
	return "", lastErr
}

func sendNntpCommandToOnce(host, port, cmd string) (string, error) {
	return sendNntpCommandToOnceWithAuth(host, port, cmd, true)
}

type nntpCommandSession struct {
	addr   string
	conn   net.Conn
	reader *bufio.Reader
}

func openNntpCommandSession(host, port string, authenticate bool) (*nntpCommandSession, error) {
	addr := host + ":" + port
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", addr, err)
	}

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	greeting, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read greeting from %s: %w", addr, err)
	}
	if !strings.HasPrefix(greeting, "200") && !strings.HasPrefix(greeting, "201") {
		conn.Close()
		return nil, fmt.Errorf("unexpected greeting from %s: %s", addr, strings.TrimSpace(greeting))
	}
	if authenticate {
		if err := authenticateNNTPConnection(conn, reader, addr); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return &nntpCommandSession{addr: addr, conn: conn, reader: reader}, nil
}

func (session *nntpCommandSession) send(cmd string) (string, error) {
	session.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := session.conn.Write([]byte(cmd + "\r\n")); err != nil {
		return "", fmt.Errorf("write command %q to %s: %w", cmd, session.addr, err)
	}

	session.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := session.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read response for %q from %s: %w", cmd, session.addr, err)
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return "", fmt.Errorf("empty response for %q from %s", cmd, session.addr)
	}
	return line, nil
}

func (session *nntpCommandSession) close() {
	_ = session.conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = session.conn.Write([]byte("QUIT\r\n"))
	_ = session.conn.Close()
}

func sendNntpCommandToOnceWithAuth(host, port, cmd string, authenticate bool) (string, error) {
	session, err := openNntpCommandSession(host, port, authenticate)
	if err != nil {
		return "", err
	}
	defer session.close()
	return session.send(cmd)
}

func resetNntpServer(host, port, label string) error {
	resp, err := sendNntpCommandToWithRetry(host, port, "CHAOS off", 10)
	if err != nil {
		return fmt.Errorf("reset %s chaos: %w", label, err)
	}
	if !strings.HasPrefix(resp, "290") {
		return fmt.Errorf("reset %s chaos: unexpected response %q", label, resp)
	}
	log.Printf("%s chaos reset: %s", label, resp)
	if _, err := sendNntpCommandToWithRetry(host, port, "RELOAD", 3); err != nil {
		log.Printf("warning: %s reload failed: %v", label, err)
	}
	return nil
}

func backupNntpRunning() bool {
	return dockerContainerRunning("nntp2")
}

func ensureNntpChaosOff() error {
	if err := resetNntpServer(nntpHost(), nntpPort(), "primary NNTP"); err != nil {
		return err
	}
	if backupNntpRunning() {
		if err := resetNntpServer(nntpHost(), backupNntpPort(), "backup NNTP"); err != nil {
			return err
		}
	}
	return nil
}

type containerEncryptionKeyState struct {
	Fingerprint string
	Mode        string
}

var encryptionKeyFingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

const e2eContainerEncryptionKeyPath = "/data/encryption.key"

func cmdContainerRestartTest() {
	if err := os.Setenv("E2E_WEAVER_ENCRYPTION_KEY", ""); err != nil {
		log.Fatalf("clear fixed Weaver encryption key for container restart test: %v", err)
	}

	emitProgressEvent(progressEvent{Kind: "phase_total", Total: 2, Detail: "Docker boot and restart"})
	if err := dockerComposeUp("nntp", "nntp2", "newznab", "weaver"); err != nil {
		log.Fatalf("start Docker Weaver restart stack: %v", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		log.Fatalf("refresh runtime ports after starting Docker Weaver: %v", err)
	}

	weaverURL := strings.TrimRight(defaultWeaverURL(), "/") + "/"
	waitForHTTP(weaverURL, 2*time.Minute)
	containerID, err := dockerComposeServiceContainerID("weaver")
	if err != nil {
		log.Fatalf("resolve fresh Weaver container: %v", err)
	}
	before, err := inspectContainerEncryptionKeyState(containerID)
	if err != nil {
		log.Fatalf("inspect fresh Weaver encryption key: %v", err)
	}
	if before.Mode != "600" {
		log.Fatalf("fresh Weaver encryption key mode = %s, want 600", before.Mode)
	}
	freshLogs, err := dockerContainerLogs(containerID)
	if err != nil {
		log.Fatalf("read fresh Weaver container logs: %v", err)
	}
	if !strings.Contains(freshLogs, "persisted encryption master key in key file") {
		log.Fatal("fresh Weaver container did not report persisting its encryption key")
	}
	emitProgressEvent(progressEvent{Kind: "phase_progress", Current: 1, Total: 2, Status: "pass", Detail: "fresh container key persisted"})

	if err := dockerComposeRestart("weaver"); err != nil {
		log.Fatalf("restart Docker Weaver service: %v", err)
	}
	waitForHTTP(weaverURL, 2*time.Minute)

	restartedContainerID, err := dockerComposeServiceContainerID("weaver")
	if err != nil {
		log.Fatalf("resolve restarted Weaver container: %v", err)
	}
	if restartedContainerID != containerID {
		log.Fatalf("Docker restart replaced Weaver container: before=%s after=%s", containerID, restartedContainerID)
	}
	after, err := inspectContainerEncryptionKeyState(restartedContainerID)
	if err != nil {
		log.Fatalf("inspect restarted Weaver encryption key: %v", err)
	}
	if after.Mode != "600" {
		log.Fatalf("restarted Weaver encryption key mode = %s, want 600", after.Mode)
	}
	if after.Fingerprint != before.Fingerprint {
		log.Fatalf("Docker restart changed Weaver encryption key fingerprint: before=%s after=%s", before.Fingerprint, after.Fingerprint)
	}
	restartedLogs, err := dockerContainerLogs(restartedContainerID)
	if err != nil {
		log.Fatalf("read restarted Weaver container logs: %v", err)
	}
	if !strings.Contains(restartedLogs, "using encryption master key from key file") {
		log.Fatal("restarted Weaver container did not report reusing its persisted encryption key")
	}

	emitProgressEvent(progressEvent{Kind: "phase_progress", Current: 2, Total: 2, Status: "pass", Detail: "restarted container reused key"})
	log.Printf("Docker Weaver restart preserved %s (%s, mode %s)", e2eContainerEncryptionKeyPath, after.Fingerprint, after.Mode)
}

func inspectContainerEncryptionKeyState(containerID string) (containerEncryptionKeyState, error) {
	fingerprintOutput, err := dockerExecOutput(containerID, "sha256sum", e2eContainerEncryptionKeyPath)
	if err != nil {
		return containerEncryptionKeyState{}, err
	}
	modeOutput, err := dockerExecOutput(containerID, "stat", "-c", "%a", e2eContainerEncryptionKeyPath)
	if err != nil {
		return containerEncryptionKeyState{}, err
	}
	return parseContainerEncryptionKeyState(fingerprintOutput, modeOutput)
}

func dockerContainerLogs(containerID string) (string, error) {
	cmd := exec.Command("docker", "logs", containerID)
	cmd.Dir = e2eDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker logs %s: %w: %s", containerID, err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func dockerExecOutput(containerID string, args ...string) (string, error) {
	dockerArgs := append([]string{"exec", containerID}, args...)
	cmd := exec.Command("docker", dockerArgs...)
	cmd.Dir = e2eDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker exec %s %s: %w: %s", containerID, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func parseContainerEncryptionKeyState(fingerprintOutput, modeOutput string) (containerEncryptionKeyState, error) {
	fields := strings.Fields(fingerprintOutput)
	if len(fields) == 0 {
		return containerEncryptionKeyState{}, fmt.Errorf("empty encryption key fingerprint output")
	}
	fingerprint := strings.ToLower(fields[0])
	if !encryptionKeyFingerprintPattern.MatchString(fingerprint) {
		return containerEncryptionKeyState{}, fmt.Errorf("invalid encryption key fingerprint %q", fields[0])
	}
	mode := strings.TrimSpace(modeOutput)
	if mode == "" || strings.ContainsAny(mode, "\r\n \t") {
		return containerEncryptionKeyState{}, fmt.Errorf("invalid encryption key mode %q", modeOutput)
	}
	return containerEncryptionKeyState{Fingerprint: fingerprint, Mode: mode}, nil
}

func dockerComposeUp(services ...string) error {
	if requiresWeaverService(services) {
		if err := ensureLocalWeaverImage(); err != nil {
			return err
		}
	}
	const maxPortBindRetries = 2
	for attempt := 0; ; attempt++ {
		log.Printf("starting docker services: %s", strings.Join(services, ", "))
		args := append(dockerComposeArgs("up", "-d", "--quiet-pull"), services...)
		cmd := exec.Command("docker", args...)
		cmd.Dir = e2eDir()
		err := runExternalCommand(cmd, "docker compose up")
		if err == nil || !isDockerHostPortBindCollision(err) || attempt == maxPortBindRetries {
			return err
		}
		if retryErr := reallocateRuntimePortsForDockerRetry(); retryErr != nil {
			return fmt.Errorf("%w; reallocate runtime ports for retry: %v", err, retryErr)
		}
		log.Printf("docker host-port collision; retrying compose with fresh runtime ports (attempt %d/%d)", attempt+1, maxPortBindRetries)
	}
}

func requiresWeaverService(services []string) bool {
	for _, service := range services {
		if service == "weaver" {
			return true
		}
	}
	return false
}

func dockerComposeRestart(services ...string) error {
	log.Printf("restarting docker services: %s", strings.Join(services, ", "))
	args := append(dockerComposeArgs("restart"), services...)
	cmd := exec.Command("docker", args...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "docker compose restart")
}

func dockerImageExists(image string) bool {
	if strings.TrimSpace(image) == "" {
		return false
	}
	cmd := exec.Command("docker", "image", "inspect", image)
	cmd.Dir = e2eDir()
	return cmd.Run() == nil
}

func dockerComposeDown() error {
	cmd := exec.Command("docker", dockerComposeArgs("down", "-v", "--remove-orphans")...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "docker compose down")
}

func ensureBackupNntpReady() error {
	if !backupNntpRunning() {
		log.Println("starting backup NNTP container...")
		if err := dockerComposeUp("nntp2"); err != nil {
			return fmt.Errorf("start backup NNTP: %w", err)
		}
		if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
			return fmt.Errorf("refresh runtime ports after starting backup NNTP: %w", err)
		}
	}
	waitForTCP("localhost:"+backupNntpPort(), 15*time.Second)
	if err := resetNntpServer(nntpHost(), backupNntpPort(), "backup NNTP"); err != nil {
		return err
	}
	if err := syncArticlesToBackup(); err != nil {
		return fmt.Errorf("sync backup NNTP articles: %w", err)
	}
	return nil
}

// --- toxiproxy helpers ---

func toxiproxyURL() string {
	if value := strings.TrimSpace(os.Getenv("TOXIPROXY_URL")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return fmt.Sprintf("http://localhost:%s", os.Getenv("E2E_TOXIPROXY_API_PORT"))
}

func findWeaverBin() string {
	if configured := strings.TrimSpace(os.Getenv("WEAVER_BIN")); configured != "" {
		return absolutePath(configured)
	}
	bin, err := ensureE2EWeaverBinary()
	if err != nil {
		log.Fatalf("build e2e weaver binary: %v", err)
	}
	return bin
}

// rustupCargo returns the cargo the weaver build should use: rustup's shim,
// by absolute path, whenever one is installed.
//
// The shim is what reads `rust-toolchain.toml`. A bare `cargo` only reaches it
// if the shim directory wins on PATH — and it does not have to. A toolchain's
// own bin directory placed ahead of it resolves `cargo` straight to that
// toolchain's real binary, which has no idea the repository pinned anything,
// and no amount of `rustup run` or `RUSTUP_TOOLCHAIN` changes that because
// both are read by the shim being bypassed.
//
// The failure is not subtle when it lands, but it is very indirect: a bare
// `cargo` resolves to an older toolchain than the tree pins, `cargo build`
// refuses with "rustc X is not supported by the following packages", and
// *every* phase dies at `ensureE2EWeaverBinary` — the phases that skip
// seeding instantly and the rest a few minutes later, which reads like several
// broken phases rather than one broken build.
//
// So ask rustup instead of guessing. `rustup which cargo`, evaluated in the
// weaver repository, resolves `rust-toolchain.toml` and prints the absolute
// cargo for the pinned toolchain — no assumption about where the shim lives
// (a package-manager rustup may put it under its own prefix rather than
// `~/.cargo/bin`) and none about PATH order.
//
// Returns that cargo *and* the directory holding it, because naming the cargo
// is not sufficient on its own: cargo finds `rustc` by searching PATH, so the
// pinned 1.97.1 cargo invoked with this PATH still drove the 1.96.0 rustc and
// failed identically. The caller puts the returned directory at the front of
// the child's PATH so both halves of the toolchain agree.
//
// Empty strings when rustup is absent or cannot answer: the build then uses a
// bare `cargo` and an unmodified PATH, exactly as it did before this existed,
// which is correct for a machine that installed Rust some other way.
func rustupPinnedToolchain() (cargoPath string, binDir string) {
	rustup, err := exec.LookPath("rustup")
	if err != nil {
		return "", ""
	}
	probe := exec.Command(rustup, "which", "cargo")
	probe.Dir = weaverRepoPath()
	out, err := probe.Output()
	if err != nil {
		return "", ""
	}
	resolved := strings.TrimSpace(string(out))
	if resolved == "" {
		return "", ""
	}
	if info, statErr := os.Stat(resolved); statErr != nil || info.IsDir() {
		return "", ""
	}
	return resolved, filepath.Dir(resolved)
}

// prependPathEnv returns env with dir at the front of PATH.
func prependPathEnv(env []string, dir string) []string {
	if strings.TrimSpace(dir) == "" {
		return env
	}
	out := make([]string, 0, len(env))
	replaced := false
	for _, entry := range env {
		if strings.HasPrefix(entry, "PATH=") {
			out = append(out, "PATH="+dir+string(os.PathListSeparator)+strings.TrimPrefix(entry, "PATH="))
			replaced = true
			continue
		}
		out = append(out, entry)
	}
	if !replaced {
		out = append(out, "PATH="+dir)
	}
	return out
}

func ensureE2EWeaverBinary() (string, error) {
	// Stable across runs, deliberately. A per-PID dir made every run a cold
	// optimized build and orphaned ~1.4 GB of artifacts each time. The only
	// thing it has to stay clear of is the dev `weaver/target`,
	// which a stable name outside the repo keeps just as well. If the cache
	// ever goes bad the retry below wipes it and rebuilds clean.
	targetDir := filepath.Join(os.TempDir(), "weaver-e2e-target")
	weaverBin := filepath.Join(targetDir, "e2e", "weaver")

	weaverBuildOnce.Do(func() {
		build := func() error {
			cargoPath, toolchainBin := rustupPinnedToolchain()
			if cargoPath == "" {
				cargoPath = "cargo"
			}
			cmd := exec.Command(cargoPath, "build", "--profile", "e2e", "-p", "weaver", "--locked")
			cmd.Dir = weaverRepoPath()
			cmd.Env = append(prependPathEnv(os.Environ(), toolchainBin), "CARGO_TARGET_DIR="+targetDir)
			return runExternalCommand(cmd, "cargo build --profile e2e -p weaver --locked")
		}

		log.Printf("building optimized e2e weaver binary from %s at %s", weaverRepoPath(), targetDir)
		weaverBuildErr = build()
		if weaverBuildErr == nil {
			weaverBuildPath = weaverBin
			return
		}

		if removeErr := os.RemoveAll(targetDir); removeErr != nil {
			weaverBuildErr = fmt.Errorf("%w (also failed to reset e2e target dir %s: %v)", weaverBuildErr, targetDir, removeErr)
			return
		}

		log.Printf("e2e weaver build failed; retrying with a clean target dir %s", targetDir)
		weaverBuildErr = build()
		if weaverBuildErr == nil {
			weaverBuildPath = weaverBin
		}
	})

	if weaverBuildErr != nil {
		return "", weaverBuildErr
	}
	if weaverBuildPath == "" {
		weaverBuildPath = weaverBin
	}
	if _, err := os.Stat(weaverBuildPath); err != nil {
		return "", fmt.Errorf("e2e weaver binary missing at %s: %w", weaverBuildPath, err)
	}
	return weaverBuildPath, nil
}

func ensureStandardManagedWeaver() error {
	return startStandardManagedWeaver(false)
}

func restartStandardManagedWeaverPreservingState() error {
	return startStandardManagedWeaver(true)
}

func startStandardManagedWeaver(preserveState bool) error {
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForHTTP(newznabURL()+"/admin/health", 30*time.Second)
	waitForTCP("localhost:"+backupNntpPort(), 30*time.Second)
	if weaverUsesPostgresDatastore() {
		if err := waitForWeaverPostgresReady(30 * time.Second); err != nil {
			return err
		}
	}

	weaverBin := findWeaverBin()
	weaverURL := fmt.Sprintf("http://localhost:%s", localWeaverPort())

	killWeaver()
	if !preserveState {
		cleanWeaverState()
		writeWeaverConfig(
			localWeaverConfigPath(),
			mustPortInt("NNTP_PORT", nntpPort()),
			mustPortInt("NNTP_BACKUP_PORT", backupNntpPort()),
		)
	}
	// Weaver imports the TOML into its datastore on first startup and renames it
	// to `.migrated`. Passing the original path on restart is intentional: the
	// database at that derived location remains authoritative even when the TOML
	// itself no longer exists.

	_ = os.MkdirAll(filepath.Dir(localWeaverLogPath()), 0o755)
	logFlags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if preserveState {
		logFlags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	logFile, err := os.OpenFile(localWeaverLogPath(), logFlags, 0o644)
	if err != nil {
		return fmt.Errorf("open managed weaver log: %w", err)
	}

	cmd := exec.Command(weaverBin, "--config", localWeaverConfigPath(), "serve", "--port", localWeaverPort())
	// `weaver::pipeline` matches nothing — the pipeline lives in the
	// `weaver_server_core` crate, so every `debug!` on this path has been
	// silently dropped for as long as the filter has existed. That is why three
	// separate diagnoses of the PAR2 repair guards had to be inferred from info
	// lines. Scoped to `completion` rather than the whole pipeline on purpose:
	// the full pipeline at debug is a firehose that perturbs the timing of the
	// very starvation behaviour these runs are trying to measure.
	cmd.Env = managedWeaverEnv(os.Environ(), localRunDir(), "info,weaver::pipeline=debug,weaver_server_core::pipeline::completion=debug")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start managed weaver: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(localWeaverPIDPath()), 0o755); err == nil {
		if err := os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644); err != nil {
			log.Printf("warning: write local weaver pid file: %v", err)
		}
	}
	generation := armManagedWeaverExitWatch()
	go func() {
		waitErr := cmd.Wait()
		noteManagedWeaverExit(generation, waitErr)
		_ = logFile.Close()
	}()

	setEnv("WEAVER_URL", weaverURL)
	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)
	return nil
}

// Managed-weaver liveness.
//
// Weaver's release profile is `panic = "abort"`, so one panic anywhere in the
// pipeline takes the entire server down. Every scenario still in flight then
// fails its GraphQL poll with `connection refused`, and the harness scores each
// one as `timeout` — which reads as "weaver was slow" and hides the crash
// behind the symptom of every *other* scenario. That once turned a
// 14-second abort into a 20-minute run reported as 68 timeouts.
//
// Watching the process directly is what distinguishes "slow" from "dead".
var (
	managedWeaverMu   sync.Mutex
	managedWeaverGen  int
	managedWeaverDead bool
	managedWeaverErr  error
)

// armManagedWeaverExitWatch marks a freshly started weaver as the live one and
// returns its generation. The generation is what keeps a superseded weaver —
// one killWeaver just stopped, whose Wait may not have returned yet — from
// reporting its own shutdown as the death of its replacement.
func armManagedWeaverExitWatch() int {
	managedWeaverMu.Lock()
	defer managedWeaverMu.Unlock()
	managedWeaverGen++
	managedWeaverDead = false
	managedWeaverErr = nil
	return managedWeaverGen
}

func noteManagedWeaverExit(generation int, waitErr error) {
	managedWeaverMu.Lock()
	defer managedWeaverMu.Unlock()
	if generation != managedWeaverGen {
		return
	}
	managedWeaverDead = true
	managedWeaverErr = waitErr
}

// managedWeaverDied reports whether the live managed weaver has exited, and the
// wait error if it did. A deliberate shutdown also trips this, so callers must
// only consult it while they still expect weaver to be serving.
func managedWeaverDied() (bool, error) {
	managedWeaverMu.Lock()
	defer managedWeaverMu.Unlock()
	return managedWeaverDead, managedWeaverErr
}

// managedWeaverDeathReport returns the tail of weaver's log, preferring the
// panic if there is one. The harness's own logs cannot explain an abort — only
// weaver's can — so the reason is surfaced at the point of detection rather
// than left for someone to find in an artifacts directory later.
func managedWeaverDeathReport() string {
	data, err := os.ReadFile(localWeaverLogPath())
	if err != nil {
		return fmt.Sprintf("(could not read %s: %v)", localWeaverLogPath(), err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	for i, line := range lines {
		if strings.Contains(line, "panicked at") || strings.Contains(line, "unexpected panic") {
			end := i + 6
			if end > len(lines) {
				end = len(lines)
			}
			return strings.Join(lines[i:end], "\n")
		}
	}
	const tail = 15
	if len(lines) > tail {
		lines = lines[len(lines)-tail:]
	}
	return strings.Join(lines, "\n")
}

func killWeaver() {
	pidData, err := os.ReadFile(localWeaverPIDPath())
	if err == nil {
		if pid, parseErr := strconv.Atoi(strings.TrimSpace(string(pidData))); parseErr == nil && pid > 0 {
			if process, findErr := os.FindProcess(pid); findErr == nil {
				_ = process.Signal(os.Interrupt)
				if !waitForPIDExit(pid, 10*time.Second) {
					_ = process.Kill()
				}
			}
		}
	}
	_ = os.Remove(localWeaverPIDPath())
	killWeaverListenersOnPort(localWeaverPort())
	time.Sleep(time.Second)
}

func stopManagedWeaverAfterProfileCollection() {
	if strings.TrimSpace(os.Getenv("E2E_WEAVER_PROFILE_DIR")) == "" {
		return
	}
	pidData, err := os.ReadFile(localWeaverPIDPath())
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil || pid <= 0 {
		return
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	_ = process.Signal(os.Interrupt)
	if !waitForPIDExit(pid, 30*time.Second) {
		log.Printf("warning: managed Weaver did not exit after profile collection signal; forcing shutdown")
		_ = process.Kill()
		_ = waitForPIDExit(pid, 5*time.Second)
	}
	_ = os.Remove(localWeaverPIDPath())
}

func stopManagedWeaverCommand(cmd *exec.Cmd, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		_, _ = cmd.Process.Wait()
		close(done)
	}()
	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case <-done:
		return
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
	}
}

func waitForPIDExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if !pidExists(pid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func pidExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

func killWeaverListenersOnPort(port string) {
	port = strings.TrimSpace(port)
	if port == "" {
		return
	}

	out, err := exec.Command("lsof", "-tiTCP:"+port, "-sTCP:LISTEN").Output()
	if err != nil || len(out) == 0 {
		return
	}

	for _, field := range strings.Fields(string(out)) {
		pid, parseErr := strconv.Atoi(strings.TrimSpace(field))
		if parseErr != nil || pid <= 0 {
			continue
		}
		if process, findErr := os.FindProcess(pid); findErr == nil {
			_ = process.Kill()
		}
	}
}

func cleanWeaverState() {
	root := localWeaverDir()
	_ = os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	_ = os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	os.Remove(localWeaverConfigPath() + ".migrated")
	os.Remove(filepath.Join(root, "weaver.db"))
	os.Remove(filepath.Join(root, "weaver.db-shm"))
	os.Remove(filepath.Join(root, "weaver.db-wal"))
	filepath.Walk(filepath.Join(root, "intermediate"), func(path string, info os.FileInfo, err error) error {
		if err != nil || path == filepath.Join(root, "intermediate") {
			return nil
		}
		os.RemoveAll(path)
		return nil
	})
	filepath.Walk(filepath.Join(root, "complete"), func(path string, info os.FileInfo, err error) error {
		if err != nil || path == filepath.Join(root, "complete") {
			return nil
		}
		os.RemoveAll(path)
		return nil
	})
	if weaverUsesPostgresDatastore() {
		if err := resetWeaverPostgresDatabase(); err != nil {
			log.Fatalf("reset Weaver Postgres state: %v", err)
		}
	}
}

func writeWeaverConfig(path string, port1, port2 int) {
	root := localWeaverDir()
	os.MkdirAll(filepath.Dir(path), 0o755)
	os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	config := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true

[[servers]]
id = 1
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = 8
active = true
priority = 0

[[servers]]
id = 2
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = 4
active = true
priority = 1

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "series"
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), port1, port2)
	_ = os.WriteFile(path, []byte(config), 0o644)
}

func writeAdaptiveDispatchWeaverConfig(path string, latentPort, directPort, connections int) {
	root := localWeaverDir()
	os.MkdirAll(filepath.Dir(path), 0o755)
	os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	config := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true
max_retries = 3

[[servers]]
id = 1
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = %d
active = true
priority = 0

[[servers]]
id = 2
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = %d
active = true
priority = 0

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "series"
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), latentPort, connections, directPort, connections)
	_ = os.WriteFile(path, []byte(config), 0o644)
}

// addToxic adds a toxic to a toxiproxy proxy.
// proxyName: "nntp1" or "nntp2"
// toxicName: unique name for this toxic
// toxicType: "latency", "bandwidth", "slow_close", "timeout", "reset_peer", "slicer", "limit_data"
// stream: "downstream" or "upstream"
// attrs: toxic-specific attributes
func addToxic(proxyName, toxicName, toxicType, stream string, attrs map[string]interface{}) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"name":       toxicName,
		"type":       toxicType,
		"stream":     stream,
		"toxicity":   1.0,
		"attributes": attrs,
	})
	url := fmt.Sprintf("%s/proxies/%s/toxics", toxiproxyURL(), proxyName)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("add toxic %s: %w", toxicName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add toxic %s: %s %s", toxicName, resp.Status, string(body))
	}
	return nil
}

// removeToxic removes a named toxic from a proxy.
func removeToxic(proxyName, toxicName string) {
	url := fmt.Sprintf("%s/proxies/%s/toxics/%s", toxiproxyURL(), proxyName, toxicName)
	req, _ := http.NewRequest("DELETE", url, nil)
	http.DefaultClient.Do(req)
}

// removeAllToxics removes all toxics from both proxies.
func removeAllToxics() {
	for _, proxy := range []string{"nntp1", "nntp2"} {
		url := fmt.Sprintf("%s/proxies/%s/toxics", toxiproxyURL(), proxy)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		var toxics []struct{ Name string }
		json.NewDecoder(resp.Body).Decode(&toxics)
		resp.Body.Close()
		for _, t := range toxics {
			removeToxic(proxy, t.Name)
		}
	}
}

// filterChaosScenarios returns a representative subset of scenarios for chaos
// testing. The full suite takes too long with retry delays.
func filterChaosScenarios(all []*Scenario) []*Scenario {
	return filterScenariosBySlug(all, chaosFixtureSlugs)
}

func filterTcpChaosScenarios(all []*Scenario) []*Scenario {
	return filterScenariosBySlug(all, tcpChaosFixtureSlugs)
}

func filterScenariosBySlug(all []*Scenario, slugs []string) []*Scenario {
	bySlug := make(map[string]*Scenario, len(all))
	for _, s := range all {
		bySlug[s.Slug] = s
	}
	var out []*Scenario
	for _, slug := range slugs {
		if s, ok := bySlug[slug]; ok {
			out = append(out, s)
		}
	}
	return out
}

func runChaosStatProbeScenario(
	weaverURL string,
	scenario *Scenario,
	roundName string,
	statChaos string,
) (string, error) {
	if scenario == nil {
		return "", fmt.Errorf("missing STAT probe scenario")
	}
	statChaos = strings.TrimSpace(statChaos)
	if statChaos == "" {
		return "", fmt.Errorf("missing STAT-only chaos config for probe")
	}
	log.Printf("  restarting managed Weaver before isolated STAT probe for %s", roundName)
	if err := restartStandardManagedWeaverPreservingState(); err != nil {
		return "", fmt.Errorf("restart managed weaver before %s STAT probe: %w", roundName, err)
	}

	if err := setNntpChaosOnServer(nntpHost(), nntpPort(), statChaos); err != nil {
		return "", fmt.Errorf("enable primary STAT chaos %q: %w", statChaos, err)
	}
	defer func() {
		if err := setNntpChaosOnServer(nntpHost(), nntpPort(), "off"); err != nil {
			log.Printf("  WARNING: disable primary STAT chaos after probe: %v", err)
		}
	}()

	if backupNntpRunning() {
		if err := setNntpChaosOnServer(nntpHost(), backupNntpPort(), statChaos); err != nil {
			return "", fmt.Errorf("enable backup STAT chaos %q: %w", statChaos, err)
		}
		defer func() {
			if err := setNntpChaosOnServer(nntpHost(), backupNntpPort(), "off"); err != nil {
				log.Printf("  WARNING: disable backup STAT chaos after probe: %v", err)
			}
		}()
	}

	if err := resetNntpMetrics(); err != nil {
		return "", fmt.Errorf("reset NNTP metrics for isolated STAT probe: %w", err)
	}
	log.Printf("  isolated STAT probe for %s with %q after state-preserving restart", roundName, statChaos)

	jobID, err := submitOneNZB(weaverURL, scenario)
	if err != nil {
		return "", fmt.Errorf("submit %s: %w", scenario.Slug, err)
	}
	log.Printf("  probing STAT path with %s — submitted job=%d", scenario.Slug, jobID)

	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
		if err == nil && snapshot.Found && facadeTerminalStatus(snapshot.Status) {
			actual, _ := applyTerminalStateCheck(localWeaverDBPath(), jobID, scenario.Slug, snapshot.Status)
			if actual == "" {
				actual = snapshot.Status
			}
			return actual, nil
		}
		mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("%s STAT probe", roundName))
	}

	reconciled := reconcileTerminalSnapshots(
		weaverURL,
		[]int{jobID},
		20*time.Second,
		fmt.Sprintf("%s STAT probe final reconciliation", roundName),
	)
	if snapshot, ok := reconciled[jobID]; ok {
		actual, _ := applyTerminalStateCheck(localWeaverDBPath(), jobID, scenario.Slug, snapshot.Status)
		if actual == "" {
			actual = snapshot.Status
		}
		return actual, nil
	}

	if err := cancelJobGraphQL(weaverURL, jobID); err != nil {
		log.Printf("  WARNING: cancel timed out STAT probe job %s (%d): %v", scenario.Slug, jobID, err)
	}
	return "TIMEOUT", fmt.Errorf("timeout waiting for STAT probe scenario %s", scenario.Slug)
}

func statOnlyChaosConfig(config string) string {
	var statParts []string
	for _, part := range strings.Split(config, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "stat_bad_code=") || strings.HasPrefix(part, "stat_short=") {
			statParts = append(statParts, part)
		}
	}
	return strings.Join(statParts, ",")
}

func cmdChaos(config string) {
	resp := sendNntpCommand("CHAOS " + config)
	log.Println(resp)
}

type submitNZBOptions struct {
	force bool
}

func submitOneNZB(weaverURL string, scenario *Scenario) (int, error) {
	return submitOneNZBWithOptions(weaverURL, scenario, submitNZBOptions{})
}

func submitOneNZBWithOptions(weaverURL string, scenario *Scenario, options submitNZBOptions) (int, error) {
	slug := scenario.Slug
	nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
	nzbData, err := os.ReadFile(nzbPath)
	if err != nil {
		return 0, fmt.Errorf("read NZB: %w", err)
	}

	nzbB64 := base64.StdEncoding.EncodeToString(nzbData)
	query := `mutation($input: SubmitNzbInput!) {
		submitNzb(input: $input) {
			accepted
			item {
				id
			}
		}
	}`
	input := map[string]interface{}{
		"nzbBase64": nzbB64,
		"filename":  scenario.Title + ".nzb",
		"category":  scenario.Category,
	}
	if scenario.Password != "" {
		input["password"] = scenario.Password
	}
	if options.force {
		input["force"] = true
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"query":     query,
		"variables": map[string]interface{}{"input": input},
	})

	resp, err := postGraphQL(weaverURL, payload)
	if err != nil {
		return 0, fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	var gqlResp struct {
		Data struct {
			SubmitNzb struct {
				Accepted bool `json:"accepted"`
				Item     struct {
					ID int `json:"id"`
				} `json:"item"`
			} `json:"submitNzb"`
		} `json:"data"`
		Errors []struct{ Message string } `json:"errors"`
	}
	json.Unmarshal(respBody, &gqlResp)
	if len(gqlResp.Errors) > 0 {
		return 0, fmt.Errorf("gql: %s", gqlResp.Errors[0].Message)
	}
	if !gqlResp.Data.SubmitNzb.Accepted {
		return 0, fmt.Errorf("submitNzb rejected for %s", slug)
	}
	if gqlResp.Data.SubmitNzb.Item.ID <= 0 {
		return 0, fmt.Errorf("submitNzb returned invalid job id %d for %s", gqlResp.Data.SubmitNzb.Item.ID, slug)
	}
	return gqlResp.Data.SubmitNzb.Item.ID, nil
}

func reconcileTerminalSnapshots(
	weaverURL string,
	jobIDs []int,
	timeout time.Duration,
	detail string,
) map[int]facadeItemSnapshot {
	results := make(map[int]facadeItemSnapshot, len(jobIDs))
	pending := make(map[int]struct{}, len(jobIDs))
	for _, jobID := range jobIDs {
		if jobID > 0 {
			pending[jobID] = struct{}{}
		}
	}
	if len(pending) == 0 {
		return results
	}

	deadline := time.Now().Add(timeout)
	for {
		for jobID := range pending {
			snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
			if err != nil || !snapshot.Found {
				continue
			}
			if facadeTerminalStatus(snapshot.Status) {
				results[jobID] = snapshot
				delete(pending, jobID)
			}
		}
		if len(pending) == 0 || time.Now().After(deadline) {
			return results
		}
		mustSleepWithSuspendDetection(2*time.Second, detail)
	}
}

// cmdChaosTest runs the full test suite cleanly first (baseline), then repeats
// it 5 times with different chaos configurations active. This verifies weaver
// can recover from transient NNTP failures and still complete all downloads.
func cmdChaosTest() {
	ensureStandardDockerInfrastructure()
	if err := ensureBackupNntpReady(); err != nil {
		log.Fatalf("prepare backup NNTP for chaos test: %v", err)
	}
	if err := ensureStandardManagedWeaver(); err != nil {
		log.Fatalf("restart managed weaver for chaos test: %v", err)
	}
	weaverURL := defaultWeaverURL()
	prepareStandardTestRun(weaverURL, true)
	defer func() {
		if err := ensureNntpChaosOff(); err != nil {
			log.Printf("warning: final NNTP chaos reset failed: %v", err)
		}
	}()

	// Load the success workload from the canonical fixture set, but load the
	// explicit STAT probe helper directly by slug because it is intentionally
	// seeded for chaos only and must not join the canonical success workload.
	allScenarios := loadCanonicalScenarios()
	statProbeScenario, err := loadScenario(filepath.Join(testdataDir(), chaosStatProbeSlug))
	if err != nil {
		log.Fatalf("load NNTP chaos STAT probe scenario %q: %v", chaosStatProbeSlug, err)
	}
	var scenarios []*Scenario
	for _, s := range allScenarios {
		// Only use scenarios that expect success — skip error/failure scenarios
		if s.ExpectedOutcome == "success" || s.ExpectedOutcome == "repair_then_success" {
			scenarios = append(scenarios, s)
		}
	}

	scenarios = filterChaosScenarios(scenarios)
	log.Printf("loaded %d scenarios for chaos testing", len(scenarios))

	type chaosRound struct {
		name             string
		config           string
		requireStatChaos bool
	}

	rounds := []chaosRound{
		{"baseline (no chaos)", "", false},
		{"201 greetings on all connects", "greet_201=100", false},
		{"400 greetings on 30% of connects", "greet_400=30", false},
		{"drop 30% connections", "drop_conn=30", false},
		{"reject 50% auth", "reject_auth=50", false},
		{"force BODY re-auth on 50% of requests", "reauth_body=50", false},
		{"split BODY terminator on all requests", "split_term=100", false},
		{"drop 10% of BODY responses mid-transfer", "drop_mid_body=10", false},
		{"malformed terminator on 10% of BODY responses", "bad_term=10", false},
		{"corrupt 5% bodies", "corrupt_body=5", false},
		{"timeout 10% bodies", "timeout_body=10", false},
		{"STAT bad code on all requests", "stat_bad_code=100", true},
		{"STAT short response on all requests", "stat_short=100", true},
		{"combined: STAT bad code 100% + drop mid-body 5%", "stat_bad_code=100,drop_mid_body=5", true},
		{"combined: reauth 30% + drop mid-body 5% + slow 5ms", "reauth_body=30,drop_mid_body=5,slow_body=5", false},
	}

	onlyRound := 0
	if value := os.Getenv("CHAOS_ONLY_ROUND"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > len(rounds) {
			log.Fatalf("invalid CHAOS_ONLY_ROUND=%q (expected 1-%d)", value, len(rounds))
		}
		onlyRound = parsed
		log.Printf("running only chaos round %d: %s", parsed, rounds[parsed-1].name)
	}
	totalRounds := len(rounds)
	if onlyRound != 0 {
		totalRounds = 1
	}
	phaseTotal := len(scenarios) * totalRounds
	overallResolved := 0
	emitProgressEvent(progressEvent{Kind: "phase_total", Total: phaseTotal, Detail: "NNTP chaos"})

	totalPass := 0
	totalFail := 0
	executedRounds := 0
	chaosRunRoot := filepath.Join(localRunDir(), "nntp-chaos", time.Now().Format("20060102-150405"))
	if err := os.MkdirAll(chaosRunRoot, 0o755); err != nil {
		log.Fatalf("create NNTP chaos artifact dir: %v", err)
	}

	for roundIdx, round := range rounds {
		if onlyRound != 0 && roundIdx+1 != onlyRound {
			continue
		}
		executedRounds++
		fmt.Printf("\n=== ROUND %d/%d: %s ===\n", roundIdx+1, len(rounds), round.name)
		emitProgressEvent(progressEvent{Kind: "phase_note", Detail: round.name})

		// Keep control-plane operations out of the chaos blast radius. The
		// workload below still runs with the round's chaos enabled.
		if err := ensureNntpChaosOff(); err != nil {
			log.Fatalf("reset NNTP chaos before round %q: %v", round.name, err)
		}
		if err := resetNntpMetrics(); err != nil {
			log.Fatalf("reset NNTP metrics before round %q: %v", round.name, err)
		}
		if round.config != "" {
			if err := setNntpChaos(round.config); err != nil {
				log.Fatalf("enable NNTP chaos for round %q: %v", round.name, err)
			}
			log.Printf("chaos enabled: %s", round.config)
		}

		// Submit in small batches so NNTP chaos exercises recovery behavior
		// instead of turning into a queue-length timeout test.
		type job struct {
			slug   string
			jobID  int
			status string
		}
		var jobs []job

		timeout := 3 * time.Minute
		if round.config != "" {
			timeout = 5 * time.Minute
		}
		const chaosBatchSize = 4
		for batchStart := 0; batchStart < len(scenarios); batchStart += chaosBatchSize {
			batchEnd := batchStart + chaosBatchSize
			if batchEnd > len(scenarios) {
				batchEnd = len(scenarios)
			}
			batch := scenarios[batchStart:batchEnd]
			batchJobs := make([]job, 0, len(batch))

			for _, s := range batch {
				jobID, err := submitOneNZBWithOptions(weaverURL, s, submitNZBOptions{force: roundIdx > 0})
				if err != nil {
					log.Printf("  %s: submit error: %v", s.Slug, err)
					batchJobs = append(batchJobs, job{slug: s.Slug, status: "ERROR"})
					overallResolved++
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: overallResolved,
						Total:   phaseTotal,
						Status:  "error",
						Detail:  s.Slug,
					})
					continue
				}
				batchJobs = append(batchJobs, job{slug: s.Slug, jobID: jobID})
			}

			deadline := time.Now().Add(timeout)
			pending := 0
			for _, j := range batchJobs {
				if j.status == "" {
					pending++
				}
			}

			for pending > 0 && time.Now().Before(deadline) {
				mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("NNTP chaos round %d batch polling", roundIdx+1))
				for i := range batchJobs {
					if batchJobs[i].status != "" {
						continue
					}
					s := pollJobOnce(weaverURL, batchJobs[i].jobID)
					if s == "COMPLETE" || s == "FAILED" {
						batchJobs[i].status, _ = applyTerminalStateCheck(
							localWeaverDBPath(),
							batchJobs[i].jobID,
							batchJobs[i].slug,
							s,
						)
						pending--
						overallResolved++
						emitProgressEvent(progressEvent{
							Kind:    "phase_progress",
							Current: overallResolved,
							Total:   phaseTotal,
							Status:  strings.ToLower(batchJobs[i].status),
							Detail:  batchJobs[i].slug,
						})
					}
				}
			}

			if pending > 0 {
				remainingIDs := make([]int, 0, pending)
				for _, job := range batchJobs {
					if job.status == "" && job.jobID > 0 {
						remainingIDs = append(remainingIDs, job.jobID)
					}
				}
				if len(remainingIDs) > 0 {
					log.Printf("reconciling %d unresolved NNTP chaos job(s) before timeout scoring", len(remainingIDs))
					reconciled := reconcileTerminalSnapshots(
						weaverURL,
						remainingIDs,
						20*time.Second,
						fmt.Sprintf("NNTP chaos round %d final reconciliation", roundIdx+1),
					)
					for i := range batchJobs {
						if batchJobs[i].status != "" {
							continue
						}
						snapshot, ok := reconciled[batchJobs[i].jobID]
						if !ok {
							continue
						}
						batchJobs[i].status, _ = applyTerminalStateCheck(
							localWeaverDBPath(),
							batchJobs[i].jobID,
							batchJobs[i].slug,
							snapshot.Status,
						)
						pending--
						overallResolved++
						emitProgressEvent(progressEvent{
							Kind:    "phase_progress",
							Current: overallResolved,
							Total:   phaseTotal,
							Status:  strings.ToLower(batchJobs[i].status),
							Detail:  batchJobs[i].slug,
						})
					}
				}
			}

			timedOutJobs := 0
			for i := range batchJobs {
				if batchJobs[i].status == "" {
					batchJobs[i].status = "TIMEOUT"
					timedOutJobs++
					overallResolved++
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: overallResolved,
						Total:   phaseTotal,
						Status:  "timeout",
						Detail:  batchJobs[i].slug,
					})
				}
			}

			if timedOutJobs > 0 {
				log.Printf("canceling %d timed out job(s) before next NNTP chaos batch", timedOutJobs)
				for _, j := range batchJobs {
					if j.status != "TIMEOUT" || j.jobID == 0 {
						continue
					}
					if err := cancelJobGraphQL(weaverURL, j.jobID); err != nil {
						log.Printf("  WARNING: cancel timed out job %s (%d): %v", j.slug, j.jobID, err)
						continue
					}
					if err := waitForJobCancelSettledGraphQL(
						weaverURL,
						j.jobID,
						weaverCancelSettleTimeout,
						weaverCancelSettlePollInterval,
					); err != nil {
						log.Printf("  queue snapshot after failed cancel settle: %s", describeJobsGraphQL(weaverURL))
						log.Printf("  Weaver log tail after failed cancel settle:\n%s", localWeaverLogTail(40))
						log.Fatalf("timed out NNTP chaos job %s (%d) did not settle after cancel: %v", j.slug, j.jobID, err)
					}
				}
				mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("NNTP chaos round %d batch cleanup", roundIdx+1))
			}

			jobs = append(jobs, batchJobs...)
		}

		// Score
		roundPass := 0
		roundFail := 0
		for _, j := range jobs {
			if j.status == "COMPLETE" {
				roundPass++
			} else {
				roundFail++
				log.Printf("  FAIL: %s = %s", j.slug, j.status)
			}
		}
		totalPass += roundPass
		totalFail += roundFail
		fmt.Printf("  Round result: %d/%d passed\n", roundPass, len(jobs))

		if round.requireStatChaos {
			metrics, err := fetchNntpStatMetrics("")
			if err != nil {
				log.Printf("  FAIL: fetch STAT metrics for round %q: %v", round.name, err)
				roundFail++
				totalFail++
			} else if metrics.StatChaosHits <= 0 {
				probeStatus, probeErr := runChaosStatProbeScenario(
					weaverURL,
					statProbeScenario,
					round.name,
					statOnlyChaosConfig(round.config),
				)
				if probeErr != nil {
					log.Printf("  FAIL: round %q STAT probe scenario %q: %v", round.name, statProbeScenario.Slug, probeErr)
					roundFail++
					totalFail++
				} else {
					log.Printf(
						"  STAT probe scenario %s reached terminal status %s",
						statProbeScenario.Slug,
						probeStatus,
					)
					metrics, err = fetchNntpStatMetrics("")
					if err != nil {
						log.Printf("  FAIL: fetch STAT metrics after probe scenario for round %q: %v", round.name, err)
						roundFail++
						totalFail++
					} else if metrics.StatChaosHits <= 0 {
						log.Printf("  FAIL: round %q recorded no STAT chaos hits after probe scenario %q", round.name, statProbeScenario.Slug)
						roundFail++
						totalFail++
					} else {
						log.Printf(
							"  STAT metrics: %d requests, %d chaos hits after probe scenario %s",
							len(metrics.StatCounts),
							metrics.StatChaosHits,
							statProbeScenario.Slug,
						)
					}
				}
			} else {
				log.Printf("  STAT metrics: %d requests, %d chaos hits", len(metrics.StatCounts), metrics.StatChaosHits)
			}
		}

		sendNntpCommand("CHAOS off")

		// Post-round diagnostics
		var diagIDs []int
		var diagStatuses []string
		roundArtifacts := make([]chaosRoundJobArtifact, 0, len(jobs))
		for _, j := range jobs {
			diagIDs = append(diagIDs, j.jobID)
			diagStatuses = append(diagStatuses, j.status)
			roundArtifacts = append(roundArtifacts, chaosRoundJobArtifact{
				Slug:   j.slug,
				JobID:  j.jobID,
				Status: j.status,
			})
		}
		printRoundDiagnostics(weaverURL, diagIDs, diagStatuses, true)
		writeChaosRoundArtifacts(chaosRunRoot, roundIdx+1, round.name, round.config, roundArtifacts, weaverURL)

		// Clean up weaver state between rounds — cancel active jobs and
		// delete history so stale jobs don't clog the pipeline.
		for _, j := range jobs {
			cancelJobGraphQL(weaverURL, j.jobID)
		}
		deleteAllHistoryGraphQL(weaverURL)
		time.Sleep(3 * time.Second)
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Printf("CHAOS TEST TOTAL: %d passed, %d failed across %d rounds\n", totalPass, totalFail, executedRounds)

	if totalFail > 0 {
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: phaseTotal, Total: phaseTotal, Status: "fail"})
		os.Exit(1)
	}
	emitProgressEvent(progressEvent{Kind: "phase_done", Current: phaseTotal, Total: phaseTotal, Status: "pass"})
}

// cmdTcpChaosTest runs tests through toxiproxy, injecting real TCP-level
// failures: latency, connection resets, bandwidth limits, and timeouts.
// Manages the full lifecycle: starts toxiproxy, restarts weaver with
// toxiproxy ports, runs chaos rounds, then restores original config.
func cmdTcpChaosTest() {
	weaverBin := env("WEAVER_BIN", findWeaverBin())
	weaverPort := localWeaverPort()
	weaverURL := fmt.Sprintf("http://localhost:%s", weaverPort)
	configPath := localWeaverConfigPath()

	// Ensure toxiproxy and the clean backup NNTP server are running.
	log.Println("starting toxiproxy and backup NNTP containers...")
	if err := dockerComposeUp("nntp", "newznab", "nntp2", "toxiproxy"); err != nil {
		log.Fatalf("failed to start tcp-chaos infrastructure: %v", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForHTTP(newznabURL()+"/admin/health", 30*time.Second)
	waitForTCP("localhost:"+backupNntpPort(), 15*time.Second)
	waitForHTTP(toxiproxyURL()+"/version", 15*time.Second)
	if err := ensureNntpChaosOff(); err != nil {
		log.Fatalf("reset NNTP chaos before tcp-chaos: %v", err)
	}
	syncArticlesToBackup()

	// Verify toxiproxy proxies are configured
	resp, err := http.Get(toxiproxyURL() + "/proxies")
	if err != nil {
		log.Fatalf("toxiproxy not reachable at %s: %v", toxiproxyURL(), err)
	}
	resp.Body.Close()
	removeAllToxics()
	log.Println("cleared existing toxiproxy toxics before startup")

	// Kill any existing weaver
	killWeaver()

	// Write weaver config pointing through toxiproxy
	port1 := mustPortInt("TOXIPROXY_NNTP1_PORT", toxiproxyNntp1Port())
	port2 := mustPortInt("TOXIPROXY_NNTP2_PORT", toxiproxyNntp2Port())
	log.Printf("configuring weaver to use toxiproxy ports (%d/%d)...", port1, port2)
	writeWeaverConfig(configPath, port1, port2)

	// Clean weaver state
	cleanWeaverState()

	// Start weaver
	log.Println("starting weaver...")
	weaverCmd := exec.Command(weaverBin, "--config", configPath, "serve", "--port", weaverPort)
	weaverCmd.Env = managedWeaverEnv(os.Environ(), localRunDir(), "info,weaver::pipeline=debug")
	_ = os.MkdirAll(filepath.Dir(localWeaverLogPath()), 0o755)
	logFile, _ := os.Create(localWeaverLogPath())
	weaverCmd.Stdout = logFile
	weaverCmd.Stderr = logFile
	if err := weaverCmd.Start(); err != nil {
		log.Fatalf("failed to start weaver: %v", err)
	}
	_ = os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(weaverCmd.Process.Pid)+"\n"), 0o644)
	defer func() {
		weaverCmd.Process.Kill()
		weaverCmd.Wait()
		_ = os.Remove(localWeaverPIDPath())
		logFile.Close()
	}()
	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)
	log.Println("weaver ready")

	// Load canonical success-path scenarios
	var scenarios []*Scenario
	for _, s := range loadCanonicalScenarios() {
		if s.ExpectedOutcome == "success" || s.ExpectedOutcome == "repair_then_success" {
			scenarios = append(scenarios, s)
		}
	}
	scenarios = filterTcpChaosScenarios(scenarios)
	log.Printf("loaded %d scenarios for TCP chaos testing", len(scenarios))

	type tcpChaosRound struct {
		name  string
		setup func() // add toxics
	}

	rounds := []tcpChaosRound{
		{
			name: "200ms latency + 50ms jitter on primary",
			setup: func() {
				addToxic("nntp1", "latency", "latency", "downstream", map[string]interface{}{
					"latency": 200, "jitter": 50,
				})
			},
		},
		{
			name: "reset 20% connections on primary",
			setup: func() {
				addToxic("nntp1", "reset", "reset_peer", "downstream", map[string]interface{}{
					"timeout": 500,
				})
			},
		},
		{
			name: "1MB/s bandwidth limit on primary",
			setup: func() {
				addToxic("nntp1", "bandwidth", "bandwidth", "downstream", map[string]interface{}{
					"rate": 1024, // KB/s = 1MB/s
				})
			},
		},
		{
			name: "30s timeout on primary, clean backup",
			setup: func() {
				// Toxiproxy cuts connection after 30s of data transfer.
				// Weaver's command_timeout is 60s, so this simulates a server
				// that starts responding then dies mid-transfer.
				addToxic("nntp1", "timeout", "timeout", "downstream", map[string]interface{}{
					"timeout": 30000,
				})
			},
		},
		{
			name: "combined: 100ms latency + 500KB/s limit on primary",
			setup: func() {
				addToxic("nntp1", "latency", "latency", "downstream", map[string]interface{}{
					"latency": 100, "jitter": 30,
				})
				addToxic("nntp1", "bandwidth", "bandwidth", "downstream", map[string]interface{}{
					"rate": 512, // KB/s
				})
			},
		},
	}

	onlyRound := 0
	if value := os.Getenv("TCP_CHAOS_ONLY_ROUND"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > len(rounds) {
			log.Fatalf("invalid TCP_CHAOS_ONLY_ROUND=%q (expected 1-%d)", value, len(rounds))
		}
		onlyRound = parsed
		log.Printf("running only TCP chaos round %d: %s", parsed, rounds[parsed-1].name)
	}
	totalRounds := len(rounds)
	if onlyRound != 0 {
		totalRounds = 1
	}
	phaseTotal := len(scenarios) * totalRounds
	overallResolved := 0
	emitProgressEvent(progressEvent{Kind: "phase_total", Total: phaseTotal, Detail: "TCP chaos"})

	totalPass := 0
	totalFail := 0

	for roundIdx, round := range rounds {
		if onlyRound != 0 && roundIdx+1 != onlyRound {
			continue
		}
		fmt.Printf("\n=== TCP CHAOS ROUND %d/%d: %s ===\n", roundIdx+1, len(rounds), round.name)
		emitProgressEvent(progressEvent{Kind: "phase_note", Detail: round.name})

		// Clean slate
		removeAllToxics()
		round.setup()
		log.Printf("toxics configured for round %d", roundIdx+1)

		// Submit in small batches so TCP chaos exercises failover behavior
		// instead of turning into a queue-length timeout test.
		type job struct {
			slug   string
			jobID  int
			status string
		}
		var jobs []job
		const tcpChaosBatchSize = 4
		for batchStart := 0; batchStart < len(scenarios); batchStart += tcpChaosBatchSize {
			batchEnd := batchStart + tcpChaosBatchSize
			if batchEnd > len(scenarios) {
				batchEnd = len(scenarios)
			}
			batch := scenarios[batchStart:batchEnd]
			batchJobs := make([]job, 0, len(batch))

			for _, s := range batch {
				jobID, err := submitOneNZBWithOptions(weaverURL, s, submitNZBOptions{force: roundIdx > 0})
				if err != nil {
					log.Printf("  %s: submit error: %v", s.Slug, err)
					batchJobs = append(batchJobs, job{slug: s.Slug, status: "ERROR"})
					overallResolved++
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: overallResolved,
						Total:   phaseTotal,
						Status:  "error",
						Detail:  s.Slug,
					})
					continue
				}
				batchJobs = append(batchJobs, job{slug: s.Slug, jobID: jobID})
			}

			// Keep the timeout generous enough to allow real failover work to
			// finish, but scope it to the active batch instead of the whole round.
			deadline := time.Now().Add(10 * time.Minute)
			pending := 0
			for _, j := range batchJobs {
				if j.status == "" {
					pending++
				}
			}

			for pending > 0 && time.Now().Before(deadline) {
				mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("TCP chaos round %d batch polling", roundIdx+1))
				for i := range batchJobs {
					if batchJobs[i].status != "" {
						continue
					}
					s := pollJobOnce(weaverURL, batchJobs[i].jobID)
					if s == "COMPLETE" || s == "FAILED" {
						batchJobs[i].status, _ = applyTerminalStateCheck(
							localWeaverDBPath(),
							batchJobs[i].jobID,
							batchJobs[i].slug,
							s,
						)
						pending--
						overallResolved++
						emitProgressEvent(progressEvent{
							Kind:    "phase_progress",
							Current: overallResolved,
							Total:   phaseTotal,
							Status:  strings.ToLower(batchJobs[i].status),
							Detail:  batchJobs[i].slug,
						})
					}
				}
			}

			if pending > 0 {
				remainingIDs := make([]int, 0, pending)
				for _, job := range batchJobs {
					if job.status == "" && job.jobID > 0 {
						remainingIDs = append(remainingIDs, job.jobID)
					}
				}
				if len(remainingIDs) > 0 {
					log.Printf("reconciling %d unresolved TCP chaos job(s) before timeout scoring", len(remainingIDs))
					reconciled := reconcileTerminalSnapshots(
						weaverURL,
						remainingIDs,
						20*time.Second,
						fmt.Sprintf("TCP chaos round %d final reconciliation", roundIdx+1),
					)
					for i := range batchJobs {
						if batchJobs[i].status != "" {
							continue
						}
						snapshot, ok := reconciled[batchJobs[i].jobID]
						if !ok {
							continue
						}
						batchJobs[i].status, _ = applyTerminalStateCheck(
							localWeaverDBPath(),
							batchJobs[i].jobID,
							batchJobs[i].slug,
							snapshot.Status,
						)
						pending--
						overallResolved++
						emitProgressEvent(progressEvent{
							Kind:    "phase_progress",
							Current: overallResolved,
							Total:   phaseTotal,
							Status:  strings.ToLower(batchJobs[i].status),
							Detail:  batchJobs[i].slug,
						})
					}
				}
			}

			timedOutJobs := 0
			for i := range batchJobs {
				if batchJobs[i].status == "" {
					batchJobs[i].status = "TIMEOUT"
					timedOutJobs++
					overallResolved++
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: overallResolved,
						Total:   phaseTotal,
						Status:  "timeout",
						Detail:  batchJobs[i].slug,
					})
				}
			}

			if timedOutJobs > 0 {
				log.Printf("canceling %d timed out job(s) before next TCP chaos batch", timedOutJobs)
				for _, j := range batchJobs {
					if j.status != "TIMEOUT" || j.jobID == 0 {
						continue
					}
					if err := cancelJobGraphQL(weaverURL, j.jobID); err != nil {
						log.Printf("  WARNING: cancel timed out job %s (%d): %v", j.slug, j.jobID, err)
						continue
					}
					if err := waitForJobCancelSettledGraphQL(
						weaverURL,
						j.jobID,
						weaverCancelSettleTimeout,
						weaverCancelSettlePollInterval,
					); err != nil {
						log.Printf("  queue snapshot after failed cancel settle: %s", describeJobsGraphQL(weaverURL))
						log.Printf("  Weaver log tail after failed cancel settle:\n%s", localWeaverLogTail(40))
						log.Fatalf("timed out TCP chaos job %s (%d) did not settle after cancel: %v", j.slug, j.jobID, err)
					}
				}
				mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("TCP chaos round %d batch cleanup", roundIdx+1))
			}

			jobs = append(jobs, batchJobs...)
		}

		// Remove toxics before scoring
		removeAllToxics()

		// Score
		roundPass := 0
		for _, j := range jobs {
			if j.status == "COMPLETE" {
				roundPass++
			} else {
				log.Printf("  FAIL: %s = %s", j.slug, j.status)
			}
		}
		totalPass += roundPass
		totalFail += len(jobs) - roundPass
		fmt.Printf("  Round result: %d/%d passed\n", roundPass, len(jobs))

		// Post-round diagnostics
		var diagIDs []int
		var diagStatuses []string
		for _, j := range jobs {
			diagIDs = append(diagIDs, j.jobID)
			diagStatuses = append(diagStatuses, j.status)
		}
		printRoundDiagnostics(weaverURL, diagIDs, diagStatuses, false)

		// Cleanup between rounds
		for _, j := range jobs {
			cancelJobGraphQL(weaverURL, j.jobID)
		}
		deleteAllHistoryGraphQL(weaverURL)
		time.Sleep(3 * time.Second)
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Printf("TCP CHAOS TEST TOTAL: %d passed, %d failed across %d rounds\n", totalPass, totalFail, totalRounds)

	if totalFail > 0 {
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: phaseTotal, Total: phaseTotal, Status: "fail"})
		os.Exit(1)
	}
	emitProgressEvent(progressEvent{Kind: "phase_done", Current: phaseTotal, Total: phaseTotal, Status: "pass"})
}

// cmdAdaptiveDispatchTest verifies that Weaver's latency-aware server ordering
// prefers the lower-latency server within a priority group.
func cmdAdaptiveDispatchTest() {
	scenarioSlug := strings.TrimSpace(env("ADAPTIVE_DISPATCH_SCENARIO", "large-segments"))
	latencyMs := envInt("ADAPTIVE_DISPATCH_LATENCY_MS", 75)
	jitterMs := envInt("ADAPTIVE_DISPATCH_JITTER_MS", 10)
	connections := envInt("ADAPTIVE_DISPATCH_CONNECTIONS", 8)
	minDirectPct := envInt("ADAPTIVE_DISPATCH_MIN_DIRECT_PCT", 60)
	sampleIntervalMs := envInt("ADAPTIVE_DISPATCH_SAMPLE_MS", 250)
	timeoutSec := envInt("ADAPTIVE_DISPATCH_TIMEOUT_SEC", 300)
	if scenarioSlug == "" {
		log.Fatalf("ADAPTIVE_DISPATCH_SCENARIO must not be empty")
	}
	if latencyMs <= 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_LATENCY_MS must be positive")
	}
	if jitterMs < 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_JITTER_MS must not be negative")
	}
	if connections <= 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_CONNECTIONS must be positive")
	}
	if minDirectPct < 1 || minDirectPct > 100 {
		log.Fatalf("ADAPTIVE_DISPATCH_MIN_DIRECT_PCT must be between 1 and 100")
	}
	if sampleIntervalMs <= 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_SAMPLE_MS must be positive")
	}
	if timeoutSec <= 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_TIMEOUT_SEC must be positive")
	}

	weaverBin := env("WEAVER_BIN", findWeaverBin())
	weaverPort := localWeaverPort()
	weaverURL := fmt.Sprintf("http://localhost:%s", weaverPort)
	configPath := localWeaverConfigPath()
	outputDir := filepath.Join(localRunDir(), "adaptive-dispatch")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create adaptive-dispatch output dir: %v", err)
	}

	log.Println("starting adaptive-dispatch infrastructure...")
	if err := ensureSeedingInfrastructureErr(); err != nil {
		log.Fatalf("start seeding infrastructure: %v", err)
	}
	if err := dockerComposeUp("nntp2", "toxiproxy"); err != nil {
		log.Fatalf("start adaptive-dispatch backup/proxy infrastructure: %v", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		log.Fatalf("refresh runtime ports after starting adaptive-dispatch infrastructure: %v", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForHTTP(newznabURL()+"/admin/health", 30*time.Second)
	waitForTCP("localhost:"+backupNntpPort(), 30*time.Second)
	waitForHTTP(toxiproxyURL()+"/version", 15*time.Second)
	if err := ensureNntpChaosOff(); err != nil {
		log.Fatalf("reset NNTP chaos before adaptive-dispatch: %v", err)
	}
	removeAllToxics()
	defer removeAllToxics()

	scenarioDir := filepath.Join(testdataDir(), scenarioSlug)
	scenario, err := loadScenario(scenarioDir)
	if err != nil {
		log.Fatalf("load adaptive-dispatch scenario %s: %v", scenarioSlug, err)
	}
	if scenario.ExpectedOutcome != "success" {
		log.Fatalf("adaptive-dispatch scenario %s must be a success-path fixture, got %q", scenario.Slug, scenario.ExpectedOutcome)
	}
	if err := seedFixtureWithRetry(scenarioDir, envInt("ADAPTIVE_DISPATCH_SEED_RETRIES", 3)); err != nil {
		log.Fatalf("seed adaptive-dispatch scenario %s: %v", scenario.Slug, err)
	}
	syncArticlesToBackup()

	killWeaver()
	cleanWeaverState()

	latentPort := mustPortInt("TOXIPROXY_NNTP1_PORT", toxiproxyNntp1Port())
	directPort := mustPortInt("NNTP_BACKUP_PORT", backupNntpPort())
	writeAdaptiveDispatchWeaverConfig(configPath, latentPort, directPort, connections)
	if err := addToxic("nntp1", "adaptive-latency", "latency", "downstream", map[string]interface{}{
		"latency": latencyMs,
		"jitter":  jitterMs,
	}); err != nil {
		log.Fatalf("add adaptive-dispatch latency toxic: %v", err)
	}
	log.Printf(
		"adaptive-dispatch config: server1=toxiproxy:%d +%dms/%dms jitter, server2=nntp2:%d direct, connections=%d",
		latentPort,
		latencyMs,
		jitterMs,
		directPort,
		connections,
	)

	log.Println("starting weaver for adaptive-dispatch...")
	weaverCmd := exec.Command(weaverBin, "--config", configPath, "serve", "--port", weaverPort)
	weaverCmd.Env = managedWeaverEnv(os.Environ(), localRunDir(), "info,weaver::pipeline=debug,weaver_nntp=debug")
	_ = os.MkdirAll(filepath.Dir(localWeaverLogPath()), 0o755)
	logFile, err := os.Create(localWeaverLogPath())
	if err != nil {
		log.Fatalf("create managed weaver log: %v", err)
	}
	weaverCmd.Stdout = logFile
	weaverCmd.Stderr = logFile
	if err := weaverCmd.Start(); err != nil {
		_ = logFile.Close()
		log.Fatalf("start adaptive-dispatch weaver: %v", err)
	}
	_ = os.MkdirAll(filepath.Dir(localWeaverPIDPath()), 0o755)
	_ = os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(weaverCmd.Process.Pid)+"\n"), 0o644)
	defer func() {
		stopManagedWeaverCommand(weaverCmd, 30*time.Second)
		_ = os.Remove(localWeaverPIDPath())
		_ = logFile.Close()
	}()

	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)
	prepareStandardTestRun(weaverURL, true)
	if err := resetNntpMetrics(); err != nil {
		log.Fatalf("reset NNTP metrics before adaptive-dispatch workload: %v", err)
	}

	run := runDownloadBenchIteration(
		weaverURL,
		scenario,
		1,
		outputDir,
		time.Duration(sampleIntervalMs)*time.Millisecond,
		time.Duration(timeoutSec)*time.Second,
	)
	log.Printf(
		"adaptive-dispatch %s: status=%s duration=%s first_byte=%s all_bytes=%s",
		run.Scenario,
		run.Status,
		formatMilliseconds(run.DurationMs),
		formatOptionalMilliseconds(run.TimeToFirstByteMs),
		formatOptionalMilliseconds(run.TimeToAllBytesMs),
	)
	if run.Status != "COMPLETE" {
		log.Printf("Weaver log tail after adaptive-dispatch failure:\n%s", localWeaverLogTail(60))
		os.Exit(1)
	}

	metricsPrefix := strings.TrimSpace(env("ADAPTIVE_DISPATCH_MESSAGE_PREFIX", fmt.Sprintf("e2e-%s-", scenario.Slug)))
	latentMetrics, err := fetchNntpBodyMetricsFrom(nntpHost(), nntpPort(), metricsPrefix)
	if err != nil {
		log.Fatalf("fetch latent NNTP BODY metrics: %v", err)
	}
	directMetrics, err := fetchNntpBodyMetricsFrom(nntpHost(), backupNntpPort(), metricsPrefix)
	if err != nil {
		log.Fatalf("fetch direct NNTP BODY metrics: %v", err)
	}
	latentFetches := totalBodyFetches(latentMetrics)
	directFetches := totalBodyFetches(directMetrics)
	totalFetches := latentFetches + directFetches
	directPct := 0.0
	if totalFetches > 0 {
		directPct = float64(directFetches) * 100.0 / float64(totalFetches)
	}

	summaryPath := filepath.Join(outputDir, "adaptive-dispatch-summary.json")
	summary := map[string]interface{}{
		"scenario":          scenario.Slug,
		"message_prefix":    metricsPrefix,
		"latency_ms":        latencyMs,
		"jitter_ms":         jitterMs,
		"connections":       connections,
		"latent_body_count": latentFetches,
		"direct_body_count": directFetches,
		"total_body_count":  totalFetches,
		"direct_pct":        directPct,
		"min_direct_pct":    minDirectPct,
		"run":               run,
	}
	if data, err := json.MarshalIndent(summary, "", "  "); err != nil {
		log.Printf("warning: marshal adaptive-dispatch summary: %v", err)
	} else if err := os.WriteFile(summaryPath, data, 0o644); err != nil {
		log.Printf("warning: write adaptive-dispatch summary: %v", err)
	} else {
		log.Printf("adaptive-dispatch summary written to %s", summaryPath)
	}

	fmt.Printf(
		"\nADAPTIVE DISPATCH: latent=%d direct=%d total=%d direct=%.1f%% (minimum %d%%)\n",
		latentFetches,
		directFetches,
		totalFetches,
		directPct,
		minDirectPct,
	)
	if totalFetches < 20 {
		log.Printf("FAIL: only %d filtered BODY fetches were observed for prefix %q", totalFetches, metricsPrefix)
		os.Exit(1)
	}
	if directFetches <= latentFetches || directPct < float64(minDirectPct) {
		log.Printf(
			"FAIL: non-latent server was not materially preferred (latent=%d direct=%d direct=%.1f%%)",
			latentFetches,
			directFetches,
			directPct,
		)
		log.Printf("Weaver log tail after adaptive-dispatch preference failure:\n%s", localWeaverLogTail(80))
		os.Exit(1)
	}
}

func totalBodyFetches(metrics restartNntpMetrics) int {
	total := 0
	for _, count := range metrics.BodyCounts {
		total += count
	}
	return total
}

// cmdTlsTest exercises weaver's TLS NNTP path with a custom CA cert.
// Runs a small subset of scenarios (single-mkv, rar5-single, 7z-encrypted,
// gzip-single, zip-encrypted) over the TLS port to verify negotiation works.
func cmdTlsTest() {
	weaverBin := env("WEAVER_BIN", findWeaverBin())
	weaverPort := localWeaverPort()
	weaverURL := fmt.Sprintf("http://localhost:%s", weaverPort)
	configPath := localWeaverConfigPath()
	caPath := filepath.Join(localWeaverDir(), "nntp-ca.pem")

	ensureStandardDockerInfrastructure()

	// Extract CA cert from NNTP container
	log.Println("extracting NNTP CA cert...")
	containerID, err := dockerComposeServiceContainerID("nntp")
	if err != nil {
		log.Fatalf("resolve NNTP container: %v", err)
	}
	_ = os.MkdirAll(filepath.Dir(caPath), 0o755)
	extractCA := exec.Command("docker", "cp", containerID+":/certs/ca.pem", caPath)
	if err := extractCA.Run(); err != nil {
		log.Fatalf("failed to extract CA cert: %v", err)
	}
	caPem, err := os.ReadFile(caPath)
	if err != nil {
		log.Fatalf("read extracted CA cert: %v", err)
	}
	log.Printf("  CA cert written to %s (%d bytes)", caPath, len(caPem))

	// Kill existing weaver, clean state
	killWeaver()
	cleanWeaverState()

	// Write config with TLS server on the runtime-assigned host TLS port.
	root := localWeaverDir()
	os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	tlsPort := nntpTLSPort()
	tlsConfig := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true

[[servers]]
id = 1
host = "localhost"
port = %s
tls = true
tls_ca_cert = "%s"
username = %q
password = %q
connections = 4
active = true
priority = 0

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "series"
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), tlsPort, caPath, env("E2E_NNTP_USERNAME", "e2e-user"), env("E2E_NNTP_PASSWORD", "e2e-pass"))
	os.WriteFile(configPath, []byte(tlsConfig), 0o600)

	// Start weaver
	log.Println("starting weaver with TLS NNTP config...")
	weaverCmd := exec.Command(weaverBin, "--config", configPath, "serve", "--port", weaverPort)
	weaverCmd.Env = managedWeaverEnv(os.Environ(), localRunDir(), "info,weaver::pipeline=debug,weaver_nntp=debug")
	_ = os.MkdirAll(filepath.Dir(localWeaverLogPath()), 0o755)
	logFile, _ := os.Create(localWeaverLogPath())
	weaverCmd.Stdout = logFile
	weaverCmd.Stderr = logFile
	if err := weaverCmd.Start(); err != nil {
		log.Fatalf("failed to start weaver: %v", err)
	}
	_ = os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(weaverCmd.Process.Pid)+"\n"), 0o644)
	defer func() {
		weaverCmd.Process.Kill()
		weaverCmd.Wait()
		_ = os.Remove(localWeaverPIDPath())
		logFile.Close()
	}()
	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)
	log.Println("weaver ready (TLS mode)")

	// Small subset of scenarios to exercise TLS
	tlsSlugs := []string{"single-mkv", "rar5-single", "7z-encrypted", "gzip-single", "zip-encrypted"}
	if configured := strings.TrimSpace(os.Getenv("E2E_TLS_SCENARIOS")); configured != "" {
		tlsSlugs = nil
		for _, slug := range strings.Split(configured, ",") {
			if slug = strings.TrimSpace(slug); slug != "" {
				tlsSlugs = append(tlsSlugs, slug)
			}
		}
		if len(tlsSlugs) == 0 {
			log.Fatal("E2E_TLS_SCENARIOS did not contain a scenario slug")
		}
	}

	var scenarios []*Scenario
	for _, slug := range tlsSlugs {
		s, err := loadScenario(filepath.Join(testdataDir(), slug))
		if err != nil {
			log.Printf("  WARNING: scenario %s not found: %v", slug, err)
			continue
		}
		scenarios = append(scenarios, s)
	}
	log.Printf("running %d scenarios over TLS...", len(scenarios))

	// Submit all
	type job struct {
		slug   string
		jobID  int
		status string
	}
	var jobs []job
	for _, s := range scenarios {
		jobID, err := submitOneNZB(weaverURL, s)
		if err != nil {
			log.Printf("  %s: submit error: %v", s.Slug, err)
			jobs = append(jobs, job{slug: s.Slug, status: "ERROR"})
			continue
		}
		jobs = append(jobs, job{slug: s.Slug, jobID: jobID})
	}

	// Poll
	deadline := time.Now().Add(120 * time.Second)
	pending := 0
	for _, j := range jobs {
		if j.status == "" {
			pending++
		}
	}
	for pending > 0 && time.Now().Before(deadline) {
		mustSleepWithSuspendDetection(2*time.Second, "TLS test polling")
		for i := range jobs {
			if jobs[i].status != "" {
				continue
			}
			s := pollJobOnce(weaverURL, jobs[i].jobID)
			if s == "COMPLETE" || s == "FAILED" {
				jobs[i].status, _ = applyTerminalStateCheck(localWeaverDBPath(), jobs[i].jobID, jobs[i].slug, s)
				pending--
			}
		}
	}
	for i := range jobs {
		if jobs[i].status == "" {
			jobs[i].status = "TIMEOUT"
		}
	}

	// Score
	passed := 0
	for _, j := range jobs {
		if j.status == "COMPLETE" {
			passed++
			log.Printf("  PASS: %s", j.slug)
		} else {
			log.Printf("  FAIL: %s = %s", j.slug, j.status)
		}
	}

	fmt.Printf("\nTLS TEST: %d/%d passed\n", passed, len(jobs))
	if passed != len(jobs) {
		os.Exit(1)
	}
}

// printRoundDiagnostics queries weaver for per-job health details and
// greps the log file for retry/failover events to show how weaver handled
// the chaos round.
func printRoundDiagnostics(weaverURL string, jobIDs []int, statuses []string, useDockerLogs bool) {
	var totalHealth float64
	var healthCount int
	var failedJobs, degradedJobs int

	for i, jobID := range jobIDs {
		if jobID == 0 {
			continue
		}
		snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
		if err != nil {
			continue
		}
		if !snapshot.Found {
			continue
		}
		h := float64(snapshot.Health)
		totalHealth += h
		healthCount++
		if statuses[i] == "FAILED" || statuses[i] == "TIMEOUT" {
			failedJobs++
		}
		if h < 100 && h > 0 {
			degradedJobs++
		}
	}

	avgHealth := 0.0
	if healthCount > 0 {
		avgHealth = totalHealth / float64(healthCount)
	}

	// Grep weaver log for key events.
	logStr := readWeaverLogForDiagnostics(useDockerLogs)

	retries := strings.Count(logStr, "decode failed \xe2\x80\x94 re-downloading")
	failovers := strings.Count(logStr, "transient error, trying next server") + strings.Count(logStr, "soft timeout, trying next server")
	permFails := strings.Count(logStr, "decode failed permanently")
	connResets := strings.Count(logStr, "connection reset") + strings.Count(logStr, "broken pipe")

	fmt.Printf("  Diagnostics: avg_health=%.1f%% degraded=%d failed=%d\n",
		avgHealth/10.0, degradedJobs, failedJobs)
	fmt.Printf("  Log events: decode_retries=%d server_failovers=%d perm_failures=%d conn_resets=%d\n",
		retries, failovers, permFails, connResets)

	if !useDockerLogs {
		_ = os.Truncate(localWeaverLogPath(), 0)
	}
}

func readWeaverLogForDiagnostics(useDockerLogs bool) string {
	if useDockerLogs {
		containerID, err := dockerComposeServiceContainerID("weaver")
		if err != nil {
			return ""
		}
		cmd := exec.Command("docker", "logs", "--tail", "5000", containerID)
		cmd.Dir = e2eDir()
		out, err := cmd.CombinedOutput()
		if err != nil && len(out) == 0 {
			return ""
		}
		return string(out)
	}

	logData, _ := os.ReadFile(localWeaverLogPath())
	return string(logData)
}

func pollJobOnce(weaverURL string, jobID int) string {
	snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
	if err != nil {
		return ""
	}
	if !snapshot.Found {
		return ""
	}
	return snapshot.Status
}
