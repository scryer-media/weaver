package weaver

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type restartProfile string

const (
	restartProfileHardened restartProfile = "hardened"
	restartProfileCurrent  restartProfile = "current"
)

const restartExtractDelayEnv = "WEAVER_E2E_DELAY=extract.member_start=20000"

type restartClassification string

const (
	restartPass          restartClassification = "PASS"
	restartDocumentedGap restartClassification = "DOCUMENTED_GAP"
	restartFail          restartClassification = "FAIL"
)

type restartCase struct {
	Name        string
	Description string
	Slugs       []string
	Timeout     time.Duration
	Run         func(*restartCaseContext) (restartCaseResult, error)
}

type restartCaseResult struct {
	Classification restartClassification `json:"classification"`
	Summary        string                `json:"summary"`
	Notes          []string              `json:"notes,omitempty"`
}

type restartWeaverProcess struct {
	URL     string
	PID     int
	LogPath string
	cmd     *exec.Cmd
	logFile *os.File
	done    chan error
}

type restartCaseContext struct {
	Case        restartCase
	Profile     restartProfile
	CaseDir     string
	Timeout     time.Duration
	Scenarios   map[string]*Scenario
	configPath  string
	weaverURL   string
	weaver      *restartWeaverProcess
	caseLogPath string
	connections int
}

type restartDBSnapshot struct {
	TakenAt          string                    `json:"taken_at"`
	ActiveJobs       []restartActiveJob        `json:"active_jobs"`
	HistoryJobs      []restartHistoryJob       `json:"history_jobs"`
	JobMetrics       map[int]restartJobMetrics `json:"job_metrics"`
	ExtractionChunks []restartExtractionChunk  `json:"extraction_chunks"`
	ExtractedMembers []restartExtractedMember  `json:"extracted_members"`
}

type restartActiveJob struct {
	JobID                  int     `json:"job_id"`
	Status                 string  `json:"status"`
	DownloadState          string  `json:"download_state,omitempty"`
	PostState              string  `json:"post_state,omitempty"`
	RunState               string  `json:"run_state,omitempty"`
	QueuedRepairAtEpochMs  float64 `json:"queued_repair_at_epoch_ms,omitempty"`
	QueuedExtractAtEpochMs float64 `json:"queued_extract_at_epoch_ms,omitempty"`
	Error                  string  `json:"error,omitempty"`
	OutputDir              string  `json:"output_dir"`
}

type restartHistoryJob struct {
	JobID  int    `json:"job_id"`
	Status string `json:"status"`
}

type restartJobMetrics struct {
	Segments          int    `json:"segments"`
	SegmentBytes      uint64 `json:"segment_bytes"`
	FileProgressBytes uint64 `json:"file_progress_bytes"`
	Files             int    `json:"files"`
	Par2Files         int    `json:"par2_files"`
	ExtractionChunks  int    `json:"extraction_chunks"`
	Extracted         int    `json:"extracted"`
}

type restartExtractionChunk struct {
	JobID        int    `json:"job_id"`
	SetName      string `json:"set_name"`
	MemberName   string `json:"member_name"`
	VolumeIndex  int    `json:"volume_index"`
	BytesWritten uint64 `json:"bytes_written"`
	TempPath     string `json:"temp_path"`
	StartOffset  uint64 `json:"start_offset"`
	EndOffset    uint64 `json:"end_offset"`
	Verified     bool   `json:"verified"`
	Appended     bool   `json:"appended"`
}

type restartExtractedMember struct {
	JobID      int    `json:"job_id"`
	MemberName string `json:"member_name"`
	OutputPath string `json:"output_path"`
}

type restartFilesystemSnapshot struct {
	TakenAt      string             `json:"taken_at"`
	Intermediate []restartTreeEntry `json:"intermediate"`
	Complete     []restartTreeEntry `json:"complete"`
}

type restartTreeEntry struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

type restartNntpMetrics struct {
	BodyCounts      map[string]int `json:"body_counts"`
	BodyBytes       uint64         `json:"body_bytes"`
	BodyTransfers   int64          `json:"body_transfers"`
	BodyFirstUnixNS int64          `json:"body_first_unix_nano"`
	BodyLastUnixNS  int64          `json:"body_last_unix_nano"`
	StatCounts      map[string]int `json:"stat_counts"`
	StatChaosHits   int            `json:"stat_chaos_hits"`
}

func cmdRestartAll() {
	runRestartSuite(selectRestartCases(nil))
}

func cmdRestartTest(args []string) {
	if len(args) == 0 {
		log.Fatalf("usage: %s restart-test <case> [case...]", cliProgramName)
	}
	runRestartSuite(selectRestartCases(args))
}

func restartCases() []restartCase {
	return []restartCase{
		{
			Name:        "download_completed_file_survives_restart",
			Description: "Completed archive files should survive a crash/restart without redownloading",
			Slugs:       []string{"rar5-multi-member"},
			Timeout:     12 * time.Minute,
			Run:         runDownloadCompletedFileSurvivesRestart,
		},
		{
			Name:        "download_progress_floor_survives_restart",
			Description: "Crash after a persisted restart checkpoint may redownload, but must still complete after restart",
			Slugs:       []string{"rar5-multi-member"},
			Timeout:     12 * time.Minute,
			Run:         runDownloadProgressFloorSurvivesRestart,
		},
		{
			Name:        "queued_repair_survives_and_keeps_place",
			Description: "Concurrent repair work should restart from durable files even when transient repair state is lossy",
			Slugs:       []string{"par2-heavy-damage", "par2-heavy-damage-a"},
			Timeout:     20 * time.Minute,
			Run:         runQueuedRepairSurvivesAndKeepsPlace,
		},
		{
			Name:        "queued_extract_survives_and_keeps_place",
			Description: "Concurrent extract work should restart from durable files even when transient extract state is lossy",
			Slugs:       []string{"rar5-multi-member"},
			Timeout:     20 * time.Minute,
			Run:         runQueuedExtractSurvivesAndKeepsPlace,
		},
		{
			Name:        "paused_job_restores_resume_target",
			Description: "Paused downloading jobs should restore paused state and resume downloading after restart",
			Slugs:       []string{"rar5-multi-member"},
			Timeout:     20 * time.Minute,
			Run:         runPausedJobRestoresResumeTarget,
		},
		{
			Name:        "verifying_restarts_cleanly",
			Description: "Verification should restart cleanly from the phase start",
			Slugs:       []string{"par2-small-repair"},
			Timeout:     15 * time.Minute,
			Run:         runVerifyingRestartsCleanly,
		},
		{
			Name:        "repairing_restarts_cleanly",
			Description: "Repairing should restart cleanly from the phase start",
			Slugs:       []string{"par2-small-repair"},
			Timeout:     15 * time.Minute,
			Run:         runRepairingRestartsCleanly,
		},
		{
			Name:        "rar_extraction_retries_from_durable_files",
			Description: "RAR extraction may lose attempt progress, but restart should retry from durable archive files",
			Slugs:       []string{"rar5-multi-member"},
			Timeout:     20 * time.Minute,
			Run:         runRarExtractionResumesFromCheckpoint,
		},
		{
			Name:        "rar_finalize_rediscovers_after_rename",
			Description: "Finalize-after-rename may be retried, but restart should rediscover durable archive files and complete",
			Slugs:       []string{"rar5-multi-member"},
			Timeout:     20 * time.Minute,
			Run:         runRarFinalizeReconcilesAfterRename,
		},
		{
			Name:        "stale_active_extracted_rows_validate_after_restart",
			Description: "Restart should ignore stale completed-member markers whose outputs no longer validate",
			Slugs:       []string{"rar5-multi-member"},
			Timeout:     20 * time.Minute,
			Run:         runStaleActiveExtractedRowsClearAfterRestart,
		},
		{
			Name:        "verification_reconciles_stale_extracting_runtime",
			Description: "Restart should reconcile stale extracting runtime when verification already started",
			Slugs:       []string{"par2-small-repair"},
			Timeout:     15 * time.Minute,
			Run:         runVerificationReconcilesStaleExtractingRuntime,
		},
	}
}

func selectRestartCases(requested []string) []restartCase {
	all := restartCases()
	if only := strings.TrimSpace(os.Getenv("E2E_RESTART_ONLY_CASE")); only != "" {
		requested = append(requested, only)
	}
	if len(requested) == 0 {
		return all
	}

	keep := map[string]bool{}
	for _, name := range requested {
		name = strings.TrimSpace(name)
		if name == "download_committed_progress_survives_restart" {
			name = "download_completed_file_survives_restart"
		}
		switch name {
		case "rar_extraction_resumes_from_checkpoint":
			name = "rar_extraction_retries_from_durable_files"
		case "rar_finalize_reconciles_after_rename":
			name = "rar_finalize_rediscovers_after_rename"
		case "stale_active_extracted_rows_clear_after_restart":
			name = "stale_active_extracted_rows_validate_after_restart"
		}
		keep[name] = true
	}
	var selected []restartCase
	for _, tc := range all {
		if keep[tc.Name] {
			selected = append(selected, tc)
		}
	}
	if len(selected) != len(keep) {
		var known []string
		for _, tc := range all {
			known = append(known, tc.Name)
		}
		sort.Strings(known)
		log.Fatalf("unknown restart case requested; known cases: %s", strings.Join(known, ", "))
	}
	return selected
}

func runRestartSuite(cases []restartCase) {
	if len(cases) == 0 {
		log.Fatal("no restart cases selected")
	}

	profile := restartProfileValue()
	timeout := restartTimeoutValue()
	runRoot := filepath.Join(localRunDir(), "restart", time.Now().Format("20060102-150405"))
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		log.Fatalf("create restart run dir: %v", err)
	}

	ensureRestartInfrastructure()
	if err := ensureNntpChaosOff(); err != nil {
		log.Fatalf("reset NNTP chaos before restart suite: %v", err)
	}

	if err := ensureRestartFixturesSeeded(uniqueRestartSlugs(cases)); err != nil {
		log.Fatalf("seed restart fixtures: %v", err)
	}

	emitProgressEvent(progressEvent{Kind: "phase_total", Total: len(cases), Detail: "Restart litmus"})
	log.Printf("restart suite: profile=%s cases=%d artifacts=%s", profile, len(cases), runRoot)

	passCount := 0
	docGapCount := 0
	failCount := 0

	for index, tc := range cases {
		emitProgressEvent(progressEvent{Kind: "phase_note", Detail: tc.Name})
		caseDir := filepath.Join(runRoot, fmt.Sprintf("%02d-%s", index+1, tc.Name))
		if err := os.MkdirAll(caseDir, 0o755); err != nil {
			log.Fatalf("create restart case dir for %s: %v", tc.Name, err)
		}

		log.Printf("restart case %d/%d: %s", index+1, len(cases), tc.Name)
		ctx := &restartCaseContext{
			Case:        tc,
			Profile:     profile,
			CaseDir:     caseDir,
			Timeout:     timeoutForCase(timeout, tc.Timeout),
			Scenarios:   map[string]*Scenario{},
			configPath:  filepath.Join(caseDir, "weaver.toml"),
			weaverURL:   fmt.Sprintf("http://localhost:%s", localWeaverPort()),
			caseLogPath: filepath.Join(caseDir, "weaver.log"),
		}
		for _, slug := range tc.Slugs {
			scenario, err := loadScenario(filepath.Join(testdataDir(), slug))
			if err != nil {
				log.Fatalf("load scenario %s for %s: %v", slug, tc.Name, err)
			}
			ctx.Scenarios[slug] = scenario
		}

		result, err := runOneRestartCase(ctx)
		if err != nil {
			result = restartCaseResult{
				Classification: restartFail,
				Summary:        err.Error(),
			}
		}
		writeRestartJSON(filepath.Join(caseDir, "result.json"), result)

		switch result.Classification {
		case restartPass:
			passCount++
			emitProgressEvent(progressEvent{Kind: "phase_progress", Current: index + 1, Total: len(cases), Status: "complete", Detail: tc.Name})
		case restartDocumentedGap:
			docGapCount++
			emitProgressEvent(progressEvent{Kind: "phase_progress", Current: index + 1, Total: len(cases), Status: "warning", Detail: tc.Name})
		default:
			failCount++
			emitProgressEvent(progressEvent{Kind: "phase_progress", Current: index + 1, Total: len(cases), Status: "fail", Detail: tc.Name})
		}

		fmt.Printf("  %-40s %s - %s\n", tc.Name, result.Classification, result.Summary)
		if len(result.Notes) > 0 {
			for _, note := range result.Notes {
				fmt.Printf("    note: %s\n", note)
			}
		}

		ctx.cleanup()
		if !restartKeepArtifacts() && result.Classification == restartPass {
			_ = os.RemoveAll(caseDir)
		}
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Printf("RESTART SUITE SUMMARY (%s): %d pass, %d documented_gap, %d fail\n", profile, passCount, docGapCount, failCount)
	fmt.Printf("artifacts: %s\n", runRoot)
	if failCount > 0 {
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(cases), Total: len(cases), Status: "fail"})
		os.Exit(1)
	}
	emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(cases), Total: len(cases), Status: "pass"})
}

func runOneRestartCase(ctx *restartCaseContext) (restartCaseResult, error) {
	defer ctx.cleanup()
	killWeaver()
	cleanWeaverState()
	if err := resetNntpMetrics(); err != nil {
		return restartCaseResult{}, err
	}
	result, err := ctx.Case.Run(ctx)
	if err != nil {
		return restartCaseResult{}, err
	}

	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	if err := assertNoOrphanActiveStatePath(dbPath); err != nil {
		result.Classification = restartFail
		result.Summary = fmt.Sprintf("%s (orphan active state mismatch: %v)", result.Summary, err)
		return result, nil
	}
	if err := assertNoLingeringWeaverActiveState(dbPath); err != nil {
		result.Classification = restartFail
		result.Summary = fmt.Sprintf("%s (active state mismatch: %v)", result.Summary, err)
		return result, nil
	}

	return result, nil
}

func restartProfileValue() restartProfile {
	switch strings.ToLower(strings.TrimSpace(env("E2E_RESTART_PROFILE", string(restartProfileHardened)))) {
	case "", string(restartProfileHardened):
		return restartProfileHardened
	case string(restartProfileCurrent):
		return restartProfileCurrent
	default:
		log.Fatalf("invalid E2E_RESTART_PROFILE=%q (expected hardened|current)", os.Getenv("E2E_RESTART_PROFILE"))
		return restartProfileHardened
	}
}

func restartTimeoutValue() time.Duration {
	return time.Duration(envInt("E2E_RESTART_TIMEOUT_SEC", 900)) * time.Second
}

func restartKeepArtifacts() bool {
	return envBool("E2E_RESTART_KEEP_ARTIFACTS", true)
}

func timeoutForCase(defaultTimeout, caseTimeout time.Duration) time.Duration {
	if caseTimeout > 0 {
		return caseTimeout
	}
	return defaultTimeout
}

func uniqueRestartSlugs(cases []restartCase) []string {
	set := map[string]bool{}
	for _, tc := range cases {
		for _, slug := range tc.Slugs {
			set[slug] = true
		}
	}
	out := make([]string, 0, len(set))
	staticCount := 0
	for _, slug := range restartFixtureSlugs {
		if set[slug] {
			out = append(out, slug)
			delete(set, slug)
			staticCount++
		}
	}
	for slug := range set {
		out = append(out, slug)
	}
	sort.Strings(out[staticCount:])
	return out
}

func ensureRestartFixturesSeeded(slugs []string) error {
	if envBool(nntpSeedImageActiveEnv, false) {
		log.Printf("restart NNTP fixtures already present in preseeded image")
		return nil
	}

	for _, slug := range slugs {
		log.Printf("ensuring restart fixture seeded: %s", slug)
		if err := seedFixture(filepath.Join(testdataDir(), slug)); err != nil {
			return err
		}
	}
	return nil
}

func ensureRestartInfrastructure() {
	log.Println("starting restart-suite infrastructure with fresh NNTP build...")
	if err := ensureNyuuImageBuilt(); err != nil {
		log.Fatalf("build nyuu image for restart suite: %v", err)
	}
	services := []string{"nntp", "nyuu"}
	if weaverUsesPostgresDatastore() {
		services = append(services, "weaver-postgres")
	}
	args := append(dockerComposeArgs("up", "-d", "--build", "--quiet-pull"), services...)
	cmd := exec.Command("docker", args...)
	cmd.Dir = e2eDir()
	if err := runExternalCommand(cmd, "docker compose up --build for restart suite"); err != nil {
		log.Fatalf("start restart-suite infrastructure: %v", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		log.Fatalf("refresh runtime ports after starting restart-suite infrastructure: %v", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	if weaverUsesPostgresDatastore() {
		if err := waitForWeaverPostgresReady(30 * time.Second); err != nil {
			log.Fatalf("wait for Weaver Postgres: %v", err)
		}
	}
}

func (ctx *restartCaseContext) startWeaver(failpoint string) error {
	return ctx.startWeaverWithOptions(failpoint, 8)
}

func (ctx *restartCaseContext) startWeaverWithConnections(failpoint string, connections int) error {
	return ctx.startWeaverWithOptions(failpoint, connections)
}

func (ctx *restartCaseContext) startWeaverWithOptions(failpoint string, connections int, extraEnv ...string) error {
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	if err := ensureNntpChaosOff(); err != nil {
		return err
	}

	weaverBin, err := ensureRestartWeaverBinary()
	if err != nil {
		return err
	}
	logFile, err := os.OpenFile(ctx.caseLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open restart weaver log: %w", err)
	}

	writeRestartWeaverConfig(ctx.configPath, mustPortInt("NNTP_PORT", nntpPort()), connections)
	ctx.connections = connections
	cmd := exec.Command(weaverBin, "--config", ctx.configPath, "serve", "--port", localWeaverPort())

	var filteredEnv []string
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "WEAVER_E2E_FAILPOINT=") {
			continue
		}
		if strings.HasPrefix(entry, "WEAVER_E2E_DELAY=") {
			continue
		}
		if strings.HasPrefix(entry, "WEAVER_MAX_CONCURRENT_EXTRACTIONS=") {
			continue
		}
		filteredEnv = append(filteredEnv, entry)
	}
	filteredEnv = managedWeaverEnv(filteredEnv, ctx.CaseDir, "info,weaver::pipeline=debug,weaver_nntp=info")
	filteredEnv = appendOrReplaceEnv(filteredEnv, "WEAVER_ENCRYPTION_KEY", restartSuiteEncryptionKey(ctx))
	filteredEnv = append(filteredEnv, "WEAVER_MAX_CONCURRENT_EXTRACTIONS="+strings.TrimSpace(env("E2E_RESTART_MAX_CONCURRENT_EXTRACTIONS", "1")))
	if strings.TrimSpace(failpoint) != "" {
		filteredEnv = append(filteredEnv, "WEAVER_E2E_FAILPOINT="+failpoint)
	}
	filteredEnv = append(filteredEnv, extraEnv...)
	cmd.Env = filteredEnv
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start managed weaver: %w", err)
	}
	_ = os.MkdirAll(filepath.Dir(localWeaverPIDPath()), 0o755)
	_ = os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		close(done)
		_ = logFile.Close()
	}()

	ctx.weaver = &restartWeaverProcess{
		URL:     ctx.weaverURL,
		PID:     cmd.Process.Pid,
		LogPath: ctx.caseLogPath,
		cmd:     cmd,
		logFile: logFile,
		done:    done,
	}
	if err := ctx.waitForManagedWeaverGraphQLReady(90 * time.Second); err != nil {
		return err
	}
	return nil
}

func restartSuiteEncryptionKey(ctx *restartCaseContext) string {
	runRoot := filepath.Dir(ctx.CaseDir)
	sum := sha256.Sum256([]byte("weaver restart e2e encryption key\x00" + filepath.Clean(runRoot) + "\x00" + string(ctx.Profile)))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func ensureRestartWeaverBinary() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("WEAVER_BIN")); configured != "" {
		return configured, nil
	}
	return ensureE2EWeaverBinary()
}

func (ctx *restartCaseContext) restartWeaver() error {
	return ctx.startWeaverWithConnections("", ctx.connections)
}

func (ctx *restartCaseContext) killWeaverForRestart() error {
	if ctx.weaver != nil && ctx.weaver.cmd != nil && ctx.weaver.cmd.Process != nil {
		_ = ctx.weaver.cmd.Process.Kill()
	}
	_ = os.Remove(localWeaverPIDPath())
	return waitForGraphQLDown(graphqlURL(ctx.weaverURL), 20*time.Second)
}

func (ctx *restartCaseContext) waitForCrash(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return waitForGraphQLDown(graphqlURL(ctx.weaverURL), timeout)
}

func (ctx *restartCaseContext) cleanup() {
	if ctx.weaver != nil && ctx.weaver.cmd != nil && ctx.weaver.cmd.Process != nil {
		_ = ctx.weaver.cmd.Process.Kill()
		select {
		case <-ctx.weaver.done:
		case <-time.After(3 * time.Second):
		}
	}
	_ = os.Remove(localWeaverPIDPath())
	ctx.weaver = nil
}

func writeRestartWeaverConfig(path string, nntpPort int, connections int) {
	if connections <= 0 {
		connections = 8
	}
	root := localWeaverDir()
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	_ = os.MkdirAll(filepath.Join(root, "complete"), 0o755)
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

func waitForGraphQLReady(url string, timeout time.Duration) error {
	client := weaverHTTPClient(url, 10*time.Second)
	body := []byte(`{"query":"{ version }"}`)
	deadline := time.Now().Add(timeout)
	lastFailure := "not attempted"
	for time.Now().Before(deadline) {
		if err := refreshWeaverBrowserSession(client, url); err != nil {
			lastFailure = "load UI: " + err.Error()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		resp, err := postGraphQLWithClient(client, url, body)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		lastFailure = describeGraphQLAttempt(resp, err)
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for GraphQL readiness at %s (last failure: %s)", url, lastFailure)
}

func (ctx *restartCaseContext) waitForManagedWeaverGraphQLReady(timeout time.Duration) error {
	if ctx.weaver == nil {
		return waitForGraphQLReady(graphqlURL(ctx.weaverURL), timeout)
	}

	url := graphqlURL(ctx.weaverURL)
	client := weaverHTTPClient(url, 10*time.Second)
	body := []byte(`{"query":"{ version }"}`)
	deadline := time.Now().Add(timeout)
	lastFailure := "not attempted"
	for time.Now().Before(deadline) {
		if err := managedWeaverExit(ctx.weaver); err != nil {
			return err
		}
		if err := refreshWeaverBrowserSession(client, url); err != nil {
			lastFailure = "load UI: " + err.Error()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		resp, err := postGraphQLWithClient(client, url, body)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		lastFailure = describeGraphQLAttempt(resp, err)
		time.Sleep(500 * time.Millisecond)
	}
	if err := managedWeaverExit(ctx.weaver); err != nil {
		return err
	}
	return fmt.Errorf("timeout waiting for GraphQL readiness at %s (last failure: %s)", url, lastFailure)
}

func managedWeaverExit(process *restartWeaverProcess) error {
	select {
	case err, ok := <-process.done:
		if !ok {
			return fmt.Errorf("managed weaver exited before GraphQL readiness; log=%s%s", process.LogPath, restartLogTail(process.LogPath, 12))
		}
		if err != nil {
			return fmt.Errorf("managed weaver exited before GraphQL readiness: %v; log=%s%s", err, process.LogPath, restartLogTail(process.LogPath, 12))
		}
		return fmt.Errorf("managed weaver exited before GraphQL readiness; log=%s%s", process.LogPath, restartLogTail(process.LogPath, 12))
	default:
		return nil
	}
}

func restartLogTail(path string, maxLines int) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf(" (could not read log tail: %v)", err)
	}
	text := strings.TrimRight(string(data), "\n")
	if text == "" {
		return " (log is empty)"
	}
	lines := strings.Split(text, "\n")
	if maxLines > 0 && len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return "\n--- weaver.log tail ---\n" + strings.Join(lines, "\n")
}

func waitForGraphQLDown(url string, timeout time.Duration) error {
	client := &http.Client{Timeout: 2 * time.Second}
	body := []byte(`{"query":"{ version }"}`)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := postGraphQLWithClient(client, url, body)
		if err != nil {
			return nil
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for GraphQL shutdown at %s", url)
}

func resetNntpMetrics() error {
	if err := resetNntpMetricsOn(nntpHost(), nntpPort()); err != nil {
		return err
	}
	if backupNntpRunning() {
		if err := resetNntpMetricsOn(nntpHost(), backupNntpPort()); err != nil {
			return err
		}
	}
	return nil
}

func setNntpChaos(config string) error {
	config = strings.TrimSpace(config)
	if config == "" || strings.EqualFold(config, "off") {
		return ensureNntpChaosOff()
	}

	return setNntpChaosOnServer(nntpHost(), nntpPort(), config)
}

func fetchNntpBodyMetrics(prefix string) (restartNntpMetrics, error) {
	return fetchNntpBodyMetricsFrom(nntpHost(), nntpPort(), prefix)
}

func fetchNntpBodyMetricsFrom(host, port, prefix string) (restartNntpMetrics, error) {
	cmd := "METRICS BODY"
	if strings.TrimSpace(prefix) != "" {
		cmd += " " + prefix
	}
	resp, err := sendNntpCommandToWithRetry(host, port, cmd, 5)
	if err != nil {
		return restartNntpMetrics{}, err
	}
	if !strings.HasPrefix(resp, "290 ") {
		return restartNntpMetrics{}, fmt.Errorf("unexpected metrics response: %s", resp)
	}
	var metrics restartNntpMetrics
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(resp, "290 "))), &metrics); err != nil {
		return restartNntpMetrics{}, err
	}
	if metrics.BodyCounts == nil {
		metrics.BodyCounts = map[string]int{}
	}
	return metrics, nil
}

func fetchNntpStatMetrics(prefix string) (restartNntpMetrics, error) {
	metrics, err := fetchNntpStatMetricsFrom(nntpHost(), nntpPort(), prefix)
	if err != nil {
		return restartNntpMetrics{}, err
	}
	if backupNntpRunning() {
		backupMetrics, err := fetchNntpStatMetricsFrom(nntpHost(), backupNntpPort(), prefix)
		if err != nil {
			return restartNntpMetrics{}, err
		}
		mergeRestartNntpMetrics(&metrics, backupMetrics)
	}
	return metrics, nil
}

func resetNntpMetricsOn(host, port string) error {
	resp, err := sendNntpCommandToWithRetry(host, port, "METRICS RESET", 5)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "290 ") {
		return fmt.Errorf("unexpected metrics reset response: %s", resp)
	}
	return nil
}

func setNntpChaosOnServer(host, port, config string) error {
	resp, err := sendNntpCommandToWithRetry(host, port, "CHAOS "+config, 5)
	if err != nil {
		return err
	}
	return validateNntpChaosResponse(resp)
}

func validateNntpChaosResponse(resp string) error {
	if !strings.HasPrefix(resp, "290 ") {
		return fmt.Errorf("unexpected chaos response: %s", resp)
	}
	return nil
}

// holdNntpChaosOnServer keeps the authenticated control connection alive so
// greeting failures cannot lock the harness out of clearing its own gate.
func holdNntpChaosOnServer(host, port, config string) (func() error, error) {
	session, err := openNntpCommandSession(host, port, true)
	if err != nil {
		return nil, err
	}
	resp, err := session.send("CHAOS " + config)
	if err != nil {
		session.close()
		return nil, err
	}
	if err := validateNntpChaosResponse(resp); err != nil {
		session.close()
		return nil, err
	}

	return func() error {
		defer session.close()
		resp, err := session.send("CHAOS off")
		if err != nil {
			return err
		}
		return validateNntpChaosResponse(resp)
	}, nil
}

func fetchNntpStatMetricsFrom(host, port, prefix string) (restartNntpMetrics, error) {
	cmd := "METRICS STAT"
	if strings.TrimSpace(prefix) != "" {
		cmd += " " + prefix
	}
	resp, err := sendNntpCommandToWithRetry(host, port, cmd, 5)
	if err != nil {
		return restartNntpMetrics{}, err
	}
	if !strings.HasPrefix(resp, "290 ") {
		return restartNntpMetrics{}, fmt.Errorf("unexpected metrics response: %s", resp)
	}
	var metrics restartNntpMetrics
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(resp, "290 "))), &metrics); err != nil {
		return restartNntpMetrics{}, err
	}
	if metrics.StatCounts == nil {
		metrics.StatCounts = map[string]int{}
	}
	return metrics, nil
}

func mergeRestartNntpMetrics(dst *restartNntpMetrics, src restartNntpMetrics) {
	if dst.BodyCounts == nil {
		dst.BodyCounts = map[string]int{}
	}
	for msgID, count := range src.BodyCounts {
		dst.BodyCounts[msgID] += count
	}
	if dst.StatCounts == nil {
		dst.StatCounts = map[string]int{}
	}
	for msgID, count := range src.StatCounts {
		dst.StatCounts[msgID] += count
	}
	dst.StatChaosHits += src.StatChaosHits
}

func captureRestartDBSnapshot(dbPath string) (restartDBSnapshot, error) {
	snapshot := restartDBSnapshot{
		TakenAt:    time.Now().Format(time.RFC3339Nano),
		JobMetrics: map[int]restartJobMetrics{},
	}
	available, err := weaverStateDBAvailable(dbPath)
	if err != nil {
		return snapshot, err
	}
	if !available {
		return snapshot, nil
	}
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return snapshot, err
	}
	defer db.Close()

	rows, err := db.Query(rebindWeaverSQL(datastore, `
		SELECT job_id, status, COALESCE(download_state, ''), COALESCE(post_state, ''), COALESCE(run_state, ''),
		       COALESCE(queued_repair_at_epoch_ms, 0), COALESCE(queued_extract_at_epoch_ms, 0),
		       COALESCE(error, ''), output_dir
		FROM active_jobs
		ORDER BY job_id
	`))
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	for rows.Next() {
		var job restartActiveJob
		if err := rows.Scan(
			&job.JobID,
			&job.Status,
			&job.DownloadState,
			&job.PostState,
			&job.RunState,
			&job.QueuedRepairAtEpochMs,
			&job.QueuedExtractAtEpochMs,
			&job.Error,
			&job.OutputDir,
		); err != nil {
			return snapshot, err
		}
		snapshot.ActiveJobs = append(snapshot.ActiveJobs, job)
	}
	if err := rows.Err(); err != nil {
		return snapshot, err
	}

	historyRows, err := db.Query(rebindWeaverSQL(datastore, `SELECT job_id, status FROM job_history ORDER BY job_id`))
	if err != nil {
		return snapshot, err
	}
	defer historyRows.Close()
	for historyRows.Next() {
		var job restartHistoryJob
		if err := historyRows.Scan(&job.JobID, &job.Status); err != nil {
			return snapshot, err
		}
		snapshot.HistoryJobs = append(snapshot.HistoryJobs, job)
	}
	if err := historyRows.Err(); err != nil {
		return snapshot, err
	}

	type aggregateSpec struct {
		query string
		apply func(*restartJobMetrics, int64, int64)
	}
	aggregates := []aggregateSpec{
		{
			query: `SELECT job_id, COUNT(*), COALESCE(SUM(contiguous_bytes_written), 0) FROM active_file_progress GROUP BY job_id`,
			apply: func(m *restartJobMetrics, count, sum int64) {
				m.Segments = int(count)
				m.SegmentBytes = uint64(sum)
				m.FileProgressBytes = uint64(sum)
			},
		},
		{
			query: `SELECT job_id, COUNT(*), 0 FROM active_files GROUP BY job_id`,
			apply: func(m *restartJobMetrics, count, _ int64) {
				m.Files = int(count)
			},
		},
		{
			query: `SELECT job_id, COUNT(*), 0 FROM active_par2_files GROUP BY job_id`,
			apply: func(m *restartJobMetrics, count, _ int64) {
				m.Par2Files = int(count)
			},
		},
		{
			query: `SELECT job_id, COUNT(*), 0 FROM active_extraction_chunks GROUP BY job_id`,
			apply: func(m *restartJobMetrics, count, _ int64) {
				m.ExtractionChunks = int(count)
			},
		},
		{
			query: `SELECT job_id, COUNT(*), 0 FROM active_extracted GROUP BY job_id`,
			apply: func(m *restartJobMetrics, count, _ int64) {
				m.Extracted = int(count)
			},
		},
	}
	for _, aggregate := range aggregates {
		rows, err := db.Query(rebindWeaverSQL(datastore, aggregate.query))
		if err != nil {
			return snapshot, err
		}
		for rows.Next() {
			var (
				jobID int
				count int64
				sum   int64
			)
			if err := rows.Scan(&jobID, &count, &sum); err != nil {
				rows.Close()
				return snapshot, err
			}
			metrics := snapshot.JobMetrics[jobID]
			aggregate.apply(&metrics, count, sum)
			snapshot.JobMetrics[jobID] = metrics
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return snapshot, err
		}
		rows.Close()
	}

	rows, err = db.Query(rebindWeaverSQL(datastore, `
		SELECT job_id, set_name, member_name, volume_index, bytes_written, temp_path, start_offset, end_offset, verified, appended
		FROM active_extraction_chunks
		ORDER BY job_id, set_name, member_name, volume_index
	`))
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	for rows.Next() {
		var chunk restartExtractionChunk
		if err := rows.Scan(
			&chunk.JobID,
			&chunk.SetName,
			&chunk.MemberName,
			&chunk.VolumeIndex,
			&chunk.BytesWritten,
			&chunk.TempPath,
			&chunk.StartOffset,
			&chunk.EndOffset,
			&chunk.Verified,
			&chunk.Appended,
		); err != nil {
			return snapshot, err
		}
		snapshot.ExtractionChunks = append(snapshot.ExtractionChunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return snapshot, err
	}

	rows, err = db.Query(rebindWeaverSQL(datastore, `SELECT job_id, member_name, output_path FROM active_extracted ORDER BY job_id, member_name`))
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	for rows.Next() {
		var member restartExtractedMember
		if err := rows.Scan(&member.JobID, &member.MemberName, &member.OutputPath); err != nil {
			return snapshot, err
		}
		snapshot.ExtractedMembers = append(snapshot.ExtractedMembers, member)
	}
	return snapshot, rows.Err()
}

type forcedActiveRuntimeState struct {
	Status                    string
	DownloadState             *string
	PostState                 *string
	RunState                  *string
	QueuedRepairAtEpochMs     *float64
	QueuedExtractAtEpochMs    *float64
	PausedResumeStatus        *string
	PausedResumeDownloadState *string
	PausedResumePostState     *string
}

func stringPtr(value string) *string {
	return &value
}

func normalizeForcedRuntimeValue(value *string) interface{} {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func inferForcedRuntimeLanes(status string) (*string, *string, *string) {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "queued":
		return stringPtr("queued"), stringPtr("idle"), stringPtr("active")
	case "downloading":
		return stringPtr("downloading"), stringPtr("idle"), stringPtr("active")
	case "checking":
		return stringPtr("checking"), stringPtr("idle"), stringPtr("active")
	case "verifying":
		return stringPtr("complete"), stringPtr("verifying"), stringPtr("active")
	case "queued_repair":
		return stringPtr("complete"), stringPtr("queued_repair"), stringPtr("active")
	case "repairing":
		return stringPtr("complete"), stringPtr("repairing"), stringPtr("active")
	case "queued_extract":
		return stringPtr("complete"), stringPtr("queued_extract"), stringPtr("active")
	case "extracting":
		return stringPtr("downloading"), stringPtr("extracting"), stringPtr("active")
	case "moving":
		return stringPtr("complete"), stringPtr("finalizing"), stringPtr("active")
	case "complete", "completed":
		return stringPtr("complete"), stringPtr("completed"), stringPtr("active")
	case "failed":
		return stringPtr("failed"), stringPtr("failed"), stringPtr("active")
	case "paused":
		return nil, nil, stringPtr("paused")
	default:
		return nil, nil, nil
	}
}

func forcedRuntimeLaneValues(state forcedActiveRuntimeState) (interface{}, interface{}, interface{}, interface{}, interface{}) {
	downloadState := state.DownloadState
	postState := state.PostState
	runState := state.RunState

	if downloadState == nil || postState == nil || runState == nil {
		inferredDownload, inferredPost, inferredRun := inferForcedRuntimeLanes(state.Status)
		if downloadState == nil {
			downloadState = inferredDownload
		}
		if postState == nil {
			postState = inferredPost
		}
		if runState == nil {
			runState = inferredRun
		}
	}

	pausedResumeDownloadState := state.PausedResumeDownloadState
	pausedResumePostState := state.PausedResumePostState
	if (pausedResumeDownloadState == nil || pausedResumePostState == nil) && state.PausedResumeStatus != nil {
		inferredDownload, inferredPost, _ := inferForcedRuntimeLanes(*state.PausedResumeStatus)
		if pausedResumeDownloadState == nil {
			pausedResumeDownloadState = inferredDownload
		}
		if pausedResumePostState == nil {
			pausedResumePostState = inferredPost
		}
	}

	return normalizeForcedRuntimeValue(downloadState),
		normalizeForcedRuntimeValue(postState),
		normalizeForcedRuntimeValue(runState),
		normalizeForcedRuntimeValue(pausedResumeDownloadState),
		normalizeForcedRuntimeValue(pausedResumePostState)
}

func activeJobFromSnapshot(snapshot restartDBSnapshot, jobID int) (restartActiveJob, bool) {
	for _, job := range snapshot.ActiveJobs {
		if job.JobID == jobID {
			return job, true
		}
	}
	return restartActiveJob{}, false
}

func insertActiveExtractedMembers(dbPath string, members []restartExtractedMember) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(rebindWeaverSQL(datastore, `
		INSERT INTO active_extracted (job_id, member_name, output_path)
		VALUES (?, ?, ?)
		ON CONFLICT(job_id, member_name) DO UPDATE SET output_path = excluded.output_path
	`))
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, member := range members {
		if _, err := stmt.Exec(member.JobID, member.MemberName, member.OutputPath); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func countActiveExtractedMembers(dbPath string, jobID int) (int, error) {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(rebindWeaverSQL(datastore, `SELECT COUNT(*) FROM active_extracted WHERE job_id = ?`), jobID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func jobEventKinds(dbPath string, jobID int) ([]string, error) {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(rebindWeaverSQL(datastore, `SELECT kind FROM job_events WHERE job_id = ? ORDER BY id`), jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			return nil, err
		}
		out = append(out, kind)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func removeFileIfExists(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return err
}

func forceActiveJobRuntimeStates(dbPath string, states map[int]forcedActiveRuntimeState) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(rebindWeaverSQL(datastore, `
		UPDATE active_jobs
		SET status = ?, error = NULL,
		    download_state = ?,
		    post_state = ?,
		    run_state = ?,
		    queued_repair_at_epoch_ms = ?,
		    queued_extract_at_epoch_ms = ?,
		    paused_resume_status = ?,
		    paused_resume_download_state = ?,
		    paused_resume_post_state = ?
		WHERE job_id = ?
	`))
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for jobID, state := range states {
		downloadState, postState, runState, pausedResumeDownloadState, pausedResumePostState :=
			forcedRuntimeLaneValues(state)
		if _, err := stmt.Exec(
			state.Status,
			downloadState,
			postState,
			runState,
			state.QueuedRepairAtEpochMs,
			state.QueuedExtractAtEpochMs,
			state.PausedResumeStatus,
			pausedResumeDownloadState,
			pausedResumePostState,
			jobID,
		); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func captureRestartFilesystemSnapshot() restartFilesystemSnapshot {
	return restartFilesystemSnapshot{
		TakenAt:      time.Now().Format(time.RFC3339Nano),
		Intermediate: captureTreeEntries(filepath.Join(localWeaverDir(), "intermediate")),
		Complete:     captureTreeEntries(filepath.Join(localWeaverDir(), "complete")),
	}
}

func captureTreeEntries(root string) []restartTreeEntry {
	var entries []restartTreeEntry
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}
		entries = append(entries, restartTreeEntry{
			Path: filepath.ToSlash(rel),
			Size: info.Size(),
		})
		return nil
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries
}

func writeRestartJSON(path string, value interface{}) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Printf("warning: marshal %s: %v", path, err)
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Printf("warning: write %s: %v", path, err)
	}
}

func (ctx *restartCaseContext) captureEvidence(prefix, bodyPrefix string) (restartDBSnapshot, restartFilesystemSnapshot, restartNntpMetrics, error) {
	dbSnap, err := captureRestartDBSnapshot(filepath.Join(ctx.CaseDir, "weaver.db"))
	if err != nil {
		return restartDBSnapshot{}, restartFilesystemSnapshot{}, restartNntpMetrics{}, err
	}
	fsSnap := captureRestartFilesystemSnapshot()
	metrics, err := fetchNntpBodyMetrics(bodyPrefix)
	if err != nil {
		return restartDBSnapshot{}, restartFilesystemSnapshot{}, restartNntpMetrics{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, prefix+"_db.json"), dbSnap)
	writeRestartJSON(filepath.Join(ctx.CaseDir, prefix+"_fs.json"), fsSnap)
	writeRestartJSON(filepath.Join(ctx.CaseDir, prefix+"_nntp.json"), metrics)
	return dbSnap, fsSnap, metrics, nil
}

func (ctx *restartCaseContext) submitSlug(slug string) (int, error) {
	return submitOneNZB(ctx.weaverURL, ctx.Scenarios[slug])
}

func (ctx *restartCaseContext) submitSlugNTimes(slug string, count int) ([]int, error) {
	ids := make([]int, 0, count)
	for i := 0; i < count; i++ {
		id, err := submitOneNZBWithOptions(ctx.weaverURL, ctx.Scenarios[slug], submitNZBOptions{force: i > 0})
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (ctx *restartCaseContext) waitForFacade(jobID int, timeout time.Duration, predicate func(facadeItemSnapshot) bool) (facadeItemSnapshot, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot, err := fetchFacadeItemSnapshot(ctx.weaverURL, jobID)
		if err == nil && predicate(snapshot) {
			return snapshot, nil
		}
		time.Sleep(1 * time.Second)
	}
	return facadeItemSnapshot{}, fmt.Errorf("timeout waiting for facade snapshot for job %d", jobID)
}

func (ctx *restartCaseContext) observeFacade(jobID int, timeout time.Duration) (facadeItemSnapshot, bool, error) {
	deadline := time.Now().Add(timeout)
	var last facadeItemSnapshot
	foundAny := false
	for time.Now().Before(deadline) {
		snapshot, err := fetchFacadeItemSnapshot(ctx.weaverURL, jobID)
		if err == nil && snapshot.Found {
			last = snapshot
			foundAny = true
			if snapshot.Status == "PAUSED" {
				return snapshot, true, nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	if foundAny {
		return last, false, nil
	}
	return facadeItemSnapshot{}, false, fmt.Errorf("timeout waiting for facade snapshot for job %d", jobID)
}

func (ctx *restartCaseContext) waitForDB(timeout time.Duration, predicate func(restartDBSnapshot) bool) (restartDBSnapshot, error) {
	deadline := time.Now().Add(timeout)
	var last restartDBSnapshot
	for time.Now().Before(deadline) {
		snapshot, err := captureRestartDBSnapshot(filepath.Join(ctx.CaseDir, "weaver.db"))
		if err == nil {
			last = snapshot
			if predicate(snapshot) {
				return snapshot, nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return last, fmt.Errorf("timeout waiting for DB predicate")
}

func normalizeRestartDBStatus(status string) string {
	normalized := strings.ToLower(strings.TrimSpace(status))
	normalized = strings.ReplaceAll(normalized, "_", "")
	switch normalized {
	case "complete", "completed":
		return "COMPLETE"
	case "queuedrepair":
		return "QUEUED_REPAIR"
	case "queuedextract":
		return "QUEUED_EXTRACT"
	case "repairing":
		return "REPAIRING"
	case "extracting":
		return "EXTRACTING"
	case "verifying":
		return "VERIFYING"
	case "downloading":
		return "DOWNLOADING"
	case "checking":
		return "CHECKING"
	case "paused":
		return "PAUSED"
	case "queued":
		return "QUEUED"
	case "moving":
		return "MOVING"
	case "waitingforvolumes":
		return "WAITING_FOR_VOLUMES"
	case "awaitingrepair":
		return "AWAITING_REPAIR"
	case "failed":
		return "FAILED"
	default:
		return strings.ToUpper(strings.TrimSpace(status))
	}
}

func restartJobStatusFromDB(dbPath string, jobID int) (string, bool) {
	available, err := weaverStateDBAvailable(dbPath)
	if err != nil || !available {
		return "", false
	}
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return "", false
	}
	defer db.Close()

	var status string
	if err := db.QueryRow(rebindWeaverSQL(datastore, `SELECT status FROM active_jobs WHERE job_id = ?`), jobID).Scan(&status); err == nil {
		return normalizeRestartDBStatus(status), true
	}
	if err := db.QueryRow(rebindWeaverSQL(datastore, `SELECT status FROM job_history WHERE job_id = ?`), jobID).Scan(&status); err == nil {
		return normalizeRestartDBStatus(status), true
	}
	return "", false
}

func (ctx *restartCaseContext) pollJobOnce(jobID int) string {
	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	if status, ok := restartJobStatusFromDB(dbPath, jobID); ok {
		return status
	}
	return pollJobOnce(ctx.weaverURL, jobID)
}

func (ctx *restartCaseContext) waitForAllTerminal(jobIDs []int, timeout time.Duration) (map[int]string, error) {
	deadline := time.Now().Add(timeout)
	statuses := map[int]string{}
	for time.Now().Before(deadline) {
		complete := true
		for _, jobID := range jobIDs {
			status := ctx.pollJobOnce(jobID)
			statuses[jobID] = status
			if status != "COMPLETE" && status != "FAILED" {
				complete = false
			}
		}
		if complete {
			return statuses, nil
		}
		time.Sleep(2 * time.Second)
	}
	return statuses, fmt.Errorf("timeout waiting for terminal status for jobs %v", jobIDs)
}

func pauseQueueItemGraphQL(weaverURL string, jobID int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"query":     `mutation($id: Int!) { pauseQueueItem(id: $id) { success message } }`,
		"variables": map[string]interface{}{"id": jobID},
	})
	resp, err := postGraphQLWithClient(&http.Client{Timeout: 30 * time.Second}, weaverURL, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var gqlResp struct {
		Data struct {
			Pause struct {
				Success bool   `json:"success"`
				Message string `json:"message"`
			} `json:"pauseQueueItem"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return err
	}
	if len(gqlResp.Errors) > 0 {
		return fmt.Errorf("pauseQueueItem: %s", gqlResp.Errors[0].Message)
	}
	if !gqlResp.Data.Pause.Success {
		return fmt.Errorf("pauseQueueItem rejected: %s", gqlResp.Data.Pause.Message)
	}
	return nil
}

func resumeQueueItemGraphQL(weaverURL string, jobID int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"query":     `mutation($id: Int!) { resumeQueueItem(id: $id) { success message } }`,
		"variables": map[string]interface{}{"id": jobID},
	})
	resp, err := postGraphQLWithClient(&http.Client{Timeout: 30 * time.Second}, weaverURL, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var gqlResp struct {
		Data struct {
			Resume struct {
				Success bool   `json:"success"`
				Message string `json:"message"`
			} `json:"resumeQueueItem"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return err
	}
	if len(gqlResp.Errors) > 0 {
		return fmt.Errorf("resumeQueueItem: %s", gqlResp.Errors[0].Message)
	}
	if !gqlResp.Data.Resume.Success {
		return fmt.Errorf("resumeQueueItem rejected: %s", gqlResp.Data.Resume.Message)
	}
	return nil
}

func jobStatusFromDB(snapshot restartDBSnapshot, jobID int) string {
	for _, job := range snapshot.ActiveJobs {
		if job.JobID == jobID {
			return normalizeDBStatus(job.Status)
		}
	}
	for _, job := range snapshot.HistoryJobs {
		if job.JobID == jobID {
			return normalizeDBStatus(job.Status)
		}
	}
	return ""
}

func normalizeDBStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return ""
	}

	var out strings.Builder
	prevLowerOrDigit := false
	prevSeparator := false
	for _, r := range status {
		switch {
		case r == '_' || r == '-' || r == ' ':
			if out.Len() > 0 && !prevSeparator {
				out.WriteByte('_')
			}
			prevLowerOrDigit = false
			prevSeparator = true
		case r >= 'A' && r <= 'Z':
			if out.Len() > 0 && !prevSeparator && prevLowerOrDigit {
				out.WriteByte('_')
			}
			out.WriteRune(r + ('a' - 'A'))
			prevLowerOrDigit = false
			prevSeparator = false
		default:
			out.WriteRune(r)
			prevLowerOrDigit = (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
			prevSeparator = false
		}
	}
	return strings.Trim(out.String(), "_")
}

func dbStatusTerminal(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "complete", "completed", "failed":
		return true
	default:
		return false
	}
}

func allJobsTerminalInSnapshot(snapshot restartDBSnapshot, jobIDs []int) bool {
	if len(jobIDs) == 0 {
		return false
	}
	for _, jobID := range jobIDs {
		if !dbStatusTerminal(jobStatusFromDB(snapshot, jobID)) {
			return false
		}
	}
	return true
}

func activeSubmittedJobCount(snapshot restartDBSnapshot, jobIDs []int) int {
	active := 0
	for _, jobID := range jobIDs {
		status := jobStatusFromDB(snapshot, jobID)
		if status != "" && !dbStatusTerminal(status) {
			active++
		}
	}
	return active
}

func waitForDelayHookRestartPoint(ctx *restartCaseContext, jobIDs []int, hook string, timeout time.Duration) (restartDBSnapshot, error) {
	deadline := time.Now().Add(timeout)
	needle := fmt.Sprintf("hook=\"%s\"", hook)
	var last restartDBSnapshot
	for time.Now().Before(deadline) {
		hookObserved := logContains(ctx.caseLogPath, needle)
		snapshot, err := captureRestartDBSnapshot(filepath.Join(ctx.CaseDir, "weaver.db"))
		if err == nil {
			last = snapshot
			if hookObserved && activeSubmittedJobCount(snapshot, jobIDs) > 0 {
				return snapshot, nil
			}
			if allJobsTerminalInSnapshot(snapshot, jobIDs) {
				if hookObserved {
					return snapshot, fmt.Errorf("%s delay hook fired but submitted jobs completed before restart point", hook)
				}
				return snapshot, fmt.Errorf("submitted jobs completed before %s delay hook was observed", hook)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return last, fmt.Errorf("timeout waiting for %s delay hook restart point", hook)
}

func logContains(logPath string, needle string) bool {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return false
	}
	return strings.Contains(stripANSIEscapeSequences(string(data)), needle)
}

func stripANSIEscapeSequences(line string) string {
	var b strings.Builder
	b.Grow(len(line))
	for i := 0; i < len(line); i++ {
		if line[i] != '\x1b' {
			b.WriteByte(line[i])
			continue
		}
		if i+1 >= len(line) || line[i+1] != '[' {
			continue
		}
		i += 2
		for i < len(line) {
			ch := line[i]
			if ch >= '@' && ch <= '~' {
				break
			}
			i++
		}
	}
	return b.String()
}

func maxBodyFetchCount(metrics restartNntpMetrics) int {
	max := 0
	for _, count := range metrics.BodyCounts {
		if count > max {
			max = count
		}
	}
	return max
}

func repeatedBodyFetchStats(metrics restartNntpMetrics) (ids int, extra int) {
	for _, count := range metrics.BodyCounts {
		if count <= 1 {
			continue
		}
		ids++
		extra += count - 1
	}
	return ids, extra
}

func nzbFullSegmentFloor(slug string, rawFloor uint64) (uint64, error) {
	path := filepath.Join(fixturesDir(), slug, slug+".nzb")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read NZB %s: %w", path, err)
	}
	var doc struct {
		Files []struct {
			Segments []struct {
				Bytes uint64 `xml:"bytes,attr"`
			} `xml:"segments>segment"`
		} `xml:"file"`
	}
	if err := xml.Unmarshal(data, &doc); err != nil {
		return 0, fmt.Errorf("parse NZB %s: %w", path, err)
	}
	if len(doc.Files) == 0 {
		return 0, fmt.Errorf("NZB %s has no files", path)
	}

	var floor uint64
	var segmentEnd uint64
	for _, segment := range doc.Files[0].Segments {
		segmentEnd += segment.Bytes
		if segmentEnd > rawFloor {
			break
		}
		floor = segmentEnd
	}
	return floor, nil
}

func stagingMemberFromSnapshot(snapshot restartFilesystemSnapshot, jobID int, partial bool) (string, string, int64, bool) {
	prefix := fmt.Sprintf(".weaver-staging/%d/tmp/", jobID)
	for _, entry := range snapshot.Complete {
		if !strings.HasPrefix(entry.Path, prefix) {
			continue
		}
		isPartial := strings.HasSuffix(entry.Path, ".partial")
		if isPartial != partial {
			continue
		}
		memberName := strings.TrimPrefix(entry.Path, prefix)
		if partial {
			memberName = strings.TrimSuffix(memberName, ".partial")
		}
		return memberName, entry.Path, entry.Size, true
	}
	return "", "", 0, false
}

func completeSnapshotContainsMember(snapshot restartFilesystemSnapshot, memberName string) bool {
	for _, entry := range snapshot.Complete {
		if strings.HasPrefix(entry.Path, ".weaver-staging/") {
			continue
		}
		if entry.Path == memberName || strings.HasSuffix(entry.Path, "/"+memberName) {
			return true
		}
	}
	return false
}

func completeRootPath(relativePath string) string {
	return filepath.Join(localWeaverDir(), "complete", filepath.FromSlash(relativePath))
}

func classifyRestartResult(profile restartProfile, pass bool, passSummary string, gap bool, gapSummary string, failSummary string, notes ...string) restartCaseResult {
	if pass {
		return restartCaseResult{Classification: restartPass, Summary: passSummary, Notes: notes}
	}
	if profile == restartProfileCurrent && gap {
		return restartCaseResult{Classification: restartDocumentedGap, Summary: gapSummary, Notes: notes}
	}
	return restartCaseResult{Classification: restartFail, Summary: failSummary, Notes: notes}
}

func runDownloadCompletedFileSurvivesRestart(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", 8, restartExtractDelayEnv); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("rar5-multi-member")
	if err != nil {
		return restartCaseResult{}, err
	}

	preDB, _, _, err := func() (restartDBSnapshot, restartFilesystemSnapshot, restartNntpMetrics, error) {
		_, err := ctx.waitForDB(3*time.Minute, func(snapshot restartDBSnapshot) bool {
			status := jobStatusFromDB(snapshot, jobID)
			metrics := snapshot.JobMetrics[jobID]
			return metrics.Files > 0 && status != "" && !dbStatusTerminal(status)
		})
		if err != nil {
			return restartDBSnapshot{}, restartFilesystemSnapshot{}, restartNntpMetrics{}, err
		}
		return ctx.captureEvidence("pre_crash", "e2e-rar5-multi-member-")
	}()
	if err != nil {
		return restartCaseResult{}, err
	}
	preCompletedFiles := preDB.JobMetrics[jobID].Files

	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	postFacade, err := ctx.waitForFacade(jobID, 2*time.Minute, func(snapshot facadeItemSnapshot) bool {
		return snapshot.Found
	})
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, metrics, err := ctx.captureEvidence("post_restart", "e2e-rar5-multi-member-")
	if err != nil {
		return restartCaseResult{}, err
	}
	maxFetch := maxBodyFetchCount(metrics)

	pass := preCompletedFiles > 0 && statuses[jobID] == "COMPLETE" && maxFetch <= 1
	return classifyRestartResult(
		ctx.Profile,
		pass,
		fmt.Sprintf("restored %d completed file(s) without redownloading archive articles (post=%d max BODY fetch count %d)", preCompletedFiles, postFacade.DownloadedBytes, maxFetch),
		false,
		"",
		fmt.Sprintf("completed file restart redownloaded or failed (files=%d post=%d max BODY fetch count %d final=%s)", preCompletedFiles, postFacade.DownloadedBytes, maxFetch, statuses[jobID]),
	), nil
}

func runDownloadProgressFloorSurvivesRestart(ctx *restartCaseContext) (restartCaseResult, error) {
	const checkpointBytes = 4 * 1024 * 1024
	const checkpointEnv = "WEAVER_E2E_DOWNLOAD_RESTART_CHECKPOINT_BYTES=4194304"
	const checkpointDelayEnv = "WEAVER_E2E_DELAY=download.after_progress_floor_flush=20000"

	if err := ctx.startWeaverWithOptions("", 8, checkpointEnv, checkpointDelayEnv); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("rar5-multi-member")
	if err != nil {
		return restartCaseResult{}, err
	}

	deadline := time.Now().Add(6 * time.Minute)
	lastSnapshot := restartDBSnapshot{}
	sawActiveJob := false
	var preDB restartDBSnapshot
	foundProgressFloorGap := false
	gapSummary := ""

	for time.Now().Before(deadline) {
		snapshot, snapErr := captureRestartDBSnapshot(filepath.Join(ctx.CaseDir, "weaver.db"))
		if snapErr == nil {
			lastSnapshot = snapshot
			status := jobStatusFromDB(snapshot, jobID)
			metrics := snapshot.JobMetrics[jobID]
			if status != "" {
				sawActiveJob = true
			}
			if status != "" && !dbStatusTerminal(status) && metrics.FileProgressBytes >= checkpointBytes {
				preDB, _, _, err = ctx.captureEvidence("pre_crash", "e2e-rar5-multi-member-")
				if err != nil {
					return restartCaseResult{}, err
				}
				foundProgressFloorGap = true
				break
			}
			if sawActiveJob && status == "" {
				_, _, _, _ = ctx.captureEvidence("pre_timeout", "e2e-rar5-multi-member-")
				gapSummary = fmt.Sprintf(
					"job left active state before a persisted progress-floor gap appeared (last_active_jobs=%v last_metrics=%+v)",
					lastSnapshot.ActiveJobs,
					lastSnapshot.JobMetrics[jobID],
				)
				break
			}
			if status == "complete" || status == "failed" {
				_, _, _, _ = ctx.captureEvidence("pre_timeout", "e2e-rar5-multi-member-")
				gapSummary = fmt.Sprintf(
					"job reached %s before a persisted progress-floor gap appeared (floor=%d committed=%d)",
					status,
					metrics.FileProgressBytes,
					metrics.SegmentBytes,
				)
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}

	if !foundProgressFloorGap {
		if gapSummary == "" {
			_, _, _, _ = ctx.captureEvidence("pre_timeout", "e2e-rar5-multi-member-")
			gapSummary = fmt.Sprintf(
				"timeout waiting for persisted progress-floor gap (last_active_jobs=%v last_metrics=%+v)",
				lastSnapshot.ActiveJobs,
				lastSnapshot.JobMetrics[jobID],
			)
		}
		return classifyRestartResult(
			ctx.Profile,
			false,
			"",
			true,
			gapSummary,
			gapSummary,
		), nil
	}
	progressFloorBytes := preDB.JobMetrics[jobID].FileProgressBytes
	committedBytes := preDB.JobMetrics[jobID].SegmentBytes
	expectedRestoredBytes, err := nzbFullSegmentFloor("rar5-multi-member", progressFloorBytes)
	if err != nil {
		return restartCaseResult{}, err
	}

	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	postFacade, err := ctx.waitForFacade(jobID, 2*time.Minute, func(snapshot facadeItemSnapshot) bool {
		return snapshot.Found
	})
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, metrics, err := ctx.captureEvidence("post_restart", "e2e-rar5-multi-member-")
	if err != nil {
		return restartCaseResult{}, err
	}
	maxFetch := maxBodyFetchCount(metrics)
	repeatedIDs, extraFetches := repeatedBodyFetchStats(metrics)
	pass := statuses[jobID] == "COMPLETE"
	return classifyRestartResult(
		ctx.Profile,
		pass,
		fmt.Sprintf("completed after restart with lossy redownload allowed (raw_floor=%d expected=%d committed=%d post=%d max BODY fetch count %d repeated BODY ids %d extra BODY fetches %d)", progressFloorBytes, expectedRestoredBytes, committedBytes, postFacade.DownloadedBytes, maxFetch, repeatedIDs, extraFetches),
		false,
		"",
		fmt.Sprintf("job did not complete after restart (raw_floor=%d expected=%d committed=%d post=%d max BODY fetch count %d repeated BODY ids %d extra BODY fetches %d final=%s)", progressFloorBytes, expectedRestoredBytes, committedBytes, postFacade.DownloadedBytes, maxFetch, repeatedIDs, extraFetches, statuses[jobID]),
	), nil
}

func runQueuedRepairSurvivesAndKeepsPlace(ctx *restartCaseContext) (restartCaseResult, error) {
	const repairDelayEnv = "WEAVER_E2E_DELAY=repair.task_start=5000"

	if err := ctx.startWeaverWithOptions("", 12, repairDelayEnv); err != nil {
		return restartCaseResult{}, err
	}

	submitOrder := []string{
		"par2-heavy-damage",
		"par2-heavy-damage-a",
	}
	jobIDs := make([]int, 0, len(submitOrder))
	for _, slug := range submitOrder {
		jobID, err := ctx.submitSlug(slug)
		if err != nil {
			return restartCaseResult{}, fmt.Errorf("submit %s: %w", slug, err)
		}
		jobIDs = append(jobIDs, jobID)
	}
	if len(jobIDs) < 2 {
		return restartCaseResult{}, fmt.Errorf("queued repair case requires at least two submitted jobs")
	}

	firstJobID := jobIDs[0]
	secondJobID := jobIDs[1]
	preDB, err := waitForDelayHookRestartPoint(ctx, jobIDs, "repair.task_start", 2*time.Minute)
	if err != nil {
		return restartCaseResult{}, fmt.Errorf("wait for repair restart point: %w", err)
	}

	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	if _, _, _, err := ctx.captureEvidence("pre_crash", ""); err != nil {
		return restartCaseResult{}, err
	}

	if err := ctx.startWeaverWithOptions("", 12, repairDelayEnv); err != nil {
		return restartCaseResult{}, err
	}
	postDB, _, _, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal(jobIDs, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	allComplete := true
	for _, jobID := range jobIDs {
		if statuses[jobID] != "COMPLETE" {
			allComplete = false
		}
	}
	return classifyRestartResult(
		ctx.Profile,
		allComplete,
		"lossy repair restart rediscovered durable repair inputs and completed both jobs",
		false,
		"",
		fmt.Sprintf("lossy repair restart failed (pre_crash=%v post_restart=%v final=%v)", preDB.ActiveJobs, postDB.ActiveJobs, statuses),
		fmt.Sprintf("pre_crash observed repair start for job %d with candidate job %d still active", firstJobID, secondJobID),
	), nil
}

func runQueuedExtractSurvivesAndKeepsPlace(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", 8, restartExtractDelayEnv); err != nil {
		return restartCaseResult{}, err
	}
	jobIDs, err := ctx.submitSlugNTimes("rar5-multi-member", 2)
	if err != nil {
		return restartCaseResult{}, err
	}
	preDB, err := waitForDelayHookRestartPoint(ctx, jobIDs, "extract.member_start", 2*time.Minute)
	if err != nil {
		return restartCaseResult{}, fmt.Errorf("wait for extract restart point: %w", err)
	}
	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	preDB, _, _, err = ctx.captureEvidence("pre_crash", "")
	if err != nil {
		return restartCaseResult{}, err
	}

	if err := ctx.startWeaverWithOptions("", 8, restartExtractDelayEnv); err != nil {
		return restartCaseResult{}, err
	}
	postDB, err := ctx.waitForDB(2*time.Minute, func(snapshot restartDBSnapshot) bool {
		for _, jobID := range jobIDs {
			if jobStatusFromDB(snapshot, jobID) == "" {
				return false
			}
		}
		return true
	})
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, err = ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}

	statuses, err := ctx.waitForAllTerminal(jobIDs, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	allComplete := true
	for _, jobID := range jobIDs {
		if statuses[jobID] != "COMPLETE" {
			allComplete = false
		}
	}
	return classifyRestartResult(
		ctx.Profile,
		allComplete,
		"lossy extract restart rediscovered durable archive files and completed both jobs",
		false,
		"",
		fmt.Sprintf("lossy extract restart failed (pre_crash=%v post_restart=%v final=%v)", preDB.ActiveJobs, postDB.ActiveJobs, statuses),
	), nil
}

func runPausedJobRestoresResumeTarget(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", 1); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("rar5-multi-member")
	if err != nil {
		return restartCaseResult{}, err
	}
	_, err = ctx.waitForFacade(jobID, 3*time.Minute, func(snapshot facadeItemSnapshot) bool {
		return snapshot.InQueue &&
			snapshot.Status == "DOWNLOADING" &&
			snapshot.DownloadedBytes >= 4*1024*1024
	})
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := pauseQueueItemGraphQL(ctx.weaverURL, jobID); err != nil {
		return restartCaseResult{}, err
	}
	_, err = ctx.waitForDB(60*time.Second, func(snapshot restartDBSnapshot) bool {
		return jobStatusFromDB(snapshot, jobID) == "paused"
	})
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	pausedResumeStatus := "downloading"
	if err := forceActiveJobRuntimeStates(filepath.Join(ctx.CaseDir, "weaver.db"), map[int]forcedActiveRuntimeState{
		jobID: {
			Status:             "paused",
			PausedResumeStatus: &pausedResumeStatus,
		},
	}); err != nil {
		return restartCaseResult{}, fmt.Errorf("force paused downloading runtime state for restart case: %w", err)
	}
	if _, _, _, err := ctx.captureEvidence("pre_crash", ""); err != nil {
		return restartCaseResult{}, err
	}

	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	postPause, restoredPaused, err := ctx.observeFacade(jobID, 30*time.Second)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_restart", "")

	firstResumedDBStatus := ""
	if restoredPaused {
		if err := resumeQueueItemGraphQL(ctx.weaverURL, jobID); err != nil {
			return restartCaseResult{}, err
		}
		resumeSnapshot, err := ctx.waitForDB(2*time.Minute, func(snapshot restartDBSnapshot) bool {
			status := jobStatusFromDB(snapshot, jobID)
			if status == "" || status == "paused" {
				return false
			}
			firstResumedDBStatus = status
			return true
		})
		if err != nil {
			return restartCaseResult{}, err
		}
		writeRestartJSON(filepath.Join(ctx.CaseDir, "post_resume_db.json"), resumeSnapshot)
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	allComplete := statuses[jobID] == "COMPLETE"
	pass := allComplete && restoredPaused && firstResumedDBStatus == "downloading"
	gap := allComplete && (!restoredPaused || (firstResumedDBStatus != "" && firstResumedDBStatus != "downloading"))
	return classifyRestartResult(
		ctx.Profile,
		pass,
		fmt.Sprintf("paused job %d restored paused state and resumed into %s", jobID, firstResumedDBStatus),
		gap,
		fmt.Sprintf("current pause recovery lost the download resume target (post_restart=%s resumed=%s)", postPause.Status, firstResumedDBStatus),
		fmt.Sprintf("pause recovery failed (post_restart=%s resumed=%s final=%v)", postPause.Status, firstResumedDBStatus, statuses),
	), nil
}

func runVerifyingRestartsCleanly(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaver("status.enter_verifying"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("par2-small-repair")
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(5 * time.Minute); err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("pre_crash", "")
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_restart", "")
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	pass := statuses[jobID] == "COMPLETE"
	return classifyRestartResult(
		ctx.Profile,
		pass,
		"verification restarted from the phase boundary and still completed",
		false,
		"",
		fmt.Sprintf("verification restart case did not complete successfully: %s", statuses[jobID]),
	), nil
}

func runRepairingRestartsCleanly(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaver("status.enter_repairing"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("par2-small-repair")
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(8 * time.Minute); err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("pre_crash", "")
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_restart", "")
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	pass := statuses[jobID] == "COMPLETE"
	return classifyRestartResult(
		ctx.Profile,
		pass,
		"repair restarted from the phase boundary and still completed",
		false,
		"",
		fmt.Sprintf("repair restart case did not complete successfully: %s", statuses[jobID]),
	), nil
}

func runRarExtractionResumesFromCheckpoint(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaver("extract.after_volume_checkpoint"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("rar5-multi-member")
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(10 * time.Minute); err != nil {
		return restartCaseResult{}, err
	}
	_, preFS, _, err := ctx.captureEvidence("pre_crash", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	memberName, partialPath, partialSize, ok := stagingMemberFromSnapshot(preFS, jobID, true)
	if !ok {
		return restartCaseResult{}, fmt.Errorf("no staged partial member found before crash")
	}
	if partialSize <= 0 {
		return restartCaseResult{}, fmt.Errorf("staged partial member %s was empty before restart", partialPath)
	}

	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, postFS, metrics, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	maxFetch := maxBodyFetchCount(metrics)
	memberPresent := completeSnapshotContainsMember(postFS, memberName)
	pass := statuses[jobID] == "COMPLETE" && memberPresent && maxFetch <= 1
	return classifyRestartResult(
		ctx.Profile,
		pass,
		fmt.Sprintf("lossy extraction restart retried from durable archive files and completed %s (partial=%s size=%d max BODY fetch count %d)", memberName, partialPath, partialSize, maxFetch),
		false,
		"",
		fmt.Sprintf("lossy extraction restart failed (member=%s present=%v max BODY fetch count %d final=%s)", memberName, memberPresent, maxFetch, statuses[jobID]),
	), nil
}

func runRarFinalizeReconcilesAfterRename(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaver("extract.after_finalize_rename_before_record"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("rar5-multi-member")
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(12 * time.Minute); err != nil {
		return restartCaseResult{}, err
	}
	_, preFS, _, err := ctx.captureEvidence("pre_crash", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	memberName, stagedPath, stagedSize, ok := stagingMemberFromSnapshot(preFS, jobID, false)
	if !ok {
		return restartCaseResult{}, fmt.Errorf("no staged finalized member found before crash")
	}
	if stagedSize <= 0 {
		return restartCaseResult{}, fmt.Errorf("staged finalized member %s was empty before restart", stagedPath)
	}

	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, postFS, metrics, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	maxFetch := maxBodyFetchCount(metrics)
	memberPresent := completeSnapshotContainsMember(postFS, memberName)
	pass := statuses[jobID] == "COMPLETE" && memberPresent && maxFetch <= 1
	return classifyRestartResult(
		ctx.Profile,
		pass,
		fmt.Sprintf("finalize-after-rename restart rediscovered durable archive files and completed %s (staged=%s size=%d max BODY fetch count %d)", memberName, stagedPath, stagedSize, maxFetch),
		false,
		"",
		fmt.Sprintf("finalize-after-rename restart failed (member=%s present=%v max BODY fetch count %d final=%s)", memberName, memberPresent, maxFetch, statuses[jobID]),
	), nil
}

func runStaleActiveExtractedRowsClearAfterRestart(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaver("extract.after_volume_checkpoint"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("rar5-multi-member")
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(10 * time.Minute); err != nil {
		return restartCaseResult{}, err
	}

	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	_, preFS, _, err := ctx.captureEvidence("pre_crash", "")
	if err != nil {
		return restartCaseResult{}, err
	}

	memberName, partialPath, partialSize, ok := stagingMemberFromSnapshot(preFS, jobID, true)
	if !ok {
		return restartCaseResult{}, fmt.Errorf("no staged partial member found before crash")
	}
	if partialSize <= 0 {
		return restartCaseResult{}, fmt.Errorf("staged partial member %s was empty before restart", partialPath)
	}
	staleMember := restartExtractedMember{
		JobID:      jobID,
		MemberName: memberName,
		OutputPath: completeRootPath(strings.TrimSuffix(partialPath, ".partial")),
	}
	if err := removeFileIfExists(staleMember.OutputPath); err != nil {
		return restartCaseResult{}, fmt.Errorf("remove stale extracted output %s: %w", staleMember.OutputPath, err)
	}
	if err := insertActiveExtractedMembers(dbPath, []restartExtractedMember{staleMember}); err != nil {
		return restartCaseResult{}, fmt.Errorf("insert stale active_extracted row: %w", err)
	}
	downloading := "downloading"
	idle := "idle"
	active := "active"
	if err := forceActiveJobRuntimeStates(dbPath, map[int]forcedActiveRuntimeState{
		jobID: {
			Status:        "downloading",
			DownloadState: &downloading,
			PostState:     &idle,
			RunState:      &active,
		},
	}); err != nil {
		return restartCaseResult{}, fmt.Errorf("force stale active_extracted runtime state: %w", err)
	}
	insertedCount, err := countActiveExtractedMembers(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, fmt.Errorf("count stale active_extracted rows before restart: %w", err)
	}
	if insertedCount == 0 {
		return restartCaseResult{}, fmt.Errorf("expected stale active_extracted row before restart")
	}
	if _, _, _, err := ctx.captureEvidence("pre_crash", ""); err != nil {
		return restartCaseResult{}, err
	}

	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}

	clearedWhileActive := false
	recoveredWhileActive := false
	recoveredStagePath := filepath.ToSlash(
		filepath.Join(".weaver-staging", strconv.Itoa(jobID), "tmp", staleMember.MemberName),
	)
	observeDeadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(observeDeadline) {
		snapshot, snapErr := captureRestartDBSnapshot(dbPath)
		if snapErr == nil {
			fsSnapshot := captureRestartFilesystemSnapshot()
			count, countErr := countActiveExtractedMembers(dbPath, jobID)
			if countErr == nil && count == 0 {
				if status := jobStatusFromDB(snapshot, jobID); !dbStatusTerminal(status) {
					clearedWhileActive = true
				}
				break
			}
			if status := jobStatusFromDB(snapshot, jobID); !dbStatusTerminal(status) {
				for _, entry := range fsSnapshot.Complete {
					if entry.Path == recoveredStagePath {
						recoveredWhileActive = true
						break
					}
				}
				if recoveredWhileActive {
					break
				}
			}
			if allJobsTerminalInSnapshot(snapshot, []int{jobID}) {
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}

	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	postCount, err := countActiveExtractedMembers(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, fmt.Errorf("count stale active_extracted rows after restart: %w", err)
	}
	_, postFS, metrics, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}

	maxFetch := maxBodyFetchCount(metrics)
	memberPresent := completeSnapshotContainsMember(postFS, staleMember.MemberName)
	pass := statuses[jobID] == "COMPLETE" && postCount == 0 && memberPresent && maxFetch <= 1
	return classifyRestartResult(
		ctx.Profile,
		pass,
		fmt.Sprintf(
			"stale extracted-member marker for %s was ignored until output validation succeeded (cleared_while_active=%v recovered_while_active=%v max BODY fetch count %d)",
			staleMember.MemberName,
			clearedWhileActive,
			recoveredWhileActive,
			maxFetch,
		),
		false,
		"",
		fmt.Sprintf("stale active_extracted validation failed (member=%s present=%v post_count=%d max BODY fetch count %d final=%s cleared_while_active=%v recovered_while_active=%v)", staleMember.MemberName, memberPresent, postCount, maxFetch, statuses[jobID], clearedWhileActive, recoveredWhileActive),
	), nil
}

func runVerificationReconcilesStaleExtractingRuntime(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaver("status.enter_verifying"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("par2-small-repair")
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(5 * time.Minute); err != nil {
		return restartCaseResult{}, err
	}

	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	if _, _, _, err := ctx.captureEvidence("pre_verifying", ""); err != nil {
		return restartCaseResult{}, err
	}
	if !logContains(ctx.caseLogPath, "status.enter_verifying") {
		return restartCaseResult{}, fmt.Errorf("expected verifying failpoint evidence in log before restart")
	}

	complete := "complete"
	extracting := "extracting"
	active := "active"
	if err := forceActiveJobRuntimeStates(dbPath, map[int]forcedActiveRuntimeState{
		jobID: {
			Status:        "extracting",
			DownloadState: &complete,
			PostState:     &extracting,
			RunState:      &active,
		},
	}); err != nil {
		return restartCaseResult{}, fmt.Errorf("force stale extracting runtime before restart: %w", err)
	}
	if _, _, _, err := ctx.captureEvidence("pre_crash", ""); err != nil {
		return restartCaseResult{}, err
	}

	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}

	reconciledOutOfExtracting := false
	observeDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(observeDeadline) {
		snapshot, snapErr := captureRestartDBSnapshot(dbPath)
		if snapErr == nil {
			status := jobStatusFromDB(snapshot, jobID)
			job, activeJob := activeJobFromSnapshot(snapshot, jobID)
			if status != "" && strings.ToLower(status) != "extracting" {
				if !activeJob || strings.ToLower(job.PostState) != "extracting" {
					reconciledOutOfExtracting = true
					break
				}
			}
			if dbStatusTerminal(status) {
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}

	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_restart", "")

	pass := statuses[jobID] == "COMPLETE" && reconciledOutOfExtracting
	gap := statuses[jobID] == "COMPLETE" && !reconciledOutOfExtracting
	return classifyRestartResult(
		ctx.Profile,
		pass,
		"verification restart reconciled stale extracting runtime and still completed",
		gap,
		"job completed, but the stale extracting runtime was never observed leaving extracting before terminalization",
		fmt.Sprintf("verification restart failed to reconcile stale extracting runtime (final=%s log=%s)", statuses[jobID], ctx.caseLogPath),
	), nil
}
