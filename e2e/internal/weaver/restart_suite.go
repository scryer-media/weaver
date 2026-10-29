// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
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

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

type restartProfile string

const (
	restartProfileHardened restartProfile = "hardened"
	restartProfileCurrent  restartProfile = "current"
)

const (
	// An archive's engine still producing members once every volume it reads
	// is in: the job is extracting, so it cannot terminalize.
	restartExtractDelayEnv = "WEAVER_E2E_DELAY=status.enter_extracting=20000"
	restartPar2AliasSet    = "a31f592e0b874d3ea8c44161b77f3049"
)

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

type restartPar2AliasState struct {
	IdentityCount       int            `json:"identity_count"`
	AliasClaimants      int            `json:"alias_claimants"`
	CurrentFilenames    []string       `json:"current_filenames"`
	CanonicalFilenames  []string       `json:"canonical_filenames"`
	ClassificationSets  []string       `json:"classification_sets"`
	FactSets            map[string]int `json:"fact_sets"`
	ExistingVolumePaths []string       `json:"existing_volume_paths"`
	ActiveExtracted     int            `json:"active_extracted"`
	ExtractionChunks    int            `json:"extraction_chunks"`
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
	cases := append([]restartCase{
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
			Description: "A file moved to the name its committed checkpoint recorded before the move itself was recorded: restart should adopt it there, fetch nothing it held, and complete",
			Slugs:       []string{renameRestartSlug},
			Timeout:     20 * time.Minute,
			Run:         runRenamedFileAdoptedAfterRestart,
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
		{
			Name:        "direct_store_par2_alias_claimant_completes_after_restart",
			Description: "A consumed direct-store RAR set must not wedge behind its unmaterialized PAR2 alias after restart",
			Slugs:       []string{"direct-store-par2-alias-restart"},
			Timeout:     8 * time.Minute,
			Run:         runDirectStorePar2AliasClaimantCompletesAfterRestart,
		},
		{
			Name:        "conventional_7z_damaged_block_repairs_once",
			Description: "A split 7z settled clean on the strong-decode claim whose conventional extraction hits a damaged block is repaired once and completes",
			Slugs:       []string{conventional7zRepairSlug},
			Run:         runConventional7zDamagedBlockRepairsOnce,
		},
		{
			Name:        "early_gap_restart_preserves_bytes_and_refusal",
			Description: "A restart keeps healthy durable bytes past an early missing article and preserves its definitive refusal",
			Slugs:       []string{"single-mkv"},
			Timeout:     15 * time.Minute,
			Run:         runEarlyGapRestartPreservesBytesAndRefusal,
		},
		{
			Name:        "interrupt_during_verifying_refetches_nothing",
			Description: "A Ctrl-C while a job verifies restarts verification from the files without a single fetch",
			Slugs:       []string{"par2-small-repair"},
			Timeout:     10 * time.Minute,
			Run:         runInterruptDuringVerifyingRefetchesNothing,
		},
		{
			Name:        "interrupt_during_par2_pass_stops_the_pass",
			Description: "A Ctrl-C during a PAR2 pass stops the pass before the process exits, and the restart scans again and repairs",
			Slugs:       []string{"par2-heavy-damage"},
			Timeout:     15 * time.Minute,
			Run:         runInterruptDuringPar2PassStopsThePass,
		},
		{
			Name:        "credentials_refused_mid_job_settles",
			Description: "The only server refuses the account mid-job; the queued articles are retired in one sweep and the job settles",
			Slugs:       []string{"single-mkv"},
			Timeout:     10 * time.Minute,
			Run:         runCredentialsRefusedMidJobSettles,
		},
		{
			Name:        "shutdown_mid_batch_is_not_an_unclean_batch",
			Description: "A Ctrl-C mid-batch records no unclean batch, keeps the persisted depth and dumps no stall diagnostics",
			Slugs:       []string{"single-mkv"},
			Timeout:     10 * time.Minute,
			Run:         runShutdownMidBatchIsNotAnUncleanBatch,
		},
		{
			Name:        "early_gap_does_not_throttle",
			Description: "With one early article missing, the rest of the file downloads while it is missing",
			Slugs:       []string{"single-mkv"},
			Timeout:     10 * time.Minute,
			Run:         runEarlyGapDoesNotThrottle,
		},
		{
			Name:        "pre_65_checkpoint_row_refused",
			Description: "A prefix floor left by a pre-65 database is dropped by the upgrade, and the file downloads again from nothing",
			Slugs:       []string{"single-mkv"},
			Timeout:     15 * time.Minute,
			Run:         runPre65CheckpointRowRefused,
		},
		{
			Name:        "poison_extent_does_not_spread",
			Description: "An article whose yEnc extent runs into its neighbour stays its own failure across a restart",
			Slugs:       []string{"par2-small-repair"},
			Timeout:     10 * time.Minute,
			Run:         runPoisonExtentDoesNotSpread,
		},
		{
			Name:        "repair_data_wait_reads_fetching_repair_data",
			Description: "A damaged job whose recovery data the server lacks reads FETCHING_REPAIR_DATA, then repairs once it is back",
			Slugs:       []string{"par2-small-repair"},
			Timeout:     10 * time.Minute,
			Run:         runRepairDataWaitReadsFetchingRepairData,
		},
	}, graphRestartCases()...)
	cases = append(cases, heldPartRestartCases()...)
	cases = append(cases, feedVerdictRestartCases()...)
	cases = append(cases, namespaceRestartCases()...)
	return append(cases, probeLaneRestartCases()...)
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
	for slug := range set {
		out = append(out, slug)
	}
	sort.Strings(out)
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
	if err := ensureNNTPHealthBinary(); err != nil {
		log.Fatalf("build NNTP healthcheck for restart suite: %v", err)
	}
	if err := ensureNyuuImageBuilt(); err != nil {
		log.Fatalf("build nyuu image for restart suite: %v", err)
	}
	services := []string{"nntp", "nyuu"}
	if weaverUsesPostgresDatastore() {
		services = append(services, "weaver-postgres")
	}
	args := append(dockerComposeArgs("up", "-d", "--build", "--quiet-pull"), services...)
	cmd := containerengine.Command(args...)
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
		if strings.HasPrefix(entry, "WEAVER_E2E_DELAY=") || strings.HasPrefix(entry, "WEAVER_E2E_HOLD=") {
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
	return ctx.startWeaverWithOptions("", ctx.connections)
}

// restartWeaverWithEnv restarts without a failpoint but keeps the environment a
// case needs on both sides of the crash, so the path it exercised before the
// kill is the same path it resumes on.
func (ctx *restartCaseContext) restartWeaverWithEnv(extraEnv ...string) error {
	return ctx.startWeaverWithOptions("", ctx.connections, extraEnv...)
}

func (ctx *restartCaseContext) killWeaverForRestart() error {
	if ctx.weaver != nil && ctx.weaver.cmd != nil && ctx.weaver.cmd.Process != nil {
		_ = ctx.weaver.cmd.Process.Kill()
	}
	_ = os.Remove(localWeaverPIDPath())
	return waitForGraphQLDown(graphqlURL(ctx.weaverURL), 20*time.Second)
}

// waitForCrash waits for the failpoint to take the process down, and gives up
// the moment that can no longer happen.
//
// A case arms a failpoint on a seam and then waits for the process to die on
// it. If the job runs to a terminal status without ever touching that seam the
// process will never exit, so waiting out the full timeout only converts a
// certain failure into a slow one. Once every submitted job is terminal in the
// case database the verdict is already decided, so report it immediately and
// say which seam was never reached.
func (ctx *restartCaseContext) waitForCrash(timeout time.Duration, jobIDs ...int) error {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	url := graphqlURL(ctx.weaverURL)
	if len(jobIDs) == 0 {
		return waitForGraphQLDown(url, timeout)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	body := []byte(`{"query":"{ version }"}`)
	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if graphQLDown(client, url, body) {
			return nil
		}
		snapshot, err := captureRestartDBSnapshot(dbPath)
		if err == nil && allJobsTerminalInSnapshot(snapshot, jobIDs) {
			// The status flip and the process exit race each other; only call
			// it a miss once the process is still answering after the flip.
			time.Sleep(250 * time.Millisecond)
			if graphQLDown(client, url, body) {
				return nil
			}
			return fmt.Errorf(
				"submitted jobs %v reached terminal status without tripping the armed failpoint at %s",
				jobIDs,
				url,
			)
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for GraphQL shutdown at %s", url)
}

func graphQLDown(client *http.Client, url string, body []byte) bool {
	resp, err := postGraphQLWithClient(client, url, body)
	if err != nil {
		return true
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return false
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
		if graphQLDown(client, url, body) {
			return nil
		}
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

func fetchNntpConnectionMetricsFrom(host, port string) (weaverReleaseNntpConnections, error) {
	resp, err := sendNntpCommandToWithRetry(host, port, "METRICS CONNECTIONS", 5)
	if err != nil {
		return weaverReleaseNntpConnections{}, err
	}
	return decodeNntpConnectionMetrics(resp)
}

func decodeNntpConnectionMetrics(resp string) (weaverReleaseNntpConnections, error) {
	if !strings.HasPrefix(resp, "290 ") {
		return weaverReleaseNntpConnections{}, fmt.Errorf("unexpected connection metrics response: %s", resp)
	}
	var metrics weaverReleaseNntpConnections
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(resp, "290 "))), &metrics); err != nil {
		return weaverReleaseNntpConnections{}, err
	}
	return metrics, nil
}

// How long the provider may go on holding a stopped Weaver's connection
// slots before the harness calls it a fault rather than a teardown.
const nntpConnectionDrainBudget = 60 * time.Second

// waitForFreeNntpConnectionMetrics reads the provider's connection counters
// once every previous NNTP session has drained. Control sessions consume
// admission capacity but are excluded from the active and peak metrics.
//
// A capped round ends with every slot held by Weaver's lanes, and stopping
// Weaver does not hand them back at once: the provider goes on seeing those
// sockets established for seconds after the process is gone, and its cap
// counts the harness's own connection, so the read is refused until the last
// one drains. Waiting costs the assertions nothing — the counters this
// returns only ever climb, and a refused connection never records a peak.
func waitForFreeNntpConnectionMetrics(host, port string) (weaverReleaseNntpConnections, error) {
	deadline := time.Now().Add(nntpConnectionDrainBudget)
	for {
		metrics, err := fetchNntpConnectionMetricsFrom(host, port)
		if err == nil && metrics.Active == 0 {
			return metrics, nil
		}
		// Only a saturated cap is worth waiting out. Anything else is the
		// provider itself being wrong, and sitting on it for a minute would
		// only delay the report.
		if err != nil && !errors.Is(err, errNntpConnectionsSaturated) {
			return weaverReleaseNntpConnections{}, err
		}
		if !time.Now().Before(deadline) {
			return metrics, fmt.Errorf("provider sessions did not drain: active=%d: %v", metrics.Active, err)
		}
	}
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
	defer closeWeaverStateDB(db, datastore)

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
		table string
		query string
		apply func(*restartJobMetrics, int64, int64)
	}
	// A posted file's restart checkpoint is its member of the graph's article
	// root: the decoded runs on disk, and whole once they cover its length.
	coverage, err := readFileCoverageRows(db, datastore)
	if err != nil {
		return snapshot, err
	}
	for _, row := range coverage {
		metrics := snapshot.JobMetrics[row.JobID]
		metrics.Segments++
		metrics.SegmentBytes += row.Coverage.CoveredBytes()
		metrics.FileProgressBytes += row.Coverage.CoveredBytes()
		if row.Whole() {
			metrics.Files++
		}
		snapshot.JobMetrics[row.JobID] = metrics
	}

	aggregates := []aggregateSpec{
		{
			table: "active_par2_files",
			query: `SELECT job_id, COUNT(*), 0 FROM active_par2_files GROUP BY job_id`,
			apply: func(m *restartJobMetrics, count, _ int64) {
				m.Par2Files = int(count)
			},
		},
		{
			table: "active_extraction_chunks",
			query: `SELECT job_id, COUNT(*), 0 FROM active_extraction_chunks GROUP BY job_id`,
			apply: func(m *restartJobMetrics, count, _ int64) {
				m.ExtractionChunks = int(count)
			},
		},
		{
			table: "active_extracted",
			query: `SELECT job_id, COUNT(*), 0 FROM active_extracted GROUP BY job_id`,
			apply: func(m *restartJobMetrics, count, _ int64) {
				m.Extracted = int(count)
			},
		},
	}
	for _, aggregate := range aggregates {
		// A table the graph-engine migration dropped reads as empty.
		if present, err := weaverStateTableExists(db, datastore, aggregate.table); err != nil {
			return snapshot, err
		} else if !present {
			continue
		}
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

	if present, err := weaverStateTableExists(db, datastore, "active_extraction_chunks"); err != nil || !present {
		if err != nil {
			return snapshot, err
		}
		return readRestartExtractedMembers(db, datastore, snapshot)
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
	return readRestartExtractedMembers(db, datastore, snapshot)
}

func readRestartExtractedMembers(db *sql.DB, datastore weaverDatastore, snapshot restartDBSnapshot) (restartDBSnapshot, error) {
	rows, err := db.Query(rebindWeaverSQL(datastore, `SELECT job_id, member_name, output_path FROM active_extracted ORDER BY job_id, member_name`))
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
	defer closeWeaverStateDB(db, datastore)

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

// jobArchived reports whether the job's history row exists. The archive that
// writes it deletes the job's active rows in the same transaction, so once it
// exists the active side is gone too — whatever GraphQL, which answers from
// memory, reported before that commit reached the datastore.
func jobArchived(dbPath string, jobID int) (bool, error) {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return false, err
	}
	defer closeWeaverStateDB(db, datastore)

	var count int
	if err := db.QueryRow(rebindWeaverSQL(datastore, `SELECT COUNT(*) FROM job_history WHERE job_id = ?`), jobID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// waitForJobArchived polls until the job's archive has committed. A terminal
// status seen through GraphQL precedes it; anything that reads the job's
// active rows or its history row after a terminal wait waits for this first.
// It has no deadline of its own: the phase runner's bound ends a wait that
// never resolves.
func waitForJobArchived(dbPath string, jobID int) error {
	return awaitJobArchived(func() (bool, error) { return jobArchived(dbPath, jobID) }, func() {
		time.Sleep(2 * time.Second)
	})
}

func awaitJobArchived(archived func() (bool, error), pause func()) error {
	for {
		done, err := archived()
		if err != nil {
			return fmt.Errorf("read job archive state: %w", err)
		}
		if done {
			return nil
		}
		pause()
	}
}

func countActiveExtractedMembers(dbPath string, jobID int) (int, error) {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return 0, err
	}
	defer closeWeaverStateDB(db, datastore)

	var count int
	if err := db.QueryRow(rebindWeaverSQL(datastore, `SELECT COUNT(*) FROM active_extracted WHERE job_id = ?`), jobID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func capturePar2AliasState(dbPath string, jobID int) (restartPar2AliasState, error) {
	state := restartPar2AliasState{FactSets: map[string]int{}}
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return state, err
	}
	defer closeWeaverStateDB(db, datastore)

	rows, err := db.Query(rebindWeaverSQL(datastore, `
		SELECT current_filename,
		       COALESCE(canonical_filename, ''),
		       COALESCE(classification_set_name, '')
		FROM active_file_identities
		WHERE job_id = ? AND LOWER(COALESCE(classification_kind, '')) = 'rar'
		ORDER BY file_index
	`), jobID)
	if err != nil {
		return state, err
	}
	classificationSets := map[string]bool{}
	for rows.Next() {
		var current, canonical, classificationSet string
		if err := rows.Scan(&current, &canonical, &classificationSet); err != nil {
			rows.Close()
			return state, err
		}
		state.IdentityCount++
		state.CurrentFilenames = append(state.CurrentFilenames, current)
		state.CanonicalFilenames = append(state.CanonicalFilenames, canonical)
		if classificationSet != "" {
			classificationSets[classificationSet] = true
		}
		if classificationSet == restartPar2AliasSet {
			state.AliasClaimants++
		}
	}
	if err := rows.Close(); err != nil {
		return state, err
	}
	if err := rows.Err(); err != nil {
		return state, err
	}
	for setName := range classificationSets {
		state.ClassificationSets = append(state.ClassificationSets, setName)
	}
	sort.Strings(state.ClassificationSets)

	// The graph-engine migration drops the facts and chunk tables; a
	// database without them holds none.
	factsPresent, err := weaverStateTableExists(db, datastore, "active_rar_volume_facts")
	if err != nil {
		return state, err
	}
	if factsPresent {
		factRows, err := db.Query(rebindWeaverSQL(datastore, `SELECT set_name, COUNT(*) FROM active_rar_volume_facts WHERE job_id = ? GROUP BY set_name ORDER BY set_name`), jobID)
		if err != nil {
			return state, err
		}
		for factRows.Next() {
			var setName string
			var count int
			if err := factRows.Scan(&setName, &count); err != nil {
				factRows.Close()
				return state, err
			}
			state.FactSets[setName] = count
		}
		if err := factRows.Close(); err != nil {
			return state, err
		}
		if err := factRows.Err(); err != nil {
			return state, err
		}
	}

	for table, destination := range map[string]*int{
		"active_extracted":         &state.ActiveExtracted,
		"active_extraction_chunks": &state.ExtractionChunks,
	} {
		if present, err := weaverStateTableExists(db, datastore, table); err != nil {
			return state, err
		} else if !present {
			continue
		}
		query := rebindWeaverSQL(datastore, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE job_id = ?", table))
		if err := db.QueryRow(query, jobID).Scan(destination); err != nil {
			return state, err
		}
	}

	var outputDir string
	if err := db.QueryRow(rebindWeaverSQL(datastore, `SELECT output_dir FROM active_jobs WHERE job_id = ?`), jobID).Scan(&outputDir); err != nil {
		return state, err
	}
	for _, filename := range state.CurrentFilenames {
		path := filepath.Join(outputDir, filename)
		if _, err := os.Stat(path); err == nil {
			state.ExistingVolumePaths = append(state.ExistingVolumePaths, path)
		} else if !os.IsNotExist(err) {
			return state, err
		}
	}
	sort.Strings(state.ExistingVolumePaths)
	return state, nil
}

func jobEventKinds(dbPath string, jobID int) ([]string, error) {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer closeWeaverStateDB(db, datastore)

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
	defer closeWeaverStateDB(db, datastore)

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

// submitSlugNTimes submits the slug count times, every one forced: a case
// that asks for N jobs of one post gets N jobs whatever the history already
// holds, including a finished copy of the same post.
func (ctx *restartCaseContext) submitSlugNTimes(slug string, count int) ([]int, error) {
	ids := make([]int, 0, count)
	for i := 0; i < count; i++ {
		id, err := submitOneNZBWithOptions(ctx.weaverURL, ctx.Scenarios[slug], submitNZBOptions{force: true})
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (ctx *restartCaseContext) waitForFacade(jobID int, timeout time.Duration, predicate func(facadeItemSnapshot) bool) (facadeItemSnapshot, error) {
	deadline := time.Now().Add(timeout)
	var last facadeItemSnapshot
	var lastErr error
	for time.Now().Before(deadline) {
		snapshot, err := fetchFacadeItemSnapshot(ctx.weaverURL, jobID)
		if err == nil && predicate(snapshot) {
			return snapshot, nil
		}
		last, lastErr = snapshot, err
		time.Sleep(1 * time.Second)
	}
	return facadeItemSnapshot{}, facadeWaitTimeoutError(jobID, last, lastErr)
}

func facadeWaitTimeoutError(jobID int, last facadeItemSnapshot, lastErr error) error {
	switch {
	case lastErr != nil:
		return fmt.Errorf("timeout waiting for facade snapshot for job %d (last error: %v)", jobID, lastErr)
	case !last.Found:
		return fmt.Errorf("timeout waiting for facade snapshot for job %d (last observed: not found)", jobID)
	default:
		return fmt.Errorf("timeout waiting for facade snapshot for job %d (last observed: state=%s in_queue=%t downloaded=%d/%d error=%q)",
			jobID, last.Status, last.InQueue, last.DownloadedBytes, last.TotalBytes, last.Error)
	}
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
	defer closeWeaverStateDB(db, datastore)

	var status string
	if err := db.QueryRow(rebindWeaverSQL(datastore, `SELECT status FROM active_jobs WHERE job_id = ?`), jobID).Scan(&status); err == nil {
		// Weaver writes a terminal status to the active row before the
		// archive moves the job to job_history, as a separate write. Treating
		// that row as terminal lets a caller check the active tables before
		// the archive has emptied them, so the job only counts as terminal
		// once its history row exists.
		normalized := normalizeRestartDBStatus(status)
		if normalized == "COMPLETE" || normalized == "FAILED" {
			return "ARCHIVING", true
		}
		return normalized, true
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
			if !facadeTerminalStatus(status) {
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

// Articles the news server served again AFTER the restart point, and the extra
// BODY commands that took.
//
// This is the measurement the durable-file restart cases actually want. A
// whole-case fetch count also carries whatever the run did BEFORE the crash —
// a direct-store set that demotes before it learns a layout refetches the
// articles it had already consumed — and that says nothing about whether the
// restart reused what was durable. Comparing the two snapshots isolates the
// restart, which is the only thing these cases claim.
func bodyFetchesSinceRestartPoint(before, after restartNntpMetrics) (ids int, extra int) {
	for id, count := range after.BodyCounts {
		grew := count - before.BodyCounts[id]
		if grew > 0 {
			ids++
			extra += grew
		}
	}
	return ids, extra
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

func directStorePartialFromSnapshot(snapshot restartFilesystemSnapshot, jobID, fileSet int) (string, int64, bool) {
	prefix := fmt.Sprintf(".weaver-staging/%d/", jobID)
	suffix := fmt.Sprintf(".f%d.direct.partial", fileSet)
	for _, entry := range snapshot.Complete {
		if !strings.HasPrefix(entry.Path, prefix) {
			continue
		}
		relativePath := strings.TrimPrefix(entry.Path, prefix)
		if strings.Contains(relativePath, "/") || !strings.HasSuffix(relativePath, suffix) {
			continue
		}
		return entry.Path, entry.Size, true
	}
	return "", 0, false
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

	// The restart point is the extracting-phase delay hook, not "the database
	// has a row and the job is not terminal yet". The case asserts that nothing
	// is refetched after the restart, which only holds once every article is
	// already durable; and a bare "not terminal yet" predicate matches the
	// instant the job is queued, so on a fixture this small the job can reach a
	// terminal status while the pre-crash evidence is still being captured, and
	// the case silently turns into a check of what survives in history. Parking
	// the process inside the extraction delay pins both ends: downloads are
	// complete and the job provably cannot terminalize before the kill. An
	// archive's engine starts reading while its volumes still arrive, so the
	// hook fires once every volume it reads is in, not at the engine's start.
	if _, err := waitForDelayHookRestartPoint(ctx, []int{jobID}, "status.enter_extracting", 3*time.Minute); err != nil {
		return restartCaseResult{}, fmt.Errorf("wait for completed-download restart point: %w", err)
	}
	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	preDB, _, preMetrics, err := ctx.captureEvidence("pre_crash", "e2e-rar5-multi-member-")
	if err != nil {
		return restartCaseResult{}, err
	}
	preCompletedFiles := preDB.JobMetrics[jobID].Files

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
	refetchedIDs, refetchedBodies := bodyFetchesSinceRestartPoint(preMetrics, metrics)

	pass := preCompletedFiles > 0 && statuses[jobID] == "COMPLETE" && refetchedIDs == 0
	return classifyRestartResult(
		ctx.Profile,
		pass,
		fmt.Sprintf("restored %d completed file(s) without redownloading archive articles (post=%d refetched articles %d bodies %d)", preCompletedFiles, postFacade.DownloadedBytes, refetchedIDs, refetchedBodies),
		false,
		"",
		fmt.Sprintf("completed file restart redownloaded or failed (files=%d post=%d refetched articles %d bodies %d final=%s)", preCompletedFiles, postFacade.DownloadedBytes, refetchedIDs, refetchedBodies, statuses[jobID]),
	), nil
}

func runDownloadProgressFloorSurvivesRestart(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "rar5-multi-member"
	const checkpointBytes = 4 * 1024 * 1024
	const checkpointEnv = "WEAVER_E2E_DOWNLOAD_RESTART_CHECKPOINT_BYTES=4194304"

	// The post's last article is held on the news server, so the job cannot
	// finish downloading before the checkpoint covers the floor and the
	// process is killed: the restart point is reached on what the checkpoint
	// says, whatever the machine's speed.
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	if len(segments) == 0 {
		return restartCaseResult{}, fmt.Errorf("%s has no segments", slug)
	}
	held := segments[len(segments)-1].MessageID

	if err := ctx.startWeaverWithOptions("", 8, checkpointEnv); err != nil {
		return restartCaseResult{}, err
	}
	// Weaver's start resets the server's chaos, so the hold is set after it.
	if err := setNntpChaosOnServer(nntpHost(), nntpPort(), "hold_ids="+held); err != nil {
		return restartCaseResult{}, err
	}
	released := false
	defer func() {
		if !released {
			_ = setNntpChaosOnServer(nntpHost(), nntpPort(), "off")
		}
	}()
	jobID, err := ctx.submitSlug(slug)
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
	if err := setNntpChaosOnServer(nntpHost(), nntpPort(), "off"); err != nil {
		return restartCaseResult{}, err
	}
	released = true
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
	preDB, err := waitForDelayHookRestartPoint(ctx, jobIDs, "status.enter_extracting", 2*time.Minute)
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
	// The job downloads in well under a second, so polling cannot catch it
	// mid-download. Refuse every BODY until the job has been observed in the
	// state under test; the job then waits on the provider instead of racing
	// the harness.
	const articleGate = "greet_400=100,reauth_body=100"
	var releaseArticleGate func() error
	defer func() {
		if releaseArticleGate != nil {
			if err := releaseArticleGate(); err != nil {
				log.Printf("warning: release primary NNTP article gate: %v", err)
			}
		}
	}()
	openArticleGate := func() error {
		release := releaseArticleGate
		releaseArticleGate = nil
		return release()
	}

	if err := ctx.startWeaverWithOptions("", 1); err != nil {
		return restartCaseResult{}, err
	}
	releaseArticleGate, err := holdNntpChaosOnServer(nntpHost(), nntpPort(), articleGate)
	if err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("rar5-multi-member")
	if err != nil {
		return restartCaseResult{}, err
	}
	// The job enters its download phase when its first segment is dispatched,
	// which records DownloadStarted. The queue item cannot show this: with every
	// BODY refused the job has no live download, so it reads QUEUED, and the
	// transition is not persisted to the job row until it pauses.
	if err := waitForJobEvents(filepath.Join(ctx.CaseDir, "weaver.db"), jobID, []string{"DownloadStarted"}, 3*time.Minute); err != nil {
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
	if err := openArticleGate(); err != nil {
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
		// Hold the resumed job the same way, so the first status it takes
		// cannot be overtaken by the job completing between two polls.
		releaseArticleGate, err = holdNntpChaosOnServer(nntpHost(), nntpPort(), articleGate)
		if err != nil {
			return restartCaseResult{}, err
		}
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
		if err := openArticleGate(); err != nil {
			return restartCaseResult{}, err
		}
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
	if err := ctx.waitForCrash(5*time.Minute, jobID); err != nil {
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

// The process dies as a recovery set's repair starts, before it reads
// anything. A repair of a few slices is over in milliseconds, so the case
// arms the repair's own start instead of waiting to sample the job as
// repairing.
func runRepairingRestartsCleanly(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaver("repair.task_start"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug("par2-small-repair")
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(8*time.Minute, jobID); err != nil {
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

// rar5MultiMemberMembers are the members the rar5-multi-member archive holds,
// by their paths inside it.
var rar5MultiMemberMembers = []string{
	"tmp/episodes/Test.Show.S01E01.1080p.mkv",
	"tmp/episodes/Test.Show.S01E02.1080p.mkv",
	"tmp/episodes/Test.Show.S01E03.1080p.mkv",
}

func assertRar5MultiMemberBytes(dbPath string, jobID int) error {
	_, digest, err := singleMKVSource()
	if err != nil {
		return err
	}
	expected := map[string]string{}
	for _, member := range rar5MultiMemberMembers {
		expected[member] = digest
	}
	return assertOutputBLAKE3(dbPath, jobID, expected)
}

// missingMembers lists the members the snapshot's complete tree lacks.
func missingMembers(snapshot restartFilesystemSnapshot, members []string) []string {
	var missing []string
	for _, member := range members {
		if !completeSnapshotContainsMember(snapshot, member) {
			missing = append(missing, member)
		}
	}
	return missing
}

// archiveCheckpointAtCrash reads, once the process is gone, which of the
// slug's articles have both healthy coverage and a recorded direct backing
// file. Coverage alone, or an unrecorded envelope on disk, is not durability.
func archiveCheckpointAtCrash(ctx *restartCaseContext, jobID int, segments []nzbSegmentRef, ranges map[string]decodedRange) (map[string]bool, error) {
	evidence, err := readCheckpointEvidence(caseDBPath(ctx), jobID)
	if err != nil {
		return nil, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "pre_crash_coverage.json"), evidence.Coverage)
	writeRestartJSON(filepath.Join(ctx.CaseDir, "pre_crash_checkpoint.json"), evidence)
	return directDurableArticles(segments, ranges, evidence), nil
}

// The process dies as the archive's engine run finds its first member: the
// run reads while its volumes may still arrive, and nothing of the run itself
// is kept. There may be no committed durable articles at this boundary.
// The restart must reconstruct every output; proven direct inputs are also
// checked for refetch without claiming durability for uncommitted scratch.
func runRarExtractionResumesFromCheckpoint(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "rar5-multi-member"
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ranges, err := segmentDecodedRanges(nntpHost(), nntpPort(), segments)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.startWeaverWithOptions("extract.member_start", 8); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(10*time.Minute, jobID); err != nil {
		return restartCaseResult{}, err
	}
	_, _, preMetrics, err := ctx.captureEvidence("pre_crash", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	covered, err := archiveCheckpointAtCrash(ctx, jobID, segments, ranges)
	if err != nil {
		return restartCaseResult{}, err
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
	fetchedAfter, _ := bodyFetchesSinceRestartPoint(preMetrics, metrics)
	refetched := refetchedArticles(covered, preMetrics, metrics)
	missing := missingMembers(postFS, rar5MultiMemberMembers)
	if err := waitForJobArchived(caseDBPath(ctx), jobID); err != nil {
		return restartCaseResult{}, err
	}
	digestErr := assertRar5MultiMemberBytes(caseDBPath(ctx), jobID)
	detail := fmt.Sprintf("covered_at_crash=%d of %d articles fetched_after_restart=%d refetched_covered=%d missing=%v output=%v final=%s",
		len(covered), len(segments), fetchedAfter, len(refetched), missing, digestErr, statuses[jobID])
	pass := statuses[jobID] == "COMPLETE" && len(missing) == 0 && len(refetched) == 0 && digestErr == nil
	return classifyRestartResult(
		ctx.Profile,
		pass,
		"extraction restart ran the engine again over the checkpoint's articles and delivered every member: "+detail,
		false,
		"",
		fmt.Sprintf("extraction restart: %s refetched covered ids: %v", detail, refetched),
	), nil
}

// renameRestartSlug posts one payload under an obfuscated hex name beside
// the PAR3 set that names it. The set recognises the file by the bytes that
// have arrived of it, so the file is on disk under the hex name when the set
// renames it, and moves to the set's name.
const renameRestartSlug = "par3-obfuscated-names"

// The process dies right after the file moved to the name a committed
// checkpoint recorded for it, before the checkpoint that says it moved. The
// restart finds the file at that name and adopts it: the job completes, the
// fixture's expected output is delivered, and no article the checkpoint held
// whole is fetched again.
func runRenamedFileAdoptedAfterRestart(ctx *restartCaseContext) (restartCaseResult, error) {
	segments, err := nzbSegments(renameRestartSlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ranges, err := segmentDecodedRanges(nntpHost(), nntpPort(), segments)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.startWeaverWithOptions("extract.after_rename_before_record", 8); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(renameRestartSlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(12*time.Minute, jobID); err != nil {
		return restartCaseResult{}, err
	}
	_, _, preMetrics, err := ctx.captureEvidence("pre_crash", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	covered, err := archiveCheckpointAtCrash(ctx, jobID, segments, ranges)
	if err != nil {
		return restartCaseResult{}, err
	}
	if len(covered) == 0 {
		return restartCaseResult{}, fmt.Errorf("the checkpoint held no whole article when the file moved")
	}

	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	_, _, metrics, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	fetchedAfter, _ := bodyFetchesSinceRestartPoint(preMetrics, metrics)
	refetched := refetchedArticles(covered, preMetrics, metrics)
	digestErr := assertOutputBLAKE3(dbPath, jobID, ctx.Scenarios[renameRestartSlug].ExpectedOutputBLAKE3)
	detail := fmt.Sprintf("covered_at_crash=%d of %d articles fetched_after_restart=%d refetched_covered=%d output=%v final=%s",
		len(covered), len(segments), fetchedAfter, len(refetched), digestErr, statuses[jobID])
	pass := statuses[jobID] == "COMPLETE" && digestErr == nil && len(refetched) == 0
	return classifyRestartResult(
		ctx.Profile,
		pass,
		"restart after a file moved ahead of its record adopted it and delivered the expected output: "+detail,
		false,
		"",
		fmt.Sprintf("restart after a file moved ahead of its record: %s refetched covered ids: %v", detail, refetched),
	), nil
}

// The process dies as the archive's engine run finds its first member, and
// before the restart a completed-member marker is planted for a member whose
// output is not there, with the job's runtime forced back to downloading. The
// graph restores from its own checkpoint and never trusts the marker: the
// restart delivers every member, fetches none of the checkpoint's articles
// again, and the marker goes with the job.
func runStaleActiveExtractedRowsClearAfterRestart(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "rar5-multi-member"
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ranges, err := segmentDecodedRanges(nntpHost(), nntpPort(), segments)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.startWeaverWithOptions("extract.member_start", 8); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(10*time.Minute, jobID); err != nil {
		return restartCaseResult{}, err
	}

	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	covered, err := archiveCheckpointAtCrash(ctx, jobID, segments, ranges)
	if err != nil {
		return restartCaseResult{}, err
	}
	memberName := rar5MultiMemberMembers[0]
	staleMember := restartExtractedMember{
		JobID:      jobID,
		MemberName: memberName,
		OutputPath: completeRootPath(memberName),
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
	_, _, preMetrics, err := ctx.captureEvidence("pre_crash", "")
	if err != nil {
		return restartCaseResult{}, err
	}

	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := waitForJobArchived(dbPath, jobID); err != nil {
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

	refetched := refetchedArticles(covered, preMetrics, metrics)
	missing := missingMembers(postFS, rar5MultiMemberMembers)
	digestErr := assertRar5MultiMemberBytes(dbPath, jobID)
	detail := fmt.Sprintf("stale_member=%s covered_at_crash=%d of %d articles refetched_covered=%d missing=%v post_count=%d output=%v final=%s",
		staleMember.MemberName, len(covered), len(segments), len(refetched), missing, postCount, digestErr, statuses[jobID])
	pass := statuses[jobID] == "COMPLETE" && postCount == 0 && len(missing) == 0 && len(refetched) == 0 && digestErr == nil
	return classifyRestartResult(
		ctx.Profile,
		pass,
		"a stale completed-member marker changed nothing and went with the job: "+detail,
		false,
		"",
		fmt.Sprintf("stale active_extracted restart: %s refetched covered ids: %v", detail, refetched),
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
	if err := ctx.waitForCrash(5*time.Minute, jobID); err != nil {
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

// runDirectStorePar2AliasClaimantCompletesAfterRestart kills the process as
// the job enters verification, with one connection so the late-sorted,
// extensionless PAR2 index arrives after every archive volume. The restart
// resumes from the graph checkpoint: it fetches none of the articles the
// checkpoint held whole, names the volumes from what is on disk, and
// completes with the expected output.
func runDirectStorePar2AliasClaimantCompletesAfterRestart(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "direct-store-par2-alias-restart"
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ranges, err := segmentDecodedRanges(nntpHost(), nntpPort(), segments)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.startWeaverWithConnections("status.enter_verifying", 1); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(5*time.Minute, jobID); err != nil {
		return restartCaseResult{}, err
	}
	if !logContains(ctx.caseLogPath, "status.enter_verifying") {
		return restartCaseResult{}, fmt.Errorf("expected verifying failpoint evidence in log before PAR2 alias restart")
	}

	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	_, _, preMetrics, err := ctx.captureEvidence("pre_crash", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	aliasState, err := capturePar2AliasState(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, fmt.Errorf("capture PAR2 alias state: %w", err)
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "pre_crash_par2_alias_state.json"), aliasState)
	covered, err := archiveCheckpointAtCrash(ctx, jobID, segments, ranges)
	if err != nil {
		return restartCaseResult{}, err
	}
	if len(covered) == 0 {
		return restartCaseResult{}, fmt.Errorf("the checkpoint held no whole article when the job entered verification")
	}

	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, 3*time.Minute)
	if err != nil {
		return restartCaseResult{}, fmt.Errorf("PAR2 alias job did not reach a terminal state after restart: %w", err)
	}
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	_, _, metrics, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	fetchedAfter, _ := bodyFetchesSinceRestartPoint(preMetrics, metrics)
	refetched := refetchedArticles(covered, preMetrics, metrics)
	digestErr := assertOutputBLAKE3(dbPath, jobID, ctx.Scenarios[slug].ExpectedOutputBLAKE3)
	digestFailure := ""
	if digestErr != nil {
		digestFailure = digestErr.Error()
	}
	detail := fmt.Sprintf("covered_at_crash=%d of %d articles fetched_after_restart=%d refetched_covered=%d final=%s",
		len(covered), len(segments), fetchedAfter, len(refetched), statuses[jobID])

	pass := statuses[jobID] == "COMPLETE" && digestErr == nil && len(refetched) == 0
	return classifyRestartResult(
		ctx.Profile,
		pass,
		"PAR2 alias job resumed from the checkpoint after a restart in verification and completed: "+detail,
		false,
		"",
		fmt.Sprintf("PAR2 alias restart failed: %s digest=%s refetched covered ids: %v", detail, digestFailure, refetched),
	), nil
}

// conventional7zRepairSlug is a six-volume PPMd split 7z with one damaged
// volume and PAR2 recovery that covers it. Its parity slices are straddled by
// every article, so no in-stream block verdict exists and nothing names the
// damage before extraction: the completion check settles the set clean on the
// strong-decode claim, and the conventional decode is what finds the damage.
const conventional7zRepairSlug = "direct-unpack-repair-unvouched"

// runConventional7zDamagedBlockRepairsOnce submits a split 7z with one
// damaged volume and recovery that covers it. The contract is what the job
// ends as: complete, with the expected output, after exactly one repair. The
// path the engine takes to find the damage is its own; the case reads only
// the terminal status, the output digest and the job's repair events.
func runConventional7zDamagedBlockRepairsOnce(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(conventional7zRepairSlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	digestErr := assertOutputBLAKE3(dbPath, jobID, ctx.Scenarios[conventional7zRepairSlug].ExpectedOutputBLAKE3)
	events, eventsErr := jobEventKinds(dbPath, jobID)
	repairs := 0
	for _, kind := range events {
		if kind == "RepairComplete" {
			repairs++
		}
	}
	pass := statuses[jobID] == "COMPLETE" &&
		digestErr == nil &&
		eventsErr == nil &&
		repairs == 1
	digestFailure := ""
	if digestErr != nil {
		digestFailure = digestErr.Error()
	}
	eventsFailure := ""
	if eventsErr != nil {
		eventsFailure = eventsErr.Error()
	}
	return classifyRestartResult(
		ctx.Profile,
		pass,
		"damaged 7z set repaired once and completed with the expected output",
		false,
		"",
		fmt.Sprintf(
			"conventional 7z damaged-block case failed (final=%s repairs=%d digest=%s events=%s)",
			statuses[jobID], repairs, digestFailure, eventsFailure,
		),
	), nil
}
