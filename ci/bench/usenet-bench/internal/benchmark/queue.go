package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

// SubmissionMode controls how a durable client handles a suite's NZBs.
// Sequential is the default benchmark mode: submit one fixture and wait for
// its terminal state before submitting the next. QueueDrain intentionally
// submits a duplicate workload as one queue and reports only drain time.
type SubmissionMode string

const (
	SubmissionModeQueued     SubmissionMode = "queued"
	SubmissionModeSequential SubmissionMode = "sequential"
	SubmissionModeQueueDrain SubmissionMode = "queue-drain"
)

func (mode SubmissionMode) Valid() bool {
	return mode == SubmissionModeQueued || mode == SubmissionModeSequential || mode == SubmissionModeQueueDrain
}

// QueueInput is the immutable job list passed from the neutral runner to one
// long-lived client adapter.
type QueueInput struct {
	SchemaVersion  int             `json:"schema_version"`
	SuiteID        string          `json:"suite_id"`
	SubmissionMode SubmissionMode  `json:"submission_mode,omitempty"`
	Jobs           []QueueInputJob `json:"jobs"`
}

type QueueInputJob struct {
	RunID           string `json:"run_id"`
	FixtureID       string `json:"fixture_id"`
	NZBPath         string `json:"nzb_path"`
	ArchivePassword string `json:"archive_password,omitempty"`
	SubmissionName  string `json:"submission_name,omitempty"`
	ForceAccept     bool   `json:"force_accept,omitempty"`
}

func LoadQueueInput(path string) (QueueInput, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return QueueInput{}, fmt.Errorf("read queue input %s: %w", path, err)
	}
	var input QueueInput
	if err := json.Unmarshal(contents, &input); err != nil {
		return QueueInput{}, fmt.Errorf("decode queue input %s: %w", path, err)
	}
	if err := input.Validate(); err != nil {
		return QueueInput{}, err
	}
	return input, nil
}

func (q QueueInput) Validate() error {
	if q.SchemaVersion != 3 || strings.TrimSpace(q.SuiteID) == "" || len(q.Jobs) == 0 {
		return fmt.Errorf("queue input is empty or has unsupported schema")
	}
	if !q.SubmissionMode.Valid() {
		return fmt.Errorf("queue input %s has invalid submission mode %q", q.SuiteID, q.SubmissionMode)
	}
	seen := map[string]bool{}
	for _, job := range q.Jobs {
		if strings.TrimSpace(job.RunID) == "" || strings.TrimSpace(job.FixtureID) == "" || strings.TrimSpace(job.NZBPath) == "" {
			return fmt.Errorf("queue input %s contains an incomplete job", q.SuiteID)
		}
		if seen[job.RunID] {
			return fmt.Errorf("queue input %s repeats run %s", q.SuiteID, job.RunID)
		}
		seen[job.RunID] = true
	}
	return nil
}

// QueueAdapterResult is emitted once for one uninterrupted client queue. The
// process-level resource counters cover all jobs in this result together.
type QueueAdapterResult struct {
	SchemaVersion            int               `json:"schema_version"`
	SuiteID                  string            `json:"suite_id"`
	SubmissionMode           SubmissionMode    `json:"submission_mode"`
	Client                   Client            `json:"client"`
	ArchiveToolchain         ArchiveToolchain  `json:"archive_toolchain"`
	ArchiveToolchainIdentity string            `json:"archive_toolchain_identity"`
	ExecutionTarget          ExecutionTarget   `json:"execution_target"`
	Transport                Transport         `json:"transport"`
	TLSValidation            TLSValidation     `json:"tls_validation"`
	TransportLabel           string            `json:"transport_label"`
	ServerLink               ServerLinkProfile `json:"server_link"`
	QueueStartedAt           time.Time         `json:"queue_started_at"`
	QueueCompletedAt         time.Time         `json:"queue_completed_at"`
	StatusPollIntervalNanos  int64             `json:"status_poll_interval_nanoseconds"`
	Jobs                     []QueueJobResult  `json:"jobs"`
	ClientIdentity           string            `json:"client_identity"`
	ClientVersion            string            `json:"client_version"`
	RenderedConfigSHA256     string            `json:"rendered_config_sha256"`
	ResourceMetrics          ResourceMetrics   `json:"resource_metrics"`
}

type QueueJobResult struct {
	RunID                           string           `json:"run_id"`
	JobID                           string           `json:"job_id"`
	SubmissionStartedAt             time.Time        `json:"submission_started_at"`
	AcceptedAt                      time.Time        `json:"accepted_at"`
	QueuedAt                        time.Time        `json:"queued_at"`
	FixtureWallClockNanoseconds     int64            `json:"fixture_wall_clock_nanoseconds"`
	ResourceMetrics                 *ResourceMetrics `json:"resource_metrics,omitempty"`
	TerminalStatus                  string           `json:"terminal_status"`
	TerminalError                   string           `json:"terminal_error,omitempty"`
	ProcessingTimingAvailable       bool             `json:"processing_timing_available"`
	ProcessingTimingError           string           `json:"processing_timing_error,omitempty"`
	ProcessingStartedAt             time.Time        `json:"processing_started_at"`
	CompletionAt                    time.Time        `json:"completion_at"`
	TerminalObservationLowerBound   time.Time        `json:"terminal_observation_lower_bound"`
	TerminalObservedAt              time.Time        `json:"terminal_observed_at"`
	TerminalObservationUncertainty  int64            `json:"terminal_observation_uncertainty_nanoseconds"`
	SubmissionToTerminalNanoseconds int64            `json:"submission_to_terminal_nanoseconds"`
	ProcessingWallClockNanoseconds  int64            `json:"processing_wall_clock_nanoseconds"`
}

type QueueArtifact struct {
	SchemaVersion                int                 `json:"schema_version"`
	SuiteID                      string              `json:"suite_id"`
	SubmissionMode               SubmissionMode      `json:"submission_mode"`
	Runs                         []Run               `json:"runs"`
	Status                       string              `json:"status"`
	AdapterResult                *QueueAdapterResult `json:"adapter_result,omitempty"`
	ShaperBefore                 *ShaperSnapshot     `json:"shaper_before,omitempty"`
	ShaperAfter                  *ShaperSnapshot     `json:"shaper_after,omitempty"`
	ShaperDownstreamBytes        uint64              `json:"shaper_downstream_bytes,omitempty"`
	Jobs                         []QueueJobArtifact  `json:"jobs,omitempty"`
	QueueWallClockNanoseconds    int64               `json:"queue_wall_clock_nanoseconds,omitempty"`
	VerifiedWallClockNanoseconds int64               `json:"verified_wall_clock_nanoseconds,omitempty"`
	QueueVerifiedAt              *time.Time          `json:"queue_verified_at,omitempty"`
	Error                        string              `json:"error,omitempty"`
}

type QueueJobArtifact struct {
	Run                              Run                   `json:"run"`
	Repair                           fixture.RepairDetails `json:"repair"`
	AdapterResult                    QueueJobResult        `json:"adapter_result"`
	Outcome                          string                `json:"outcome"`
	Verification                     *OutputVerification   `json:"verification,omitempty"`
	UsableOutputAt                   *time.Time            `json:"usable_output_at,omitempty"`
	VerificationWallClockNanoseconds int64                 `json:"verification_wall_clock_nanoseconds,omitempty"`
	Error                            string                `json:"error,omitempty"`
}

func queueJobOutcome(result QueueJobResult) string {
	if result.TerminalStatus != "succeeded" {
		return "dnf"
	}
	return "completed"
}

type queueSuite struct {
	ID   string
	Runs []Run
}

// ExecuteQueuePlan retains the original burst-queue benchmark behaviour.
func ExecuteQueuePlan(ctx context.Context, config RunConfig) ([]QueueArtifact, error) {
	return executeQueuePlan(ctx, config, SubmissionModeQueued)
}

// ExecuteSequentialPlan runs every fixture in a lane against one durable
// client, waiting for completion before submitting the next fixture.
func ExecuteSequentialPlan(ctx context.Context, config RunConfig) ([]QueueArtifact, error) {
	return executeQueuePlan(ctx, config, SubmissionModeSequential)
}

// ExecuteQueueTransitionPlan queues exactly twenty copies of one direct
// fixture per client lane and reports only the queue-drain wall clock.
func ExecuteQueueTransitionPlan(ctx context.Context, config RunConfig) ([]QueueArtifact, error) {
	return executeQueuePlan(ctx, config, SubmissionModeQueueDrain)
}

func executeQueuePlan(ctx context.Context, config RunConfig, mode SubmissionMode) ([]QueueArtifact, error) {
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.ArtifactRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact root: %w", err)
	}
	var suites []queueSuite
	if mode == SubmissionModeQueueDrain {
		var err error
		suites, err = queueTransitionSuites(config.Plan, config.Target)
		if err != nil {
			return nil, err
		}
	} else {
		suites = queueSuites(config.Plan, config.Target)
		if mode == SubmissionModeSequential {
			for index := range suites {
				suites[index].ID = strings.Replace(suites[index].ID, "queue-", "sequential-", 1)
			}
		}
	}
	artifacts := make([]QueueArtifact, 0, len(suites))
	var failures []string
	for _, suite := range suites {
		artifact := executeQueueSuite(ctx, config, suite, mode)
		artifacts = append(artifacts, artifact)
		if queueArtifactFailed(artifact) {
			failures = append(failures, fmt.Sprintf("%s: %s", suite.ID, artifact.Error))
		}
	}
	if len(failures) > 0 {
		return artifacts, fmt.Errorf("%d benchmark queue suite(s) failed: %s", len(failures), strings.Join(failures, "; "))
	}
	return artifacts, nil
}

func queueArtifactFailed(artifact QueueArtifact) bool {
	return artifact.Status != "passed"
}

func queueTransitionSuites(plan Plan, target ExecutionTarget) ([]queueSuite, error) {
	if len(plan.FixtureIDs) != 1 {
		return nil, fmt.Errorf("queue-transition requires exactly one fixture, got %d", len(plan.FixtureIDs))
	}
	type lane struct {
		client    Client
		toolchain ArchiveToolchain
		transport Transport
		tls       TLSValidation
		label     string
	}
	byLane := map[lane]int{}
	var suites []queueSuite
	for _, run := range plan.Runs {
		if run.ExecutionTarget != target {
			continue
		}
		if run.FixtureID != plan.FixtureIDs[0] {
			return nil, fmt.Errorf("queue-transition run %s does not use fixture %s", run.ID, plan.FixtureIDs[0])
		}
		key := lane{run.Client, run.ArchiveToolchain, run.Transport, run.TLSValidation, run.TransportLabel}
		index, ok := byLane[key]
		if !ok {
			index = len(suites)
			byLane[key] = index
			suites = append(suites, queueSuite{ID: fmt.Sprintf("queue-transition-%04d", index+1)})
		}
		suites[index].Runs = append(suites[index].Runs, run)
	}
	for _, suite := range suites {
		if len(suite.Runs) != 20 {
			return nil, fmt.Errorf("%s has %d jobs; queue-transition requires exactly 20", suite.ID, len(suite.Runs))
		}
	}
	return suites, nil
}

func queueSuites(plan Plan, target ExecutionTarget) []queueSuite {
	suites := make([]queueSuite, 0, len(plan.Runs))
	for _, run := range plan.Runs {
		if run.ExecutionTarget != target {
			continue
		}
		// A normal benchmark invocation represents precisely one persisted
		// plan entry. Only queue-transition intentionally batches runs.
		suites = append(suites, queueSuite{ID: fmt.Sprintf("queue-%04d", len(suites)+1), Runs: []Run{run}})
	}
	return suites
}

func verifyQueueTransitionArtifact(suite queueSuite, result QueueAdapterResult, manifests map[string]fixture.GeneratedManifest, fixtureDirs map[string]string, outputDir string) ([]QueueJobArtifact, string) {
	jobsByRun := make(map[string]QueueJobResult, len(result.Jobs))
	for _, job := range result.Jobs {
		jobsByRun[job.RunID] = job
	}
	artifacts := make([]QueueJobArtifact, 0, len(suite.Runs))
	for _, run := range suite.Runs {
		job := jobsByRun[run.ID]
		artifact := QueueJobArtifact{Run: run, Repair: manifests[run.ID].Repair, AdapterResult: job, Outcome: queueJobOutcome(job)}
		if job.TerminalStatus != "succeeded" {
			artifact.Error = "client terminal failure: " + job.TerminalError
		}
		artifacts = append(artifacts, artifact)
	}
	for _, artifact := range artifacts {
		if artifact.Error != "" {
			return artifacts, fmt.Sprintf("queue-transition job %s: %s", artifact.Run.ID, artifact.Error)
		}
	}
	verifications, failedIndex, err := verifyQueueTransitionOutputs(fixtureDirs[suite.Runs[0].ID], outputDir, len(suite.Runs))
	if err != nil {
		if failedIndex < 0 || failedIndex >= len(artifacts) {
			failedIndex = 0
		}
		artifacts[failedIndex].Outcome = "dnf"
		artifacts[failedIndex].Error = err.Error()
		return artifacts, err.Error()
	}
	for index := range artifacts {
		artifacts[index].Verification = &verifications[index]
		verifiedAt := time.Now()
		artifacts[index].UsableOutputAt = &verifiedAt
	}
	return artifacts, ""
}

// verifyQueueTransitionOutputs verifies each duplicate job against distinct
// output members. Re-running VerifyOutput would allow one completed copy to
// satisfy every job, so this keeps one used-file set across all instances.
func verifyQueueTransitionOutputs(fixtureDir, outputDir string, copies int) ([]OutputVerification, int, error) {
	if copies < 1 {
		return nil, -1, fmt.Errorf("queue-transition must verify at least one output instance")
	}
	manifest, err := fixture.LoadGeneratedManifest(filepath.Join(fixtureDir, "fixture-manifest.json"))
	if err != nil {
		return nil, -1, err
	}
	actual, err := discoverFiles(outputDir)
	if err != nil {
		return nil, -1, err
	}
	allCandidates := make([]discoveredFile, 0)
	for _, candidates := range actual {
		allCandidates = append(allCandidates, candidates...)
	}
	sort.Slice(allCandidates, func(i, j int) bool { return allCandidates[i].path < allCandidates[j].path })
	used := make(map[string]bool, len(manifest.ExpectedFiles)*copies)
	digests := make(map[string]string)
	verifications := make([]OutputVerification, 0, copies)
	for copyIndex := 0; copyIndex < copies; copyIndex++ {
		verification := OutputVerification{FixtureID: manifest.Case.ID, Files: make([]VerifiedOutputFile, 0, len(manifest.ExpectedFiles))}
		for _, expected := range manifest.ExpectedFiles {
			verified, err := verifyExpectedFile(expected, actual[filepath.Base(expected.Path)], used, digests, outputDir)
			if err != nil {
				return verifications, copyIndex, fmt.Errorf("verify queue-transition output instance %d: %w", copyIndex+1, err)
			}
			if verified == nil {
				verified, err = verifyExpectedFile(expected, allCandidates, used, digests, outputDir)
				if err != nil {
					return verifications, copyIndex, fmt.Errorf("verify queue-transition output instance %d: %w", copyIndex+1, err)
				}
			}
			if verified == nil {
				return verifications, copyIndex, fmt.Errorf("verify queue-transition output instance %d: no unused output file matching %s passed size and BLAKE3 verification", copyIndex+1, expected.Path)
			}
			used[filepath.Clean(filepath.Join(outputDir, filepath.FromSlash(verified.ActualPath)))] = true
			verification.Files = append(verification.Files, *verified)
		}
		verifications = append(verifications, verification)
	}
	for _, candidate := range allCandidates {
		if !used[candidate.path] {
			return verifications, copies - 1, fmt.Errorf("verify queue-transition outputs: unexpected unconsumed output file %s", candidate.path)
		}
	}
	return verifications, -1, nil
}

func executeQueueSuite(parent context.Context, config RunConfig, suite queueSuite, mode SubmissionMode) (artifact QueueArtifact) {
	artifact = QueueArtifact{SchemaVersion: 6, SuiteID: suite.ID, SubmissionMode: mode, Runs: append([]Run(nil), suite.Runs...), Status: "failed"}
	suiteDir := filepath.Join(config.ArtifactRoot, suite.ID)
	if err := os.Mkdir(suiteDir, 0o755); err != nil {
		artifact.Error = fmt.Sprintf("create queue suite directory: %v", err)
		return artifact
	}
	defer func() {
		persistQueueArtifact(filepath.Join(suiteDir, "queue.json"), &artifact)
	}()
	outputDir := filepath.Join(suiteDir, "complete")
	configDir := filepath.Join(suiteDir, "config")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		artifact.Error = fmt.Sprintf("create queue completion directory: %v", err)
		return artifact
	}
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		artifact.Error = fmt.Sprintf("create queue client config directory: %v", err)
		return artifact
	}
	input := QueueInput{SchemaVersion: 3, SuiteID: suite.ID, SubmissionMode: mode, Jobs: make([]QueueInputJob, 0, len(suite.Runs))}
	manifests := make(map[string]fixture.GeneratedManifest, len(suite.Runs))
	fixtureDirs := make(map[string]string, len(suite.Runs))
	for index, run := range suite.Runs {
		fixtureDir := filepath.Join(config.FixtureRoot, run.FixtureID)
		manifest, err := fixture.LoadGeneratedManifest(filepath.Join(fixtureDir, "fixture-manifest.json"))
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		nzbPath, err := fixtureNZBPath(fixtureDir, run.FixtureID)
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		archivePassword, err := fixtureArchivePassword(fixtureDir)
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		job := QueueInputJob{RunID: run.ID, FixtureID: run.FixtureID, NZBPath: nzbPath, ArchivePassword: archivePassword}
		if mode == SubmissionModeQueueDrain {
			job.ForceAccept = true
			job.SubmissionName = fmt.Sprintf("queue-transition-%02d.nzb", index+1)
		}
		input.Jobs = append(input.Jobs, job)
		manifests[run.ID] = manifest
		fixtureDirs[run.ID] = fixtureDir
	}
	inputPath := filepath.Join(suiteDir, "queue-input.json")
	if err := writeQueueInput(inputPath, input); err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	first := suite.Runs[0]
	if config.ShaperControlURL != "" {
		leaseID, err := NewShaperExecutionLeaseID()
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		shaperBefore, err := AcquireShaperExecutionLease(parent, nil, config.ShaperControlURL, leaseID)
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		defer func() {
			if err := releaseShaperExecutionLeaseAfterRun(config.ShaperControlURL, leaseID); err != nil {
				if artifact.Error != "" {
					artifact.Error += "; "
				}
				artifact.Error += "release shaper execution lease: " + err.Error()
				artifact.Status = "failed"
			}
		}()
		if err := shaperBefore.ValidateFor(first.ServerLink); err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		artifact.ShaperBefore = &shaperBefore
	}
	adapter, ok := config.Catalog.For(first.Client, first.ArchiveToolchain, first.ExecutionTarget)
	if !ok {
		artifact.Error = fmt.Sprintf("adapter catalog has no entry for %s/%s/%s", first.Client, first.ArchiveToolchain, first.ExecutionTarget)
		return artifact
	}
	resultPath := filepath.Join(suiteDir, "adapter-result.json")
	logPath := filepath.Join(suiteDir, "adapter.log")
	if err := invokeQueueAdapter(parent, config, first, adapter, input.Jobs[0].NZBPath, input.Jobs[0].ArchivePassword, outputDir, configDir, resultPath, logPath, inputPath); err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	result, err := loadQueueAdapterResult(resultPath)
	if err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	if err := result.ValidateFor(suite, mode); err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	if artifact.ShaperBefore != nil {
		shaperAfter, err := FetchShaperSnapshot(parent, nil, config.ShaperControlURL)
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		if err := shaperAfter.ValidateFor(first.ServerLink); err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		delivered, err := ValidateShaperSnapshotPair(*artifact.ShaperBefore, shaperAfter)
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		if delivered == 0 {
			artifact.Error = "shaper reported zero downstream bytes for the measured client run"
			return artifact
		}
		artifact.ShaperAfter = &shaperAfter
		artifact.ShaperDownstreamBytes = delivered
	}
	artifact.AdapterResult = &result
	artifact.QueueWallClockNanoseconds = result.QueueCompletedAt.Sub(result.QueueStartedAt).Nanoseconds()
	if mode == SubmissionModeQueueDrain {
		artifact.Jobs, artifact.Error = verifyQueueTransitionArtifact(suite, result, manifests, fixtureDirs, outputDir)
		verifiedAt := time.Now()
		cleanupErr := DeleteOutputFiles(outputDir)
		if cleanupErr != nil {
			if artifact.Error != "" {
				artifact.Error += "; "
			}
			artifact.Error += fmt.Sprintf("delete queue-transition outputs: %v", cleanupErr)
			artifact.Status = "failed"
			return artifact
		}
		if artifact.Error != "" {
			artifact.Status = "completed_with_dnf"
			return artifact
		}
		artifact.QueueVerifiedAt = &verifiedAt
		artifact.VerifiedWallClockNanoseconds = verifiedAt.Sub(result.QueueStartedAt).Nanoseconds()
		artifact.Status = "passed"
		return artifact
	}
	jobsByRun := make(map[string]QueueJobResult, len(result.Jobs))
	for _, job := range result.Jobs {
		jobsByRun[job.RunID] = job
	}
	artifact.Jobs = make([]QueueJobArtifact, 0, len(suite.Runs))
	var jobFailures []string
	for _, run := range suite.Runs {
		adapterResult := jobsByRun[run.ID]
		jobArtifact := QueueJobArtifact{
			Run:           run,
			Repair:        manifests[run.ID].Repair,
			AdapterResult: adapterResult,
			Outcome:       queueJobOutcome(adapterResult),
		}
		if adapterResult.TerminalStatus != "succeeded" {
			jobArtifact.Error = "client terminal failure: " + adapterResult.TerminalError
			jobFailures = append(jobFailures, fmt.Sprintf("%s: %s", run.ID, jobArtifact.Error))
			artifact.Jobs = append(artifact.Jobs, jobArtifact)
			continue
		}
		verificationStartedAt := time.Now()
		verification, err := VerifyOutput(fixtureDirs[run.ID], outputDir)
		jobArtifact.VerificationWallClockNanoseconds = time.Since(verificationStartedAt).Nanoseconds()
		if err != nil {
			jobArtifact.Outcome = "dnf"
			if jobArtifact.Error != "" {
				jobArtifact.Error += "; output verification: " + err.Error()
			} else {
				jobArtifact.Error = err.Error()
			}
			jobFailures = append(jobFailures, fmt.Sprintf("%s: %s", run.ID, jobArtifact.Error))
			artifact.Jobs = append(artifact.Jobs, jobArtifact)
			continue
		}
		jobArtifact.Verification = &verification
		verifiedAt := time.Now()
		jobArtifact.UsableOutputAt = &verifiedAt
		artifact.Jobs = append(artifact.Jobs, jobArtifact)
		if jobArtifact.Error != "" {
			jobFailures = append(jobFailures, fmt.Sprintf("%s: %s", run.ID, jobArtifact.Error))
		}
	}
	jobFailureError := ""
	if len(jobFailures) > 0 {
		jobFailureError = fmt.Sprintf("%d queue job(s) did not finish: %s", len(jobFailures), strings.Join(jobFailures, "; "))
	}
	if err := DeleteOutputFiles(outputDir); err != nil {
		artifact.Error = jobFailureError
		if artifact.Error != "" {
			artifact.Error += "; "
		}
		artifact.Error += fmt.Sprintf("delete verified outputs: %v", err)
		return artifact
	}
	if len(jobFailures) > 0 {
		artifact.Status = "completed_with_dnf"
		artifact.Error = jobFailureError
		return artifact
	}
	verifiedAt := time.Now()
	artifact.QueueVerifiedAt = &verifiedAt
	artifact.VerifiedWallClockNanoseconds = verifiedAt.Sub(result.QueueStartedAt).Nanoseconds()
	artifact.Status = "passed"
	return artifact
}

func (r QueueAdapterResult) ValidateFor(suite queueSuite, mode SubmissionMode) error {
	if r.SchemaVersion != 5 || r.SuiteID != suite.ID || r.SubmissionMode != mode || len(suite.Runs) == 0 {
		return fmt.Errorf("queue adapter result does not match suite %s", suite.ID)
	}
	first := suite.Runs[0]
	if r.Client != first.Client || r.ArchiveToolchain != first.ArchiveToolchain || r.ExecutionTarget != first.ExecutionTarget || r.Transport != first.Transport || r.TLSValidation != first.TLSValidation || r.TransportLabel != first.TransportLabel || r.ServerLink != first.ServerLink {
		return fmt.Errorf("queue adapter result does not match suite %s metadata", suite.ID)
	}
	if r.QueueStartedAt.IsZero() || r.QueueCompletedAt.IsZero() || r.QueueCompletedAt.Before(r.QueueStartedAt) {
		return fmt.Errorf("queue adapter result for %s has invalid suite timing", suite.ID)
	}
	if r.StatusPollIntervalNanos <= 0 {
		return fmt.Errorf("queue adapter result for %s lacks status polling precision", suite.ID)
	}
	if strings.TrimSpace(r.ClientIdentity) == "" || strings.TrimSpace(r.ClientVersion) == "" || strings.TrimSpace(r.ArchiveToolchainIdentity) == "" || len(r.RenderedConfigSHA256) != 64 {
		return fmt.Errorf("queue adapter result for %s lacks client identity, version, archive provenance, or config SHA-256", suite.ID)
	}
	if err := r.ResourceMetrics.Validate(); err != nil {
		return err
	}
	expected := make(map[string]bool, len(suite.Runs))
	for _, run := range suite.Runs {
		expected[run.ID] = true
	}
	if len(r.Jobs) != len(expected) {
		return fmt.Errorf("queue adapter result for %s reports %d jobs, expected %d", suite.ID, len(r.Jobs), len(expected))
	}
	for _, job := range r.Jobs {
		fixtureWall := job.CompletionAt.Sub(job.QueuedAt).Nanoseconds()
		if !expected[job.RunID] || strings.TrimSpace(job.JobID) == "" || job.QueuedAt.IsZero() || job.CompletionAt.IsZero() || job.CompletionAt.Before(job.QueuedAt) || job.QueuedAt.Before(r.QueueStartedAt) || job.CompletionAt.After(r.QueueCompletedAt) || job.FixtureWallClockNanoseconds != fixtureWall {
			return fmt.Errorf("queue adapter result for %s contains invalid job timing", suite.ID)
		}
		hasObservationTiming := !job.SubmissionStartedAt.IsZero() || !job.AcceptedAt.IsZero() || !job.TerminalObservationLowerBound.IsZero() || !job.TerminalObservedAt.IsZero() || job.TerminalObservationUncertainty != 0 || job.SubmissionToTerminalNanoseconds != 0
		if hasObservationTiming {
			terminalUncertainty := job.TerminalObservedAt.Sub(job.TerminalObservationLowerBound).Nanoseconds()
			submissionToTerminal := job.TerminalObservedAt.Sub(job.SubmissionStartedAt).Nanoseconds()
			if job.SubmissionStartedAt.IsZero() || job.AcceptedAt.IsZero() || job.AcceptedAt.Before(job.SubmissionStartedAt) || !job.AcceptedAt.Equal(job.QueuedAt) || job.TerminalObservationLowerBound.IsZero() || job.TerminalObservedAt.IsZero() || !job.TerminalObservedAt.Equal(job.CompletionAt) || job.TerminalObservationLowerBound.Before(job.QueuedAt) || job.TerminalObservedAt.Before(job.TerminalObservationLowerBound) || job.TerminalObservationUncertainty != terminalUncertainty || job.SubmissionToTerminalNanoseconds != submissionToTerminal {
				return fmt.Errorf("queue adapter result for %s contains invalid terminal observation timing", suite.ID)
			}
		}
		if mode == SubmissionModeSequential {
			if !hasObservationTiming {
				return fmt.Errorf("sequential adapter result for %s lacks terminal observation timing", suite.ID)
			}
			if job.SubmissionToTerminalNanoseconds <= 0 || job.TerminalObservationUncertainty > job.SubmissionToTerminalNanoseconds/100 {
				return fmt.Errorf("sequential adapter result for %s has terminal observation uncertainty above 1%% of submission-to-terminal duration", suite.ID)
			}
		}
		if job.TerminalStatus != "succeeded" && job.TerminalStatus != "failed" {
			return fmt.Errorf("queue adapter result for %s contains invalid terminal status", suite.ID)
		}
		if job.TerminalStatus == "failed" && strings.TrimSpace(job.TerminalError) == "" {
			return fmt.Errorf("queue adapter result for %s omits a terminal failure reason", suite.ID)
		}
		if mode == SubmissionModeSequential {
			if job.ResourceMetrics == nil {
				return fmt.Errorf("sequential adapter result for %s lacks fixture resource metrics", suite.ID)
			}
			if err := job.ResourceMetrics.Validate(); err != nil {
				return fmt.Errorf("sequential adapter result for %s has invalid fixture resource metrics: %w", suite.ID, err)
			}
		} else if job.ResourceMetrics != nil {
			return fmt.Errorf("non-sequential adapter result for %s unexpectedly reports fixture resource metrics", suite.ID)
		}
		if job.ProcessingTimingAvailable {
			processingWall := job.CompletionAt.Sub(job.ProcessingStartedAt).Nanoseconds()
			if job.ProcessingStartedAt.IsZero() || job.ProcessingStartedAt.Before(job.QueuedAt) || job.CompletionAt.Before(job.ProcessingStartedAt) || job.ProcessingWallClockNanoseconds != processingWall || job.ProcessingTimingError != "" {
				return fmt.Errorf("queue adapter result for %s contains invalid active-processing timing", suite.ID)
			}
		} else if !job.ProcessingStartedAt.IsZero() || job.ProcessingWallClockNanoseconds != 0 || strings.TrimSpace(job.ProcessingTimingError) == "" {
			return fmt.Errorf("queue adapter result for %s lacks a processing-timing failure reason", suite.ID)
		}
		delete(expected, job.RunID)
	}
	if len(expected) != 0 {
		return fmt.Errorf("queue adapter result for %s is missing planned jobs", suite.ID)
	}
	return nil
}

func writeQueueInput(path string, input QueueInput) error {
	contents, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("write queue input %s: %w", path, err)
	}
	if _, err := file.Write(contents); err != nil {
		return fmt.Errorf("write queue input %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close queue input %s: %w", path, err)
	}
	return nil
}

func loadQueueAdapterResult(path string) (QueueAdapterResult, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return QueueAdapterResult{}, fmt.Errorf("read queue adapter result %s: %w", path, err)
	}
	var result QueueAdapterResult
	if err := json.Unmarshal(contents, &result); err != nil {
		return QueueAdapterResult{}, fmt.Errorf("decode queue adapter result %s: %w", path, err)
	}
	return result, nil
}

func writeQueueArtifact(path string, artifact QueueArtifact) error {
	contents, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	return os.WriteFile(path, contents, 0o644)
}

func persistQueueArtifact(path string, artifact *QueueArtifact) {
	if err := writeQueueArtifact(path, *artifact); err != nil {
		artifact.Status = "failed"
		if artifact.Error != "" {
			artifact.Error += "; "
		}
		artifact.Error += fmt.Sprintf("write queue artifact: %v", err)
	}
}
