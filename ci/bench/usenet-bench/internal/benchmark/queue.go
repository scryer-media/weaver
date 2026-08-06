package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	if input.SchemaVersion == 1 {
		input.SubmissionMode = SubmissionModeQueued
	}
	if err := input.Validate(); err != nil {
		return QueueInput{}, err
	}
	return input, nil
}

func (q QueueInput) Validate() error {
	if (q.SchemaVersion != 1 && q.SchemaVersion != 2) || strings.TrimSpace(q.SuiteID) == "" || len(q.Jobs) == 0 {
		return fmt.Errorf("queue input is empty or has unsupported schema")
	}
	if q.SchemaVersion == 1 && q.SubmissionMode != "" && q.SubmissionMode != SubmissionModeQueued {
		return fmt.Errorf("queue input %s uses submission mode %q with schema 1", q.SuiteID, q.SubmissionMode)
	}
	if q.SchemaVersion == 2 && !q.SubmissionMode.Valid() {
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
	RunID                          string              `json:"run_id"`
	JobID                          string              `json:"job_id"`
	QueuedAt                       time.Time           `json:"queued_at"`
	FixtureWallClockNanoseconds    int64               `json:"fixture_wall_clock_nanoseconds"`
	UsableOutputAt                 time.Time           `json:"usable_output_at,omitempty"`
	UsableWallClockNanoseconds     int64               `json:"usable_wall_clock_nanoseconds,omitempty"`
	ResourceMetrics                *ResourceMetrics    `json:"resource_metrics,omitempty"`
	TerminalStatus                 string              `json:"terminal_status"`
	Verification                   *OutputVerification `json:"verification,omitempty"`
	OutputVerificationError        string              `json:"output_verification_error,omitempty"`
	OutputDeleted                  bool                `json:"output_deleted,omitempty"`
	TerminalError                  string              `json:"terminal_error,omitempty"`
	ProcessingTimingAvailable      bool                `json:"processing_timing_available"`
	ProcessingTimingError          string              `json:"processing_timing_error,omitempty"`
	ProcessingStartedAt            time.Time           `json:"processing_started_at"`
	CompletionAt                   time.Time           `json:"completion_at"`
	ProcessingWallClockNanoseconds int64               `json:"processing_wall_clock_nanoseconds"`
}

type QueueArtifact struct {
	SchemaVersion                int                 `json:"schema_version"`
	SuiteID                      string              `json:"suite_id"`
	SubmissionMode               SubmissionMode      `json:"submission_mode"`
	Runs                         []Run               `json:"runs"`
	Status                       string              `json:"status"`
	AdapterResult                *QueueAdapterResult `json:"adapter_result,omitempty"`
	Jobs                         []QueueJobArtifact  `json:"jobs,omitempty"`
	QueueWallClockNanoseconds    int64               `json:"queue_wall_clock_nanoseconds,omitempty"`
	VerifiedWallClockNanoseconds int64               `json:"verified_wall_clock_nanoseconds,omitempty"`
	QueueVerifiedAt              *time.Time          `json:"queue_verified_at,omitempty"`
	Error                        string              `json:"error,omitempty"`
}

type QueueJobArtifact struct {
	Run            Run                   `json:"run"`
	Repair         fixture.RepairDetails `json:"repair"`
	AdapterResult  QueueJobResult        `json:"adapter_result"`
	Outcome        string                `json:"outcome"`
	Verification   *OutputVerification   `json:"verification,omitempty"`
	UsableOutputAt *time.Time            `json:"usable_output_at,omitempty"`
	Error          string                `json:"error,omitempty"`
}

func queueJobOutcome(result QueueJobResult) string {
	if result.TerminalStatus != "succeeded" || result.OutputVerificationError != "" {
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
		if artifact.Status == "failed" {
			failures = append(failures, fmt.Sprintf("%s: %s", suite.ID, artifact.Error))
		}
	}
	if len(failures) > 0 {
		return artifacts, fmt.Errorf("%d benchmark queue suite(s) failed: %s", len(failures), strings.Join(failures, "; "))
	}
	return artifacts, nil
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
	type lane struct {
		client     Client
		toolchain  ArchiveToolchain
		transport  Transport
		tls        TLSValidation
		label      string
		repetition int
	}
	byLane := map[lane]int{}
	var suites []queueSuite
	for _, run := range plan.Runs {
		if run.ExecutionTarget != target {
			continue
		}
		key := lane{run.Client, run.ArchiveToolchain, run.Transport, run.TLSValidation, run.TransportLabel, run.Repetition}
		index, ok := byLane[key]
		if !ok {
			index = len(suites)
			byLane[key] = index
			suites = append(suites, queueSuite{ID: fmt.Sprintf("queue-%04d", index+1)})
		}
		suites[index].Runs = append(suites[index].Runs, run)
	}
	return suites
}

func executeQueueSuite(parent context.Context, config RunConfig, suite queueSuite, mode SubmissionMode) QueueArtifact {
	artifact := QueueArtifact{SchemaVersion: 5, SuiteID: suite.ID, SubmissionMode: mode, Runs: append([]Run(nil), suite.Runs...), Status: "failed"}
	suiteDir := filepath.Join(config.ArtifactRoot, suite.ID)
	if err := os.Mkdir(suiteDir, 0o755); err != nil {
		artifact.Error = fmt.Sprintf("create queue suite directory: %v", err)
		return artifact
	}
	defer func() { _ = writeQueueArtifact(filepath.Join(suiteDir, "queue.json"), artifact) }()
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
	input := QueueInput{SchemaVersion: 2, SuiteID: suite.ID, SubmissionMode: mode, Jobs: make([]QueueInputJob, 0, len(suite.Runs))}
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
	adapter, _ := config.Catalog.For(first.Client, first.ArchiveToolchain, first.ExecutionTarget)
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
	artifact.AdapterResult = &result
	artifact.QueueWallClockNanoseconds = result.QueueCompletedAt.Sub(result.QueueStartedAt).Nanoseconds()
	if mode == SubmissionModeQueueDrain {
		for _, job := range result.Jobs {
			if job.TerminalStatus != "succeeded" {
				artifact.Error = fmt.Sprintf("queue-transition job %s ended %s: %s", job.RunID, job.TerminalStatus, job.TerminalError)
				return artifact
			}
		}
		if err := DeleteOutputFiles(outputDir); err != nil {
			artifact.Error = fmt.Sprintf("delete queue-transition outputs: %v", err)
			return artifact
		}
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
		var verification OutputVerification
		if mode == SubmissionModeSequential {
			if adapterResult.OutputVerificationError != "" {
				jobArtifact.Error = "output verification: " + adapterResult.OutputVerificationError
				jobFailures = append(jobFailures, fmt.Sprintf("%s: %s", run.ID, jobArtifact.Error))
				artifact.Jobs = append(artifact.Jobs, jobArtifact)
				continue
			}
			if adapterResult.Verification == nil || !adapterResult.OutputDeleted || adapterResult.UsableOutputAt.IsZero() || adapterResult.UsableOutputAt.Before(adapterResult.CompletionAt) {
				jobArtifact.Error = "adapter did not report verified usable output after completion"
				jobFailures = append(jobFailures, fmt.Sprintf("%s: %s", run.ID, jobArtifact.Error))
				artifact.Jobs = append(artifact.Jobs, jobArtifact)
				continue
			}
			verification = *adapterResult.Verification
			usableAt := adapterResult.UsableOutputAt
			jobArtifact.UsableOutputAt = &usableAt
		} else {
			verification, err = VerifyOutput(fixtureDirs[run.ID], outputDir)
		}
		if err != nil {
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
		if mode != SubmissionModeSequential {
			verifiedAt := time.Now().UTC()
			jobArtifact.UsableOutputAt = &verifiedAt
		}
		artifact.Jobs = append(artifact.Jobs, jobArtifact)
		if jobArtifact.Error != "" {
			jobFailures = append(jobFailures, fmt.Sprintf("%s: %s", run.ID, jobArtifact.Error))
		}
	}
	if len(jobFailures) > 0 {
		artifact.Status = "completed_with_dnf"
		artifact.Error = fmt.Sprintf("%d queue job(s) did not finish: %s", len(jobFailures), strings.Join(jobFailures, "; "))
		return artifact
	}
	if mode != SubmissionModeSequential {
		if err := DeleteOutputFiles(outputDir); err != nil {
			artifact.Error = fmt.Sprintf("delete queued outputs: %v", err)
			return artifact
		}
	}
	verifiedAt := time.Now().UTC()
	artifact.QueueVerifiedAt = &verifiedAt
	artifact.VerifiedWallClockNanoseconds = verifiedAt.Sub(result.QueueStartedAt).Nanoseconds()
	artifact.Status = "passed"
	return artifact
}

func (r QueueAdapterResult) ValidateFor(suite queueSuite, mode SubmissionMode) error {
	if r.SchemaVersion != 4 || r.SuiteID != suite.ID || r.SubmissionMode != mode || len(suite.Runs) == 0 {
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
			if job.TerminalStatus == "succeeded" {
				if job.OutputVerificationError != "" {
					if job.Verification != nil || !job.OutputDeleted || !job.UsableOutputAt.IsZero() || job.UsableWallClockNanoseconds != 0 {
						return fmt.Errorf("sequential adapter result for %s has invalid failed output verification", suite.ID)
					}
				} else {
					usableWall := job.UsableOutputAt.Sub(job.QueuedAt).Nanoseconds()
					if job.Verification == nil || !job.OutputDeleted || job.UsableOutputAt.IsZero() || job.UsableOutputAt.Before(job.CompletionAt) || job.UsableWallClockNanoseconds != usableWall {
						return fmt.Errorf("sequential adapter result for %s did not record verified usable fixture output", suite.ID)
					}
				}
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
	defer file.Close()
	if _, err := file.Write(contents); err != nil {
		return fmt.Errorf("write queue input %s: %w", path, err)
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
