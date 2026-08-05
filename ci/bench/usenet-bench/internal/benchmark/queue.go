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

// QueueInput is the immutable job list passed from the neutral runner to one
// long-lived client adapter. Each listed NZB is accepted before the adapter
// waits for any terminal state, so product queue behaviour is measured rather
// than a sequence of isolated single-job launches.
type QueueInput struct {
	SchemaVersion int             `json:"schema_version"`
	SuiteID       string          `json:"suite_id"`
	Jobs          []QueueInputJob `json:"jobs"`
}

type QueueInputJob struct {
	RunID           string `json:"run_id"`
	FixtureID       string `json:"fixture_id"`
	NZBPath         string `json:"nzb_path"`
	ArchivePassword string `json:"archive_password,omitempty"`
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
	if q.SchemaVersion != 1 || strings.TrimSpace(q.SuiteID) == "" || len(q.Jobs) == 0 {
		return fmt.Errorf("queue input is empty or has unsupported schema")
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
	Jobs                     []QueueJobResult  `json:"jobs"`
	ClientIdentity           string            `json:"client_identity"`
	ClientVersion            string            `json:"client_version"`
	RenderedConfigSHA256     string            `json:"rendered_config_sha256"`
	ResourceMetrics          ResourceMetrics   `json:"resource_metrics"`
}

type QueueJobResult struct {
	RunID        string    `json:"run_id"`
	JobID        string    `json:"job_id"`
	QueuedAt     time.Time `json:"queued_at"`
	CompletionAt time.Time `json:"completion_at"`
}

type QueueArtifact struct {
	SchemaVersion                int                 `json:"schema_version"`
	SuiteID                      string              `json:"suite_id"`
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
	Verification   *OutputVerification   `json:"verification,omitempty"`
	UsableOutputAt *time.Time            `json:"usable_output_at,omitempty"`
}

type queueSuite struct {
	ID   string
	Runs []Run
}

// ExecuteQueuePlan executes every lane as one fresh, long-lived client queue.
// Different client lanes never overlap, but a lane's fixture jobs share client
// state exactly as a real download queue does.
func ExecuteQueuePlan(ctx context.Context, config RunConfig) ([]QueueArtifact, error) {
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.ArtifactRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact root: %w", err)
	}
	suites := queueSuites(config.Plan, config.Target)
	artifacts := make([]QueueArtifact, 0, len(suites))
	var failures []string
	for _, suite := range suites {
		artifact := executeQueueSuite(ctx, config, suite)
		artifacts = append(artifacts, artifact)
		if artifact.Status != "passed" {
			failures = append(failures, fmt.Sprintf("%s: %s", suite.ID, artifact.Error))
		}
	}
	if len(failures) > 0 {
		return artifacts, fmt.Errorf("%d benchmark queue suite(s) failed: %s", len(failures), strings.Join(failures, "; "))
	}
	return artifacts, nil
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

func executeQueueSuite(parent context.Context, config RunConfig, suite queueSuite) QueueArtifact {
	artifact := QueueArtifact{SchemaVersion: 1, SuiteID: suite.ID, Runs: append([]Run(nil), suite.Runs...), Status: "failed"}
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
	input := QueueInput{SchemaVersion: 1, SuiteID: suite.ID, Jobs: make([]QueueInputJob, 0, len(suite.Runs))}
	manifests := make(map[string]fixture.GeneratedManifest, len(suite.Runs))
	fixtureDirs := make(map[string]string, len(suite.Runs))
	for _, run := range suite.Runs {
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
		input.Jobs = append(input.Jobs, QueueInputJob{RunID: run.ID, FixtureID: run.FixtureID, NZBPath: nzbPath, ArchivePassword: archivePassword})
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
	if err := result.ValidateFor(suite); err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	artifact.AdapterResult = &result
	artifact.QueueWallClockNanoseconds = result.QueueCompletedAt.Sub(result.QueueStartedAt).Nanoseconds()
	jobsByRun := make(map[string]QueueJobResult, len(result.Jobs))
	for _, job := range result.Jobs {
		jobsByRun[job.RunID] = job
	}
	artifact.Jobs = make([]QueueJobArtifact, 0, len(suite.Runs))
	for _, run := range suite.Runs {
		verification, err := VerifyOutput(fixtureDirs[run.ID], outputDir)
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		verifiedAt := time.Now().UTC()
		artifact.Jobs = append(artifact.Jobs, QueueJobArtifact{
			Run:            run,
			Repair:         manifests[run.ID].Repair,
			AdapterResult:  jobsByRun[run.ID],
			Verification:   &verification,
			UsableOutputAt: &verifiedAt,
		})
	}
	verifiedAt := time.Now().UTC()
	artifact.QueueVerifiedAt = &verifiedAt
	artifact.VerifiedWallClockNanoseconds = verifiedAt.Sub(result.QueueStartedAt).Nanoseconds()
	artifact.Status = "passed"
	return artifact
}

func (r QueueAdapterResult) ValidateFor(suite queueSuite) error {
	if r.SchemaVersion != 1 || r.SuiteID != suite.ID || len(suite.Runs) == 0 {
		return fmt.Errorf("queue adapter result does not match suite %s", suite.ID)
	}
	first := suite.Runs[0]
	if r.Client != first.Client || r.ArchiveToolchain != first.ArchiveToolchain || r.ExecutionTarget != first.ExecutionTarget || r.Transport != first.Transport || r.TLSValidation != first.TLSValidation || r.TransportLabel != first.TransportLabel || r.ServerLink != first.ServerLink {
		return fmt.Errorf("queue adapter result does not match suite %s metadata", suite.ID)
	}
	if r.QueueStartedAt.IsZero() || r.QueueCompletedAt.IsZero() || r.QueueCompletedAt.Before(r.QueueStartedAt) {
		return fmt.Errorf("queue adapter result for %s has invalid suite timing", suite.ID)
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
		if !expected[job.RunID] || strings.TrimSpace(job.JobID) == "" || job.QueuedAt.IsZero() || job.CompletionAt.IsZero() || job.CompletionAt.Before(job.QueuedAt) || job.QueuedAt.Before(r.QueueStartedAt) || job.CompletionAt.After(r.QueueCompletedAt) {
			return fmt.Errorf("queue adapter result for %s contains invalid job timing", suite.ID)
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
