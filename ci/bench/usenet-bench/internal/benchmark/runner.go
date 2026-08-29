package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

// AdapterCatalog declares one external, client-specific adapter for each
// product. Keeping adapters out-of-process lets the public benchmark harness
// remain independent from the clients' private APIs and container images.
type AdapterCatalog struct {
	SchemaVersion int       `json:"schema_version"`
	Adapters      []Adapter `json:"adapters"`
}

type Adapter struct {
	Client           Client            `json:"client"`
	ArchiveToolchain ArchiveToolchain  `json:"archive_toolchain"`
	Target           ExecutionTarget   `json:"target"`
	Command          []string          `json:"command"`
	Environment      map[string]string `json:"environment,omitempty"`
}

// AdapterResult is emitted by an adapter at BENCH_RESULT_PATH after it has
// observed client completion. The generic runner independently verifies the
// output before accepting CompletionAt as usable-output time.
type AdapterResult struct {
	SchemaVersion            int               `json:"schema_version"`
	RunID                    string            `json:"run_id"`
	Client                   Client            `json:"client"`
	ArchiveToolchain         ArchiveToolchain  `json:"archive_toolchain"`
	ArchiveToolchainIdentity string            `json:"archive_toolchain_identity"`
	ExecutionTarget          ExecutionTarget   `json:"execution_target"`
	Transport                Transport         `json:"transport"`
	TLSValidation            TLSValidation     `json:"tls_validation"`
	TransportLabel           string            `json:"transport_label"`
	ServerLink               ServerLinkProfile `json:"server_link"`
	QueuedAt                 time.Time         `json:"queued_at"`
	FirstArticleAt           *time.Time        `json:"first_article_at,omitempty"`
	CompletionAt             time.Time         `json:"completion_at"`
	ClientIdentity           string            `json:"client_identity"`
	ClientVersion            string            `json:"client_version"`
	RenderedConfigSHA256     string            `json:"rendered_config_sha256"`
	ResourceMetrics          ResourceMetrics   `json:"resource_metrics"`
}

type RunConfig struct {
	Plan             Plan
	Catalog          AdapterCatalog
	Target           ExecutionTarget
	FixtureRoot      string
	ArtifactRoot     string
	NNTPHost         string
	PlaintextPort    string
	TLSPort          string
	TLSCAFile        string
	ShaperControlURL string
	NNTPUsername     string
	NNTPPassword     string
	Connections      int
	Profile          string
	Timeout          time.Duration
}

type RunArtifact struct {
	SchemaVersion                    int                   `json:"schema_version"`
	Run                              Run                   `json:"run"`
	Repair                           fixture.RepairDetails `json:"repair"`
	Status                           string                `json:"status"`
	AdapterResult                    *AdapterResult        `json:"adapter_result,omitempty"`
	ShaperBefore                     *ShaperSnapshot       `json:"shaper_before,omitempty"`
	ShaperAfter                      *ShaperSnapshot       `json:"shaper_after,omitempty"`
	ShaperDownstreamBytes            uint64                `json:"shaper_downstream_bytes,omitempty"`
	Verification                     *OutputVerification   `json:"verification,omitempty"`
	VerificationWallClockNanoseconds int64                 `json:"verification_wall_clock_nanoseconds,omitempty"`
	UsableOutputAt                   *time.Time            `json:"usable_output_at,omitempty"`
	WallClockNanoseconds             int64                 `json:"wall_clock_nanoseconds,omitempty"`
	Error                            string                `json:"error,omitempty"`
}

func LoadAdapterCatalog(path string) (AdapterCatalog, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return AdapterCatalog{}, fmt.Errorf("read adapter catalog %s: %w", path, err)
	}
	var catalog AdapterCatalog
	if err := json.Unmarshal(contents, &catalog); err != nil {
		return AdapterCatalog{}, fmt.Errorf("decode adapter catalog %s: %w", path, err)
	}
	if err := catalog.Validate(); err != nil {
		return AdapterCatalog{}, err
	}
	return catalog, nil
}

func (c AdapterCatalog) Validate() error {
	if c.SchemaVersion != 4 || len(c.Adapters) == 0 {
		return fmt.Errorf("adapter catalog is empty or has unsupported schema")
	}
	seen := map[string]bool{}
	for _, adapter := range c.Adapters {
		if adapter.Client != Weaver && adapter.Client != SABnzbd && adapter.Client != NZBGet {
			return fmt.Errorf("adapter catalog has unsupported client %q", adapter.Client)
		}
		if _, err := DescribeExecutionTarget(adapter.Target); err != nil {
			return fmt.Errorf("adapter %q: %w", adapter.Client, err)
		}
		if !archiveToolchainAllowed(adapter.Client, adapter.ArchiveToolchain, adapter.Target) {
			return fmt.Errorf("adapter %q has unsupported %s archive toolchain on target %q", adapter.Client, adapter.ArchiveToolchain, adapter.Target)
		}
		key := string(adapter.Client) + "\x00" + string(adapter.ArchiveToolchain) + "\x00" + string(adapter.Target)
		if seen[key] {
			return fmt.Errorf("adapter catalog repeats %s/%s for target %q", adapter.Client, adapter.ArchiveToolchain, adapter.Target)
		}
		seen[key] = true
		if len(adapter.Command) == 0 || strings.TrimSpace(adapter.Command[0]) == "" {
			return fmt.Errorf("adapter %q has no command", adapter.Client)
		}
		for key := range adapter.Environment {
			if strings.TrimSpace(key) == "" || strings.HasPrefix(key, "BENCH_") || strings.Contains(key, "=") {
				return fmt.Errorf("adapter %q has invalid environment key %q", adapter.Client, key)
			}
		}
	}
	return nil
}

func (c AdapterCatalog) ValidateFor(plan Plan, target ExecutionTarget) error {
	if err := c.Validate(); err != nil {
		return err
	}
	for _, run := range plan.Runs {
		if run.ExecutionTarget != target {
			continue
		}
		if _, ok := c.For(run.Client, run.ArchiveToolchain, run.ExecutionTarget); !ok {
			return fmt.Errorf("adapter catalog has no adapter for planned %s/%s on target %q", run.Client, run.ArchiveToolchain, run.ExecutionTarget)
		}
	}
	return nil
}

func (c AdapterCatalog) For(client Client, toolchain ArchiveToolchain, target ExecutionTarget) (Adapter, bool) {
	for _, adapter := range c.Adapters {
		if adapter.Client == client && adapter.ArchiveToolchain == toolchain && adapter.Target == target {
			return adapter, true
		}
	}
	return Adapter{}, false
}

// ExecutePlan runs all plan entries sequentially. It records failures and
// continues with later entries so a single bad client/fixture combination does
// not hide the rest of the matrix.
func ExecutePlan(ctx context.Context, config RunConfig) ([]RunArtifact, error) {
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.ArtifactRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact root: %w", err)
	}
	selectedRuns := make([]Run, 0, len(config.Plan.Runs))
	for _, run := range config.Plan.Runs {
		if run.ExecutionTarget == config.Target {
			selectedRuns = append(selectedRuns, run)
		}
	}
	artifacts := make([]RunArtifact, 0, len(selectedRuns))
	var failures []string
	for _, run := range selectedRuns {
		artifact := executeRun(ctx, config, run)
		artifacts = append(artifacts, artifact)
		if artifact.Status != "passed" {
			failures = append(failures, fmt.Sprintf("%s: %s", run.ID, artifact.Error))
		}
	}
	if len(failures) > 0 {
		return artifacts, fmt.Errorf("%d benchmark run(s) failed: %s", len(failures), strings.Join(failures, "; "))
	}
	return artifacts, nil
}

func (c RunConfig) withDefaults() RunConfig {
	if c.PlaintextPort == "" {
		c.PlaintextPort = "119"
	}
	if c.TLSPort == "" {
		c.TLSPort = "563"
	}
	if c.Connections == 0 {
		c.Connections = 8
	}
	if c.Profile == "" {
		c.Profile = c.Plan.Profile
	}
	if c.Target == "" && len(c.Plan.ExecutionTargets) == 1 {
		c.Target = c.Plan.ExecutionTargets[0]
	}
	if c.Timeout == 0 {
		c.Timeout = 45 * time.Minute
	}
	return c
}

func (c RunConfig) Validate() error {
	if err := c.Plan.Validate(); err != nil {
		return err
	}
	if _, err := DescribeExecutionTarget(c.Target); err != nil {
		return fmt.Errorf("run target: %w", err)
	}
	targetPlanned := false
	for _, target := range c.Plan.ExecutionTargets {
		if target == c.Target {
			targetPlanned = true
			break
		}
	}
	if !targetPlanned {
		return fmt.Errorf("run target %q is not included in the persisted plan", c.Target)
	}
	if err := c.Catalog.ValidateFor(c.Plan, c.Target); err != nil {
		return err
	}
	if c.FixtureRoot == "" || c.ArtifactRoot == "" || c.NNTPHost == "" || c.NNTPUsername == "" || c.NNTPPassword == "" {
		return fmt.Errorf("fixture root, artifact root, NNTP host, username, and password are required")
	}
	if c.Connections < 1 || c.Timeout <= 0 {
		return fmt.Errorf("connections and timeout must be positive")
	}
	if c.Profile != c.Plan.Profile {
		return fmt.Errorf("run profile %q does not match persisted plan profile %q", c.Profile, c.Plan.Profile)
	}
	if c.Plan.ServerLink.EgressBitsPerSecond > 0 && c.ShaperControlURL == "" {
		return fmt.Errorf("shaper control URL is required for shaped benchmark plans")
	}
	if c.ShaperControlURL != "" {
		if err := ValidateShaperControlURL(c.ShaperControlURL); err != nil {
			return err
		}
	}
	if planNeedsVerifiedTLS(c.Plan) && c.TLSCAFile == "" {
		return fmt.Errorf("TLS CA file is required because the plan contains CA-verified TLS runs")
	}
	return nil
}

func planNeedsVerifiedTLS(plan Plan) bool {
	for _, run := range plan.Runs {
		if run.Transport == TLS && run.TLSValidation == TLSCAVerified {
			return true
		}
	}
	return false
}

func executeRun(parent context.Context, config RunConfig, run Run) (artifact RunArtifact) {
	artifact = RunArtifact{SchemaVersion: 6, Run: run, Status: "failed"}
	runDir := filepath.Join(config.ArtifactRoot, run.ID)
	if err := os.Mkdir(runDir, 0o755); err != nil {
		artifact.Error = fmt.Sprintf("create isolated run directory: %v", err)
		return artifact
	}
	defer func() {
		persistRunArtifact(filepath.Join(runDir, "run.json"), &artifact)
	}()
	outputDir := filepath.Join(runDir, "complete")
	configDir := filepath.Join(runDir, "config")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		artifact.Error = fmt.Sprintf("create completion directory: %v", err)
		return artifact
	}
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		artifact.Error = fmt.Sprintf("create client config directory: %v", err)
		return artifact
	}
	fixtureDir := filepath.Join(config.FixtureRoot, run.FixtureID)
	manifest, err := fixture.LoadGeneratedManifest(filepath.Join(fixtureDir, "fixture-manifest.json"))
	if err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	artifact.Repair = manifest.Repair
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
	adapter, ok := config.Catalog.For(run.Client, run.ArchiveToolchain, run.ExecutionTarget)
	if !ok {
		artifact.Error = fmt.Sprintf("adapter catalog has no entry for %s/%s/%s", run.Client, run.ArchiveToolchain, run.ExecutionTarget)
		return artifact
	}
	resultPath := filepath.Join(runDir, "adapter-result.json")
	logPath := filepath.Join(runDir, "adapter.log")
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
		if err := shaperBefore.ValidateFor(run.ServerLink); err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		artifact.ShaperBefore = &shaperBefore
	}
	if err := invokeAdapter(parent, config, run, adapter, fixtureDir, nzbPath, archivePassword, outputDir, configDir, resultPath, logPath); err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	result, err := loadAdapterResult(resultPath)
	if err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	if err := result.ValidateFor(run); err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	if artifact.ShaperBefore != nil {
		shaperAfter, err := FetchShaperSnapshot(parent, nil, config.ShaperControlURL)
		if err != nil {
			artifact.Error = err.Error()
			return artifact
		}
		if err := shaperAfter.ValidateFor(run.ServerLink); err != nil {
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
	artifact.WallClockNanoseconds = result.CompletionAt.Sub(result.QueuedAt).Nanoseconds()
	verificationStartedAt := time.Now()
	verification, err := VerifyOutput(fixtureDir, outputDir)
	artifact.VerificationWallClockNanoseconds = time.Since(verificationStartedAt).Nanoseconds()
	if err != nil {
		artifact.Error = err.Error()
		return artifact
	}
	verifiedAt := time.Now()
	artifact.Verification = &verification
	artifact.UsableOutputAt = &verifiedAt
	if err := DeleteOutputFiles(outputDir); err != nil {
		artifact.Error = fmt.Sprintf("delete verified output: %v", err)
		return artifact
	}
	artifact.Status = "passed"
	return artifact
}

func invokeAdapter(parent context.Context, config RunConfig, run Run, adapter Adapter, fixtureDir, nzbPath, archivePassword, outputDir, configDir, resultPath, logPath string) error {
	return invokeAdapterWithExtraEnvironment(parent, config, run, adapter, fixtureDir, nzbPath, archivePassword, outputDir, configDir, resultPath, logPath, nil)
}

func invokeQueueAdapter(parent context.Context, config RunConfig, run Run, adapter Adapter, nzbPath, archivePassword, outputDir, configDir, resultPath, logPath, queuePath string) error {
	return invokeAdapterWithExtraEnvironment(parent, config, run, adapter, filepath.Dir(nzbPath), nzbPath, archivePassword, outputDir, configDir, resultPath, logPath, []string{"BENCH_QUEUE_PATH=" + queuePath})
}

func invokeAdapterWithExtraEnvironment(parent context.Context, config RunConfig, run Run, adapter Adapter, fixtureDir, nzbPath, archivePassword, outputDir, configDir, resultPath, logPath string, extraEnvironment []string) error {
	ctx, cancel := context.WithTimeout(parent, config.Timeout)
	defer cancel()
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create adapter log: %w", err)
	}
	defer logFile.Close()
	port := config.PlaintextPort
	if run.Transport == TLS {
		port = config.TLSPort
	}
	command := exec.CommandContext(ctx, adapter.Command[0], adapter.Command[1:]...)
	command.Dir = filepath.Dir(resultPath)
	command.Stdout = logFile
	command.Stderr = logFile
	command.Env = append(os.Environ(), adapterEnvironment(config, run, fixtureDir, nzbPath, archivePassword, outputDir, configDir, resultPath, port)...)
	command.Env = append(command.Env, extraEnvironment...)
	for key, value := range adapter.Environment {
		command.Env = append(command.Env, key+"="+value)
	}
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("adapter %s/%s exceeded %s (see %s)", run.Client, run.ArchiveToolchain, config.Timeout, logPath)
		}
		return fmt.Errorf("adapter %s/%s failed: %w (see %s)", run.Client, run.ArchiveToolchain, err, logPath)
	}
	return nil
}

func adapterEnvironment(config RunConfig, run Run, fixtureDir, nzbPath, archivePassword, outputDir, configDir, resultPath, port string) []string {
	return []string{
		"BENCH_RUN_ID=" + run.ID,
		"BENCH_CLIENT=" + string(run.Client),
		"BENCH_ARCHIVE_TOOLCHAIN=" + string(run.ArchiveToolchain),
		"BENCH_EXECUTION_TARGET=" + string(run.ExecutionTarget),
		"BENCH_TRANSPORT=" + string(run.Transport),
		"BENCH_TRANSPORT_LABEL=" + run.TransportLabel,
		"BENCH_TLS_VALIDATION=" + string(run.TLSValidation),
		"BENCH_FIXTURE_DIR=" + fixtureDir,
		"BENCH_NZB_PATH=" + nzbPath,
		"BENCH_OUTPUT_DIR=" + outputDir,
		"BENCH_CONFIG_DIR=" + configDir,
		"BENCH_RESULT_PATH=" + resultPath,
		"BENCH_NNTP_HOST=" + config.NNTPHost,
		"BENCH_NNTP_PORT=" + port,
		"BENCH_NNTP_USERNAME=" + config.NNTPUsername,
		"BENCH_NNTP_PASSWORD=" + config.NNTPPassword,
		"BENCH_NNTP_TLS=" + strconv.FormatBool(run.Transport == TLS),
		"BENCH_NNTP_CA_FILE=" + config.TLSCAFile,
		"BENCH_ARCHIVE_PASSWORD=" + archivePassword,
		"BENCH_CONNECTIONS=" + strconv.Itoa(config.Connections),
		"BENCH_PROFILE=" + config.Profile,
		"BENCH_SERVER_LINK_ID=" + run.ServerLink.ID,
		"BENCH_SERVER_LINK_SCOPE=" + run.ServerLink.Scope,
		"BENCH_SERVER_EGRESS_BITS_PER_SECOND=" + strconv.FormatUint(run.ServerLink.EgressBitsPerSecond, 10),
		"BENCH_SERVER_EGRESS_BURST_BYTES=" + strconv.FormatUint(run.ServerLink.BurstBytes, 10),
	}
}

func fixtureArchivePassword(fixtureDir string) (string, error) {
	manifest, err := fixture.LoadGeneratedManifest(filepath.Join(fixtureDir, "fixture-manifest.json"))
	if err != nil {
		return "", fmt.Errorf("load fixture manifest for archive password: %w", err)
	}
	if manifest.Case.RequiresPassword() {
		return fixture.FixturePassword, nil
	}
	return "", nil
}

func fixtureNZBPath(fixtureDir, fixtureID string) (string, error) {
	if _, err := os.Stat(filepath.Join(fixtureDir, "fixture-manifest.json")); err != nil {
		return "", fmt.Errorf("fixture %s is unavailable: %w", fixtureID, err)
	}
	nzbPath := filepath.Join(fixtureDir, fixtureID+".nzb")
	if _, err := os.Stat(nzbPath); err != nil {
		return "", fmt.Errorf("fixture %s has no seeded NZB %s: %w", fixtureID, nzbPath, err)
	}
	return nzbPath, nil
}

func loadAdapterResult(path string) (AdapterResult, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return AdapterResult{}, fmt.Errorf("read adapter result %s: %w", path, err)
	}
	var result AdapterResult
	if err := json.Unmarshal(contents, &result); err != nil {
		return AdapterResult{}, fmt.Errorf("decode adapter result %s: %w", path, err)
	}
	return result, nil
}

func (r AdapterResult) ValidateFor(run Run) error {
	if r.SchemaVersion != 5 || r.RunID != run.ID || r.Client != run.Client || r.ArchiveToolchain != run.ArchiveToolchain || r.ExecutionTarget != run.ExecutionTarget || r.Transport != run.Transport || r.TLSValidation != run.TLSValidation || r.TransportLabel != run.TransportLabel || r.ServerLink != run.ServerLink {
		return fmt.Errorf("adapter result does not match planned run %s", run.ID)
	}
	if r.QueuedAt.IsZero() || r.CompletionAt.IsZero() || r.CompletionAt.Before(r.QueuedAt) {
		return fmt.Errorf("adapter result for %s has invalid queue/completion timing", run.ID)
	}
	if r.FirstArticleAt != nil && (r.FirstArticleAt.Before(r.QueuedAt) || r.FirstArticleAt.After(r.CompletionAt)) {
		return fmt.Errorf("adapter result for %s has invalid first article timing", run.ID)
	}
	if strings.TrimSpace(r.ClientIdentity) == "" || strings.TrimSpace(r.ClientVersion) == "" || strings.TrimSpace(r.ArchiveToolchainIdentity) == "" || len(r.RenderedConfigSHA256) != 64 {
		return fmt.Errorf("adapter result for %s lacks client identity, version, archive toolchain provenance, or config SHA-256", run.ID)
	}
	return r.ResourceMetrics.Validate()
}

func writeArtifact(path string, artifact RunArtifact) error {
	contents, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	return os.WriteFile(path, contents, 0o644)
}

func persistRunArtifact(path string, artifact *RunArtifact) {
	if err := writeArtifact(path, *artifact); err != nil {
		artifact.Status = "failed"
		if artifact.Error != "" {
			artifact.Error += "; "
		}
		artifact.Error += fmt.Sprintf("write run artifact: %v", err)
	}
}

// SortedAdapters is useful for deterministic diagnostic output in adapters.
func (c AdapterCatalog) SortedAdapters() []Adapter {
	adapters := append([]Adapter(nil), c.Adapters...)
	sort.Slice(adapters, func(i, j int) bool {
		if adapters[i].Client != adapters[j].Client {
			return adapters[i].Client < adapters[j].Client
		}
		return adapters[i].ArchiveToolchain < adapters[j].ArchiveToolchain
	})
	return adapters
}
