package weaver

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/composeutil"
)

type weaverReleaseFlowKind string

const (
	weaverReleaseFlowBrowser  weaverReleaseFlowKind = "browser"
	weaverReleaseFlowBehavior weaverReleaseFlowKind = "behavior"
	weaverReleaseFlowCommand  weaverReleaseFlowKind = "command"
)

type weaverReleaseFlowSpec struct {
	Name             string
	Kind             weaverReleaseFlowKind
	PlaywrightScript string
	Services         []string
	Datastores       []weaverDatastore
	SeedFixtures     []string
	Artifacts        []string
	Timeout          time.Duration
	Env              map[string]string
}

var weaverReleaseFlowSpecs = []weaverReleaseFlowSpec{
	browserReleaseFlow("ui-settings-crud", 6*time.Minute),
	browserReleaseFlow("ui-security", 6*time.Minute),
	ingressReleaseFlow(),
	// Covers the execution matrix (failure, continue-after-failure, timeout,
	// artifact-condition skip) on top of the settings/profile surface, so it
	// needs more room than the single-behaviour browser flows.
	browserReleaseFlow("ui-post-processing", 14*time.Minute),
	browserReleaseFlow("ui-runtime-observability", 5*time.Minute),
	behaviorReleaseFlow("rate-limits", 7*time.Minute),
	behaviorReleaseFlow("bandwidth-and-server-quotas", 8*time.Minute),
	behaviorReleaseFlow("provider-connection-cap", 8*time.Minute),
	encryptionKeyReleaseFlow(),
	behaviorReleaseFlow("duplicate-and-queue-policy", 7*time.Minute),
	{
		Name:             "ui-backup-restore-sqlite-to-sqlite",
		Kind:             weaverReleaseFlowBrowser,
		PlaywrightScript: "ui-backup-restore",
		Services:         []string{"nntp", "nntp2", "weaver"},
		Datastores:       []weaverDatastore{weaverDatastoreSQLite},
		Artifacts:        defaultWeaverReleaseArtifacts(),
		Timeout:          10 * time.Minute,
		Env: map[string]string{
			"E2E_WEAVER_BACKUP_SOURCE_DATASTORE": "sqlite",
			"E2E_WEAVER_BACKUP_TARGET_DATASTORE": "sqlite",
		},
	},
	{
		Name:             "ui-backup-restore-sqlite-to-postgres",
		Kind:             weaverReleaseFlowBrowser,
		PlaywrightScript: "ui-backup-restore",
		Services:         []string{"nntp", "nntp2", "weaver"},
		Datastores:       []weaverDatastore{weaverDatastorePostgres},
		Artifacts:        defaultWeaverReleaseArtifacts(),
		Timeout:          10 * time.Minute,
		Env: map[string]string{
			"E2E_WEAVER_BACKUP_SOURCE_DATASTORE": "sqlite",
			"E2E_WEAVER_BACKUP_TARGET_DATASTORE": "postgres",
		},
	},
	{
		Name:             "ui-backup-restore-postgres-to-sqlite",
		Kind:             weaverReleaseFlowBrowser,
		PlaywrightScript: "ui-backup-restore",
		Services:         []string{"nntp", "nntp2", "weaver"},
		Datastores:       []weaverDatastore{weaverDatastoreSQLite},
		Artifacts:        defaultWeaverReleaseArtifacts(),
		Timeout:          10 * time.Minute,
		Env: map[string]string{
			"E2E_WEAVER_BACKUP_SOURCE_DATASTORE": "postgres",
			"E2E_WEAVER_BACKUP_TARGET_DATASTORE": "sqlite",
		},
	},
	{
		Name:             "ui-backup-restore-postgres-to-postgres",
		Kind:             weaverReleaseFlowBrowser,
		PlaywrightScript: "ui-backup-restore",
		Services:         []string{"nntp", "nntp2", "weaver"},
		Datastores:       []weaverDatastore{weaverDatastorePostgres},
		Artifacts:        defaultWeaverReleaseArtifacts(),
		Timeout:          10 * time.Minute,
		Env: map[string]string{
			"E2E_WEAVER_BACKUP_SOURCE_DATASTORE": "postgres",
			"E2E_WEAVER_BACKUP_TARGET_DATASTORE": "postgres",
		},
	},
	{
		Name:       "adaptive-dispatch",
		Kind:       weaverReleaseFlowCommand,
		Services:   []string{"nntp2", "toxiproxy", "local-weaver"},
		Datastores: []weaverDatastore{weaverDatastoreSQLite},
		Artifacts:  []string{"flow.log", "status.json", "fake-nntp-counters.json"},
		Timeout:    12 * time.Minute,
	},
}

func browserReleaseFlow(name string, timeout time.Duration) weaverReleaseFlowSpec {
	return weaverReleaseFlowSpec{
		Name:             name,
		Kind:             weaverReleaseFlowBrowser,
		PlaywrightScript: name,
		Services:         []string{"nntp", "nntp2", "weaver"},
		Datastores:       releaseDatastoreMatrix(),
		Artifacts:        defaultWeaverReleaseArtifacts(),
		Timeout:          timeout,
	}
}

func behaviorReleaseFlow(name string, timeout time.Duration) weaverReleaseFlowSpec {
	spec := browserReleaseFlow(name, timeout)
	spec.Kind = weaverReleaseFlowBehavior
	return spec
}

func ingressReleaseFlow() weaverReleaseFlowSpec {
	spec := browserReleaseFlow("ui-ingress-automation", 10*time.Minute)
	spec.Services = append(spec.Services, "newznab")
	spec.Env = map[string]string{"WEAVER_RSS_ALLOW_PRIVATE_NETWORK": "true"}
	return spec
}

func encryptionKeyReleaseFlow() weaverReleaseFlowSpec {
	spec := behaviorReleaseFlow("encryption-key-lifecycle", 8*time.Minute)
	spec.Env = map[string]string{"E2E_WEAVER_ENCRYPTION_KEY": ""}
	spec.Artifacts = append(
		spec.Artifacts,
		"encryption-key-evidence.json",
		"encryption-key-fault-missing.log",
		"encryption-key-fault-corrupt.log",
		"encryption-key-fault-unreadable.log",
	)
	return spec
}

func defaultWeaverReleaseArtifacts() []string {
	return []string{
		"status.json",
		"flow.log",
		"docker-compose.log",
		"weaver-metrics.prom",
		"fake-nntp-counters.json",
		"html-report",
		"trace.zip",
		"failure-video",
		"screenshots",
	}
}

func releaseDatastoreMatrix() []weaverDatastore {
	return []weaverDatastore{weaverDatastoreSQLite, weaverDatastorePostgres}
}

func weaverReleaseFlowSpecFor(name string) (weaverReleaseFlowSpec, bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, spec := range weaverReleaseFlowSpecs {
		if spec.Name == name {
			return cloneWeaverReleaseFlowSpec(spec), true
		}
	}
	return weaverReleaseFlowSpec{}, false
}

func cloneWeaverReleaseFlowSpec(spec weaverReleaseFlowSpec) weaverReleaseFlowSpec {
	spec.Services = append([]string(nil), spec.Services...)
	spec.Datastores = append([]weaverDatastore(nil), spec.Datastores...)
	spec.SeedFixtures = append([]string(nil), spec.SeedFixtures...)
	spec.Artifacts = append([]string(nil), spec.Artifacts...)
	if spec.Env != nil {
		env := make(map[string]string, len(spec.Env))
		for key, value := range spec.Env {
			env[key] = value
		}
		spec.Env = env
	}
	return spec
}

type weaverReleasePhase struct {
	Flow             string `json:"flow"`
	Kind             string `json:"kind"`
	Datastore        string `json:"datastore"`
	Project          string `json:"project"`
	RootDir          string `json:"root_dir"`
	RunDir           string `json:"run_dir"`
	FixturesDir      string `json:"fixtures_dir"`
	ArtifactsDir     string `json:"artifacts_dir"`
	RuntimePortsFile string `json:"runtime_ports_file"`
	NetworkSubnet    string `json:"network_subnet"`
	ComposeOverride  string `json:"compose_override"`
	LogPath          string `json:"log_path"`
	StatusPath       string `json:"status_path"`
	TimeoutSeconds   int64  `json:"timeout_seconds"`
	Spec             weaverReleaseFlowSpec
	RuntimePorts     runtimePortState
}

type weaverReleasePhaseStatus struct {
	Flow       string    `json:"flow"`
	Datastore  string    `json:"datastore"`
	Project    string    `json:"project"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	DurationMS int64     `json:"duration_ms,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type weaverReleasePhaseConfiguration struct {
	Flow         string            `json:"flow"`
	Kind         string            `json:"kind"`
	Datastore    string            `json:"datastore"`
	Project      string            `json:"project"`
	Services     []string          `json:"services"`
	SeedFixtures []string          `json:"seed_fixtures"`
	Environment  map[string]string `json:"environment"`
}

type weaverReleaseManifest struct {
	Version    int                   `json:"version"`
	Mode       string                `json:"mode"`
	OwnerPID   int                   `json:"owner_pid"`
	StartedAt  time.Time             `json:"started_at"`
	FinishedAt time.Time             `json:"finished_at,omitempty"`
	Status     string                `json:"status"`
	RunDir     string                `json:"run_dir"`
	Phases     []*weaverReleasePhase `json:"phases"`
}

type weaverReleaseTimingBaselines struct {
	Version         int     `json:"version"`
	ThresholdMinute float64 `json:"threshold_minutes"`
	PassDurationsMS []int64 `json:"pass_durations_ms"`
}

type weaverReleasePhaseResult struct {
	Phase    *weaverReleasePhase
	Duration time.Duration
	Err      error
}

const (
	defaultWeaverReleaseGateJobs = 8
	maxWeaverReleaseGateJobs     = 16
)

func cmdWeaverReleaseGate(args []string) {
	mode := "all"
	if len(args) > 0 && strings.TrimSpace(args[0]) != "" {
		mode = strings.ToLower(strings.TrimSpace(args[0]))
	}
	if err := runWeaverReleaseGate(context.Background(), mode); err != nil {
		log.Fatal(err)
	}
}

func runWeaverReleaseGate(parent context.Context, mode string) error {
	specs, err := resolveWeaverReleaseGateSpecs(mode)
	if err != nil {
		return err
	}
	ensureFixtureProfiles("release-gate")
	runDir, err := newWeaverReleaseGateRunDir()
	if err != nil {
		return err
	}
	phases, err := newWeaverReleasePhases(runDir, specs)
	if err != nil {
		return err
	}
	if err := assignWeaverReleaseNetworkSubnets(parent, phases); err != nil {
		return err
	}
	manifest := &weaverReleaseManifest{
		Version:   1,
		Mode:      mode,
		OwnerPID:  os.Getpid(),
		StartedAt: time.Now().UTC(),
		Status:    "running",
		RunDir:    runDir,
		Phases:    phases,
	}
	manifestPath := filepath.Join(runDir, "release-gate.json")
	if err := writeWeaverReleaseJSON(manifestPath, manifest); err != nil {
		return err
	}
	if err := writeWeaverReleaseLatestPointer(runDir); err != nil {
		return err
	}
	if err := ensureLocalWeaverNNTPImage(); err != nil {
		manifest.FinishedAt = time.Now().UTC()
		manifest.Status = "failed"
		_ = writeWeaverReleaseJSON(manifestPath, manifest)
		return fmt.Errorf("prepare NNTP fixture image before release fanout: %w", err)
	}
	if err := runWeaverPlaywrightAudit(); err != nil {
		manifest.FinishedAt = time.Now().UTC()
		manifest.Status = "failed"
		_ = writeWeaverReleaseJSON(manifestPath, manifest)
		return fmt.Errorf("validate Weaver Playwright project before release fanout: %w", err)
	}
	if slices.ContainsFunc(specs, func(spec weaverReleaseFlowSpec) bool {
		return spec.Kind != weaverReleaseFlowCommand
	}) {
		if err := ensureLocalWeaverImage(); err != nil {
			manifest.FinishedAt = time.Now().UTC()
			manifest.Status = "failed"
			_ = writeWeaverReleaseJSON(manifestPath, manifest)
			return fmt.Errorf("prepare Weaver image before release fanout: %w", err)
		}
		if err := ensureLocalWeaverPlaywrightImage(); err != nil {
			manifest.FinishedAt = time.Now().UTC()
			manifest.Status = "failed"
			_ = writeWeaverReleaseJSON(manifestPath, manifest)
			return fmt.Errorf("prepare Weaver Playwright image before release fanout: %w", err)
		}
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	results := runWeaverReleasePhases(ctx, phases, weaverReleaseGateJobs(len(phases)))
	manifest.FinishedAt = time.Now().UTC()
	manifest.Status = "passed"
	var failures []error
	for _, result := range results {
		if result.Err != nil {
			manifest.Status = "failed"
			failures = append(failures, fmt.Errorf("%s/%s: %w", result.Phase.Flow, result.Phase.Datastore, result.Err))
		}
	}
	if ctx.Err() != nil {
		manifest.Status = "canceled"
		failures = append(failures, ctx.Err())
	}
	if manifest.Status == "passed" && (mode == "all" || mode == "datastore-matrix") {
		duration := manifest.FinishedAt.Sub(manifest.StartedAt)
		if err := recordWeaverReleaseTimingBaseline(duration); err != nil {
			manifest.Status = "failed"
			failures = append(failures, err)
		}
	}
	if err := writeWeaverReleaseJSON(manifestPath, manifest); err != nil {
		failures = append(failures, err)
	}
	printWeaverReleaseSummary(results, runDir)
	return errors.Join(failures...)
}

func recordWeaverReleaseTimingBaseline(duration time.Duration) error {
	threshold := 45 * time.Minute
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_RELEASE_GATE_MAX_MINUTES")); value != "" {
		minutes, err := strconv.ParseFloat(value, 64)
		if err != nil || minutes <= 0 {
			return fmt.Errorf("invalid E2E_WEAVER_RELEASE_GATE_MAX_MINUTES=%q", value)
		}
		threshold = time.Duration(minutes * float64(time.Minute))
	}
	path := filepath.Join(weaverReleaseGateRoot(), "timing-baselines.json")
	baselines := weaverReleaseTimingBaselines{
		Version:         1,
		ThresholdMinute: threshold.Minutes(),
	}
	if body, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(body, &baselines); err != nil {
			return fmt.Errorf("decode Weaver release timing baselines: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	enforce := len(baselines.PassDurationsMS) >= 3
	baselines.Version = 1
	baselines.ThresholdMinute = threshold.Minutes()
	baselines.PassDurationsMS = append(baselines.PassDurationsMS, duration.Milliseconds())
	if len(baselines.PassDurationsMS) > 10 {
		baselines.PassDurationsMS = baselines.PassDurationsMS[len(baselines.PassDurationsMS)-10:]
	}
	if err := writeWeaverReleaseJSON(path, baselines); err != nil {
		return err
	}
	if enforce && duration > threshold {
		return fmt.Errorf(
			"Weaver product release gate took %s, exceeding the enforced %s threshold after three stable baselines",
			duration.Round(time.Second),
			threshold,
		)
	}
	return nil
}

func assignWeaverReleaseNetworkSubnets(
	ctx context.Context,
	phases []*weaverReleasePhase,
) error {
	used, err := composeutil.ListNetworkSubnets(ctx, e2eDir())
	if err != nil {
		return fmt.Errorf("inspect Docker networks for Weaver release gate: %w", err)
	}
	subnets, err := composeutil.SelectNonOverlappingSubnets(
		len(phases),
		weaverReleaseNetworkCandidates(phases),
		used,
	)
	if err != nil {
		return err
	}
	for index, phase := range phases {
		phase.NetworkSubnet = subnets[index]
		phase.ComposeOverride = filepath.Join(phase.RootDir, "network.compose.override.yml")
		if err := composeutil.WriteNetworkOverride(phase.ComposeOverride, phase.NetworkSubnet); err != nil {
			return err
		}
	}
	return nil
}

func weaverReleaseNetworkCandidates(phases []*weaverReleasePhase) []string {
	hasher := fnv.New32a()
	for _, phase := range phases {
		_, _ = hasher.Write([]byte(phase.Project))
	}
	start := int(hasher.Sum32() % 256)
	secondOctets := []int{250, 249, 248, 247}
	candidates := make([]string, 0, len(secondOctets)*256)
	for _, second := range secondOctets {
		for offset := range 256 {
			third := (start + offset) % 256
			candidates = append(candidates, fmt.Sprintf("10.%d.%d.0/24", second, third))
		}
	}
	return candidates
}

func resolveWeaverReleaseGateSpecs(mode string) ([]weaverReleaseFlowSpec, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "", "all", "datastore-matrix":
		out := make([]weaverReleaseFlowSpec, 0, len(weaverReleaseFlowSpecs))
		for _, spec := range weaverReleaseFlowSpecs {
			out = append(out, cloneWeaverReleaseFlowSpec(spec))
		}
		return out, nil
	default:
		spec, ok := weaverReleaseFlowSpecFor(mode)
		if !ok {
			return nil, fmt.Errorf("unknown Weaver release-gate flow %q", mode)
		}
		return []weaverReleaseFlowSpec{spec}, nil
	}
}

func newWeaverReleasePhases(runDir string, specs []weaverReleaseFlowSpec) ([]*weaverReleasePhase, error) {
	var phases []*weaverReleasePhase
	for _, spec := range specs {
		for _, datastore := range spec.Datastores {
			slug := sanitizeProjectName(spec.Name + "-" + string(datastore))
			rootDir := filepath.Join(runDir, slug)
			phase := &weaverReleasePhase{
				Flow:             spec.Name,
				Kind:             string(spec.Kind),
				Datastore:        string(datastore),
				Project:          sanitizeProjectName(fmt.Sprintf("weaver-e2e-%d-%s", time.Now().UnixNano(), slug)),
				RootDir:          rootDir,
				RunDir:           filepath.Join(rootDir, "run"),
				FixturesDir:      filepath.Join(rootDir, "fixtures"),
				ArtifactsDir:     filepath.Join(rootDir, "artifacts"),
				RuntimePortsFile: filepath.Join(rootDir, "runtime-ports.json"),
				LogPath:          filepath.Join(rootDir, "flow.log"),
				StatusPath:       filepath.Join(rootDir, "status.json"),
				TimeoutSeconds:   int64(spec.Timeout / time.Second),
				Spec:             cloneWeaverReleaseFlowSpec(spec),
			}
			usesPostgres := datastore == weaverDatastorePostgres ||
				spec.Env["E2E_WEAVER_BACKUP_SOURCE_DATASTORE"] == string(weaverDatastorePostgres)
			if usesPostgres && !slices.Contains(phase.Spec.Services, "weaver-postgres") {
				phase.Spec.Services = append(phase.Spec.Services, "weaver-postgres")
			}
			for _, dir := range []string{phase.RunDir, phase.FixturesDir, phase.ArtifactsDir} {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return nil, fmt.Errorf("create release-gate directory %s: %w", dir, err)
				}
			}
			phases = append(phases, phase)
		}
	}
	states, err := allocateRuntimePortStates(len(phases))
	if err != nil {
		return nil, err
	}
	for index, phase := range phases {
		phase.RuntimePorts = states[index]
		if err := saveRuntimePortState(phase.RuntimePortsFile, states[index]); err != nil {
			return nil, err
		}
	}
	return phases, nil
}

func weaverReleaseGateJobs(flowCount int) int {
	requested := defaultWeaverReleaseGateJobs
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_RELEASE_GATE_JOBS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			requested = parsed
		}
	}
	if requested < 1 {
		requested = 1
	}
	if requested > maxWeaverReleaseGateJobs {
		requested = maxWeaverReleaseGateJobs
	}
	if requested > flowCount {
		requested = flowCount
	}
	return requested
}

func runWeaverReleasePhases(ctx context.Context, phases []*weaverReleasePhase, jobs int) []weaverReleasePhaseResult {
	if jobs < 1 {
		jobs = 1
	}
	queue := make(chan *weaverReleasePhase)
	results := make(chan weaverReleasePhaseResult, len(phases))
	// Announce the flow set up front so the parent dashboard can draw one bar
	// per flow before any of them start, instead of discovering them as they
	// finish.
	emitProgressEvent(progressEvent{Kind: "phase_total", Total: len(phases), Detail: "release flows"})
	for _, phase := range phases {
		emitProgressEvent(progressEvent{
			Kind:   "flow_pending",
			Name:   weaverReleaseFlowKey(phase),
			Detail: "queued",
		})
	}
	var completed atomic.Int64
	var workers sync.WaitGroup
	for range jobs {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for phase := range queue {
				key := weaverReleaseFlowKey(phase)
				emitProgressEvent(progressEvent{Kind: "flow_start", Name: key})
				result := runWeaverReleasePhase(ctx, phase)
				status := "pass"
				if result.Err != nil {
					status = "fail"
				}
				emitProgressEvent(progressEvent{
					Kind:   "flow_done",
					Status: status,
					Name:   key,
					Detail: result.Duration.Round(time.Second).String(),
				})
				emitProgressEvent(progressEvent{
					Kind:    "phase_progress",
					Current: int(completed.Add(1)),
					Total:   len(phases),
					Detail:  key,
				})
				results <- result
			}
		}()
	}
	go func() {
		defer close(queue)
		for _, phase := range phases {
			select {
			case queue <- phase:
			case <-ctx.Done():
				return
			}
		}
	}()
	workers.Wait()
	close(results)
	out := make([]weaverReleasePhaseResult, 0, len(phases))
	for result := range results {
		out = append(out, result)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Phase.Flow == out[j].Phase.Flow {
			return out[i].Phase.Datastore < out[j].Phase.Datastore
		}
		return out[i].Phase.Flow < out[j].Phase.Flow
	})
	return out
}

// weaverReleaseFlowKey names a flow/datastore pair for the progress stream and
// the dashboard bar it drives. It matches the release summary's column order so
// a bar and its summary line are recognisably the same run.
func weaverReleaseFlowKey(phase *weaverReleasePhase) string {
	datastore := strings.TrimSpace(string(phase.Datastore))
	if datastore == "" {
		return phase.Flow
	}
	return phase.Flow + "/" + datastore
}

func runWeaverReleasePhase(parent context.Context, phase *weaverReleasePhase) weaverReleasePhaseResult {
	started := time.Now()
	status := weaverReleasePhaseStatus{
		Flow:      phase.Flow,
		Datastore: phase.Datastore,
		Project:   phase.Project,
		Status:    "running",
		StartedAt: started.UTC(),
	}
	_ = writeWeaverReleaseJSON(phase.StatusPath, status)
	_ = writeWeaverReleaseJSON(filepath.Join(phase.ArtifactsDir, "status.json"), status)
	_ = writeWeaverReleaseJSON(
		filepath.Join(phase.ArtifactsDir, "configuration.json"),
		weaverReleasePhaseConfiguration{
			Flow:         phase.Flow,
			Kind:         phase.Kind,
			Datastore:    phase.Datastore,
			Project:      phase.Project,
			Services:     append([]string(nil), phase.Spec.Services...),
			SeedFixtures: append([]string(nil), phase.Spec.SeedFixtures...),
			Environment:  redactWeaverReleaseEnv(phase.env()),
		},
	)

	timeout := phase.Spec.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	exe, err := os.Executable()
	if err == nil {
		logFile, createErr := os.Create(phase.LogPath)
		if createErr != nil {
			err = createErr
		} else {
			cmd := exec.CommandContext(ctx, exe, "release-flow", phase.Flow, phase.Datastore)
			configureWeaverReleaseCommandCancellation(cmd)
			cmd.Dir = e2eDir()
			cmd.Env = mergeChildEnv(os.Environ(), phase.env())
			cmd.Stdout = io.MultiWriter(logFile, os.Stdout)
			cmd.Stderr = io.MultiWriter(logFile, os.Stderr)
			err = cmd.Run()
			_ = logFile.Close()
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("flow exceeded %s", timeout)
	}
	if exe != "" {
		finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), time.Minute)
		finalizeCmd := exec.CommandContext(finalizeCtx, exe, "release-finalize")
		configureWeaverReleaseCommandCancellation(finalizeCmd)
		finalizeCmd.Dir = e2eDir()
		finalizeCmd.Env = mergeChildEnv(os.Environ(), phase.env())
		finalizeCmd.Stdout = os.Stdout
		finalizeCmd.Stderr = os.Stderr
		finalizeErr := finalizeCmd.Run()
		finalizeCancel()
		if errors.Is(finalizeCtx.Err(), context.DeadlineExceeded) {
			finalizeErr = fmt.Errorf("release-flow finalizer exceeded one minute")
		}
		err = errors.Join(err, finalizeErr)
	}
	status.FinishedAt = time.Now().UTC()
	status.DurationMS = time.Since(started).Milliseconds()
	status.Status = "passed"
	if err != nil {
		status.Status = "failed"
		status.Error = err.Error()
	}
	_ = writeWeaverReleaseJSON(phase.StatusPath, status)
	_ = writeWeaverReleaseJSON(filepath.Join(phase.ArtifactsDir, "status.json"), status)
	_ = writeWeaverReleaseArtifactIndex(phase)
	return weaverReleasePhaseResult{Phase: phase, Duration: time.Since(started), Err: err}
}

func redactWeaverReleaseEnv(env map[string]string) map[string]string {
	redacted := make(map[string]string, len(env))
	for key, value := range env {
		upper := strings.ToUpper(key)
		if strings.Contains(upper, "PASSWORD") ||
			strings.Contains(upper, "SECRET") ||
			strings.Contains(upper, "TOKEN") ||
			strings.Contains(upper, "API_KEY") ||
			strings.Contains(upper, "DATABASE_URL") ||
			strings.Contains(upper, "ENCRYPTION_KEY") {
			redacted[key] = "[REDACTED]"
			continue
		}
		redacted[key] = value
	}
	return redacted
}

func writeWeaverReleaseArtifactIndex(phase *weaverReleasePhase) error {
	entries := []string{"flow.log", "status.json"}
	err := filepath.WalkDir(phase.ArtifactsDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || path == filepath.Join(phase.ArtifactsDir, "artifact-index.json") {
			return nil
		}
		relative, err := filepath.Rel(phase.RootDir, path)
		if err != nil {
			return err
		}
		entries = append(entries, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(entries)
	return writeWeaverReleaseJSON(filepath.Join(phase.ArtifactsDir, "artifact-index.json"), map[string]any{
		"version": 1,
		"flow":    phase.Flow,
		"files":   entries,
	})
}

func (phase *weaverReleasePhase) env() map[string]string {
	env := map[string]string{
		"E2E_DIR":                                   e2eDir(),
		"E2E_PROJECT":                               phase.Project,
		"E2E_RUN_DIR":                               phase.RunDir,
		"FIXTURES_DIR":                              phase.FixturesDir,
		"E2E_RUNTIME_PORTS_FILE":                    phase.RuntimePortsFile,
		"E2E_WEAVER_DATASTORE":                      phase.Datastore,
		"E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR":       phase.ArtifactsDir,
		"E2E_WEAVER_PLAYWRIGHT_BASE_URL":            "http://weaver:9090",
		"E2E_WEAVER_POSTGRES_DB":                    "weaver",
		"E2E_WEAVER_POSTGRES_USER":                  "weaver",
		"E2E_WEAVER_POSTGRES_PASSWORD":              "weaver-pass",
		"E2E_WEAVER_DATABASE_URL":                   "",
		"E2E_WEAVER_MODE":                           "1",
		"E2E_WEAVER_CLOCK_FILE":                     "/e2e-clock/now",
		"E2E_WEAVER_BACKUP_SOURCE_DATASTORE":        phase.Spec.Env["E2E_WEAVER_BACKUP_SOURCE_DATASTORE"],
		"E2E_WEAVER_BACKUP_TARGET_DATASTORE":        phase.Spec.Env["E2E_WEAVER_BACKUP_TARGET_DATASTORE"],
		"E2E_WEAVER_RELEASE_FLOW":                   phase.Flow,
		"E2E_WEAVER_RELEASE_FLOW_DATASTORE":         phase.Datastore,
		"E2E_WEAVER_RELEASE_FLOW_ARTIFACTS_DIR":     phase.ArtifactsDir,
		"E2E_WEAVER_RELEASE_FLOW_RUNTIME_PORT_FILE": phase.RuntimePortsFile,
	}
	if phase.ComposeOverride != "" {
		env["COMPOSE_FILE"] = composeutil.ComposeFileValue(
			filepath.Join(e2eDir(), "docker-compose.yml"),
			phase.ComposeOverride,
		)
	}
	if phase.Datastore == string(weaverDatastorePostgres) {
		env["E2E_WEAVER_DATABASE_URL"] = "postgres://weaver:weaver-pass@weaver-postgres:5432/weaver?sslmode=require"
	}
	for key, value := range phase.Spec.Env {
		env[key] = value
	}
	for key, value := range runtimePortEnvValues(phase.RuntimePorts) {
		env[key] = value
	}
	return env
}

func cmdWeaverReleaseFlow(args []string) {
	if len(args) != 2 {
		log.Fatalf("usage: %s release-flow <flow> <sqlite|postgres>", cliProgramName)
	}
	spec, ok := weaverReleaseFlowSpecFor(args[0])
	if !ok {
		log.Fatalf("unknown Weaver release flow %q", args[0])
	}
	datastore, err := parseWeaverDatastore(args[1])
	if err != nil {
		log.Fatal(err)
	}
	if err := ensureLocalWeaverNNTPImage(); err != nil {
		log.Fatalf("prepare NNTP fixture image: %v", err)
	}
	if err := runWeaverReleaseFlow(context.Background(), spec, datastore); err != nil {
		log.Fatal(err)
	}
}

const weaverReleaseDiagnosticsMarker = "diagnostics-capture.json"

func cmdWeaverReleaseFinalize(args []string) {
	if len(args) != 0 {
		log.Fatalf("usage: %s release-finalize", cliProgramName)
	}
	if artifactsDir := strings.TrimSpace(os.Getenv("E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR")); artifactsDir != "" {
		artifactsDir = absolutePath(artifactsDir)
		if _, err := os.Stat(filepath.Join(artifactsDir, weaverReleaseDiagnosticsMarker)); err != nil {
			if err := captureWeaverReleaseDiagnostics(artifactsDir); err != nil {
				log.Printf("warning: capture parent release-flow evidence: %v", err)
			}
		}
	}
	killWeaver()
	if !envBool("E2E_KEEP_STACKS", false) {
		if err := cleanupExactWeaverReleaseProject(); err != nil {
			log.Fatalf("finalize exact Weaver release-flow project: %v", err)
		}
	}
}

func cleanupExactWeaverReleaseProject() error {
	project := strings.TrimSpace(os.Getenv("E2E_PROJECT"))
	if project == "" {
		return errors.New("E2E_PROJECT is required for exact Weaver release cleanup")
	}

	firstDownErr := dockerComposeDown()
	containerErr := removeExactWeaverReleaseProjectContainers(project)
	finalDownErr := dockerComposeDown()
	if containerErr != nil || finalDownErr != nil {
		return errors.Join(firstDownErr, containerErr, finalDownErr)
	}
	if firstDownErr != nil {
		log.Printf("initial exact project teardown needed a one-off-container cleanup retry: %v", firstDownErr)
	}
	return nil
}

func removeExactWeaverReleaseProjectContainers(project string) error {
	listArgs := []string{
		"ps",
		"-aq",
		"--filter",
		"label=com.docker.compose.project=" + project,
	}
	listCmd := exec.Command("docker", listArgs...)
	listCmd.Dir = e2eDir()
	output, err := listCmd.Output()
	if err != nil {
		return fmt.Errorf("list exact project containers for %s: %w", project, err)
	}
	containerIDs := strings.Fields(string(output))
	if len(containerIDs) == 0 {
		return nil
	}

	removeArgs := append([]string{"rm", "-f"}, containerIDs...)
	removeCmd := exec.Command("docker", removeArgs...)
	removeCmd.Dir = e2eDir()
	if output, err := removeCmd.CombinedOutput(); err != nil {
		return fmt.Errorf(
			"remove exact project containers for %s: %w\n%s",
			project,
			err,
			strings.TrimSpace(string(output)),
		)
	}
	return nil
}

func runWeaverReleaseFlow(ctx context.Context, spec weaverReleaseFlowSpec, datastore weaverDatastore) (err error) {
	switch spec.Kind {
	case weaverReleaseFlowCommand:
		switch spec.Name {
		case "adaptive-dispatch":
			cmdAdaptiveDispatchTest()
			return nil
		default:
			return fmt.Errorf("release flow %s has no command runner", spec.Name)
		}
	case weaverReleaseFlowBrowser, weaverReleaseFlowBehavior:
		return runWeaverBrowserReleaseFlow(ctx, spec, datastore)
	default:
		return fmt.Errorf("release flow %s has unsupported kind %q", spec.Name, spec.Kind)
	}
}

func runWeaverBrowserReleaseFlow(ctx context.Context, spec weaverReleaseFlowSpec, datastore weaverDatastore) (err error) {
	artifactsDir := absolutePath(os.Getenv("E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR"))
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		return err
	}
	defer func() {
		if evidenceErr := captureWeaverReleaseDiagnosticsWithMarker(artifactsDir); evidenceErr != nil {
			err = errors.Join(err, fmt.Errorf("capture release evidence: %w", evidenceErr))
		}
		if !envBool("E2E_KEEP_STACKS", false) {
			if downErr := dockerComposeDown(); downErr != nil {
				err = errors.Join(err, downErr)
			}
		}
	}()
	if downErr := dockerComposeDown(); downErr != nil {
		log.Printf("warning: reset Weaver release flow stack: %v", downErr)
	}
	if strings.HasPrefix(spec.Name, "ui-backup-restore-") {
		if err := ensureLocalWeaverPlaywrightImage(); err != nil {
			return err
		}
		return runWeaverBackupRestoreReleaseFlow(ctx, spec)
	}
	if spec.Name == "encryption-key-lifecycle" {
		if err := ensureLocalWeaverPlaywrightImage(); err != nil {
			return err
		}
		return runWeaverEncryptionKeyLifecycleReleaseFlow(ctx, spec, datastore, artifactsDir)
	}
	if spec.Name == "ui-runtime-observability" {
		if err := ensureLocalWeaverPlaywrightImage(); err != nil {
			return err
		}
		return runWeaverRuntimeObservabilityReleaseFlow(ctx, spec, datastore)
	}
	if spec.Name == "provider-connection-cap" {
		if err := ensureLocalWeaverPlaywrightImage(); err != nil {
			return err
		}
		return runWeaverProviderConnectionCapReleaseFlow(ctx, spec, datastore)
	}
	if spec.Name == "rate-limits" {
		if err := ensureLocalWeaverPlaywrightImage(); err != nil {
			return err
		}
		return runWeaverRateLimitsReleaseFlow(ctx, spec, datastore)
	}
	if spec.Name == "bandwidth-and-server-quotas" {
		if err := ensureLocalWeaverPlaywrightImage(); err != nil {
			return err
		}
		return runWeaverBandwidthAndServerQuotasReleaseFlow(ctx, spec, datastore)
	}
	if spec.Name == "ui-settings-crud" || spec.Name == "ui-security" {
		if err := ensureLocalWeaverPlaywrightImage(); err != nil {
			return err
		}
		return runWeaverUIPersistenceReleaseFlow(ctx, spec, datastore)
	}
	if len(spec.SeedFixtures) > 0 {
		if err := ensureSeedingInfrastructureErr(); err != nil {
			return err
		}
		for _, slug := range spec.SeedFixtures {
			if err := seedFixture(filepath.Join(testdataDir(), slug)); err != nil {
				return fmt.Errorf("seed probe fixture %s: %w", slug, err)
			}
		}
	}
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return err
	}
	if err := ensureLocalWeaverPlaywrightImage(); err != nil {
		return err
	}
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", spec.PlaywrightScript)
	return runWeaverReleasePlaywright(ctx, spec.PlaywrightScript)
}

func runWeaverUIPersistenceReleaseFlow(
	ctx context.Context,
	spec weaverReleaseFlowSpec,
	datastore weaverDatastore,
) error {
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return fmt.Errorf("start %s stack: %w", spec.Name, err)
	}
	setEnv("E2E_WEAVER_UI_STAGE", "initial")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "initial")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("%s initial stage: %w", spec.Name, err)
	}
	if err := captureWeaverReleaseStageDiagnostics("initial"); err != nil {
		return fmt.Errorf("capture %s initial evidence: %w", spec.Name, err)
	}
	if err := dockerComposeRestart("weaver"); err != nil {
		return fmt.Errorf("restart Weaver for %s persistence: %w", spec.Name, err)
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	waitForHTTP(defaultWeaverURL(), 90*time.Second)
	setEnv("E2E_WEAVER_UI_STAGE", "after-restart")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "after-restart")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("%s restart-persistence stage: %w", spec.Name, err)
	}
	return nil
}

func runWeaverRateLimitsReleaseFlow(
	ctx context.Context,
	spec weaverReleaseFlowSpec,
	datastore weaverDatastore,
) error {
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return fmt.Errorf("start rate-limits stack: %w", err)
	}
	setEnv("E2E_WEAVER_RATE_LIMIT_STAGE", "initial")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "initial")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("rate-limits initial stage: %w", err)
	}
	if err := captureWeaverReleaseStageDiagnostics("initial"); err != nil {
		return fmt.Errorf("capture rate-limits initial evidence: %w", err)
	}
	if err := dockerComposeRestart("weaver"); err != nil {
		return fmt.Errorf("restart Weaver for rate-limit persistence: %w", err)
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	waitForHTTP(defaultWeaverURL(), 90*time.Second)
	setEnv("E2E_WEAVER_RATE_LIMIT_STAGE", "restart-verify")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "restart-verify")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("rate-limits restart-persistence stage: %w", err)
	}
	return nil
}

func runWeaverBandwidthAndServerQuotasReleaseFlow(
	ctx context.Context,
	spec weaverReleaseFlowSpec,
	datastore weaverDatastore,
) error {
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return fmt.Errorf("start bandwidth-and-server-quotas stack: %w", err)
	}
	setEnv("E2E_WEAVER_QUOTA_STAGE", "initial")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "initial")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("bandwidth-and-server-quotas initial stage: %w", err)
	}
	if err := captureWeaverReleaseStageDiagnostics("initial"); err != nil {
		return fmt.Errorf("capture bandwidth-and-server-quotas initial evidence: %w", err)
	}
	if err := dockerComposeRestart("weaver"); err != nil {
		return fmt.Errorf("restart Weaver for bandwidth/quota persistence: %w", err)
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	waitForHTTP(defaultWeaverURL(), 90*time.Second)
	setEnv("E2E_WEAVER_QUOTA_STAGE", "restart-verify")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "restart-verify")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("bandwidth-and-server-quotas restart-persistence stage: %w", err)
	}
	return nil
}

func runWeaverProviderConnectionCapReleaseFlow(
	ctx context.Context,
	spec weaverReleaseFlowSpec,
	datastore weaverDatastore,
) error {
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return fmt.Errorf("start provider-cap stack: %w", err)
	}
	for _, phase := range []struct {
		stage   string
		restart bool
	}{
		{stage: "initial"},
		{stage: "restart-verify", restart: true},
		{stage: "recover"},
	} {
		if phase.restart {
			if err := dockerComposeRestart("weaver"); err != nil {
				return fmt.Errorf("restart Weaver before provider-cap relearning: %w", err)
			}
			if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
				return err
			}
		}
		setEnv("E2E_WEAVER_PROVIDER_CAP_STAGE", phase.stage)
		setEnv("E2E_WEAVER_ARTIFACT_STAGE", phase.stage)
		if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
			return fmt.Errorf("provider-cap stage %s: %w", phase.stage, err)
		}
		if err := captureWeaverReleaseStageDiagnostics(phase.stage); err != nil {
			return fmt.Errorf("capture provider-cap stage %s evidence: %w", phase.stage, err)
		}
	}
	return nil
}

func runWeaverRuntimeObservabilityReleaseFlow(
	ctx context.Context,
	spec weaverReleaseFlowSpec,
	datastore weaverDatastore,
) error {
	setEnv("E2E_WEAVER_BASE_URL", "/")
	setEnv("E2E_WEAVER_PLAYWRIGHT_BASE_URL", "http://weaver:9090/")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "root-path")
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return fmt.Errorf("start root-path runtime stack: %w", err)
	}
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("verify root-path runtime behavior: %w", err)
	}
	if err := captureWeaverReleaseStageDiagnostics("root-path"); err != nil {
		return fmt.Errorf("capture root-path runtime evidence: %w", err)
	}

	setEnv("E2E_WEAVER_BASE_URL", "/weaver")
	setEnv("E2E_WEAVER_PLAYWRIGHT_BASE_URL", "http://weaver:9090/weaver/")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "base-path")
	if err := forceRecreateWeaverReleaseService(ctx); err != nil {
		return fmt.Errorf("recreate Weaver with non-root base path: %w", err)
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	waitForHTTP(strings.TrimRight(defaultWeaverURL(), "/")+"/weaver/", 90*time.Second)
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("verify non-root runtime behavior: %w", err)
	}
	return nil
}

func startWeaverReleaseStack(spec weaverReleaseFlowSpec, datastore weaverDatastore) error {
	setEnv(weaverDatastoreEnv, string(datastore))
	if envBool("E2E_WEAVER_MODE", false) {
		if err := initializeWeaverE2EClock(); err != nil {
			return err
		}
	}
	if datastore == weaverDatastorePostgres {
		setEnv("E2E_WEAVER_DATABASE_URL", weaverComposePostgresURL())
		if err := dockerComposeUp("weaver-postgres"); err != nil {
			return err
		}
		if err := waitForDockerServiceReady("weaver-postgres", 90*time.Second); err != nil {
			return err
		}
	} else {
		setEnv("E2E_WEAVER_DATABASE_URL", "")
	}
	if err := dockerComposeUp(spec.Services...); err != nil {
		return err
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	waitForHTTP(defaultWeaverURL(), 90*time.Second)
	return nil
}

func initializeWeaverE2EClock() error {
	args := dockerComposeArgs(
		"run",
		"--rm",
		"--no-deps",
		"--entrypoint",
		"/bin/sh",
		"weaver",
		"-c",
		"umask 077; printf '%s\\n' '2032-06-01T00:00:00Z' > /e2e-clock/now; chown \"${PUID:-1000}:${PGID:-1000}\" /e2e-clock/now",
	)
	cmd := exec.Command("docker", args...)
	cmd.Dir = e2eDir()
	cmd.Env = os.Environ()
	return runExternalCommand(cmd, "initialize Weaver e2e clock")
}

func runWeaverBackupRestoreReleaseFlow(ctx context.Context, spec weaverReleaseFlowSpec) error {
	source, err := parseWeaverDatastore(spec.Env["E2E_WEAVER_BACKUP_SOURCE_DATASTORE"])
	if err != nil {
		return err
	}
	target, err := parseWeaverDatastore(spec.Env["E2E_WEAVER_BACKUP_TARGET_DATASTORE"])
	if err != nil {
		return err
	}

	if err := startWeaverReleaseStack(spec, source); err != nil {
		return fmt.Errorf("start backup source (%s): %w", source, err)
	}
	setEnv("E2E_WEAVER_BACKUP_STAGE", "source-export")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "source-export")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("export backup from %s: %w", source, err)
	}
	if err := captureWeaverReleaseStageDiagnostics("source-export"); err != nil {
		return fmt.Errorf("capture backup source evidence: %w", err)
	}
	if err := dockerComposeDown(); err != nil {
		return fmt.Errorf("tear down backup source %s: %w", source, err)
	}

	if err := startWeaverReleaseStack(spec, target); err != nil {
		return fmt.Errorf("start non-pristine backup target (%s): %w", target, err)
	}
	setEnv("E2E_WEAVER_BACKUP_STAGE", "target-blocked")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "target-blocked")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("verify restore block on non-pristine %s target: %w", target, err)
	}
	if err := captureWeaverReleaseStageDiagnostics("target-blocked"); err != nil {
		return fmt.Errorf("capture blocked backup target evidence: %w", err)
	}
	if err := dockerComposeDown(); err != nil {
		return fmt.Errorf("tear down non-pristine backup target %s: %w", target, err)
	}

	if err := startWeaverReleaseStack(spec, target); err != nil {
		return fmt.Errorf("start clean backup target (%s): %w", target, err)
	}
	setEnv("E2E_WEAVER_BACKUP_STAGE", "target-restore")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "target-restore")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("restore backup into %s: %w", target, err)
	}
	if err := captureWeaverReleaseStageDiagnostics("target-restore"); err != nil {
		return fmt.Errorf("capture staged restore evidence: %w", err)
	}
	if err := dockerComposeRestart("weaver"); err != nil {
		return fmt.Errorf("restart restored Weaver target: %w", err)
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	setEnv("E2E_WEAVER_BACKUP_STAGE", "target-verify")
	setEnv("E2E_WEAVER_ARTIFACT_STAGE", "target-verify")
	if err := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); err != nil {
		return fmt.Errorf("verify restored %s target: %w", target, err)
	}
	return nil
}

type weaverEncryptionKeyLifecyclePhase struct {
	Stage            string `json:"stage"`
	ContainerID      string `json:"container_id"`
	Fingerprint      string `json:"fingerprint"`
	Mode             string `json:"mode"`
	KeySource        string `json:"key_source"`
	BehaviorVerified bool   `json:"behavior_verified"`
}

type weaverEncryptionKeyLifecycleFault struct {
	Stage                    string   `json:"stage"`
	FailedContainerID        string   `json:"failed_container_id"`
	ContainerStatus          string   `json:"container_status"`
	HealthStatus             string   `json:"health_status,omitempty"`
	ExpectedLogMarkers       []string `json:"expected_log_markers"`
	LogArtifact              string   `json:"log_artifact"`
	FaultKeyExists           bool     `json:"fault_key_exists"`
	FaultKeyFingerprint      string   `json:"fault_key_fingerprint,omitempty"`
	FaultKeyMode             string   `json:"fault_key_mode,omitempty"`
	SilentReplacementBlocked bool     `json:"silent_replacement_blocked"`
	OriginalRestored         bool     `json:"original_restored"`
	RecoveryContainerID      string   `json:"recovery_container_id,omitempty"`
	RecoveryFingerprint      string   `json:"recovery_fingerprint,omitempty"`
	RecoveryMode             string   `json:"recovery_mode,omitempty"`
}

type weaverEncryptionKeyLifecycleEvidence struct {
	Flow       string                              `json:"flow"`
	Datastore  string                              `json:"datastore"`
	Project    string                              `json:"project"`
	DataVolume string                              `json:"data_volume"`
	BackupPath string                              `json:"backup_path"`
	Phases     []weaverEncryptionKeyLifecyclePhase `json:"phases"`
	Faults     []weaverEncryptionKeyLifecycleFault `json:"faults"`
}

const weaverE2EEncryptionKeyBackupPath = "/data/.weaver-e2e-encryption-key.original"

type weaverEncryptionKeyFaultSpec struct {
	Stage              string
	MutationScript     string
	ExpectedLogMarkers []string
}

type weaverEncryptionKeyVolumeState struct {
	Exists      bool
	Fingerprint string
	Mode        string
}

type weaverContainerRuntimeState struct {
	Status string
	Health string
}

type weaverEncryptionKeyStartupFailure struct {
	ContainerID string
	Runtime     weaverContainerRuntimeState
	Logs        string
}

func weaverEncryptionKeyFaultSpecs() []weaverEncryptionKeyFaultSpec {
	return []weaverEncryptionKeyFaultSpec{
		{
			Stage:          "missing",
			MutationScript: "test -f /data/encryption.key; rm /data/encryption.key",
			ExpectedLogMarkers: []string{
				"encrypted credentials exist but no encryption key is available",
				"refusing to generate a replacement key",
			},
		},
		{
			Stage: "corrupt",
			MutationScript: "test -f /data/encryption.key; " +
				"printf 'not-a-valid-weaver-encryption-key\\n' > /data/encryption.key; " +
				"chmod 600 /data/encryption.key",
			ExpectedLogMarkers: []string{"invalid key in key file"},
		},
		{
			Stage:              "unreadable",
			MutationScript:     "test -f /data/encryption.key; chmod 000 /data/encryption.key",
			ExpectedLogMarkers: []string{"failed to read encryption key from key file", "permission denied"},
		},
	}
}

func runWeaverEncryptionKeyLifecycleReleaseFlow(
	ctx context.Context,
	spec weaverReleaseFlowSpec,
	datastore weaverDatastore,
	artifactsDir string,
) (err error) {
	stateSecretBytes := make([]byte, 32)
	if _, err := rand.Read(stateSecretBytes); err != nil {
		return fmt.Errorf("generate encryption lifecycle state secret: %w", err)
	}
	stateSecret := base64.RawURLEncoding.EncodeToString(stateSecretBytes)
	stateFilename := ".encryption-key-lifecycle-state.enc"
	statePath := filepath.Join(artifactsDir, stateFilename)
	defer os.Remove(statePath)

	temporaryEnv := map[string]string{
		"E2E_WEAVER_ENCRYPTION_LIFECYCLE_STAGE":        "initial",
		"E2E_WEAVER_ENCRYPTION_LIFECYCLE_STATE_FILE":   filepath.Join("/artifacts", stateFilename),
		"E2E_WEAVER_ENCRYPTION_LIFECYCLE_STATE_SECRET": stateSecret,
	}
	type previousEnvValue struct {
		value string
		set   bool
	}
	previousEnv := make(map[string]previousEnvValue, len(temporaryEnv))
	for key, value := range temporaryEnv {
		previous, set := os.LookupEnv(key)
		previousEnv[key] = previousEnvValue{value: previous, set: set}
		setEnv(key, value)
	}
	defer func() {
		for key, previous := range previousEnv {
			if previous.set {
				setEnv(key, previous.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}()

	evidence := weaverEncryptionKeyLifecycleEvidence{
		Flow:       spec.Name,
		Datastore:  string(datastore),
		Project:    composeProject(),
		DataVolume: composeProject() + "_weaver-data",
		BackupPath: weaverE2EEncryptionKeyBackupPath,
	}
	evidencePath := filepath.Join(artifactsDir, "encryption-key-evidence.json")
	writeEvidence := func() error {
		body, marshalErr := json.MarshalIndent(evidence, "", "  ")
		if marshalErr != nil {
			return marshalErr
		}
		body = append(body, '\n')
		return os.WriteFile(evidencePath, body, 0o600)
	}
	defer func() {
		if writeErr := writeEvidence(); writeErr != nil {
			err = errors.Join(err, fmt.Errorf("write encryption lifecycle evidence: %w", writeErr))
		}
	}()

	recordPhase := func(stage, expectedFingerprint, expectedKeySource string) (int, error) {
		containerID, inspectErr := dockerComposeServiceContainerID("weaver")
		if inspectErr != nil {
			return -1, inspectErr
		}
		keyState, inspectErr := inspectContainerEncryptionKeyState(containerID)
		if inspectErr != nil {
			return -1, inspectErr
		}
		if keyState.Mode != "600" {
			return -1, fmt.Errorf("%s encryption key mode = %s, want 600", stage, keyState.Mode)
		}
		if expectedFingerprint != "" && keyState.Fingerprint != expectedFingerprint {
			return -1, fmt.Errorf(
				"%s changed encryption key fingerprint: got %s, want %s",
				stage,
				keyState.Fingerprint,
				expectedFingerprint,
			)
		}
		logs, inspectErr := dockerContainerLogs(containerID)
		if inspectErr != nil {
			return -1, inspectErr
		}
		if !strings.Contains(logs, expectedKeySource) {
			return -1, fmt.Errorf("%s logs do not contain %q", stage, expectedKeySource)
		}
		evidence.Phases = append(evidence.Phases, weaverEncryptionKeyLifecyclePhase{
			Stage:       stage,
			ContainerID: containerID,
			Fingerprint: keyState.Fingerprint,
			Mode:        keyState.Mode,
			KeySource:   expectedKeySource,
		})
		if writeErr := writeEvidence(); writeErr != nil {
			return -1, writeErr
		}
		return len(evidence.Phases) - 1, nil
	}
	verifyPhase := func(stage string, phaseIndex int) error {
		setEnv("E2E_WEAVER_ENCRYPTION_LIFECYCLE_STAGE", stage)
		setEnv("E2E_WEAVER_ARTIFACT_STAGE", stage)
		if playErr := runWeaverReleasePlaywright(ctx, spec.PlaywrightScript); playErr != nil {
			return playErr
		}
		if evidenceErr := captureWeaverReleaseStageDiagnostics(stage); evidenceErr != nil {
			return evidenceErr
		}
		evidence.Phases[phaseIndex].BehaviorVerified = true
		return writeEvidence()
	}

	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return fmt.Errorf("start encryption lifecycle stack: %w", err)
	}
	initialIndex, err := recordPhase("initial", "", "persisted encryption master key in key file")
	if err != nil {
		return fmt.Errorf("capture initial encryption key: %w", err)
	}
	if err := verifyPhase("initial", initialIndex); err != nil {
		return fmt.Errorf("verify initial encrypted state: %w", err)
	}
	initial := evidence.Phases[initialIndex]

	if err := dockerComposeRestart("weaver"); err != nil {
		return fmt.Errorf("restart Weaver: %w", err)
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	restartIndex, err := recordPhase(
		"restart",
		initial.Fingerprint,
		"using encryption master key from key file",
	)
	if err != nil {
		return fmt.Errorf("capture restarted encryption key: %w", err)
	}
	if evidence.Phases[restartIndex].ContainerID != initial.ContainerID {
		return fmt.Errorf("docker restart replaced Weaver container: before=%s after=%s", initial.ContainerID, evidence.Phases[restartIndex].ContainerID)
	}
	if err := verifyPhase("restart", restartIndex); err != nil {
		return fmt.Errorf("verify encrypted state after restart: %w", err)
	}

	if err := forceRecreateWeaverReleaseService(ctx); err != nil {
		return err
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	recreateIndex, err := recordPhase(
		"recreate",
		initial.Fingerprint,
		"using encryption master key from key file",
	)
	if err != nil {
		return fmt.Errorf("capture recreated-container encryption key: %w", err)
	}
	if evidence.Phases[recreateIndex].ContainerID == initial.ContainerID {
		return fmt.Errorf("forced recreation retained Weaver container %s", initial.ContainerID)
	}
	if err := verifyPhase("recreate", recreateIndex); err != nil {
		return fmt.Errorf("verify encrypted state after forced recreation: %w", err)
	}

	if err := inspectDockerVolume(evidence.DataVolume); err != nil {
		return fmt.Errorf("inspect encryption lifecycle data volume before down: %w", err)
	}
	if err := dockerComposeDownRetainingVolumes(); err != nil {
		return fmt.Errorf("stop encryption lifecycle stack retaining volumes: %w", err)
	}
	if err := inspectDockerVolume(evidence.DataVolume); err != nil {
		return fmt.Errorf("data volume was not retained across compose down: %w", err)
	}
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return fmt.Errorf("start encryption lifecycle stack with retained volumes: %w", err)
	}
	downUpIndex, err := recordPhase(
		"down-up",
		initial.Fingerprint,
		"using encryption master key from key file",
	)
	if err != nil {
		return fmt.Errorf("capture retained-volume encryption key: %w", err)
	}
	if evidence.Phases[downUpIndex].ContainerID == evidence.Phases[recreateIndex].ContainerID {
		return fmt.Errorf("compose down/up retained Weaver container %s", evidence.Phases[recreateIndex].ContainerID)
	}
	if err := verifyPhase("down-up", downUpIndex); err != nil {
		return fmt.Errorf("verify encrypted state after retained-volume down/up: %w", err)
	}

	imageID, err := inspectDockerContainerImageID(evidence.Phases[downUpIndex].ContainerID)
	if err != nil {
		return fmt.Errorf("resolve exact Weaver image for key fault injection: %w", err)
	}
	if err := prepareWeaverEncryptionKeyBackup(
		ctx,
		evidence.DataVolume,
		imageID,
		initial.Fingerprint,
	); err != nil {
		return fmt.Errorf("capture original Weaver encryption key: %w", err)
	}
	keyBackupPrepared := true
	defer func() {
		if !keyBackupPrepared {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if restoreErr := restoreWeaverEncryptionKey(
			cleanupCtx,
			evidence.DataVolume,
			imageID,
			initial.Fingerprint,
		); restoreErr != nil {
			err = errors.Join(err, fmt.Errorf("restore original encryption key during fault cleanup: %w", restoreErr))
		} else {
			if cleanupErr := removeWeaverEncryptionKeyBackup(
				cleanupCtx,
				evidence.DataVolume,
				imageID,
			); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("remove encryption key backup during fault cleanup: %w", cleanupErr))
			}
		}
	}()

	for _, faultSpec := range weaverEncryptionKeyFaultSpecs() {
		faultEvidence, faultErr := exerciseWeaverEncryptionKeyFault(
			ctx,
			evidence.DataVolume,
			imageID,
			initial.Fingerprint,
			faultSpec,
			artifactsDir,
		)
		evidence.Faults = append(evidence.Faults, faultEvidence)
		if writeErr := writeEvidence(); writeErr != nil {
			return writeErr
		}
		if faultErr != nil {
			return fmt.Errorf("exercise %s encryption key fault: %w", faultSpec.Stage, faultErr)
		}
	}

	finalRecoveryIndex, err := recordPhase(
		"final-recovery",
		initial.Fingerprint,
		"using encryption master key from key file",
	)
	if err != nil {
		return fmt.Errorf("capture final encryption key recovery: %w", err)
	}
	if err := verifyPhase("final-recovery", finalRecoveryIndex); err != nil {
		return fmt.Errorf("verify persisted secrets after final key recovery: %w", err)
	}
	if err := removeWeaverEncryptionKeyBackup(ctx, evidence.DataVolume, imageID); err != nil {
		return fmt.Errorf("remove encryption key lifecycle backup: %w", err)
	}
	keyBackupPrepared = false
	return nil
}

func exerciseWeaverEncryptionKeyFault(
	ctx context.Context,
	dataVolume string,
	imageID string,
	originalFingerprint string,
	spec weaverEncryptionKeyFaultSpec,
	artifactsDir string,
) (faultEvidence weaverEncryptionKeyLifecycleFault, err error) {
	faultEvidence = weaverEncryptionKeyLifecycleFault{
		Stage:              spec.Stage,
		ExpectedLogMarkers: append([]string(nil), spec.ExpectedLogMarkers...),
		LogArtifact:        "encryption-key-fault-" + spec.Stage + ".log",
	}
	if err := stopWeaverReleaseService(ctx); err != nil {
		return faultEvidence, err
	}
	if _, err := runWeaverDataVolumeShell(ctx, dataVolume, imageID, spec.MutationScript); err != nil {
		return faultEvidence, fmt.Errorf("apply key fault: %w", err)
	}

	needsRestore := true
	defer func() {
		if !needsRestore {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if stopErr := stopWeaverReleaseService(cleanupCtx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("stop Weaver before emergency key restore: %w", stopErr))
		}
		if restoreErr := restoreWeaverEncryptionKey(
			cleanupCtx,
			dataVolume,
			imageID,
			originalFingerprint,
		); restoreErr != nil {
			err = errors.Join(err, fmt.Errorf("emergency key restore: %w", restoreErr))
		} else {
			faultEvidence.OriginalRestored = true
		}
	}()

	faultStateBefore, err := inspectWeaverDataVolumeEncryptionKey(ctx, dataVolume, imageID)
	if err != nil {
		return faultEvidence, err
	}
	if err := validateWeaverEncryptionKeyFaultState(spec.Stage, originalFingerprint, faultStateBefore); err != nil {
		return faultEvidence, err
	}
	faultEvidence.FaultKeyExists = faultStateBefore.Exists
	faultEvidence.FaultKeyFingerprint = faultStateBefore.Fingerprint
	faultEvidence.FaultKeyMode = faultStateBefore.Mode

	if err := forceRecreateWeaverReleaseService(ctx); err != nil {
		return faultEvidence, err
	}
	failure, failureErr := waitForWeaverEncryptionKeyStartupFailure(
		ctx,
		spec.ExpectedLogMarkers,
		30*time.Second,
	)
	faultEvidence.FailedContainerID = failure.ContainerID
	faultEvidence.ContainerStatus = failure.Runtime.Status
	faultEvidence.HealthStatus = failure.Runtime.Health
	if failure.Logs != "" {
		logPath := filepath.Join(artifactsDir, faultEvidence.LogArtifact)
		if writeErr := os.WriteFile(logPath, []byte(failure.Logs), 0o600); writeErr != nil {
			failureErr = errors.Join(failureErr, fmt.Errorf("write failed-start logs: %w", writeErr))
		}
	}
	if failureErr != nil {
		return faultEvidence, failureErr
	}

	faultStateAfter, err := inspectWeaverDataVolumeEncryptionKey(ctx, dataVolume, imageID)
	if err != nil {
		return faultEvidence, err
	}
	if faultStateAfter != faultStateBefore {
		return faultEvidence, fmt.Errorf(
			"failed startup changed the %s key state: before=%+v after=%+v",
			spec.Stage,
			faultStateBefore,
			faultStateAfter,
		)
	}
	faultEvidence.SilentReplacementBlocked = true

	if err := stopWeaverReleaseService(ctx); err != nil {
		return faultEvidence, fmt.Errorf("stop failed Weaver before key restore: %w", err)
	}
	if err := restoreWeaverEncryptionKey(
		ctx,
		dataVolume,
		imageID,
		originalFingerprint,
	); err != nil {
		return faultEvidence, err
	}
	needsRestore = false
	faultEvidence.OriginalRestored = true

	if err := forceRecreateWeaverReleaseService(ctx); err != nil {
		return faultEvidence, fmt.Errorf("restart Weaver after key restore: %w", err)
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return faultEvidence, err
	}
	recoveryContainerID, err := dockerComposeServiceContainerID("weaver")
	if err != nil {
		return faultEvidence, err
	}
	recoveryState, err := inspectContainerEncryptionKeyState(recoveryContainerID)
	if err != nil {
		return faultEvidence, err
	}
	if recoveryState.Fingerprint != originalFingerprint || recoveryState.Mode != "600" {
		return faultEvidence, fmt.Errorf(
			"%s recovery key state = %+v, want fingerprint %s and mode 600",
			spec.Stage,
			recoveryState,
			originalFingerprint,
		)
	}
	recoveryLogs, err := dockerContainerLogs(recoveryContainerID)
	if err != nil {
		return faultEvidence, err
	}
	if !strings.Contains(recoveryLogs, "using encryption master key from key file") {
		return faultEvidence, fmt.Errorf("%s recovery did not load the restored key file", spec.Stage)
	}
	faultEvidence.RecoveryContainerID = recoveryContainerID
	faultEvidence.RecoveryFingerprint = recoveryState.Fingerprint
	faultEvidence.RecoveryMode = recoveryState.Mode
	return faultEvidence, nil
}

func validateWeaverEncryptionKeyFaultState(
	stage string,
	originalFingerprint string,
	state weaverEncryptionKeyVolumeState,
) error {
	switch stage {
	case "missing":
		if state.Exists {
			return fmt.Errorf("missing-key fault left an encryption key present")
		}
	case "corrupt":
		if !state.Exists || state.Fingerprint == originalFingerprint || state.Mode != "600" {
			return fmt.Errorf("corrupt-key fault state = %+v, want changed content with mode 600", state)
		}
	case "unreadable":
		if !state.Exists || state.Fingerprint != originalFingerprint || state.Mode != "0" {
			return fmt.Errorf("unreadable-key fault state = %+v, want original content with mode 0", state)
		}
	default:
		return fmt.Errorf("unsupported encryption key fault stage %q", stage)
	}
	return nil
}

func inspectDockerContainerImageID(containerID string) (string, error) {
	cmd := exec.Command("docker", "inspect", "-f", "{{.Image}}", containerID)
	cmd.Dir = e2eDir()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("inspect image for container %s: %w: %s", containerID, err, strings.TrimSpace(string(output)))
	}
	imageID := strings.TrimSpace(string(output))
	fingerprint := strings.TrimPrefix(imageID, "sha256:")
	if !strings.HasPrefix(imageID, "sha256:") || !encryptionKeyFingerprintPattern.MatchString(fingerprint) {
		return "", fmt.Errorf("container %s has invalid image ID %q", containerID, imageID)
	}
	return imageID, nil
}

func prepareWeaverEncryptionKeyBackup(
	ctx context.Context,
	dataVolume string,
	imageID string,
	expectedFingerprint string,
) error {
	output, err := runWeaverDataVolumeShell(
		ctx,
		dataVolume,
		imageID,
		"test -f /data/encryption.key; "+
			"test ! -e "+weaverE2EEncryptionKeyBackupPath+"; "+
			"cp -p /data/encryption.key /data/.weaver-e2e-encryption-key.staged; "+
			"chmod 600 /data/.weaver-e2e-encryption-key.staged; "+
			"mv /data/.weaver-e2e-encryption-key.staged "+weaverE2EEncryptionKeyBackupPath+"; "+
			"sha256sum "+weaverE2EEncryptionKeyBackupPath+"; "+
			"stat -c %a "+weaverE2EEncryptionKeyBackupPath,
	)
	if err != nil {
		return err
	}
	state, err := parseContainerEncryptionKeyState(outputLine(output, 0), outputLine(output, 1))
	if err != nil {
		return fmt.Errorf("inspect captured encryption key backup: %w", err)
	}
	if state.Fingerprint != expectedFingerprint || state.Mode != "600" {
		return fmt.Errorf("captured encryption key backup = %+v, want fingerprint %s and mode 600", state, expectedFingerprint)
	}
	return nil
}

func restoreWeaverEncryptionKey(
	ctx context.Context,
	dataVolume string,
	imageID string,
	expectedFingerprint string,
) error {
	output, err := runWeaverDataVolumeShell(
		ctx,
		dataVolume,
		imageID,
		"test -f "+weaverE2EEncryptionKeyBackupPath+"; "+
			"cp -p "+weaverE2EEncryptionKeyBackupPath+" /data/.weaver-e2e-encryption-key.restore; "+
			"chmod 600 /data/.weaver-e2e-encryption-key.restore; "+
			"mv -f /data/.weaver-e2e-encryption-key.restore /data/encryption.key; "+
			"sha256sum /data/encryption.key; "+
			"stat -c %a /data/encryption.key",
	)
	if err != nil {
		return err
	}
	state, err := parseContainerEncryptionKeyState(outputLine(output, 0), outputLine(output, 1))
	if err != nil {
		return fmt.Errorf("inspect restored encryption key: %w", err)
	}
	if state.Fingerprint != expectedFingerprint || state.Mode != "600" {
		return fmt.Errorf("restored encryption key = %+v, want fingerprint %s and mode 600", state, expectedFingerprint)
	}
	return nil
}

func removeWeaverEncryptionKeyBackup(ctx context.Context, dataVolume, imageID string) error {
	_, err := runWeaverDataVolumeShell(
		ctx,
		dataVolume,
		imageID,
		"rm -f "+weaverE2EEncryptionKeyBackupPath+" "+
			"/data/.weaver-e2e-encryption-key.staged "+
			"/data/.weaver-e2e-encryption-key.restore",
	)
	return err
}

func inspectWeaverDataVolumeEncryptionKey(
	ctx context.Context,
	dataVolume string,
	imageID string,
) (weaverEncryptionKeyVolumeState, error) {
	output, err := runWeaverDataVolumeShell(
		ctx,
		dataVolume,
		imageID,
		"if [ ! -e /data/encryption.key ]; then printf 'missing\\n'; exit 0; fi; "+
			"printf 'present\\n'; "+
			"sha256sum /data/encryption.key; "+
			"stat -c %a /data/encryption.key",
	)
	if err != nil {
		return weaverEncryptionKeyVolumeState{}, err
	}
	return parseWeaverEncryptionKeyVolumeState(output)
}

func parseWeaverEncryptionKeyVolumeState(output string) (weaverEncryptionKeyVolumeState, error) {
	lines := nonemptyLines(output)
	if len(lines) == 1 && lines[0] == "missing" {
		return weaverEncryptionKeyVolumeState{}, nil
	}
	if len(lines) != 3 || lines[0] != "present" {
		return weaverEncryptionKeyVolumeState{}, fmt.Errorf("invalid encryption key volume state %q", output)
	}
	state, err := parseContainerEncryptionKeyState(lines[1], lines[2])
	if err != nil {
		return weaverEncryptionKeyVolumeState{}, err
	}
	return weaverEncryptionKeyVolumeState{
		Exists:      true,
		Fingerprint: state.Fingerprint,
		Mode:        state.Mode,
	}, nil
}

func runWeaverDataVolumeShell(
	ctx context.Context,
	dataVolume string,
	imageID string,
	script string,
) (string, error) {
	expectedVolume := composeProject() + "_weaver-data"
	if dataVolume != expectedVolume {
		return "", fmt.Errorf("refusing data-volume command for %q; exact flow volume is %q", dataVolume, expectedVolume)
	}
	if err := inspectDockerVolume(dataVolume); err != nil {
		return "", err
	}
	args := []string{
		"run",
		"--rm",
		"--network",
		"none",
		"--mount",
		"type=volume,source=" + dataVolume + ",target=/data",
		"--entrypoint",
		"/bin/sh",
		imageID,
		"-eu",
		"-c",
		script,
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = e2eDir()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("run exact Weaver data-volume helper: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func stopWeaverReleaseService(ctx context.Context) error {
	args := dockerComposeArgs("stop", "--timeout", "10", "weaver")
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "stop exact Weaver release service")
}

func waitForWeaverEncryptionKeyStartupFailure(
	ctx context.Context,
	expectedLogMarkers []string,
	timeout time.Duration,
) (weaverEncryptionKeyStartupFailure, error) {
	deadline := time.Now().Add(timeout)
	var last weaverEncryptionKeyStartupFailure
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		containerID, err := dockerComposeServiceContainerIDIncludingStopped("weaver")
		if err == nil {
			runtimeState, stateErr := inspectDockerContainerRuntimeState(containerID)
			logs, logsErr := dockerContainerLogs(containerID)
			if stateErr == nil && logsErr == nil {
				last = weaverEncryptionKeyStartupFailure{
					ContainerID: containerID,
					Runtime:     runtimeState,
					Logs:        logs,
				}
				if runtimeState.Status == "running" && runtimeState.Health == "healthy" {
					return last, fmt.Errorf("Weaver became healthy with a faulted encryption key")
				}
				if runtimeState.Status == "exited" ||
					runtimeState.Status == "dead" ||
					runtimeState.Health == "unhealthy" {
					lowerLogs := strings.ToLower(logs)
					for _, marker := range expectedLogMarkers {
						if !strings.Contains(lowerLogs, strings.ToLower(marker)) {
							return last, fmt.Errorf(
								"failed Weaver startup logs do not contain %q",
								marker,
							)
						}
					}
					return last, nil
				}
			}
		}
		if err := sleepWithSuspendDetection(time.Second, "waiting for fail-closed Weaver startup"); err != nil {
			return last, err
		}
	}
	return last, fmt.Errorf(
		"timeout waiting for Weaver to fail closed; last runtime state=%+v",
		last.Runtime,
	)
}

func dockerComposeServiceContainerIDIncludingStopped(service string) (string, error) {
	cmd := exec.Command("docker", dockerComposeArgs("ps", "-a", "-q", service)...)
	cmd.Dir = e2eDir()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("resolve stopped container for service %s: %w: %s", service, err, strings.TrimSpace(string(output)))
	}
	ids := strings.Fields(string(output))
	if len(ids) != 1 {
		return "", fmt.Errorf("service %s resolved %d containers, want exactly one", service, len(ids))
	}
	return ids[0], nil
}

func inspectDockerContainerRuntimeState(containerID string) (weaverContainerRuntimeState, error) {
	cmd := exec.Command(
		"docker",
		"inspect",
		"-f",
		"{{.State.Status}}\t{{if .State.Health}}{{.State.Health.Status}}{{end}}",
		containerID,
	)
	cmd.Dir = e2eDir()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return weaverContainerRuntimeState{}, fmt.Errorf("inspect container runtime state %s: %w: %s", containerID, err, strings.TrimSpace(string(output)))
	}
	return parseWeaverContainerRuntimeState(string(output))
}

func parseWeaverContainerRuntimeState(output string) (weaverContainerRuntimeState, error) {
	parts := strings.SplitN(strings.TrimSpace(output), "\t", 2)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return weaverContainerRuntimeState{}, fmt.Errorf("empty container runtime state")
	}
	state := weaverContainerRuntimeState{Status: strings.TrimSpace(parts[0])}
	if len(parts) == 2 {
		state.Health = strings.TrimSpace(parts[1])
	}
	return state, nil
}

func nonemptyLines(output string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

func outputLine(output string, index int) string {
	lines := nonemptyLines(output)
	if index < 0 || index >= len(lines) {
		return ""
	}
	return lines[index]
}

func forceRecreateWeaverReleaseService(ctx context.Context) error {
	args := dockerComposeArgs("up", "-d", "--quiet-pull", "--force-recreate", "--no-deps", "weaver")
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "force-recreate Weaver release service")
}

func dockerComposeDownRetainingVolumes() error {
	cmd := exec.Command("docker", dockerComposeArgs("down", "--remove-orphans")...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "docker compose down retaining volumes")
}

func inspectDockerVolume(name string) error {
	cmd := exec.Command("docker", "volume", "inspect", "--format", "{{.Name}}", name)
	cmd.Dir = e2eDir()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect Docker volume %s: %w: %s", name, err, strings.TrimSpace(string(output)))
	}
	if actual := strings.TrimSpace(string(output)); actual != name {
		return fmt.Errorf("inspected Docker volume = %q, want %q", actual, name)
	}
	return nil
}

const (
	weaverReleasePlaywrightDefaultImage    = "weaver-e2e-playwright:local"
	weaverPlaywrightImageFingerprintLabel  = "org.weaver-e2e.playwright-source-fingerprint"
	weaverPlaywrightImageFingerprintSchema = "weaver-e2e-playwright-image-v1"
)

var weaverPlaywrightImageMu sync.Mutex

func runWeaverPlaywrightAudit() error {
	return runWeaverPlaywrightAuditWith(runExternalCommand)
}

func runWeaverPlaywrightAuditWith(run func(*exec.Cmd, string) error) error {
	cmd := exec.Command("npm", "run", "audit")
	cmd.Dir = filepath.Join(e2eDir(), "playwright-weaver")
	return run(cmd, "npm run audit (Weaver Playwright)")
}

func ensureLocalWeaverPlaywrightImage() error {
	weaverPlaywrightImageMu.Lock()
	defer weaverPlaywrightImageMu.Unlock()

	image := strings.TrimSpace(os.Getenv("E2E_WEAVER_PLAYWRIGHT_IMAGE"))
	if image == "" {
		image = weaverReleasePlaywrightDefaultImage
		setEnv("E2E_WEAVER_PLAYWRIGHT_IMAGE", image)
	}
	fingerprint, err := weaverPlaywrightImageFingerprint(filepath.Join(e2eDir(), "playwright-weaver"))
	if err != nil {
		return err
	}
	setEnv("E2E_WEAVER_PLAYWRIGHT_SOURCE_FINGERPRINT", fingerprint)
	if !envBool("E2E_FORCE_REBUILD_WEAVER_PLAYWRIGHT_IMAGE", false) &&
		dockerImageLabel(image, weaverPlaywrightImageFingerprintLabel) == fingerprint {
		log.Printf("reusing current Weaver Playwright image: %s (source fingerprint %s)", image, shortFingerprint(fingerprint))
		return nil
	}
	cmd := exec.Command("docker", dockerComposeArgs("build", "weaver-playwright")...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "docker compose build weaver-playwright")
}

func weaverPlaywrightImageFingerprint(root string) (string, error) {
	digest := sha256.New()
	writeFingerprintField(digest, weaverPlaywrightImageFingerprintSchema)
	matcher, err := dockerIgnoreMatcher(root)
	if err != nil {
		return "", err
	}
	if err := hashBuildContext(digest, "playwright-weaver", root, matcher, nil); err != nil {
		return "", fmt.Errorf("fingerprint Weaver Playwright build context %s: %w", root, err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func runWeaverReleasePlaywright(ctx context.Context, script string) error {
	err := runWeaverReleasePlaywrightOnce(ctx, script)
	if err == nil || !isWeaverBrowserCrash(err) {
		return err
	}
	log.Printf("browser infrastructure failed for %s; retrying once: %v", script, err)
	return runWeaverReleasePlaywrightOnce(ctx, script)
}

func runWeaverReleasePlaywrightOnce(ctx context.Context, script string) error {
	args := append(
		dockerComposeArgs("run", "--rm", "--no-deps", "weaver-playwright"),
		"npm", "run", "test:"+script,
	)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "docker compose run weaver-playwright "+script)
}

func isWeaverBrowserCrash(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"browser has been closed",
		"browser closed unexpectedly",
		"browser process exited",
		"target page, context or browser has been closed",
		"failed to launch browser",
		"browser crashed",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

type weaverReleaseNntpConnections struct {
	Attempted       int64 `json:"attempted"`
	Accepted        int64 `json:"accepted"`
	Rejected        int64 `json:"rejected"`
	Active          int64 `json:"active"`
	PeakActive      int64 `json:"peak_active"`
	ConfiguredLimit int   `json:"configured_limit"`
}

func captureWeaverReleaseStageDiagnostics(stage string) error {
	artifactsDir := strings.TrimSpace(os.Getenv("E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR"))
	if artifactsDir == "" {
		return errors.New("E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR is required")
	}
	stage = strings.Trim(strings.TrimSpace(stage), "/")
	if stage == "" || stage == "." || strings.Contains(stage, "..") {
		return fmt.Errorf("invalid Weaver release artifact stage %q", stage)
	}
	dir := filepath.Join(absolutePath(artifactsDir), stage)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return captureWeaverReleaseDiagnostics(dir)
}

func captureWeaverReleaseDiagnostics(dir string) error {
	var errs []error
	if err := captureDockerComposeOutput(filepath.Join(dir, "docker-compose.log"), "logs", "--no-color", "--timestamps"); err != nil {
		errs = append(errs, err)
	}
	if err := captureDockerComposeOutput(filepath.Join(dir, "docker-compose.ps.txt"), "ps", "-a"); err != nil {
		errs = append(errs, err)
	}
	basePath := strings.Trim(strings.TrimSpace(os.Getenv("E2E_WEAVER_BASE_URL")), "/")
	metricsURL := strings.TrimRight(defaultWeaverURL(), "/")
	if basePath != "" {
		metricsURL += "/" + basePath
	}
	metricsURL += "/metrics"
	client := &http.Client{Timeout: 5 * time.Second}
	if response, err := client.Get(metricsURL); err == nil {
		defer response.Body.Close()
		if body, readErr := io.ReadAll(response.Body); readErr == nil {
			if writeErr := os.WriteFile(filepath.Join(dir, "weaver-metrics.prom"), body, 0o644); writeErr != nil {
				errs = append(errs, writeErr)
			}
		}
	}
	evidence := struct {
		Body        restartNntpMetrics           `json:"body"`
		Stat        restartNntpMetrics           `json:"stat"`
		Connections weaverReleaseNntpConnections `json:"connections"`
	}{}
	if body, err := fetchNntpBodyMetricsFrom(nntpHost(), nntpPort(), ""); err != nil {
		errs = append(errs, err)
	} else {
		evidence.Body = body
	}
	if stat, err := fetchNntpStatMetricsFrom(nntpHost(), nntpPort(), ""); err != nil {
		errs = append(errs, err)
	} else {
		evidence.Stat = stat
	}
	if response, err := sendNntpCommandToWithRetry(nntpHost(), nntpPort(), "METRICS CONNECTIONS", 3); err != nil {
		errs = append(errs, err)
	} else if !strings.HasPrefix(response, "290 ") {
		errs = append(errs, fmt.Errorf("unexpected NNTP connection metrics response: %s", response))
	} else if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(response, "290 "))), &evidence.Connections); err != nil {
		errs = append(errs, err)
	}
	if err := writeWeaverReleaseJSON(filepath.Join(dir, "fake-nntp-counters.json"), evidence); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func captureWeaverReleaseDiagnosticsWithMarker(dir string) error {
	evidenceErr := captureWeaverReleaseDiagnostics(dir)
	marker := map[string]any{
		"captured_at": time.Now().UTC(),
		"status":      "passed",
	}
	if evidenceErr != nil {
		marker["status"] = "failed"
		marker["error"] = evidenceErr.Error()
	}
	markerErr := writeWeaverReleaseJSON(filepath.Join(dir, weaverReleaseDiagnosticsMarker), marker)
	return errors.Join(evidenceErr, markerErr)
}

func newWeaverReleaseGateRunDir() (string, error) {
	root := weaverReleaseGateRoot()
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	runDir := filepath.Join(root, time.Now().UTC().Format("20060102-150405.000000000"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", err
	}
	return runDir, nil
}

func weaverReleaseGateRoot() string {
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_RELEASE_GATE_ROOT")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join("/tmp", "weaver-e2e-release-gate")
}

func writeWeaverReleaseLatestPointer(runDir string) error {
	return writeWeaverReleaseJSON(filepath.Join(weaverReleaseGateRoot(), "latest.json"), map[string]string{
		"run_dir": runDir,
	})
}

func writeWeaverReleaseJSON(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func printWeaverReleaseSummary(results []weaverReleasePhaseResult, runDir string) {
	fmt.Println("\nWEAVER RELEASE GATE")
	for _, result := range results {
		status := "PASS"
		if result.Err != nil {
			status = "FAIL"
		}
		fmt.Printf("  %-4s %-42s %-8s %s\n", status, result.Phase.Flow, result.Phase.Datastore, result.Duration.Round(time.Second))
	}
	fmt.Printf("  artifacts: %s\n", runDir)
}

func cmdWeaverReleaseConsole(args []string) {
	target := "latest"
	if len(args) > 0 && strings.TrimSpace(args[0]) != "" {
		target = args[0]
	}
	runDir, err := resolveWeaverReleaseConsoleRunDir(target)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	url := "http://" + listener.Addr().String() + "/"
	log.Printf("Weaver release console: %s", url)
	server := &http.Server{Handler: weaverReleaseConsoleHandler(runDir)}
	go func() {
		_ = server.Serve(listener)
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func resolveWeaverReleaseConsoleRunDir(target string) (string, error) {
	if target != "latest" {
		path := absolutePath(target)
		if _, err := os.Stat(filepath.Join(path, "release-gate.json")); err != nil {
			return "", fmt.Errorf("open Weaver release run %s: %w", path, err)
		}
		return path, nil
	}
	body, err := os.ReadFile(filepath.Join(weaverReleaseGateRoot(), "latest.json"))
	if err != nil {
		return "", err
	}
	var pointer struct {
		RunDir string `json:"run_dir"`
	}
	if err := json.Unmarshal(body, &pointer); err != nil {
		return "", err
	}
	if pointer.RunDir == "" {
		return "", errors.New("latest Weaver release run pointer is empty")
	}
	return pointer.RunDir, nil
}

func weaverReleaseConsoleHandler(runDir string) http.Handler {
	files := http.FileServer(http.Dir(runDir))
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			files.ServeHTTP(w, request)
			return
		}
		manifestPath := filepath.Join(runDir, "release-gate.json")
		body, err := os.ReadFile(manifestPath)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var manifest weaverReleaseManifest
		if err := json.Unmarshal(body, &manifest); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		writer := bufio.NewWriter(w)
		_, _ = fmt.Fprintf(writer, "<!doctype html><meta charset=utf-8><title>Weaver release gate</title><h1>Weaver release gate: %s</h1><p>%s</p><ul>", manifest.Status, manifest.Mode)
		for _, phase := range manifest.Phases {
			relative, _ := filepath.Rel(runDir, phase.RootDir)
			_, _ = fmt.Fprintf(writer, `<li><a href="/%s/">%s / %s</a></li>`, filepath.ToSlash(relative), phase.Flow, phase.Datastore)
		}
		_, _ = io.WriteString(writer, "</ul>")
		_ = writer.Flush()
	})
}
