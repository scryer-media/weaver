package clientadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// Run starts one clean client container and executes either a single cold NZB
// or one uninterrupted queue suite. The neutral runner independently verifies
// all output hashes in both modes.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := prepareRarparToolchain(cfg); err != nil {
		return err
	}
	if cfg.QueueInput != nil {
		return runQueue(ctx, cfg)
	}
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		return err
	}
	if err := writeProductFiles(cfg, spec); err != nil {
		return err
	}
	container, err := startContainer(ctx, cfg, spec)
	if err != nil {
		return err
	}
	defer container.cleanup()

	// Start both product-neutral counters immediately after the fresh client
	// container is created. This deliberately includes client startup for every
	// product; wall time remains the queue-acceptance-to-terminal boundary.
	cpu := startCPUSampler(ctx, container.docker, container.name)
	instructions := startInstructionRecorder(ctx, cfg, container)
	metricsCollected := false
	defer func() {
		if !metricsCollected {
			// A failed queue or poll must not leave a host perf process behind.
			_ = instructions.finish()
		}
	}()

	clientVersion := container.docker.imageVersion(ctx, cfg.Image)
	var queuedAt, completionAt time.Time
	if spec.ExposeAPI {
		if err := container.resolveEndpoint(ctx, spec.APIPort); err != nil {
			return fmt.Errorf("resolve %s client API endpoint: %w", cfg.Client, err)
		}
		api, err := newProductAPI(cfg, container.endpoint)
		if err != nil {
			return err
		}
		startupCtx, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
		readyVersion, err := waitUntilReady(startupCtx, cfg.PollInterval, api)
		cancelStartup()
		if err != nil {
			return fmt.Errorf("wait for %s readiness: %w", cfg.Client, err)
		}
		if readyVersion != "" {
			clientVersion = readyVersion
		}
		jobID, err := api.queue(ctx, cfg.NZBPath, cfg.ArchivePassword, queueOptions{})
		if err != nil {
			return err
		}
		queuedAt = time.Now().UTC()
		completionAt, err = api.waitComplete(ctx, jobID, cfg.PollInterval)
		if err != nil {
			return err
		}
	} else {
		queuedAt, completionAt, err = waitForWeaverCLIReport(ctx, cfg, container, spec.CompletionReportName)
		if err != nil {
			return err
		}
	}
	telemetryCtx, cancelTelemetry := context.WithTimeout(context.Background(), 15*time.Second)
	cpuMeasurement := cpu.finish(telemetryCtx)
	cancelTelemetry()
	instructionMeasurement := instructions.finish()
	if raw := strings.TrimSpace(instructions.output.String()); raw != "" {
		if err := writeNewFile(filepath.Join(cfg.ConfigDir, "perf-instructions.txt"), []byte(raw+"\n")); err != nil {
			return fmt.Errorf("write perf instruction artifact: %w", err)
		}
	}
	metricsCollected = true
	if spec.CompletionAckName != "" {
		if err := writeNewFile(filepath.Join(cfg.ConfigDir, spec.CompletionAckName), nil); err != nil {
			return fmt.Errorf("acknowledge %s terminal report: %w", cfg.Client, err)
		}
	}
	result := benchmark.AdapterResult{
		SchemaVersion:            5,
		RunID:                    cfg.RunID,
		Client:                   cfg.Client,
		ArchiveToolchain:         cfg.ArchiveToolchain,
		ArchiveToolchainIdentity: cfg.archiveToolchainIdentity(),
		ExecutionTarget:          cfg.ExecutionTarget,
		Transport:                cfg.Transport,
		TLSValidation:            cfg.TLSValidation,
		TransportLabel:           cfg.TransportLabel,
		ServerLink:               cfg.ServerLink,
		QueuedAt:                 queuedAt,
		CompletionAt:             completionAt,
		ClientIdentity:           cfg.Image,
		ClientVersion:            clientVersion,
		RenderedConfigSHA256:     spec.ConfigSHA256,
		ResourceMetrics: benchmark.ResourceMetrics{
			CPUTimeNanoseconds:  cpuMeasurement,
			InstructionsRetired: instructionMeasurement,
		},
	}
	if err := result.ValidateFor(benchmark.Run{
		ID:               cfg.RunID,
		Client:           cfg.Client,
		ArchiveToolchain: cfg.ArchiveToolchain,
		ExecutionTarget:  cfg.ExecutionTarget,
		Transport:        cfg.Transport,
		TLSValidation:    cfg.TLSValidation,
		TransportLabel:   cfg.TransportLabel,
		ServerLink:       cfg.ServerLink,
	}); err != nil {
		return fmt.Errorf("validate adapter result: %w", err)
	}
	if err := writeResult(cfg.ResultPath, result); err != nil {
		return err
	}
	return nil
}

type weaverCLITerminalReport struct {
	SchemaVersion int       `json:"schema_version"`
	QueuedAt      time.Time `json:"queued_at"`
	CompletionAt  time.Time `json:"completion_at"`
	Status        string    `json:"status"`
}

func waitForWeaverCLIReport(ctx context.Context, cfg Config, container *runningContainer, reportName string) (time.Time, time.Time, error) {
	if reportName == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("%s CLI adapter did not declare a completion report", cfg.Client)
	}
	reportPath := filepath.Join(cfg.ConfigDir, reportName)
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	var lastReportError error
	for {
		contents, err := os.ReadFile(reportPath)
		switch {
		case err == nil:
			var report weaverCLITerminalReport
			if decodeErr := json.Unmarshal(contents, &report); decodeErr != nil {
				lastReportError = fmt.Errorf("decode Weaver CLI report: %w", decodeErr)
			} else if validationErr := validateWeaverCLIReport(report); validationErr != nil {
				lastReportError = validationErr
			} else {
				return report.QueuedAt, report.CompletionAt, nil
			}
		case os.IsNotExist(err):
			lastReportError = nil
		default:
			return time.Time{}, time.Time{}, fmt.Errorf("read Weaver CLI report: %w", err)
		}

		running, stateErr := container.docker.containerRunning(ctx, container.name)
		if stateErr != nil {
			return time.Time{}, time.Time{}, stateErr
		}
		if !running {
			if lastReportError != nil {
				return time.Time{}, time.Time{}, fmt.Errorf("Weaver CLI exited before writing a valid terminal report: %w", lastReportError)
			}
			return time.Time{}, time.Time{}, fmt.Errorf("Weaver CLI exited without a terminal report")
		}

		select {
		case <-ctx.Done():
			return time.Time{}, time.Time{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func validateWeaverCLIReport(report weaverCLITerminalReport) error {
	if report.SchemaVersion != 1 {
		return fmt.Errorf("unsupported Weaver CLI report schema %d", report.SchemaVersion)
	}
	if report.Status != "complete" {
		return fmt.Errorf("Weaver CLI reported unexpected terminal status %q", report.Status)
	}
	if report.QueuedAt.IsZero() || report.CompletionAt.IsZero() {
		return fmt.Errorf("Weaver CLI report has an empty timestamp")
	}
	if report.CompletionAt.Before(report.QueuedAt) {
		return fmt.Errorf("Weaver CLI report completes before it queues")
	}
	return nil
}

func writeProductFiles(cfg Config, spec ProductSpec) error {
	directories := []string{cfg.ConfigDir, cfg.OutputDir, filepath.Join(cfg.ConfigDir, "incomplete")}
	if cfg.Client == benchmark.NZBGet {
		// NZBGet requires its queue and working directories to exist at
		// startup; native and Docker runs must receive the same clean layout.
		for _, name := range []string{"nzb", "queue", "tmp", "scripts"} {
			directories = append(directories, filepath.Join(cfg.ConfigDir, name))
		}
	}
	for _, directory := range directories {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("create client run directory %s: %w", directory, err)
		}
	}
	if err := writeNewFile(filepath.Join(cfg.ConfigDir, spec.ConfigName), spec.ConfigContent); err != nil {
		return fmt.Errorf("write %s config: %w", cfg.Client, err)
	}
	if err := writeNewFile(filepath.Join(cfg.ConfigDir, "rendered-config.txt"), spec.Rendered); err != nil {
		return fmt.Errorf("write rendered client configuration: %w", err)
	}
	for _, extra := range spec.ExtraFiles {
		if filepath.IsAbs(extra.RelativePath) {
			return fmt.Errorf("client extra file must be relative: %q", extra.RelativePath)
		}
		path := filepath.Join(cfg.ConfigDir, extra.RelativePath)
		relative, err := filepath.Rel(cfg.ConfigDir, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("client extra file escapes config directory: %q", extra.RelativePath)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create client extra file directory: %w", err)
		}
		if err := writeNewFileMode(path, extra.Content, extra.Mode); err != nil {
			return fmt.Errorf("write client extra file %s: %w", extra.RelativePath, err)
		}
	}
	return nil
}

func writeResult(path string, result benchmark.AdapterResult) error {
	contents, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode adapter result: %w", err)
	}
	contents = append(contents, '\n')
	if err := writeNewFile(path, contents); err != nil {
		return fmt.Errorf("write adapter result: %w", err)
	}
	return nil
}

func writeNewFile(path string, contents []byte) error {
	return writeNewFileMode(path, contents, 0o644)
}

func writeNewFileMode(path string, contents []byte, mode os.FileMode) error {
	// LinuxServer images commonly run clients as a non-root UID. The benchmark
	// network credentials are intentionally limited to the ephemeral lab, and
	// the rendered configuration must be readable by that client UID.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(contents); err != nil {
		return err
	}
	return nil
}

func waitUntilReady(ctx context.Context, interval time.Duration, api productAPI) (string, error) {
	var lastErr error
	for {
		version, err := api.waitReady(ctx)
		if err == nil {
			return version, nil
		}
		lastErr = err
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return "", fmt.Errorf("%w (last readiness error: %v)", ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}

func waitUntilContainerReady(ctx context.Context, interval time.Duration, api productAPI, container *runningContainer) (string, error) {
	var lastErr error
	for {
		version, err := api.waitReady(ctx)
		if err == nil {
			return version, nil
		}
		lastErr = err
		running, stateErr := container.docker.containerRunning(ctx, container.name)
		if stateErr != nil {
			return "", fmt.Errorf("inspect %s client during readiness: %w", container.name, stateErr)
		}
		if !running {
			return "", fmt.Errorf("%s client container exited before readiness: %w", container.name, lastErr)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return "", fmt.Errorf("%w (last readiness error: %v)", ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}
