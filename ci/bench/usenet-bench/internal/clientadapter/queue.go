package clientadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// runQueue keeps one product process alive while every planned NZB is queued.
// Jobs are submitted before any terminal-state wait begins; this intentionally
// retains each product's own queueing, scheduling, and transition behaviour.
func runQueue(ctx context.Context, cfg Config) error {
	input := cfg.QueueInput
	if input == nil {
		return fmt.Errorf("queue adapter requires queue input")
	}
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		return err
	}
	if !spec.ExposeAPI {
		return fmt.Errorf("%s queue adapter requires a public control API", cfg.Client)
	}
	if err := writeProductFiles(cfg, spec); err != nil {
		return err
	}
	container, err := startContainer(ctx, cfg, spec)
	if err != nil {
		return err
	}
	defer container.cleanup()

	cpu := startCPUSampler(ctx, container.docker, container.name)
	instructions := startInstructionRecorder(ctx, cfg, container)
	metricsCollected := false
	defer func() {
		if !metricsCollected {
			_ = instructions.finish()
		}
	}()

	if err := container.resolveEndpoint(ctx, spec.APIPort); err != nil {
		return fmt.Errorf("resolve %s client API endpoint: %w", cfg.Client, err)
	}
	api, err := newProductAPI(cfg, container.endpoint)
	if err != nil {
		return err
	}
	clientVersion := container.docker.imageVersion(ctx, cfg.Image)
	startupCtx, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
	readyVersion, err := waitUntilContainerReady(startupCtx, cfg.PollInterval, api, container)
	cancelStartup()
	if err != nil {
		return fmt.Errorf("wait for %s readiness: %w", cfg.Client, err)
	}
	if readyVersion != "" {
		clientVersion = readyVersion
	}

	jobs := make([]benchmark.QueueJobResult, 0, len(input.Jobs))
	for _, inputJob := range input.Jobs {
		jobID, err := api.queue(ctx, inputJob.NZBPath, inputJob.ArchivePassword)
		if err != nil {
			return fmt.Errorf("queue %s: %w", inputJob.RunID, err)
		}
		jobs = append(jobs, benchmark.QueueJobResult{
			RunID:    inputJob.RunID,
			JobID:    jobID,
			QueuedAt: time.Now().UTC(),
		})
	}
	queueStartedAt := jobs[0].QueuedAt

	type terminalResult struct {
		index        int
		completionAt time.Time
		err          error
	}
	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()
	terminals := make(chan terminalResult, len(jobs))
	for index := range jobs {
		job := jobs[index]
		go func(index int, job benchmark.QueueJobResult) {
			completionAt, err := api.waitComplete(waitCtx, job.JobID, cfg.PollInterval)
			terminals <- terminalResult{index: index, completionAt: completionAt, err: err}
		}(index, job)
	}
	var waitErr error
	var queueCompletedAt time.Time
	for range jobs {
		terminal := <-terminals
		if terminal.err != nil {
			if waitErr == nil {
				waitErr = terminal.err
				cancelWait()
			}
			continue
		}
		jobs[terminal.index].CompletionAt = terminal.completionAt
		if terminal.completionAt.After(queueCompletedAt) {
			queueCompletedAt = terminal.completionAt
		}
	}
	if waitErr != nil {
		return fmt.Errorf("wait for queue terminal state: %w", waitErr)
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
	result := benchmark.QueueAdapterResult{
		SchemaVersion:            1,
		SuiteID:                  input.SuiteID,
		Client:                   cfg.Client,
		ArchiveToolchain:         cfg.ArchiveToolchain,
		ArchiveToolchainIdentity: cfg.archiveToolchainIdentity(),
		ExecutionTarget:          cfg.ExecutionTarget,
		Transport:                cfg.Transport,
		TLSValidation:            cfg.TLSValidation,
		TransportLabel:           cfg.TransportLabel,
		ServerLink:               cfg.ServerLink,
		QueueStartedAt:           queueStartedAt,
		QueueCompletedAt:         queueCompletedAt,
		Jobs:                     jobs,
		ClientIdentity:           cfg.Image,
		ClientVersion:            clientVersion,
		RenderedConfigSHA256:     spec.ConfigSHA256,
		ResourceMetrics: benchmark.ResourceMetrics{
			CPUTimeNanoseconds:  cpuMeasurement,
			InstructionsRetired: instructionMeasurement,
		},
	}
	if err := result.ResourceMetrics.Validate(); err != nil {
		return fmt.Errorf("validate queue resource metrics: %w", err)
	}
	if err := writeQueueResult(cfg.ResultPath, result); err != nil {
		return err
	}
	return nil
}

func writeQueueResult(path string, result benchmark.QueueAdapterResult) error {
	contents, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode queue adapter result: %w", err)
	}
	contents = append(contents, '\n')
	if err := writeNewFile(path, contents); err != nil {
		return fmt.Errorf("write queue adapter result: %w", err)
	}
	return nil
}
