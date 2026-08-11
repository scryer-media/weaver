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

	cpu := cpuSampler{docker: container.docker, name: container.name, reason: "suite-level telemetry is not reported for this submission mode"}
	instructions := unavailableInstructionRecorder("suite-level retired instructions are not reported for this submission mode")
	if input.SubmissionMode == benchmark.SubmissionModeQueued {
		cpu = startCPUSampler(ctx, container.docker, container.name)
		instructions = startInstructionRecorder(ctx, cfg, container)
	}
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

	var jobs []benchmark.QueueJobResult
	if input.SubmissionMode == benchmark.SubmissionModeSequential {
		jobs, err = runSequentialSubmission(ctx, api, cfg.PollInterval, input.Jobs, cpuSampler{docker: container.docker, name: container.name}, cfg, container)
	} else {
		jobs, err = runQueuedSubmission(ctx, api, cfg.PollInterval, input.Jobs)
	}
	if err != nil {
		return err
	}
	queueStartedAt := jobs[0].QueuedAt
	queueCompletedAt := jobs[0].CompletionAt
	for _, job := range jobs[1:] {
		if job.CompletionAt.After(queueCompletedAt) {
			queueCompletedAt = job.CompletionAt
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
	result := benchmark.QueueAdapterResult{
		SchemaVersion:            5,
		SuiteID:                  input.SuiteID,
		SubmissionMode:           input.SubmissionMode,
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
		StatusPollIntervalNanos:  cfg.PollInterval.Nanoseconds(),
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

func runQueuedSubmission(ctx context.Context, api productAPI, interval time.Duration, inputJobs []benchmark.QueueInputJob) ([]benchmark.QueueJobResult, error) {
	monitorCtx, cancelMonitor := context.WithCancel(ctx)
	defer cancelMonitor()
	registrations := make(chan queuedJob, len(inputJobs))
	monitorResult := make(chan queueMonitorResult, 1)
	go func() {
		jobs, err := monitorQueue(monitorCtx, api, interval, registrations)
		monitorResult <- queueMonitorResult{jobs: jobs, err: err}
	}()
	for _, inputJob := range inputJobs {
		submissionStartedAt := time.Now()
		jobID, err := api.queue(ctx, inputJob.NZBPath, inputJob.ArchivePassword, queueOptions{
			submissionName: inputJob.SubmissionName,
			forceAccept:    inputJob.ForceAccept,
		})
		if err != nil {
			cancelMonitor()
			return nil, fmt.Errorf("queue %s: %w", inputJob.RunID, err)
		}
		acceptedAt := time.Now()
		registrations <- queuedJob{result: benchmark.QueueJobResult{
			RunID:               inputJob.RunID,
			JobID:               jobID,
			SubmissionStartedAt: submissionStartedAt,
			AcceptedAt:          acceptedAt,
			QueuedAt:            acceptedAt,
		}}
	}
	close(registrations)
	monitored := <-monitorResult
	if monitored.err != nil {
		return nil, fmt.Errorf("monitor queue lifecycle: %w", monitored.err)
	}
	return monitored.jobs, nil
}

func runSequentialSubmission(ctx context.Context, api productAPI, interval time.Duration, inputJobs []benchmark.QueueInputJob, cpu cpuSampler, cfg Config, container *runningContainer) ([]benchmark.QueueJobResult, error) {
	jobs := make([]benchmark.QueueJobResult, 0, len(inputJobs))
	for _, inputJob := range inputJobs {
		cpuStart, cpuStartErr := cpu.read(ctx)
		instructions := startInstructionRecorder(ctx, cfg, container)
		monitorCtx, cancelMonitor := context.WithCancel(ctx)
		registrations := make(chan queuedJob, 1)
		monitorResult := make(chan queueMonitorResult, 1)
		go func() {
			observed, err := monitorQueue(monitorCtx, api, interval, registrations)
			monitorResult <- queueMonitorResult{jobs: observed, err: err}
		}()
		submissionStartedAt := time.Now()
		jobID, err := api.queue(ctx, inputJob.NZBPath, inputJob.ArchivePassword, queueOptions{
			submissionName: inputJob.SubmissionName,
			forceAccept:    inputJob.ForceAccept,
		})
		if err != nil {
			cancelMonitor()
			_ = instructions.finish()
			return nil, fmt.Errorf("queue %s: %w", inputJob.RunID, err)
		}
		acceptedAt := time.Now()
		registrations <- queuedJob{result: benchmark.QueueJobResult{
			RunID:               inputJob.RunID,
			JobID:               jobID,
			SubmissionStartedAt: submissionStartedAt,
			AcceptedAt:          acceptedAt,
			QueuedAt:            acceptedAt,
		}}
		close(registrations)
		monitored := <-monitorResult
		cancelMonitor()
		instructionMeasurement := instructions.finish()
		var cpuMeasurement benchmark.CounterMeasurement
		if cpuStartErr != nil {
			cpuMeasurement = benchmark.UnavailableMeasurement("client_container", "cgroup-cpu", "unknown", cpuStartErr.Error())
		} else {
			telemetryCtx, cancelTelemetry := context.WithTimeout(context.Background(), 15*time.Second)
			cpuMeasurement = cpu.measureFrom(telemetryCtx, cpuStart)
			cancelTelemetry()
		}
		if monitored.err != nil {
			return nil, fmt.Errorf("monitor fixture %s lifecycle: %w", inputJob.RunID, monitored.err)
		}
		if len(monitored.jobs) != 1 {
			return nil, fmt.Errorf("monitor fixture %s returned %d jobs", inputJob.RunID, len(monitored.jobs))
		}
		metrics := benchmark.ResourceMetrics{
			CPUTimeNanoseconds:  cpuMeasurement,
			InstructionsRetired: instructionMeasurement,
		}
		if err := metrics.Validate(); err != nil {
			return nil, fmt.Errorf("validate fixture %s resource metrics: %w", inputJob.RunID, err)
		}
		job := monitored.jobs[0]
		job.ResourceMetrics = &metrics
		jobs = append(jobs, job)
	}
	return jobs, nil
}

type queuedJob struct {
	result benchmark.QueueJobResult
}

type queueMonitorResult struct {
	jobs []benchmark.QueueJobResult
	err  error
}

type trackedQueueJob struct {
	result         benchmark.QueueJobResult
	lastObservedAt time.Time
	complete       bool
}

func monitorQueue(
	ctx context.Context,
	api productAPI,
	interval time.Duration,
	registrations <-chan queuedJob,
) ([]benchmark.QueueJobResult, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var jobs []*trackedQueueJob
	seenIDs := make(map[string]bool)
	registrationsOpen := true
	completed := 0

	for registrationsOpen || completed < len(jobs) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case registration, ok := <-registrations:
			if !ok {
				registrationsOpen = false
				registrations = nil
				if len(jobs) == 0 {
					return nil, fmt.Errorf("queue monitor received no jobs")
				}
				continue
			}
			if seenIDs[registration.result.JobID] {
				return nil, fmt.Errorf("queue returned duplicate job id %s", registration.result.JobID)
			}
			seenIDs[registration.result.JobID] = true
			jobs = append(jobs, &trackedQueueJob{result: registration.result, lastObservedAt: registration.result.AcceptedAt})
		case <-ticker.C:
			pendingIDs := make([]string, 0, len(jobs)-completed)
			for _, job := range jobs {
				if !job.complete {
					pendingIDs = append(pendingIDs, job.result.JobID)
				}
			}
			if len(pendingIDs) == 0 {
				continue
			}
			observations, err := api.observe(ctx, pendingIDs)
			if err != nil {
				return nil, err
			}
			observedAt := time.Now()
			for _, job := range jobs {
				if job.complete {
					continue
				}
				observation, ok := observations[job.result.JobID]
				if !ok {
					continue
				}
				switch observation.state {
				case jobUnknown:
					continue
				case jobQueued:
					job.lastObservedAt = observedAt
				case jobActive:
					if job.result.ProcessingStartedAt.IsZero() {
						job.result.ProcessingStartedAt = observedAt
					}
					job.lastObservedAt = observedAt
				case jobComplete:
					job.result.TerminalStatus = "succeeded"
					job.result.CompletionAt = observedAt
					job.result.TerminalObservationLowerBound = job.lastObservedAt
					job.result.TerminalObservedAt = observedAt
					job.result.TerminalObservationUncertainty = observedAt.Sub(job.lastObservedAt).Nanoseconds()
					job.result.SubmissionToTerminalNanoseconds = observedAt.Sub(job.result.SubmissionStartedAt).Nanoseconds()
					job.result.FixtureWallClockNanoseconds = observedAt.Sub(job.result.QueuedAt).Nanoseconds()
					finishProcessingTiming(&job.result, observedAt, observation.status)
					job.complete = true
					completed++
				case jobFailed:
					job.result.TerminalStatus = "failed"
					job.result.TerminalError = observation.status
					job.result.CompletionAt = observedAt
					job.result.TerminalObservationLowerBound = job.lastObservedAt
					job.result.TerminalObservedAt = observedAt
					job.result.TerminalObservationUncertainty = observedAt.Sub(job.lastObservedAt).Nanoseconds()
					job.result.SubmissionToTerminalNanoseconds = observedAt.Sub(job.result.SubmissionStartedAt).Nanoseconds()
					job.result.FixtureWallClockNanoseconds = observedAt.Sub(job.result.QueuedAt).Nanoseconds()
					finishProcessingTiming(&job.result, observedAt, observation.status)
					job.complete = true
					completed++
				}
			}
		}
	}

	results := make([]benchmark.QueueJobResult, len(jobs))
	for index, job := range jobs {
		results[index] = job.result
	}
	return results, nil
}

func finishProcessingTiming(result *benchmark.QueueJobResult, completionAt time.Time, terminalStatus string) {
	if result.ProcessingStartedAt.IsZero() {
		result.ProcessingTimingError = fmt.Sprintf("terminal status %q was observed before an active state; reduce CLIENT_POLL_INTERVAL", terminalStatus)
		return
	}
	result.ProcessingTimingAvailable = true
	result.ProcessingWallClockNanoseconds = completionAt.Sub(result.ProcessingStartedAt).Nanoseconds()
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
