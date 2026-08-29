package nativeadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/clientadapter"
)

// Run executes a native product through its public control API. Sequential
// queue input intentionally starts one fresh native process per fixture so
// native lanes retain the cold-run lifecycle of the direct benchmark path.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if cfg.QueueInput != nil {
		return runSequentialQueue(ctx, cfg)
	}
	nativeRun, err := runSingle(ctx, cfg)
	if err != nil {
		return err
	}
	if err := writeResult(cfg.ResultPath, nativeRun.result); err != nil {
		return err
	}
	return nil
}

type nativeRun struct {
	result              benchmark.AdapterResult
	jobID               string
	submissionStartedAt time.Time
	acceptedAt          time.Time
	terminal            clientadapter.TerminalObservation
}

func runSingle(ctx context.Context, cfg Config) (nativeRun, error) {
	spec, err := renderProduct(cfg)
	if err != nil {
		return nativeRun{}, err
	}
	if err := writeProductFiles(cfg, spec); err != nil {
		return nativeRun{}, err
	}
	identity, err := commandIdentity(spec.Command[0])
	if err != nil {
		return nativeRun{}, err
	}
	process, err := startProcess(ctx, cfg, spec)
	if err != nil {
		return nativeRun{}, err
	}
	defer process.ensureStopped()

	var queueTiming clientadapter.QueueTiming
	var completion clientadapter.TerminalObservation
	api, apiErr := clientadapter.NewAPI(cfg.Client, cfg.APIEndpoint)
	if apiErr != nil {
		err = apiErr
	} else {
		startupCtx, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
		actualVersion, readyErr := waitUntilReady(startupCtx, cfg.PollInterval, api, process)
		cancelStartup()
		if readyErr != nil {
			err = readyErr
		} else if err = reconcileAPIVersion(cfg.ClientVersion, actualVersion); err == nil {
			cfg.ClientVersion = recordedAPIVersion(cfg.ClientVersion, actualVersion)
		}
	}
	if err == nil {
		queueTiming, err = api.QueueWithTiming(ctx, cfg.NZBPath, cfg.ArchivePassword)
		if err == nil {
			completion, err = api.WaitCompleteWithObservation(ctx, queueTiming.JobID, cfg.PollInterval, queueTiming.AcceptedAt)
		}
	}
	stopErr := process.stop()
	if err == nil && stopErr != nil {
		err = stopErr
	}
	if err != nil {
		return nativeRun{}, err
	}
	result := benchmark.AdapterResult{
		SchemaVersion:            5,
		RunID:                    cfg.RunID,
		Client:                   cfg.Client,
		ArchiveToolchain:         cfg.ArchiveToolchain,
		ArchiveToolchainIdentity: "stock",
		ExecutionTarget:          cfg.ExecutionTarget,
		Transport:                cfg.Transport,
		TLSValidation:            cfg.TLSValidation,
		TransportLabel:           cfg.TransportLabel,
		ServerLink:               cfg.ServerLink,
		QueuedAt:                 queueTiming.AcceptedAt,
		CompletionAt:             completion.ObservedAt,
		ClientIdentity:           identity,
		ClientVersion:            cfg.ClientVersion,
		RenderedConfigSHA256:     spec.ConfigSHA256,
		ResourceMetrics: benchmark.ResourceMetrics{
			CPUTimeNanoseconds:  process.cpuMeasurement(),
			InstructionsRetired: nativeInstructionMeasurement(),
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
		return nativeRun{}, fmt.Errorf("validate adapter result: %w", err)
	}
	return nativeRun{
		result:              result,
		jobID:               queueTiming.JobID,
		submissionStartedAt: queueTiming.SubmissionStartedAt,
		acceptedAt:          queueTiming.AcceptedAt,
		terminal:            completion,
	}, nil
}

func runSequentialQueue(ctx context.Context, cfg Config) error {
	input := cfg.QueueInput
	if input == nil {
		return fmt.Errorf("native sequential queue requires queue input")
	}
	jobs := make([]benchmark.QueueJobResult, 0, len(input.Jobs))
	var clientIdentity, clientVersion, renderedConfigSHA256 string
	var suiteMetrics benchmark.ResourceMetrics
	var queueStartedAt, queueCompletedAt time.Time
	for index, inputJob := range input.Jobs {
		jobCfg := cfg
		jobCfg.QueueInput = nil
		jobCfg.RunID = inputJob.RunID
		jobCfg.NZBPath = inputJob.NZBPath
		jobCfg.ArchivePassword = inputJob.ArchivePassword
		jobCfg.ConfigDir = filepath.Join(cfg.ConfigDir, fmt.Sprintf("job-%03d", index+1))
		jobCfg.ResultPath = filepath.Join(jobCfg.ConfigDir, "adapter-result.json")
		nativeRun, err := runSingle(ctx, jobCfg)
		if err != nil {
			return fmt.Errorf("run native sequential job %s: %w", inputJob.RunID, err)
		}
		clientIdentity, clientVersion, renderedConfigSHA256 = nativeRun.result.ClientIdentity, nativeRun.result.ClientVersion, nativeRun.result.RenderedConfigSHA256
		suiteMetrics = nativeRun.result.ResourceMetrics
		if queueStartedAt.IsZero() {
			queueStartedAt = nativeRun.submissionStartedAt
		}
		queueCompletedAt = nativeRun.terminal.ObservedAt
		job := benchmark.QueueJobResult{
			RunID:                           inputJob.RunID,
			JobID:                           nativeRun.jobID,
			SubmissionStartedAt:             nativeRun.submissionStartedAt,
			AcceptedAt:                      nativeRun.acceptedAt,
			QueuedAt:                        nativeRun.acceptedAt,
			CompletionAt:                    nativeRun.terminal.ObservedAt,
			FixtureWallClockNanoseconds:     nativeRun.terminal.ObservedAt.Sub(nativeRun.acceptedAt).Nanoseconds(),
			TerminalStatus:                  "succeeded",
			ProcessingTimingError:           "native public API does not expose active-processing transitions",
			ResourceMetrics:                 &nativeRun.result.ResourceMetrics,
			TerminalObservationLowerBound:   nativeRun.terminal.LowerBound,
			TerminalObservedAt:              nativeRun.terminal.ObservedAt,
			TerminalObservationUncertainty:  nativeRun.terminal.ObservedAt.Sub(nativeRun.terminal.LowerBound).Nanoseconds(),
			SubmissionToTerminalNanoseconds: nativeRun.terminal.ObservedAt.Sub(nativeRun.submissionStartedAt).Nanoseconds(),
		}
		jobs = append(jobs, job)
	}
	result := benchmark.QueueAdapterResult{
		SchemaVersion:            5,
		SuiteID:                  input.SuiteID,
		SubmissionMode:           input.SubmissionMode,
		Client:                   cfg.Client,
		ArchiveToolchain:         cfg.ArchiveToolchain,
		ArchiveToolchainIdentity: "stock",
		ExecutionTarget:          cfg.ExecutionTarget,
		Transport:                cfg.Transport,
		TLSValidation:            cfg.TLSValidation,
		TransportLabel:           cfg.TransportLabel,
		ServerLink:               cfg.ServerLink,
		QueueStartedAt:           queueStartedAt,
		QueueCompletedAt:         queueCompletedAt,
		StatusPollIntervalNanos:  cfg.PollInterval.Nanoseconds(),
		Jobs:                     jobs,
		ClientIdentity:           clientIdentity,
		ClientVersion:            clientVersion,
		RenderedConfigSHA256:     renderedConfigSHA256,
		ResourceMetrics:          suiteMetrics,
	}
	if err := validateNativeSequentialQueueResult(result); err != nil {
		return err
	}
	return writeQueueResult(cfg.ResultPath, result)
}

func reconcileAPIVersion(declared, actual string) error {
	declared, actual = strings.TrimSpace(declared), strings.TrimSpace(actual)
	if actual != "" && declared != actual {
		return fmt.Errorf("NATIVE_CLIENT_VERSION %q does not match client API version %q", declared, actual)
	}
	return nil
}

func validateNativeSequentialQueueResult(result benchmark.QueueAdapterResult) error {
	if result.SubmissionMode != benchmark.SubmissionModeSequential || len(result.Jobs) != 1 {
		return fmt.Errorf("native sequential result must contain exactly one sequential job")
	}
	if result.QueueStartedAt.IsZero() || result.QueueCompletedAt.IsZero() || result.QueueCompletedAt.Before(result.QueueStartedAt) {
		return fmt.Errorf("native sequential result has invalid suite timing")
	}
	if err := result.ResourceMetrics.Validate(); err != nil {
		return fmt.Errorf("validate native queue resource metrics: %w", err)
	}
	job := result.Jobs[0]
	if job.SubmissionStartedAt.IsZero() || job.AcceptedAt.IsZero() || !job.AcceptedAt.Equal(job.QueuedAt) || job.AcceptedAt.Before(job.SubmissionStartedAt) || !result.QueueStartedAt.Equal(job.SubmissionStartedAt) || job.TerminalObservationLowerBound.IsZero() || job.TerminalObservedAt.IsZero() || !job.TerminalObservedAt.Equal(job.CompletionAt) || job.TerminalObservationLowerBound.Before(job.QueuedAt) || job.CompletionAt.Before(job.TerminalObservationLowerBound) {
		return fmt.Errorf("native sequential result has invalid public API timing")
	}
	if job.FixtureWallClockNanoseconds != job.CompletionAt.Sub(job.QueuedAt).Nanoseconds() || job.SubmissionToTerminalNanoseconds != job.TerminalObservedAt.Sub(job.SubmissionStartedAt).Nanoseconds() || job.TerminalObservationUncertainty != job.TerminalObservedAt.Sub(job.TerminalObservationLowerBound).Nanoseconds() {
		return fmt.Errorf("native sequential result has inconsistent timing durations")
	}
	if job.SubmissionToTerminalNanoseconds <= 0 || job.TerminalObservationUncertainty > job.SubmissionToTerminalNanoseconds/100 {
		return fmt.Errorf("native sequential result has terminal observation uncertainty above 1%%")
	}
	if job.ResourceMetrics == nil {
		return fmt.Errorf("native sequential result lacks fixture resource metrics")
	}
	if err := job.ResourceMetrics.Validate(); err != nil {
		return fmt.Errorf("validate native fixture resource metrics: %w", err)
	}
	return nil
}

func recordedAPIVersion(declared, actual string) string {
	if actual = strings.TrimSpace(actual); actual != "" {
		return actual
	}
	return strings.TrimSpace(declared)
}

type nativeProcess struct {
	command *exec.Cmd
	done    chan struct{}
	err     error
}

func startProcess(ctx context.Context, cfg Config, spec productSpec) (*nativeProcess, error) {
	logPath := filepath.Join(cfg.ConfigDir, "native-client.log")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create native client log: %w", err)
	}
	command := exec.CommandContext(ctx, spec.Command[0], spec.Command[1:]...)
	if cfg.WorkingDir != "" {
		command.Dir = cfg.WorkingDir
	}
	command.Stdout = logFile
	command.Stderr = logFile
	command.Env = append(os.Environ(), spec.Environment...)
	configureNativeProcess(command)
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start native %s process: %w", cfg.Client, err)
	}
	process := &nativeProcess{command: command, done: make(chan struct{})}
	go func() {
		process.err = command.Wait()
		_ = logFile.Close()
		close(process.done)
	}()
	return process, nil
}

func (process *nativeProcess) wait(ctx context.Context) error {
	select {
	case <-process.done:
		return process.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (process *nativeProcess) exited() bool {
	select {
	case <-process.done:
		return true
	default:
		return false
	}
}

func (process *nativeProcess) stop() error {
	if process.exited() {
		return nil
	}
	if err := interruptNativeProcessTree(process.command.Process); err != nil && !process.exited() {
		return fmt.Errorf("request native process-tree shutdown: %w", err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := process.wait(stopCtx); err == nil || process.exited() {
		return nil
	}
	if err := killNativeProcessTree(process.command.Process); err != nil && !process.exited() {
		return fmt.Errorf("force-stop native process tree: %w", err)
	}
	_ = process.wait(stopCtx)
	return nil
}

func (process *nativeProcess) ensureStopped() {
	if process == nil || process.exited() {
		return
	}
	_ = killNativeProcessTree(process.command.Process)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = process.wait(ctx)
}

func (process *nativeProcess) cpuMeasurement() benchmark.CounterMeasurement {
	const collector = "go-os-process-state"
	if process == nil || process.command.ProcessState == nil {
		return benchmark.UnavailableMeasurement("client_process", collector, runtime.GOOS, "native client process did not exit before CPU accounting")
	}
	user := process.command.ProcessState.UserTime()
	system := process.command.ProcessState.SystemTime()
	if user < 0 || system < 0 {
		return benchmark.UnavailableMeasurement("client_process", collector, runtime.GOOS, "native process CPU accounting was negative")
	}
	return benchmark.MeasuredMeasurement("client_process", collector, runtime.GOOS, uint64((user + system).Nanoseconds()))
}

func nativeInstructionMeasurement() benchmark.CounterMeasurement {
	return benchmark.UnavailableMeasurement(
		"client_process",
		"native-instructions",
		runtime.GOOS,
		"retired-instruction collection is not yet available for the native macOS/Windows launcher",
	)
}

func waitUntilReady(ctx context.Context, interval time.Duration, api *clientadapter.API, process *nativeProcess) (string, error) {
	var lastErr error
	for {
		if process != nil && process.exited() {
			return "", fmt.Errorf("native client exited before API readiness: %w", process.err)
		}
		version, err := api.WaitReady(ctx)
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
		case <-process.done:
			return "", fmt.Errorf("native client exited before API readiness: %w", process.err)
		case <-timer.C:
		}
	}
}

func writeProductFiles(cfg Config, spec productSpec) error {
	directories := []string{cfg.ConfigDir, cfg.OutputDir, filepath.Join(cfg.ConfigDir, "incomplete")}
	if cfg.Client == benchmark.NZBGet {
		for _, name := range []string{"nzb", "queue", "tmp", "scripts"} {
			directories = append(directories, filepath.Join(cfg.ConfigDir, name))
		}
	}
	for _, directory := range directories {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("create native run directory: %w", err)
		}
	}
	if err := writeNewFile(filepath.Join(cfg.ConfigDir, spec.ConfigName), spec.Content); err != nil {
		return fmt.Errorf("write %s config: %w", cfg.Client, err)
	}
	if err := writeNewFile(filepath.Join(cfg.ConfigDir, "rendered-config.txt"), spec.Rendered); err != nil {
		return fmt.Errorf("write rendered native configuration: %w", err)
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

func writeQueueResult(path string, result benchmark.QueueAdapterResult) error {
	contents, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode native queue adapter result: %w", err)
	}
	contents = append(contents, '\n')
	if err := writeNewFile(path, contents); err != nil {
		return fmt.Errorf("write native queue adapter result: %w", err)
	}
	return nil
}

func writeNewFile(path string, contents []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(contents); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func commandIdentity(command string) (string, error) {
	resolved, err := exec.LookPath(command)
	if err != nil {
		return "", fmt.Errorf("resolve native client executable %q: %w", command, err)
	}
	contents, err := os.ReadFile(resolved)
	if err != nil {
		return "", fmt.Errorf("read native client executable %s: %w", resolved, err)
	}
	digest := sha256.Sum256(contents)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}
