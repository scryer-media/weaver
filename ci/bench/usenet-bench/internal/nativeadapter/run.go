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
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/clientadapter"
)

// Run executes one native product process and writes the same immutable
// AdapterResult schema as the Docker adapter. The execution target makes
// platform-specific counter availability explicit instead of mixing it with
// Docker/Linux results.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	spec, err := renderProduct(cfg)
	if err != nil {
		return err
	}
	if err := writeProductFiles(cfg, spec); err != nil {
		return err
	}
	identity, err := commandIdentity(spec.Command[0])
	if err != nil {
		return err
	}
	process, err := startProcess(ctx, cfg, spec)
	if err != nil {
		return err
	}
	defer process.ensureStopped()

	var queuedAt, completionAt time.Time
	if cfg.Client == benchmark.Weaver {
		queuedAt, completionAt, err = waitForWeaverReport(ctx, cfg.PollInterval, spec.ReportPath, process)
		if err == nil {
			err = writeNewFile(spec.AckPath, nil)
		}
		if err == nil {
			err = process.wait(ctx)
		}
	} else {
		api, apiErr := clientadapter.NewAPI(cfg.Client, cfg.APIEndpoint)
		if apiErr != nil {
			err = apiErr
		} else {
			startupCtx, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
			_, err = waitUntilReady(startupCtx, cfg.PollInterval, api, process)
			cancelStartup()
		}
		if err == nil {
			var jobID string
			jobID, err = api.Queue(ctx, cfg.NZBPath, cfg.ArchivePassword)
			queuedAt = time.Now().UTC()
			if err == nil {
				completionAt, err = api.WaitComplete(ctx, jobID, cfg.PollInterval)
			}
		}
		stopErr := process.stop()
		if err == nil && stopErr != nil {
			err = stopErr
		}
	}
	if err != nil {
		return err
	}
	result := benchmark.AdapterResult{
		SchemaVersion:        3,
		RunID:                cfg.RunID,
		Client:               cfg.Client,
		ExecutionTarget:      cfg.ExecutionTarget,
		Transport:            cfg.Transport,
		TLSValidation:        cfg.TLSValidation,
		TransportLabel:       cfg.TransportLabel,
		ServerLink:           cfg.ServerLink,
		QueuedAt:             queuedAt,
		CompletionAt:         completionAt,
		ClientIdentity:       identity,
		ClientVersion:        cfg.ClientVersion,
		RenderedConfigSHA256: spec.ConfigSHA256,
		ResourceMetrics: benchmark.ResourceMetrics{
			CPUTimeNanoseconds:  process.cpuMeasurement(),
			InstructionsRetired: nativeInstructionMeasurement(),
		},
	}
	if err := result.ValidateFor(benchmark.Run{
		ID:              cfg.RunID,
		Client:          cfg.Client,
		ExecutionTarget: cfg.ExecutionTarget,
		Transport:       cfg.Transport,
		TLSValidation:   cfg.TLSValidation,
		TransportLabel:  cfg.TransportLabel,
		ServerLink:      cfg.ServerLink,
	}); err != nil {
		return fmt.Errorf("validate adapter result: %w", err)
	}
	if err := writeResult(cfg.ResultPath, result); err != nil {
		return err
	}
	return nil
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

type weaverReport struct {
	SchemaVersion int       `json:"schema_version"`
	QueuedAt      time.Time `json:"queued_at"`
	CompletionAt  time.Time `json:"completion_at"`
	Status        string    `json:"status"`
}

func waitForWeaverReport(ctx context.Context, interval time.Duration, reportPath string, process *nativeProcess) (time.Time, time.Time, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastReportError error
	for {
		contents, err := os.ReadFile(reportPath)
		switch {
		case err == nil:
			var report weaverReport
			if decodeErr := json.Unmarshal(contents, &report); decodeErr != nil {
				lastReportError = fmt.Errorf("decode Weaver CLI report: %w", decodeErr)
			} else if validationErr := validateWeaverReport(report); validationErr != nil {
				lastReportError = validationErr
			} else {
				return report.QueuedAt, report.CompletionAt, nil
			}
		case os.IsNotExist(err):
			lastReportError = nil
		default:
			return time.Time{}, time.Time{}, fmt.Errorf("read Weaver CLI report: %w", err)
		}
		if process.exited() {
			if lastReportError != nil {
				return time.Time{}, time.Time{}, fmt.Errorf("Weaver CLI exited before writing a valid terminal report: %w", lastReportError)
			}
			return time.Time{}, time.Time{}, fmt.Errorf("Weaver CLI exited without a terminal report: %w", process.err)
		}
		select {
		case <-ctx.Done():
			return time.Time{}, time.Time{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func validateWeaverReport(report weaverReport) error {
	if report.SchemaVersion != 1 {
		return fmt.Errorf("unsupported Weaver CLI report schema %d", report.SchemaVersion)
	}
	if report.Status != "complete" {
		return fmt.Errorf("Weaver CLI reported unexpected terminal status %q", report.Status)
	}
	if report.QueuedAt.IsZero() || report.CompletionAt.IsZero() || report.CompletionAt.Before(report.QueuedAt) {
		return fmt.Errorf("Weaver CLI report has invalid timestamps")
	}
	return nil
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
