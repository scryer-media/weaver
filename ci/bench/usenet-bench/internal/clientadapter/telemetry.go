package clientadapter

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

type cpuSnapshot struct {
	nanoseconds uint64
	collector   string
	version     string
}

type cpuSampler struct {
	docker dockerClient
	name   string
	start  *cpuSnapshot
	reason string
}

func startCPUSampler(ctx context.Context, docker dockerClient, containerName string) cpuSampler {
	sampler := cpuSampler{docker: docker, name: containerName}
	snapshot, err := sampler.read(ctx)
	if err != nil {
		sampler.reason = err.Error()
		return sampler
	}
	sampler.start = &snapshot
	return sampler
}

func (sampler cpuSampler) finish(ctx context.Context) benchmark.CounterMeasurement {
	if sampler.start == nil {
		return benchmark.UnavailableMeasurement("client_container", "cgroup-cpu", "unknown", sampler.reason)
	}
	end, err := sampler.read(ctx)
	if err != nil {
		return benchmark.UnavailableMeasurement("client_container", sampler.start.collector, sampler.start.version, err.Error())
	}
	if end.collector != sampler.start.collector || end.version != sampler.start.version {
		return benchmark.UnavailableMeasurement("client_container", sampler.start.collector, sampler.start.version, "CPU accounting source changed during benchmark run")
	}
	if end.nanoseconds < sampler.start.nanoseconds {
		return benchmark.UnavailableMeasurement("client_container", sampler.start.collector, sampler.start.version, "CPU usage counter moved backwards during benchmark run")
	}
	return benchmark.MeasuredMeasurement("client_container", end.collector, end.version, end.nanoseconds-sampler.start.nanoseconds)
}

func (sampler cpuSampler) read(ctx context.Context) (cpuSnapshot, error) {
	output, err := sampler.docker.run(ctx, "exec", sampler.name, "cat", "/sys/fs/cgroup/cpu.stat")
	if err == nil {
		usageUsec, parseErr := parseCgroupV2CPU(output)
		if parseErr == nil {
			if usageUsec > ^uint64(0)/1_000 {
				return cpuSnapshot{}, fmt.Errorf("cgroup v2 CPU usage overflows nanoseconds")
			}
			return cpuSnapshot{nanoseconds: usageUsec * 1_000, collector: "cgroup-v2-cpu.stat", version: "cgroup-v2"}, nil
		}
	}
	output, fallbackErr := sampler.docker.run(ctx, "exec", sampler.name, "cat", "/sys/fs/cgroup/cpuacct/cpuacct.usage")
	if fallbackErr == nil {
		usageNanos, parseErr := parseCgroupV1CPU(output)
		if parseErr == nil {
			return cpuSnapshot{nanoseconds: usageNanos, collector: "cgroup-v1-cpuacct.usage", version: "cgroup-v1"}, nil
		}
	}
	if err != nil {
		return cpuSnapshot{}, fmt.Errorf("container CPU time unavailable: cgroup v2 and v1 probes failed")
	}
	return cpuSnapshot{}, fmt.Errorf("container CPU time unavailable: malformed cgroup CPU data")
}

func parseCgroupV2CPU(contents string) (uint64, error) {
	for _, line := range strings.Split(contents, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse usage_usec: %w", err)
			}
			return value, nil
		}
	}
	return 0, fmt.Errorf("cpu.stat has no usage_usec field")
}

func parseCgroupV1CPU(contents string) (uint64, error) {
	value, err := strconv.ParseUint(strings.TrimSpace(contents), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse cpuacct.usage: %w", err)
	}
	return value, nil
}

type instructionRecorder struct {
	cmd              *exec.Cmd
	output           bytes.Buffer
	scope            string
	collector        string
	collectorVersion string
	unavailable      string
}

func startInstructionRecorder(ctx context.Context, cfg Config, container *runningContainer) *instructionRecorder {
	const (
		scope     = "client_container"
		collector = "linux-perf-cgroup"
	)
	if runtime.GOOS != "linux" {
		return &instructionRecorder{scope: scope, collector: collector, collectorVersion: runtime.GOOS, unavailable: "retired-instruction collection requires a native Linux host with perf cgroup access"}
	}
	cgroup, err := container.docker.containerCgroup(ctx, container.name)
	if err != nil {
		return &instructionRecorder{scope: scope, collector: collector, collectorVersion: "linux-perf", unavailable: err.Error()}
	}
	version := perfVersion(ctx, cfg.PerfBinary)
	command := exec.Command(cfg.PerfBinary, "stat", "--no-big-num", "-x;", "-a", "-e", "instructions", "-G", cgroup)
	command.Env = append(os.Environ(), "LC_ALL=C")
	recorder := &instructionRecorder{cmd: command, scope: scope, collector: collector, collectorVersion: version}
	command.Stdout = &recorder.output
	command.Stderr = &recorder.output
	if err := command.Start(); err != nil {
		recorder.cmd = nil
		recorder.unavailable = "could not start perf instruction counter: " + err.Error()
	}
	return recorder
}

func (recorder *instructionRecorder) finish() benchmark.CounterMeasurement {
	scope := defaultString(recorder.scope, "client_container")
	collector := defaultString(recorder.collector, "linux-perf-cgroup")
	if recorder.cmd == nil {
		reason := recorder.unavailable
		if reason == "" {
			reason = "retired-instruction collector was not started"
		}
		return benchmark.UnavailableMeasurement(scope, collector, defaultString(recorder.collectorVersion, "unavailable"), reason)
	}
	if recorder.cmd.Process != nil {
		_ = recorder.cmd.Process.Signal(os.Interrupt)
	}
	wait := make(chan error, 1)
	go func() { wait <- recorder.cmd.Wait() }()
	select {
	case <-time.After(5 * time.Second):
		_ = recorder.cmd.Process.Kill()
		<-wait
		return benchmark.UnavailableMeasurement(scope, collector, recorder.collectorVersion, "perf did not stop within five seconds")
	case waitErr := <-wait:
		value, parseErr := parsePerfInstructions(recorder.output.String())
		if parseErr == nil {
			// perf may use a non-zero status for the intentional SIGINT while
			// still flushing a valid final counter line.
			return benchmark.MeasuredMeasurement(scope, collector, recorder.collectorVersion, value)
		}
		if waitErr != nil {
			return benchmark.UnavailableMeasurement(scope, collector, recorder.collectorVersion, "perf failed: "+trimTelemetryError(waitErr.Error()))
		}
		return benchmark.UnavailableMeasurement(scope, collector, recorder.collectorVersion, parseErr.Error())
	}
}

func perfVersion(ctx context.Context, binary string) string {
	versionCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(versionCtx, binary, "--version").Output()
	if err != nil {
		return "linux-perf"
	}
	if value := strings.TrimSpace(string(output)); value != "" {
		return value
	}
	return "linux-perf"
}

func parsePerfInstructions(contents string) (uint64, error) {
	for _, line := range strings.Split(contents, "\n") {
		parts := strings.Split(line, ";")
		if len(parts) < 3 || strings.TrimSpace(parts[2]) != "instructions" {
			continue
		}
		value := strings.TrimSpace(parts[0])
		if value == "" || strings.HasPrefix(value, "<") {
			return 0, fmt.Errorf("perf reported retired instructions as %q", value)
		}
		value = strings.ReplaceAll(value, ",", "")
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse perf retired instructions: %w", err)
		}
		return parsed, nil
	}
	return 0, fmt.Errorf("perf did not report an instructions counter")
}

func trimTelemetryError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 240 {
		return value[:240] + "…"
	}
	return value
}
