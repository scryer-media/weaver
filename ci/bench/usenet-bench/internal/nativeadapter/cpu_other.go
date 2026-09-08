//go:build !windows

package nativeadapter

import (
	"os"
	"runtime"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// processStateCPU reads the exited client's user and system time from its
// wait status. The kernel charges every tick a thread runs, and a child the
// client waited for -- the unpacker SABnzbd and NZBGet shell out to -- is
// folded into the parent's figure at exit on the BSD and Linux wait paths.
type processStateCPU struct{}

func newCPUAccountant() cpuAccountant { return processStateCPU{} }

func (processStateCPU) attach(*os.Process) error { return nil }

func (processStateCPU) measurement(state *os.ProcessState) benchmark.CounterMeasurement {
	const collector = "go-os-process-state"
	user := state.UserTime()
	system := state.SystemTime()
	if user < 0 || system < 0 {
		return benchmark.UnavailableMeasurement("client_process", collector, runtime.GOOS, "native process CPU accounting was negative")
	}
	return benchmark.MeasuredMeasurement("client_process", collector, runtime.GOOS, uint64((user + system).Nanoseconds()))
}

func (processStateCPU) close() {}
