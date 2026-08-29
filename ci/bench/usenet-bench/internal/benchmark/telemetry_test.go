package benchmark

import (
	"encoding/json"
	"testing"
)

func TestResourceMetricsCarryIndependentCounterProvenance(t *testing.T) {
	metrics := ResourceMetrics{
		CPUTimeNanoseconds:  MeasuredMeasurement("client_container", "cgroup-v2-cpu.stat", "cgroup-v2", 42),
		InstructionsRetired: UnavailableMeasurement("client_process", "linux-perf", "macos", "native Linux perf is unavailable"),
	}
	if err := metrics.Validate(); err != nil {
		t.Fatal(err)
	}
	contents, err := json.Marshal(metrics)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		CPU struct {
			Scope     string `json:"scope"`
			Collector string `json:"collector"`
			Status    string `json:"status"`
			Value     uint64 `json:"value"`
		} `json:"cpu_time_nanoseconds"`
		Instructions struct {
			Scope  string `json:"scope"`
			Status string `json:"status"`
			Reason string `json:"reason"`
		} `json:"instructions_retired"`
	}
	if err := json.Unmarshal(contents, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.CPU.Scope != "client_container" || decoded.CPU.Collector != "cgroup-v2-cpu.stat" || decoded.CPU.Status != string(CounterMeasured) || decoded.CPU.Value != 42 {
		t.Fatalf("CPU provenance did not round-trip: %s", contents)
	}
	if decoded.Instructions.Scope != "client_process" || decoded.Instructions.Status != string(CounterUnavailable) || decoded.Instructions.Reason == "" {
		t.Fatalf("instruction provenance did not round-trip: %s", contents)
	}
}
