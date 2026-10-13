package weaver

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Override only the in-memory chain slots. The source files and pool retain
// their identities and settings, including their configured packet capacity.
func overrideRealUsenetChain(config *realUsenetConfig, selection string) error {
	if len(config.Profiles) != len(realUsenetWGNames) {
		return fmt.Errorf("expected eight loaded WG profiles")
	}
	names := slices.Clone(realUsenetWGNames[:3])
	if selection != "" {
		names = strings.Split(selection, ",")
	}
	if len(names) != 3 {
		return fmt.Errorf("chain-profiles requires three known private WG filenames")
	}
	profiles := slices.Clone(config.Profiles)
	for index, name := range names {
		names[index] = strings.TrimSpace(name)
		source := slices.Index(realUsenetWGNames, names[index])
		if source < 0 {
			return fmt.Errorf("chain-profiles requires known private WG filenames")
		}
		profiles[index] = maps.Clone(config.Profiles[source])
	}
	if err := validateRealUsenetEndpoints(profiles); err != nil {
		return err
	}
	config.Profiles, config.ChainProfiles = profiles, names
	return nil
}

type realUsenetProgress struct {
	ElapsedMS            int64              `json:"elapsed_ms"`
	State                string             `json:"state"`
	BytesDownloaded      uint64             `json:"bytes_downloaded"`
	CurrentDownloadSpeed float64            `json:"current_download_speed"`
	Pipeline             realUsenetPipeline `json:"pipeline"`
}

type realUsenetPipeline struct {
	ActiveDownloads                   uint32 `json:"activeDownloads"`
	DecodePendingBytes                uint64 `json:"decodePendingBytes"`
	WriteBufferedBytes                uint64 `json:"writeBufferedBytes"`
	DownloadPressureState             string `json:"downloadPressureState"`
	DownloadPressureReason            string `json:"downloadPressureReason"`
	DownloadPressureStallsTotal       uint64 `json:"downloadPressureStallsTotal"`
	DownloadPressureStallDurationMS   uint64 `json:"downloadPressureStallDurationMs"`
	DownloadPipelineTrialFailureTotal uint64 `json:"downloadPipelineTrialFailureTotal"`
	DownloadPipelineReplayItemsTotal  uint64 `json:"downloadPipelineReplayItemsTotal"`
	DecodeErrors                      uint64 `json:"decodeErrors"`
}

type realUsenetPerf struct {
	JobMS        int64                `json:"submission_to_history_ms"`
	CPUUsec      uint64               `json:"container_cpu_usec"`
	Throttled    uint64               `json:"container_throttled_usec"`
	MemoryPeak   uint64               `json:"container_memory_peak_bytes"`
	Before       map[string]uint64    `json:"counters_before"`
	After        map[string]uint64    `json:"counters_after"`
	Progress     []realUsenetProgress `json:"progress"`
	started      time.Time
	baseBytes    uint64
	readCounters func() (map[string]uint64, error)
}

// Use shell builtins to avoid a persistent in-container sampler. Two tiny execs
// bracket the job; their cost remains in the conservative cgroup CPU total.
const realUsenetCounterScript = `while read key value; do printf '%s %s\n' "$key" "$value"; done < /sys/fs/cgroup/cpu.stat
for name in current peak; do read value < /sys/fs/cgroup/memory.$name; printf 'memory_%s %s\n' "$name" "$value"; done
while read key value; do printf 'memory_event_%s %s\n' "$key" "$value"; done < /sys/fs/cgroup/memory.events
while read protocol rest; do
    if [ "$protocol" = 'Udp:' ]; then
        set -- $rest
        if [ "$1" = 'InDatagrams' ]; then
            read protocol rest
            set -- $rest
            printf 'udp_in_datagrams %s\nudp_in_errors %s\nudp_out_datagrams %s\nudp_rcvbuf_errors %s\nudp_sndbuf_errors %s\nudp_checksum_errors %s\n' "$1" "$3" "$4" "$5" "$6" "$7"
            break
        fi
    fi
done < /proc/net/snmp`

func parseRealUsenetCounters(text string) (map[string]uint64, error) {
	values := map[string]uint64{}
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid container performance counter")
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid container performance counter")
		}
		values[fields[0]] = value
	}
	for _, key := range []string{"usage_usec", "throttled_usec", "memory_current", "memory_peak"} {
		if _, ok := values[key]; !ok {
			return nil, fmt.Errorf("container cgroup v2 performance counters unavailable")
		}
	}
	return values, nil
}

func (perf *realUsenetPerf) start(api realUsenetAPI) error {
	var snapshot struct {
		Metrics struct{ BytesDownloaded uint64 }
	}
	if err := api.query("query {metrics {bytesDownloaded}}", nil, &snapshot); err != nil {
		return err
	}
	perf.baseBytes = snapshot.Metrics.BytesDownloaded
	var err error
	perf.Before, err = perf.readCounters()
	perf.started = time.Now()
	return err
}

func (perf *realUsenetPerf) observe(api realUsenetAPI, jobID int) error {
	var snapshot struct {
		Metrics struct {
			BytesDownloaded      uint64
			CurrentDownloadSpeed float64
			realUsenetPipeline
		}
		QueueItem *struct {
			ID    int
			State string
		}
	}
	if err := api.query("query($id:Int!) {metrics {bytesDownloaded currentDownloadSpeed activeDownloads decodePendingBytes writeBufferedBytes downloadPressureState downloadPressureReason downloadPressureStallsTotal downloadPressureStallDurationMs downloadPipelineTrialFailureTotal downloadPipelineReplayItemsTotal decodeErrors} queueItem(id:$id) {id state}}", map[string]any{"id": jobID}, &snapshot); err != nil {
		return err
	}
	state := "ARCHIVING"
	if snapshot.QueueItem != nil {
		if snapshot.QueueItem.ID != jobID {
			return fmt.Errorf("performance snapshot belongs to another job")
		}
		state = snapshot.QueueItem.State
	}
	if snapshot.Metrics.BytesDownloaded < perf.baseBytes {
		return fmt.Errorf("performance byte counter reset during job")
	}
	perf.Progress = append(perf.Progress, realUsenetProgress{ElapsedMS: time.Since(perf.started).Milliseconds(), State: state,
		BytesDownloaded: snapshot.Metrics.BytesDownloaded - perf.baseBytes, CurrentDownloadSpeed: snapshot.Metrics.CurrentDownloadSpeed, Pipeline: snapshot.Metrics.realUsenetPipeline})
	return nil
}

func (perf *realUsenetPerf) finish() error {
	perf.JobMS = time.Since(perf.started).Milliseconds()
	var err error
	perf.After, err = perf.readCounters()
	if err != nil {
		return err
	}
	if perf.After["usage_usec"] < perf.Before["usage_usec"] || perf.After["throttled_usec"] < perf.Before["throttled_usec"] {
		return fmt.Errorf("performance counters reset during job")
	}
	perf.CPUUsec = perf.After["usage_usec"] - perf.Before["usage_usec"]
	perf.Throttled = perf.After["throttled_usec"] - perf.Before["throttled_usec"]
	perf.MemoryPeak = perf.After["memory_peak"]
	return nil
}
