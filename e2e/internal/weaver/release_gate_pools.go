// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// The release gate runs its flows on two pools. A waiting flow spends most
// of its time on a fixture's paced delays (the advanced-networking
// partitions: a BODY paced past five minutes, a throttled 200 MiB), so it
// holds a stack but little CPU, and a wide pool of them costs the machine
// little. A busy flow (the UI, backup and restore, scripts) works the
// whole time, so its pool stays narrow enough that the two together do not
// starve each other.
const (
	defaultWeaverReleaseGateWaitingJobs = 8
	defaultWeaverReleaseGateBusyJobs    = 4
	weaverReleaseGateWaitingJobsEnv     = "E2E_WEAVER_RELEASE_GATE_WAITING_JOBS"
	weaverReleaseGateBusyJobsEnv        = "E2E_WEAVER_RELEASE_GATE_BUSY_JOBS"
	weaverReleaseGateDurationsEnv       = "E2E_WEAVER_RELEASE_GATE_DURATIONS"
)

type weaverReleasePool struct {
	Name   string
	Jobs   int
	Phases []*weaverReleasePhase
}

// weaverReleaseFlowWaits reports whether a flow belongs to the waiting pool.
func weaverReleaseFlowWaits(phase *weaverReleasePhase) bool {
	return strings.HasPrefix(phase.Flow, "advanced-networking")
}

// weaverReleaseGatePools splits the phases into the pools they run on, each
// queue ordered longest first by `durations`. Setting only
// E2E_WEAVER_RELEASE_GATE_JOBS keeps the single shared pool of that size.
func weaverReleaseGatePools(phases []*weaverReleasePhase, durations map[string]int64) []weaverReleasePool {
	set := func(name string) bool { return strings.TrimSpace(os.Getenv(name)) != "" }
	if set("E2E_WEAVER_RELEASE_GATE_JOBS") && !set(weaverReleaseGateWaitingJobsEnv) && !set(weaverReleaseGateBusyJobsEnv) {
		return nonEmptyWeaverReleasePools(weaverReleasePool{
			Name:   "shared",
			Jobs:   weaverReleaseGateJobs(len(phases)),
			Phases: longestFirst(phases, durations),
		})
	}
	var waiting, busy []*weaverReleasePhase
	for _, phase := range phases {
		if weaverReleaseFlowWaits(phase) {
			waiting = append(waiting, phase)
		} else {
			busy = append(busy, phase)
		}
	}
	return nonEmptyWeaverReleasePools(
		weaverReleasePool{
			Name:   "waiting",
			Jobs:   weaverReleasePoolJobs(weaverReleaseGateWaitingJobsEnv, defaultWeaverReleaseGateWaitingJobs, len(waiting)),
			Phases: longestFirst(waiting, durations),
		},
		weaverReleasePool{
			Name:   "busy",
			Jobs:   weaverReleasePoolJobs(weaverReleaseGateBusyJobsEnv, defaultWeaverReleaseGateBusyJobs, len(busy)),
			Phases: longestFirst(busy, durations),
		},
	)
}

func nonEmptyWeaverReleasePools(pools ...weaverReleasePool) []weaverReleasePool {
	return slices.DeleteFunc(pools, func(pool weaverReleasePool) bool { return len(pool.Phases) == 0 })
}

func weaverReleasePoolJobs(name string, fallback, flows int) int {
	jobs := fallback
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			jobs = parsed
		}
	}
	return max(1, min(jobs, maxWeaverReleaseGateJobs, max(flows, 1)))
}

// longestFirst orders a queue by the duration each flow last passed in,
// longest first, so the longest flows never start last. A flow with no
// recorded duration might be the longest of all, so it goes ahead of every
// known one; ties and unknown flows keep the registry's order.
func longestFirst(phases []*weaverReleasePhase, durations map[string]int64) []*weaverReleasePhase {
	ordered := slices.Clone(phases)
	slices.SortStableFunc(ordered, func(a, b *weaverReleasePhase) int {
		da, aKnown := durations[weaverReleaseFlowKey(a)]
		db, bKnown := durations[weaverReleaseFlowKey(b)]
		switch {
		case aKnown != bKnown:
			if !aKnown {
				return -1
			}
			return 1
		case da > db:
			return -1
		case da < db:
			return 1
		}
		return 0
	})
	return ordered
}

func weaverReleasePoolsNote(pools []weaverReleasePool, known int) string {
	parts := make([]string, 0, len(pools))
	for _, pool := range pools {
		parts = append(parts, fmt.Sprintf("%s %d workers (%d flows)", pool.Name, pool.Jobs, len(pool.Phases)))
	}
	order := "registry order"
	if known > 0 {
		order = fmt.Sprintf("longest first by the last pass of %d flows", known)
	}
	return strings.Join(parts, " · ") + "; " + order
}

// weaverReleaseGateDurationsPath is where each flow's last passing duration
// is kept between gate runs. It sits outside any one run's directory,
// because a full run gives every gate a directory of its own.
func weaverReleaseGateDurationsPath() string {
	if value := strings.TrimSpace(os.Getenv(weaverReleaseGateDurationsEnv)); value != "" {
		return absolutePath(value)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(cache, "weaver-e2e", "release-gate-durations.json")
}

type weaverReleaseGateDurations struct {
	Version int              `json:"version"`
	Flows   map[string]int64 `json:"flows_ms"`
}

// readWeaverReleaseGateDurations is empty when nothing was recorded or the
// record cannot be read: the queue then keeps the registry's order.
func readWeaverReleaseGateDurations(path string) map[string]int64 {
	if path == "" {
		return map[string]int64{}
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return map[string]int64{}
	}
	var recorded weaverReleaseGateDurations
	if json.Unmarshal(body, &recorded) != nil || recorded.Flows == nil {
		return map[string]int64{}
	}
	return recorded.Flows
}

// recordWeaverReleaseGateDurations keeps each passing flow's duration. A
// failed flow may have stopped early, so it keeps the duration it last
// passed in.
func recordWeaverReleaseGateDurations(path string, durations map[string]int64, results []weaverReleasePhaseResult) error {
	if path == "" {
		return nil
	}
	flows := make(map[string]int64, len(durations)+len(results))
	for key, ms := range durations {
		flows[key] = ms
	}
	for _, result := range results {
		if result.Err == nil && result.Phase != nil && result.Duration > 0 {
			flows[weaverReleaseFlowKey(result.Phase)] = result.Duration.Milliseconds()
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return writeWeaverReleaseJSON(path, weaverReleaseGateDurations{Version: 1, Flows: flows})
}

func weaverReleasePoolWorkers(pools []weaverReleasePool) int {
	total := 0
	for _, pool := range pools {
		total += pool.Jobs
	}
	return total
}
