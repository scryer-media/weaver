// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Where a release flow's minutes go.
//
// A flow runs in its own child process. Each stage it spends time in (the
// image check, bringing the stack up, a Playwright stage, evidence, a
// restart, the teardown) appends one line to stage-timings.jsonl in the
// flow's artifacts directory. The gate reads the lines back when the flow
// ends, writes them into the flow's status.json, and sums them across the
// gate, so the summary shows the queue the flows waited in and how much of
// each flow was its own stack rather than its tests.

const weaverReleaseStageTimingsFile = "stage-timings.jsonl"

// The two kinds of stage: the tests themselves, and everything a flow does
// to give them a stack and keep its evidence.
const (
	weaverReleaseStagePlaywright = "playwright"
	weaverReleaseStageLifecycle  = "lifecycle"
)

type weaverReleaseStageTiming struct {
	Stage      string    `json:"stage"`
	Kind       string    `json:"kind"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS int64     `json:"duration_ms"`
	Error      string    `json:"error,omitempty"`
}

var weaverReleaseStageTimingsMu sync.Mutex

// timeWeaverReleaseStage runs one stage of a flow and records how long it
// took. Outside a gate flow (no artifacts directory) it only runs the stage.
func timeWeaverReleaseStage(stage, kind string, run func() error) error {
	started := time.Now()
	err := run()
	endWeaverReleaseStage(stage, kind, started, &err)
	return err
}

// endWeaverReleaseStage records a stage that began at started. A function
// that is one stage whole times itself with
//
//	defer endWeaverReleaseStage("stack-up", weaverReleaseStageLifecycle, time.Now(), &err)
//
// since a deferred call's arguments are taken when the defer runs.
func endWeaverReleaseStage(stage, kind string, started time.Time, err *error) {
	var failure error
	if err != nil {
		failure = *err
	}
	timing := weaverReleaseStageTiming{
		Stage:      stage,
		Kind:       kind,
		StartedAt:  started.UTC(),
		DurationMS: time.Since(started).Milliseconds(),
		Error:      errorText(failure),
	}
	dir := strings.TrimSpace(os.Getenv("E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR"))
	if dir == "" {
		return
	}
	// The flow's output is its flow.log, so the log line lands there.
	log.Print(weaverReleaseStageLine(timing))
	appendWeaverReleaseStage(absolutePath(dir), timing)
}

func weaverReleaseStageLine(timing weaverReleaseStageTiming) string {
	line := fmt.Sprintf("stage %s (%s) took %s", timing.Stage, timing.Kind,
		(time.Duration(timing.DurationMS) * time.Millisecond).Round(100*time.Millisecond))
	if timing.Error != "" {
		line += " and failed"
	}
	return line
}

// recordWeaverReleaseStage records a stage the gate itself runs for a flow,
// outside the flow's process, in the flow's timings and its flow.log.
func recordWeaverReleaseStage(artifactsDir string, timing weaverReleaseStageTiming, logPath string) {
	appendWeaverReleaseStage(artifactsDir, timing)
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = fmt.Fprintln(file, weaverReleaseStageLine(timing))
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func appendWeaverReleaseStage(dir string, timing weaverReleaseStageTiming) {
	line, err := json.Marshal(timing)
	if err != nil {
		return
	}
	weaverReleaseStageTimingsMu.Lock()
	defer weaverReleaseStageTimingsMu.Unlock()
	path := filepath.Join(dir, weaverReleaseStageTimingsFile)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("warning: record stage %s: %v", timing.Stage, err)
		return
	}
	defer file.Close()
	_, _ = file.Write(append(line, '\n'))
}

// readWeaverReleaseStageTimings reads a flow's stage lines back. A flow that
// recorded none (a command flow, or one that died first) has none.
func readWeaverReleaseStageTimings(artifactsDir string) ([]weaverReleaseStageTiming, error) {
	file, err := os.Open(filepath.Join(artifactsDir, weaverReleaseStageTimingsFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parseWeaverReleaseStageTimings(file)
}

func parseWeaverReleaseStageTimings(reader io.Reader) ([]weaverReleaseStageTiming, error) {
	var timings []weaverReleaseStageTiming
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var timing weaverReleaseStageTiming
		if err := json.Unmarshal([]byte(line), &timing); err != nil {
			return timings, fmt.Errorf("decode stage timing %q: %w", line, err)
		}
		timings = append(timings, timing)
	}
	return timings, scanner.Err()
}

// weaverReleaseFlowTiming is one flow's place in the gate's wall clock.
type weaverReleaseFlowTiming struct {
	Key          string
	QueueWait    time.Duration
	Duration     time.Duration
	Playwright   time.Duration
	Lifecycle    time.Duration
	StageDetails []weaverReleaseStageTiming
}

// splitWeaverReleaseFlowTime splits a flow's own time into its tests and the
// rest. The rest is the flow's time less its Playwright stages, so time no
// stage claimed (process start, waits between stages) counts as lifecycle
// and the two always add up to the flow.
func splitWeaverReleaseFlowTime(duration time.Duration, stages []weaverReleaseStageTiming) (playwright, lifecycle time.Duration) {
	for _, stage := range stages {
		if stage.Kind == weaverReleaseStagePlaywright {
			playwright += time.Duration(stage.DurationMS) * time.Millisecond
		}
	}
	if playwright > duration {
		playwright = duration
	}
	return playwright, duration - playwright
}

// weaverReleaseGateTiming is the gate's wall clock in one place.
type weaverReleaseGateTiming struct {
	Flows      int
	Workers    int
	Wall       time.Duration
	FlowTime   time.Duration
	QueueWait  time.Duration
	Playwright time.Duration
	Lifecycle  time.Duration
	Longest    weaverReleaseFlowTiming
}

func summarizeWeaverReleaseGateTiming(flows []weaverReleaseFlowTiming, workers int, wall time.Duration) weaverReleaseGateTiming {
	summary := weaverReleaseGateTiming{Flows: len(flows), Workers: workers, Wall: wall}
	for _, flow := range flows {
		summary.FlowTime += flow.Duration
		summary.QueueWait += flow.QueueWait
		summary.Playwright += flow.Playwright
		summary.Lifecycle += flow.Lifecycle
		if flow.Duration > summary.Longest.Duration {
			summary.Longest = flow
		}
	}
	return summary
}

// lines renders the summary the gate prints. Σ flow time over the workers is
// the floor the queue can reach; the wall clock above it is the tail.
func (summary weaverReleaseGateTiming) lines() []string {
	round := func(value time.Duration) string { return value.Round(time.Second).String() }
	share := func(part time.Duration) string {
		if summary.FlowTime <= 0 {
			return "0%"
		}
		return fmt.Sprintf("%.0f%%", 100*float64(part)/float64(summary.FlowTime))
	}
	floor := time.Duration(0)
	if summary.Workers > 0 {
		floor = summary.FlowTime / time.Duration(summary.Workers)
	}
	return []string{
		fmt.Sprintf("flows %d on %d workers, wall %s (Σ flow time / workers = %s)",
			summary.Flows, summary.Workers, round(summary.Wall), round(floor)),
		fmt.Sprintf("Σ flow time %s: Playwright %s (%s), lifecycle %s (%s)",
			round(summary.FlowTime), round(summary.Playwright), share(summary.Playwright),
			round(summary.Lifecycle), share(summary.Lifecycle)),
		fmt.Sprintf("Σ queue wait %s", round(summary.QueueWait)),
		fmt.Sprintf("longest flow %s %s", summary.Longest.Key, round(summary.Longest.Duration)),
	}
}

// weaverReleaseStageTotals sums each stage name across flows, longest first,
// so the summary names where the lifecycle minutes went.
func weaverReleaseStageTotals(flows []weaverReleaseFlowTiming) []weaverReleaseStageTiming {
	totals := map[string]*weaverReleaseStageTiming{}
	for _, flow := range flows {
		for _, stage := range flow.StageDetails {
			name := weaverReleaseStageFamily(stage.Stage)
			total, ok := totals[name]
			if !ok {
				total = &weaverReleaseStageTiming{Stage: name, Kind: stage.Kind}
				totals[name] = total
			}
			total.DurationMS += stage.DurationMS
		}
	}
	out := make([]weaverReleaseStageTiming, 0, len(totals))
	for _, total := range totals {
		out = append(out, *total)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DurationMS == out[j].DurationMS {
			return out[i].Stage < out[j].Stage
		}
		return out[i].DurationMS > out[j].DurationMS
	})
	return out
}

// weaverReleaseStageFamily drops a stage's qualifier ("playwright initial"
// is a "playwright" stage) so totals group like stages.
func weaverReleaseStageFamily(stage string) string {
	family, _, _ := strings.Cut(stage, " ")
	return family
}

// weaverReleaseQueueNote is the gate's queue as the dashboard shows it.
func weaverReleaseQueueNote(queued, running, done int) string {
	return fmt.Sprintf("%d queued · %d running · %d done", queued, running, done)
}
