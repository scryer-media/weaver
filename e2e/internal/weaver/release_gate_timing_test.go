// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWeaverReleaseStagesLandInTheFlowArtifacts(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR", dir)

	if err := timeWeaverReleaseStage("stack-up", weaverReleaseStageLifecycle, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("two tests failed")
	if err := timeWeaverReleaseStage("playwright initial", weaverReleaseStagePlaywright, func() error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("stage error = %v, want the stage's own error", err)
	}

	stages, err := readWeaverReleaseStageTimings(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(stages) != 2 {
		t.Fatalf("stages = %+v, want two", stages)
	}
	if stages[0].Stage != "stack-up" || stages[0].Kind != weaverReleaseStageLifecycle || stages[0].Error != "" {
		t.Fatalf("first stage = %+v", stages[0])
	}
	if stages[1].Stage != "playwright initial" || stages[1].Kind != weaverReleaseStagePlaywright || stages[1].Error != failure.Error() {
		t.Fatalf("second stage = %+v", stages[1])
	}
}

func TestWeaverReleaseStagesOutsideAFlowRecordNothing(t *testing.T) {
	t.Setenv("E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR", "")
	ran := false
	if err := timeWeaverReleaseStage("image-check", weaverReleaseStageLifecycle, func() error { ran = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("the stage did not run")
	}
}

func TestWeaverReleaseGateRecordsItsOwnStagesInTheFlowLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "flow.log")
	recordWeaverReleaseStage(dir, weaverReleaseStageTiming{
		Stage: "finalize", Kind: weaverReleaseStageLifecycle, DurationMS: 2500,
	}, logPath)

	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "stage finalize (lifecycle) took 2.5s") {
		t.Fatalf("flow.log = %q", body)
	}
	stages, err := readWeaverReleaseStageTimings(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(stages) != 1 || stages[0].Stage != "finalize" {
		t.Fatalf("stages = %+v", stages)
	}
}

func TestWeaverReleaseFlowTimeSplitsIntoPlaywrightAndLifecycle(t *testing.T) {
	// ui-settings-crud/sqlite on a loaded host: 153 s in all, 45 s and 22 s
	// of it in the two Playwright stages.
	stages := []weaverReleaseStageTiming{
		{Stage: "image-check", Kind: weaverReleaseStageLifecycle, DurationMS: 5_000},
		{Stage: "stack-up", Kind: weaverReleaseStageLifecycle, DurationMS: 38_000},
		{Stage: "playwright initial", Kind: weaverReleaseStagePlaywright, DurationMS: 45_000},
		{Stage: "evidence initial", Kind: weaverReleaseStageLifecycle, DurationMS: 11_000},
		{Stage: "restart", Kind: weaverReleaseStageLifecycle, DurationMS: 7_000},
		{Stage: "playwright after-restart", Kind: weaverReleaseStagePlaywright, DurationMS: 22_000},
	}
	playwright, lifecycle := splitWeaverReleaseFlowTime(153*time.Second, stages)
	if playwright != 67*time.Second || lifecycle != 86*time.Second {
		t.Fatalf("split = %s / %s, want 67s / 86s", playwright, lifecycle)
	}

	// A flow whose stages claim more than its own time (clock skew between
	// processes) never reports negative lifecycle.
	playwright, lifecycle = splitWeaverReleaseFlowTime(60*time.Second, stages)
	if playwright != 60*time.Second || lifecycle != 0 {
		t.Fatalf("clamped split = %s / %s", playwright, lifecycle)
	}
}

func TestWeaverReleaseGateSummaryNamesTheWallClock(t *testing.T) {
	flows := []weaverReleaseFlowTiming{
		{Key: "ui-settings-crud/sqlite", QueueWait: 16 * time.Minute, Duration: 153 * time.Second, Playwright: 67 * time.Second, Lifecycle: 86 * time.Second},
		{Key: "network-pools/sqlite", Duration: 10 * time.Minute, Playwright: 9 * time.Minute, Lifecycle: time.Minute},
		{Key: "ui-security/postgres", QueueWait: 4 * time.Minute, Duration: 47 * time.Second, Playwright: 33 * time.Second, Lifecycle: 14 * time.Second},
	}
	summary := summarizeWeaverReleaseGateTiming(flows, 8, 31*time.Minute)
	if summary.FlowTime != 153*time.Second+10*time.Minute+47*time.Second {
		t.Fatalf("Σ flow time = %s", summary.FlowTime)
	}
	if summary.QueueWait != 20*time.Minute {
		t.Fatalf("Σ queue wait = %s", summary.QueueWait)
	}
	if summary.Longest.Key != "network-pools/sqlite" {
		t.Fatalf("longest = %s", summary.Longest.Key)
	}
	text := strings.Join(summary.lines(), "\n")
	for _, want := range []string{
		"flows 3 on 8 workers, wall 31m0s",
		"Σ flow time 13m20s: Playwright 10m40s (80%), lifecycle 2m40s (20%)",
		"Σ queue wait 20m0s",
		"longest flow network-pools/sqlite 10m0s",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("summary lacks %q:\n%s", want, text)
		}
	}
}

func TestWeaverReleaseStageTotalsGroupLikeStages(t *testing.T) {
	flows := []weaverReleaseFlowTiming{
		{StageDetails: []weaverReleaseStageTiming{
			{Stage: "evidence initial", Kind: weaverReleaseStageLifecycle, DurationMS: 3_000},
			{Stage: "evidence final", Kind: weaverReleaseStageLifecycle, DurationMS: 4_000},
			{Stage: "playwright initial", Kind: weaverReleaseStagePlaywright, DurationMS: 20_000},
		}},
		{StageDetails: []weaverReleaseStageTiming{
			{Stage: "stack-up", Kind: weaverReleaseStageLifecycle, DurationMS: 6_000},
		}},
	}
	totals := weaverReleaseStageTotals(flows)
	got := make([]string, 0, len(totals))
	for _, total := range totals {
		got = append(got, total.Stage+"="+(time.Duration(total.DurationMS)*time.Millisecond).String())
	}
	if strings.Join(got, ",") != "playwright=20s,evidence=7s,stack-up=6s" {
		t.Fatalf("totals = %v", got)
	}
}

func TestWeaverReleaseQueueNoteCountsTheQueue(t *testing.T) {
	if note := weaverReleaseQueueNote(43, 8, 8); note != "43 queued · 8 running · 8 done" {
		t.Fatalf("note = %q", note)
	}
}

func TestFullDashboardShowsWhatAPhaseWaitsOn(t *testing.T) {
	dashboard := &fullDashboard{phases: map[string]*dashboardBar{"release-gate": {Status: "pending"}}}
	dashboard.setPhaseDetail("release-gate", "waiting for Weaver image")
	phase := dashboard.phases["release-gate"]
	if phase.Detail != "waiting for Weaver image" || phase.Status != "pending" {
		t.Fatalf("phase = %+v", phase)
	}
}
