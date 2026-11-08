// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func poolPhase(flow string) *weaverReleasePhase {
	return &weaverReleasePhase{Flow: flow, Datastore: "sqlite"}
}

func poolFlows(pool weaverReleasePool) []string {
	var flows []string
	for _, phase := range pool.Phases {
		flows = append(flows, phase.Flow)
	}
	return flows
}

func clearPoolEnv(t *testing.T) {
	t.Setenv("E2E_WEAVER_RELEASE_GATE_JOBS", "")
	t.Setenv(weaverReleaseGateWaitingJobsEnv, "")
	t.Setenv(weaverReleaseGateBusyJobsEnv, "")
}

func TestReleaseGateSplitsWaitingFromBusyFlows(t *testing.T) {
	clearPoolEnv(t)
	phases := []*weaverReleasePhase{
		poolPhase("ui-settings-crud"),
		poolPhase("advanced-networking-egress"),
		poolPhase("ui-backup-restore-sqlite"),
		poolPhase("advanced-networking-pools"),
	}
	pools := weaverReleaseGatePools(phases, map[string]int64{})
	if len(pools) != 2 || pools[0].Name != "waiting" || pools[1].Name != "busy" {
		t.Fatalf("pools = %+v", pools)
	}
	if got := poolFlows(pools[0]); !slices.Equal(got, []string{"advanced-networking-egress", "advanced-networking-pools"}) {
		t.Fatalf("waiting = %v", got)
	}
	if got := poolFlows(pools[1]); !slices.Equal(got, []string{"ui-settings-crud", "ui-backup-restore-sqlite"}) {
		t.Fatalf("busy = %v", got)
	}
	// Never more workers than flows.
	if pools[0].Jobs != 2 || pools[1].Jobs != 2 {
		t.Fatalf("jobs = %d/%d, want 2/2", pools[0].Jobs, pools[1].Jobs)
	}
}

func TestReleaseGatePoolSizesFollowTheirSettings(t *testing.T) {
	clearPoolEnv(t)
	var phases []*weaverReleasePhase
	for range 20 {
		phases = append(phases, poolPhase("advanced-networking-legs"), poolPhase("ui-settings-crud"))
	}
	pools := weaverReleaseGatePools(phases, nil)
	if pools[0].Jobs != defaultWeaverReleaseGateWaitingJobs || pools[1].Jobs != defaultWeaverReleaseGateBusyJobs {
		t.Fatalf("default jobs = %d/%d", pools[0].Jobs, pools[1].Jobs)
	}
	t.Setenv(weaverReleaseGateWaitingJobsEnv, "12")
	t.Setenv(weaverReleaseGateBusyJobsEnv, "0")
	pools = weaverReleaseGatePools(phases, nil)
	if pools[0].Jobs != 12 || pools[1].Jobs != 1 {
		t.Fatalf("set jobs = %d/%d, want 12/1", pools[0].Jobs, pools[1].Jobs)
	}
	clearPoolEnv(t)
	t.Setenv("E2E_WEAVER_RELEASE_GATE_JOBS", "3")
	pools = weaverReleaseGatePools(phases, nil)
	if len(pools) != 1 || pools[0].Name != "shared" || pools[0].Jobs != 3 || len(pools[0].Phases) != 40 {
		t.Fatalf("legacy shared pool = %+v", pools)
	}
}

func TestReleaseGateQueuesLongestFirstUnknownAhead(t *testing.T) {
	phases := []*weaverReleasePhase{
		poolPhase("short"), poolPhase("new-a"), poolPhase("long"), poolPhase("mid"), poolPhase("new-b"), poolPhase("mid-too"),
	}
	durations := map[string]int64{
		"short/sqlite":   60_000,
		"long/sqlite":    600_000,
		"mid/sqlite":     120_000,
		"mid-too/sqlite": 120_000,
	}
	got := poolFlows(weaverReleasePool{Phases: longestFirst(phases, durations)})
	want := []string{"new-a", "new-b", "long", "mid", "mid-too", "short"}
	if !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	if got := poolFlows(weaverReleasePool{Phases: longestFirst(phases, nil)}); !slices.Equal(got, []string{"short", "new-a", "long", "mid", "new-b", "mid-too"}) {
		t.Fatalf("with nothing recorded the registry order stands, got %v", got)
	}
}

func TestReleaseGateKeepsEachFlowsLastPassingDuration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durations", "release-gate-durations.json")
	if got := readWeaverReleaseGateDurations(path); len(got) != 0 {
		t.Fatalf("nothing recorded yet, got %v", got)
	}
	first := []weaverReleasePhaseResult{
		{Phase: poolPhase("a"), Duration: 90 * time.Second},
		{Phase: poolPhase("b"), Duration: 30 * time.Second},
	}
	if err := recordWeaverReleaseGateDurations(path, nil, first); err != nil {
		t.Fatal(err)
	}
	recorded := readWeaverReleaseGateDurations(path)
	second := []weaverReleasePhaseResult{
		{Phase: poolPhase("a"), Duration: 5 * time.Second, Err: errors.New("stopped early")},
		{Phase: poolPhase("b"), Duration: 45 * time.Second},
	}
	if err := recordWeaverReleaseGateDurations(path, recorded, second); err != nil {
		t.Fatal(err)
	}
	got := readWeaverReleaseGateDurations(path)
	if got["a/sqlite"] != 90_000 || got["b/sqlite"] != 45_000 {
		t.Fatalf("durations = %v, want a kept at 90s and b moved to 45s", got)
	}
}
