package weaver

import (
	"reflect"
	"testing"
)

func TestFullPhaseFixtureProfilesCoversSeedingPhasesAndTheGate(t *testing.T) {
	phases := []*fullPhaseContext{
		{Command: "test-all", SeedProfile: "functional"},
		{Command: "test-all", SeedProfile: "functional"},
		{Command: "chaos-test", SeedProfile: "chaos"},
		{Command: "container-restart", SkipSeed: true},
		{Command: "restart-all", SeedProfile: "restart"},
		{Command: "release-gate", SkipSeed: true},
		nil,
	}
	got := fullPhaseFixtureProfiles(phases)
	want := []string{"chaos", "functional", "release-gate", "restart"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("profiles = %v, want %v", got, want)
	}
	if got := fullPhaseFixtureProfiles([]*fullPhaseContext{{Command: "container-restart", SkipSeed: true}}); len(got) != 0 {
		t.Fatalf("a seedless phase alone wants no profile, got %v", got)
	}
}

func TestFixtureModeDefaultsToAutoAndNormalises(t *testing.T) {
	t.Setenv("E2E_FIXTURES", "")
	if got := fixtureMode(); got != fixtureModeAuto {
		t.Fatalf("unset = %q, want auto", got)
	}
	t.Setenv("E2E_FIXTURES", " Fetch ")
	if got := fixtureMode(); got != fixtureModeFetch {
		t.Fatalf("' Fetch ' = %q, want fetch", got)
	}
	t.Setenv("E2E_FIXTURES", "OFF")
	if got := fixtureMode(); got != fixtureModeOff {
		t.Fatalf("OFF = %q, want off", got)
	}
}

func TestEnsureFixtureDirIsANoOpWhenOff(t *testing.T) {
	t.Setenv("E2E_FIXTURES", "off")
	// A directory that owns nothing and does not exist: with the check off
	// this must return without touching the ledger or the tree.
	ensureFixtureDir(t.TempDir() + "/no-such-scenario")
}
