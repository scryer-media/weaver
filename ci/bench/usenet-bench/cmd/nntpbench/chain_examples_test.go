package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

func shippedChainExamples(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "configs", "chains", "*.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no chain examples found; this test is checking nothing")
	}
	return paths
}

// The shipped chain examples are what an operator copies to start a series, so
// a broken one is discovered on their box rather than here. Building every
// phase's plan is what proves them: it resolves the storage profile, the
// fixture set and the target, which is where a hand edit goes wrong.
func TestEveryShippedChainExampleBuildsItsPlans(t *testing.T) {
	for _, path := range shippedChainExamples(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			config, err := loadChainConfig(path)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			phases, err := selectChainPhases(config.Phases, "")
			if err != nil {
				t.Fatalf("select phases: %v", err)
			}
			if len(phases) == 0 {
				t.Fatal("the example declares no phases")
			}
			target := config.Target
			if target == "" {
				// A raw chain takes its target from the host it runs on, so an
				// example that omits it is checked against a native one here.
				target = string(benchmark.MacOSNative)
			}
			if _, err := buildChainPlans(phases, target, true, func(string, ...any) {}); err != nil {
				t.Fatalf("build plans: %v", err)
			}
		})
	}
}

// A phase's plan filename records the storage profile it was built for. An
// operator reading a runs directory has only that name to go on, so a name
// that disagrees with the profile points them at the wrong measurement -- and
// a profile changed by hand is exactly when the two drift apart.
func TestChainPhasePlanNamesAgreeWithTheirStorageProfile(t *testing.T) {
	for _, path := range shippedChainExamples(t) {
		config, err := loadChainConfig(path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for _, phase := range config.Phases {
			if phase.PlanSpec == nil {
				continue
			}
			profile := phase.PlanSpec.StorageProfile
			if profile == "" || profile == benchmark.StorageProfileLocal {
				continue
			}
			want := strings.ReplaceAll(profile, "-", "")
			if !strings.Contains(strings.ToLower(phase.Plan), want) {
				t.Errorf("%s: phase %s uses storage profile %q but its plan is named %q",
					filepath.Base(path), phase.Name, profile, phase.Plan)
			}
		}
	}
}

// recoveryVolumeProfileMarkers are the substrings a fixture id carries when its
// repair profile posts .rev volumes. Taking them from the profile constants
// means a renamed profile moves the check with it rather than quietly emptying
// it.
var recoveryVolumeProfileMarkers = []string{
	string(fixture.RARRecoveryVolumeLightProfile),
	string(fixture.RARRecoveryVolumeHeavyProfile),
}

func namesARecoveryVolumeFixture(id string) bool {
	for _, marker := range recoveryVolumeProfileMarkers {
		if strings.Contains(id, marker) {
			return true
		}
	}
	return false
}

// A client excluded from one .rev fixture is excluded for a capability it does
// not have, and a capability cannot be absent for one fixture and present for
// its sibling. So a phase that excludes a client from some of its .rev
// fixtures and not the rest has missed one: the client is scheduled against a
// run it cannot finish, and the failure lands as an ordinary did-not-finish
// carrying none of the reason the exclusion would have put in the summary.
func TestAClientExcludedFromOneRecoveryVolumeFixtureIsExcludedFromAll(t *testing.T) {
	for _, path := range shippedChainExamples(t) {
		config, err := loadChainConfig(path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for _, phase := range config.Phases {
			if phase.PlanSpec == nil {
				continue
			}
			// The run drops excluded fixtures with this same resolver, so the
			// check sees the corpus the phase actually posts.
			active, err := excludeFixtures(phase.PlanSpec.Fixtures, phase.PlanSpec.ExcludeFixtures)
			if err != nil {
				t.Fatalf("%s: phase %s: %v", filepath.Base(path), phase.Name, err)
			}
			var recoveryVolume []string
			for _, id := range active {
				if namesARecoveryVolumeFixture(id) {
					recoveryVolume = append(recoveryVolume, id)
				}
			}
			if len(recoveryVolume) == 0 {
				continue
			}
			excluded := make(map[string]map[string]bool)
			for _, exclusion := range phase.PlanSpec.ExcludeClients {
				if !namesARecoveryVolumeFixture(exclusion.FixtureID) {
					continue
				}
				if excluded[exclusion.Client] == nil {
					excluded[exclusion.Client] = make(map[string]bool)
				}
				excluded[exclusion.Client][exclusion.FixtureID] = true
			}
			for client, covered := range excluded {
				for _, id := range recoveryVolume {
					if covered[id] {
						continue
					}
					t.Errorf("%s: phase %s excludes %s from %d of its %d .rev fixtures but not from %s;"+
						" a .rev exclusion is a capability the client lacks, so it holds for that fixture too",
						filepath.Base(path), phase.Name, client, len(covered), len(recoveryVolume), id)
				}
			}
		}
	}
}
