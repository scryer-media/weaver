package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
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
