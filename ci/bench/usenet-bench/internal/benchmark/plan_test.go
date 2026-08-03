package benchmark

import (
	"reflect"
	"testing"
)

func TestBuildPlanIsBalancedAndDeterministic(t *testing.T) {
	options := PlanOptions{
		FixtureIDs:  []string{"rar4-store", "rar5-normal"},
		Clients:     []Client{Weaver, SABnzbd, NZBGet},
		Transports:  []Transport{Plaintext, TLS},
		Repetitions: 3,
		Seed:        42,
	}
	first, err := BuildPlan(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildPlan(options)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed did not create the same plan")
	}
	if got, want := len(first.Runs), 108; got != want {
		t.Fatalf("runs = %d, want %d", got, want)
	}
	if got, want := len(first.ExecutionTargets), 3; got != want {
		t.Fatalf("execution targets = %d, want %d", got, want)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}
	if first.Profile != ProfileEquivalentThroughput {
		t.Fatalf("default profile = %q", first.Profile)
	}
	if first.ServerLink.ID != LinkUnlimited {
		t.Fatalf("default server link = %#v", first.ServerLink)
	}
	for _, run := range first.Runs {
		if run.Profile != first.Profile {
			t.Fatalf("run %s profile = %q, plan profile = %q", run.ID, run.Profile, first.Profile)
		}
	}
}

func TestDefaultPlanCreatesTheThreeByThreeClientPackagingMatrix(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver, SABnzbd, NZBGet},
		Transports:  []Transport{Plaintext},
		Repetitions: 1,
		Seed:        7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(plan.Runs), 9; got != want {
		t.Fatalf("runs = %d, want %d", got, want)
	}
	counts := map[ExecutionTarget]int{}
	for _, run := range plan.Runs {
		counts[run.ExecutionTarget]++
	}
	for _, target := range DefaultExecutionTargets() {
		if counts[target] != 3 {
			t.Fatalf("target %q has %d client runs, want 3", target, counts[target])
		}
	}
}

func TestPlanPersistsNamedServerLink(t *testing.T) {
	link, err := ResolveServerLinkProfile(Link10Gbit, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver},
		Transports:  []Transport{Plaintext},
		ServerLink:  link,
		Repetitions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ServerLink != link || plan.Runs[0].ServerLink != link {
		t.Fatalf("server link was not persisted: %#v / %#v", plan.ServerLink, plan.Runs[0].ServerLink)
	}
}

func TestPlanRejectsUnknownProfile(t *testing.T) {
	_, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver},
		Transports:  []Transport{Plaintext},
		Profile:     "fixture-specific-fast-path",
		Repetitions: 1,
	})
	if err == nil {
		t.Fatal("unknown profile should not validate")
	}
}

func TestPlanRejectsWarmOrDuplicateRun(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver, SABnzbd, NZBGet},
		Transports:  []Transport{Plaintext},
		Repetitions: 1,
		Seed:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan.Runs[0].FreshClientState = false
	if err := plan.Validate(); err == nil {
		t.Fatal("warm-state plan should not validate")
	}
}

func TestDefaultProfilesMakeSABTLSExplicitlyUnverified(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver, SABnzbd, NZBGet},
		Transports:  []Transport{TLS},
		Repetitions: 1,
		Seed:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range plan.Runs {
		switch run.Client {
		case SABnzbd:
			if run.TLSValidation != TLSDisabled || run.TransportLabel != "tls-unverified" {
				t.Fatalf("SAB TLS metadata = %#v", run)
			}
		default:
			if run.TLSValidation != TLSCAVerified || run.TransportLabel != "tls-ca-verified" {
				t.Fatalf("verified client TLS metadata = %#v", run)
			}
		}
	}
}
