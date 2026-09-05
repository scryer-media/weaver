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
	// The stock 3×3 matrix has nine lanes. Docker adds two separately labelled
	// Rarpar lanes (SABnzbd and NZBGet), for eleven lanes per tuple.
	if got, want := len(first.Runs), 132; got != want {
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

func TestDefaultPlanCreatesStockThreeByThreeMatrixAndDockerRarparLanes(t *testing.T) {
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
	if got, want := len(plan.Runs), 11; got != want {
		t.Fatalf("runs = %d, want %d", got, want)
	}
	counts := map[ExecutionTarget]int{}
	for _, run := range plan.Runs {
		counts[run.ExecutionTarget]++
	}
	if counts[DockerLinux] != 5 {
		t.Fatalf("Docker target has %d runs, want three vanilla plus two Rarpar lanes", counts[DockerLinux])
	}
	for _, target := range []ExecutionTarget{MacOSNative, WindowsNative} {
		if counts[target] != 3 {
			t.Fatalf("target %q has %d vanilla client runs, want 3", target, counts[target])
		}
	}
	for _, run := range plan.Runs {
		if run.ArchiveToolchain == RarparArchiveToolchain && run.ExecutionTarget != DockerLinux {
			t.Fatalf("Rarpar run escaped the Docker target: %#v", run)
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

// TestPlanClientExclusionsDropOnlyTheNamedLane covers a client the plan
// deliberately does not run on one fixture: its lane disappears from that
// fixture's blocks and nowhere else, the plan validates against the reduced
// count, and the exclusion is persisted with its reason.
func TestPlanClientExclusionsDropOnlyTheNamedLane(t *testing.T) {
	exclusion := ClientExclusion{Client: SABnzbd, FixtureID: "recovery-volume", Reason: "does not use .rev recovery volumes"}
	options := PlanOptions{
		FixtureIDs:        []string{"recovery-volume", "plain"},
		Clients:           []Client{Weaver, SABnzbd, NZBGet},
		ArchiveToolchains: []ArchiveToolchain{VanillaArchiveToolchain},
		Transports:        []Transport{TLS},
		Targets:           []ExecutionTarget{DockerLinux},
		Profile:           ProfileStock,
		Repetitions:       3,
		Seed:              5,
		ClientExclusions:  []ClientExclusion{exclusion},
	}
	plan, err := BuildPlan(options)
	if err != nil {
		t.Fatal(err)
	}
	// 3 clients × 2 fixtures × 3 repetitions, minus the 3 excluded blocks.
	if got, want := len(plan.Runs), 15; got != want {
		t.Fatalf("runs = %d, want %d", got, want)
	}
	perFixtureClient := map[string]map[Client]int{}
	for _, run := range plan.Runs {
		if perFixtureClient[run.FixtureID] == nil {
			perFixtureClient[run.FixtureID] = map[Client]int{}
		}
		perFixtureClient[run.FixtureID][run.Client]++
	}
	if perFixtureClient["recovery-volume"][SABnzbd] != 0 || perFixtureClient["recovery-volume"][Weaver] != 3 || perFixtureClient["plain"][SABnzbd] != 3 {
		t.Fatalf("exclusion did not remove exactly the named lane: %#v", perFixtureClient)
	}
	if !reflect.DeepEqual(plan.ClientExclusions, []ClientExclusion{exclusion}) {
		t.Fatalf("exclusion was not persisted: %#v", plan.ClientExclusions)
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("plan with exclusion rejected: %v", err)
	}
	if recorded, ok := ClientExclusionFor(plan.ClientExclusions, SABnzbd, "recovery-volume"); !ok || recorded != exclusion {
		t.Fatalf("ClientExclusionFor = %#v, %v", recorded, ok)
	}
	if _, ok := ClientExclusionFor(plan.ClientExclusions, SABnzbd, "plain"); ok {
		t.Fatal("ClientExclusionFor matched a fixture that is not excluded")
	}

	// A plan that schedules an excluded pair anyway, or that lost its
	// exclusion record, no longer validates.
	tampered := plan
	tampered.ClientExclusions = nil
	if err := tampered.Validate(); err == nil {
		t.Fatal("plan short of its excluded runs validated without the exclusion record")
	}
	scheduled := plan
	scheduled.Runs = append([]Run(nil), plan.Runs...)
	for index := range scheduled.Runs {
		if scheduled.Runs[index].FixtureID == "recovery-volume" && scheduled.Runs[index].Client == Weaver {
			scheduled.Runs[index].Client = SABnzbd
			break
		}
	}
	if err := scheduled.Validate(); err == nil {
		t.Fatal("plan scheduling an excluded client on its fixture validated")
	}

	for name, bad := range map[string]ClientExclusion{
		"no reason":          {Client: SABnzbd, FixtureID: "plain"},
		"undeclared client":  {Client: "other", FixtureID: "plain", Reason: "x"},
		"undeclared fixture": {Client: SABnzbd, FixtureID: "missing", Reason: "x"},
	} {
		options := options
		options.ClientExclusions = []ClientExclusion{bad}
		if _, err := BuildPlan(options); err == nil {
			t.Fatalf("%s exclusion was accepted", name)
		}
	}
	repeated := options
	repeated.ClientExclusions = []ClientExclusion{exclusion, exclusion}
	if _, err := BuildPlan(repeated); err == nil {
		t.Fatal("repeated exclusion was accepted")
	}
	everyone := options
	everyone.ClientExclusions = []ClientExclusion{
		{Client: Weaver, FixtureID: "plain", Reason: "x"},
		{Client: SABnzbd, FixtureID: "plain", Reason: "x"},
		{Client: NZBGet, FixtureID: "plain", Reason: "x"},
	}
	if _, err := BuildPlan(everyone); err == nil {
		t.Fatal("a fixture with every client excluded was accepted")
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
