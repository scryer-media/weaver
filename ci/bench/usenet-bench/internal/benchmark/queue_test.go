package benchmark

import "testing"

func TestQueueSuitesKeepEveryFixtureInOneClientQueue(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture-a", "fixture-b", "fixture-c"},
		Clients:     []Client{Weaver, SABnzbd, NZBGet},
		Transports:  []Transport{Plaintext, TLS},
		Targets:     []ExecutionTarget{DockerLinux},
		Repetitions: 2,
		Seed:        17,
	})
	if err != nil {
		t.Fatal(err)
	}
	suites := queueSuites(plan, DockerLinux)
	if want := 20; len(suites) != want {
		t.Fatalf("queue suite count = %d, want %d", len(suites), want)
	}
	seenRuns := map[string]bool{}
	for _, suite := range suites {
		if got, want := len(suite.Runs), len(plan.FixtureIDs); got != want {
			t.Fatalf("%s contains %d jobs, want %d", suite.ID, got, want)
		}
		first := suite.Runs[0]
		for _, run := range suite.Runs {
			if run.Client != first.Client || run.ArchiveToolchain != first.ArchiveToolchain || run.Transport != first.Transport || run.TLSValidation != first.TLSValidation || run.Repetition != first.Repetition {
				t.Fatalf("%s mixes queue lanes: %#v versus %#v", suite.ID, first, run)
			}
			if seenRuns[run.ID] {
				t.Fatalf("run %s appears in more than one queue suite", run.ID)
			}
			seenRuns[run.ID] = true
		}
	}
	if got, want := len(seenRuns), len(plan.Runs); got != want {
		t.Fatalf("queued runs = %d, planned runs = %d", got, want)
	}
}

func TestQueueInputRejectsRepeatedRun(t *testing.T) {
	input := QueueInput{
		SchemaVersion: 1,
		SuiteID:       "queue-0001",
		Jobs: []QueueInputJob{
			{RunID: "run-0001", FixtureID: "fixture-a", NZBPath: "/fixtures/a.nzb"},
			{RunID: "run-0001", FixtureID: "fixture-b", NZBPath: "/fixtures/b.nzb"},
		},
	}
	if err := input.Validate(); err == nil {
		t.Fatal("repeated queue run should be rejected")
	}
}
