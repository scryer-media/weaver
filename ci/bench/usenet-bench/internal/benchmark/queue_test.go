package benchmark

import (
	"strings"
	"testing"
	"time"
)

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

func TestQueueTransitionGroupsTwentyDuplicatesIntoOneLane(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"direct-mkv-200mb"},
		Clients:     []Client{Weaver, SABnzbd, NZBGet},
		Transports:  []Transport{Plaintext, TLS},
		Targets:     []ExecutionTarget{DockerLinux},
		Repetitions: 20,
		Seed:        17,
	})
	if err != nil {
		t.Fatal(err)
	}
	suites, err := queueTransitionSuites(plan, DockerLinux)
	if err != nil {
		t.Fatal(err)
	}
	if want := 10; len(suites) != want {
		t.Fatalf("queue transition suite count = %d, want %d", len(suites), want)
	}
	for _, suite := range suites {
		if got := len(suite.Runs); got != 20 {
			t.Fatalf("%s contains %d jobs, want 20", suite.ID, got)
		}
		for _, run := range suite.Runs {
			if run.FixtureID != "direct-mkv-200mb" {
				t.Fatalf("%s includes fixture %s", suite.ID, run.FixtureID)
			}
		}
	}
}

func TestQueueTransitionRejectsNonDuplicatePlan(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture-a", "fixture-b"},
		Clients:     []Client{Weaver},
		Transports:  []Transport{Plaintext},
		Targets:     []ExecutionTarget{DockerLinux},
		Repetitions: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queueTransitionSuites(plan, DockerLinux); err == nil {
		t.Fatal("queue-transition accepted a multi-fixture plan")
	}
}

func TestSequentialQueueResultRequiresImmediateUsableOutputTiming(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture-a"},
		Clients:     []Client{Weaver},
		Transports:  []Transport{Plaintext},
		Targets:     []ExecutionTarget{DockerLinux},
		Repetitions: 1,
		Seed:        17,
	})
	if err != nil {
		t.Fatal(err)
	}
	suite := queueSuites(plan, DockerLinux)[0]
	run := suite.Runs[0]
	queuedAt := time.Now().UTC()
	completedAt := queuedAt.Add(time.Second)
	resourceMetrics := ResourceMetrics{
		CPUTimeNanoseconds:  MeasuredMeasurement("client_container", "test", "1", 1),
		InstructionsRetired: UnavailableMeasurement("client_container", "test", "1", "not collected"),
	}
	result := QueueAdapterResult{
		SchemaVersion:            4,
		SuiteID:                  suite.ID,
		SubmissionMode:           SubmissionModeSequential,
		Client:                   run.Client,
		ArchiveToolchain:         run.ArchiveToolchain,
		ArchiveToolchainIdentity: "stock",
		ExecutionTarget:          run.ExecutionTarget,
		Transport:                run.Transport,
		TLSValidation:            run.TLSValidation,
		TransportLabel:           run.TransportLabel,
		ServerLink:               run.ServerLink,
		QueueStartedAt:           queuedAt,
		QueueCompletedAt:         completedAt,
		StatusPollIntervalNanos:  time.Millisecond.Nanoseconds(),
		ClientIdentity:           "test-client",
		ClientVersion:            "test-version",
		RenderedConfigSHA256:     strings.Repeat("a", 64),
		ResourceMetrics:          resourceMetrics,
		Jobs: []QueueJobResult{{
			RunID:                       run.ID,
			JobID:                       "1",
			QueuedAt:                    queuedAt,
			FixtureWallClockNanoseconds: completedAt.Sub(queuedAt).Nanoseconds(),
			ResourceMetrics:             &resourceMetrics,
			TerminalStatus:              "succeeded",
			Verification:                &OutputVerification{FixtureID: run.FixtureID},
			OutputDeleted:               true,
			ProcessingTimingError:       "terminal status was observed before active state",
			CompletionAt:                completedAt,
		}},
	}
	if err := result.ValidateFor(suite, SubmissionModeSequential); err == nil {
		t.Fatal("sequential result without verified usable timing was accepted")
	}
	usableAt := completedAt.Add(time.Millisecond)
	result.Jobs[0].UsableOutputAt = usableAt
	result.Jobs[0].UsableWallClockNanoseconds = usableAt.Sub(queuedAt).Nanoseconds()
	result.QueueCompletedAt = usableAt
	if err := result.ValidateFor(suite, SubmissionModeSequential); err != nil {
		t.Fatalf("sequential result with verified usable timing was rejected: %v", err)
	}

	result.Jobs[0].Verification = nil
	result.Jobs[0].UsableOutputAt = time.Time{}
	result.Jobs[0].UsableWallClockNanoseconds = 0
	result.Jobs[0].OutputVerificationError = "missing expected output file"
	result.QueueCompletedAt = completedAt
	if err := result.ValidateFor(suite, SubmissionModeSequential); err != nil {
		t.Fatalf("sequential result with a recorded output verification failure was rejected: %v", err)
	}
}

func TestQueueJobOutcome(t *testing.T) {
	for name, result := range map[string]QueueJobResult{
		"completed":            {TerminalStatus: "succeeded"},
		"terminal failure":     {TerminalStatus: "failed", TerminalError: "Failed"},
		"verification failure": {TerminalStatus: "succeeded", OutputVerificationError: "missing expected output file"},
	} {
		want := "completed"
		if name != "completed" {
			want = "dnf"
		}
		if got := queueJobOutcome(result); got != want {
			t.Errorf("%s outcome = %q, want %q", name, got, want)
		}
	}
}
