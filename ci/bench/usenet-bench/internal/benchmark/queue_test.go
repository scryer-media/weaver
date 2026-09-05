package benchmark

import (
	"errors"

	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

func TestQueueSuitesFollowPersistedPlanOrderWithFreshSingleRunSuites(t *testing.T) {
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
	if want := len(plan.Runs); len(suites) != want {
		t.Fatalf("queue suite count = %d, want %d", len(suites), want)
	}
	for index, suite := range suites {
		if got := len(suite.Runs); got != 1 {
			t.Fatalf("%s contains %d jobs, want one fresh client invocation", suite.ID, got)
		}
		if got, want := suite.Runs[0], plan.Runs[index]; got != want {
			t.Fatalf("suite %d run = %#v, want persisted plan run %#v", index, got, want)
		}
	}
}

func TestQueueInputRejectsRepeatedRun(t *testing.T) {
	input := QueueInput{
		SchemaVersion:  3,
		SuiteID:        "queue-0001",
		SubmissionMode: SubmissionModeQueued,
		Jobs: []QueueInputJob{
			{RunID: "run-0001", FixtureID: "fixture-a", NZBPath: "/fixtures/a.nzb"},
			{RunID: "run-0001", FixtureID: "fixture-b", NZBPath: "/fixtures/b.nzb"},
		},
	}
	if err := input.Validate(); err == nil {
		t.Fatal("repeated queue run should be rejected")
	}
}

func TestQueueInputRejectsInvalidSubmissionMode(t *testing.T) {
	input := QueueInput{
		SchemaVersion:  3,
		SuiteID:        "queue-0001",
		SubmissionMode: "bogus",
		Jobs:           []QueueInputJob{{RunID: "run-0001", FixtureID: "fixture", NZBPath: "fixture.nzb"}},
	}
	if err := input.Validate(); err == nil {
		t.Fatal("schema-3 queue input accepted an invalid submission mode")
	}
}

func TestQueueTransitionGroupsPlannedRepetitionsIntoOneLane(t *testing.T) {
	for _, copies := range []int{20, 10} {
		plan, err := BuildPlan(PlanOptions{
			FixtureIDs:  []string{"direct-mkv-200mb"},
			Clients:     []Client{Weaver, SABnzbd, NZBGet},
			Transports:  []Transport{Plaintext, TLS},
			Targets:     []ExecutionTarget{DockerLinux},
			Repetitions: copies,
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
			if got := len(suite.Runs); got != copies {
				t.Fatalf("%s contains %d jobs, want %d", suite.ID, got, copies)
			}
			for _, run := range suite.Runs {
				if run.FixtureID != "direct-mkv-200mb" {
					t.Fatalf("%s includes fixture %s", suite.ID, run.FixtureID)
				}
			}
		}
	}
	single, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"direct-mkv-200mb"},
		Clients:     []Client{Weaver},
		Transports:  []Transport{Plaintext},
		Targets:     []ExecutionTarget{DockerLinux},
		Repetitions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queueTransitionSuites(single, DockerLinux); err == nil {
		t.Fatal("queue-transition accepted a one-copy plan, which is a sequential run, not a queue")
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

func TestVerifyQueueTransitionOutputsRequiresTwentyDistinctCopies(t *testing.T) {
	fixtureDir, outputDir, contents := writeQueueTransitionVerificationFixture(t)
	for index := 0; index < 20; index++ {
		writeQueueTransitionOutputCopy(t, outputDir, index, contents)
	}
	verifications, failedIndex, err := verifyQueueTransitionOutputs(fixtureDir, outputDir, 20)
	if err != nil || failedIndex != -1 || len(verifications) != 20 {
		t.Fatalf("queue transition verification = (%d instances, failed %d, %v), want 20 verified instances", len(verifications), failedIndex, err)
	}
}

func TestVerifyQueueTransitionOutputsRejectsMissingOrCorruptCopy(t *testing.T) {
	for name, mutate := range map[string]func(t *testing.T, outputDir string, contents []byte){
		"missing": func(t *testing.T, outputDir string, _ []byte) {
			if err := os.Remove(filepath.Join(outputDir, "copy-19", "payload.mkv")); err != nil {
				t.Fatal(err)
			}
		},
		"corrupt": func(t *testing.T, outputDir string, contents []byte) {
			if err := os.WriteFile(filepath.Join(outputDir, "copy-19", "payload.mkv"), []byte(strings.Repeat("x", len(contents))), 0o644); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixtureDir, outputDir, contents := writeQueueTransitionVerificationFixture(t)
			for index := 0; index < 20; index++ {
				writeQueueTransitionOutputCopy(t, outputDir, index, contents)
			}
			mutate(t, outputDir, contents)
			if _, failedIndex, err := verifyQueueTransitionOutputs(fixtureDir, outputDir, 20); err == nil || failedIndex != 19 {
				t.Fatalf("queue transition verification = (failed %d, %v), want failure for copy 20", failedIndex, err)
			}
		})
	}
}

func TestVerifyQueueTransitionOutputsRejectsUnexpectedFile(t *testing.T) {
	fixtureDir, outputDir, contents := writeQueueTransitionVerificationFixture(t)
	for index := 0; index < 20; index++ {
		writeQueueTransitionOutputCopy(t, outputDir, index, contents)
	}
	if err := os.MkdirAll(filepath.Join(outputDir, "copy-20"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "copy-20", "unexpected.txt"), []byte("extra"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, failedIndex, err := verifyQueueTransitionOutputs(fixtureDir, outputDir, 20); err == nil || failedIndex != 19 || !strings.Contains(err.Error(), "unexpected unconsumed output") {
		t.Fatalf("queue transition verification = (failed %d, %v), want unexpected-file failure for copy 20", failedIndex, err)
	}
}

func writeQueueTransitionVerificationFixture(t *testing.T) (string, string, []byte) {
	t.Helper()
	fixtureDir := t.TempDir()
	outputDir := t.TempDir()
	contents := []byte("movie payload")
	digest, err := hashFile(writeQueueTransitionOutputFile(t, outputDir, "source", contents))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(outputDir, "source")); err != nil {
		t.Fatal(err)
	}
	writeVerificationManifest(t, fixtureDir, []fixture.FileDigest{{Path: "payload.mkv", Size: int64(len(contents)), BLAKE3: digest}})
	return fixtureDir, outputDir, contents
}

func writeQueueTransitionOutputCopy(t *testing.T, outputDir string, index int, contents []byte) {
	t.Helper()
	writeQueueTransitionOutputFile(t, outputDir, fmt.Sprintf("copy-%02d", index), contents)
}

func writeQueueTransitionOutputFile(t *testing.T, outputDir, directory string, contents []byte) string {
	t.Helper()
	path := filepath.Join(outputDir, directory, "payload.mkv")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSequentialQueueResultLeavesOutputForNeutralVerification(t *testing.T) {
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
	queuedAt := time.Now()
	completedAt := queuedAt.Add(time.Second)
	resourceMetrics := ResourceMetrics{
		CPUTimeNanoseconds:  MeasuredMeasurement("client_container", "test", "1", 1),
		InstructionsRetired: UnavailableMeasurement("client_container", "test", "1", "not collected"),
	}
	result := QueueAdapterResult{
		SchemaVersion:            6,
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
		StorageProfile:           run.StorageProfile,
		QueueStartedAt:           queuedAt,
		QueueCompletedAt:         completedAt,
		StatusPollIntervalNanos:  time.Millisecond.Nanoseconds(),
		ClientIdentity:           "test-client",
		ClientVersion:            "test-version",
		RenderedConfigSHA256:     strings.Repeat("a", 64),
		ResourceMetrics:          resourceMetrics,
		Jobs: []QueueJobResult{{
			RunID:                           run.ID,
			JobID:                           "1",
			SubmissionStartedAt:             queuedAt.Add(-time.Second),
			AcceptedAt:                      queuedAt,
			QueuedAt:                        queuedAt,
			FixtureWallClockNanoseconds:     completedAt.Sub(queuedAt).Nanoseconds(),
			ResourceMetrics:                 &resourceMetrics,
			TerminalStatus:                  "succeeded",
			ProcessingTimingError:           "terminal status was observed before active state",
			CompletionAt:                    completedAt,
			TerminalObservationLowerBound:   completedAt.Add(-time.Millisecond),
			TerminalObservedAt:              completedAt,
			TerminalObservationUncertainty:  time.Millisecond.Nanoseconds(),
			SubmissionToTerminalNanoseconds: completedAt.Sub(queuedAt.Add(-time.Second)).Nanoseconds(),
		}},
	}
	if err := result.ValidateFor(suite, SubmissionModeSequential); err != nil {
		t.Fatalf("sequential result that leaves output for neutral verification was rejected: %v", err)
	}
	// The run lasts two seconds, so 1 % would be 20 ms; the 100 ms absolute
	// floor governs here.
	result.Jobs[0].TerminalObservationLowerBound = completedAt.Add(-100 * time.Millisecond)
	result.Jobs[0].TerminalObservationUncertainty = (100 * time.Millisecond).Nanoseconds()
	if err := result.ValidateFor(suite, SubmissionModeSequential); err != nil {
		t.Fatalf("sequential result at the observation uncertainty floor was rejected: %v", err)
	}
	result.Jobs[0].TerminalObservationLowerBound = completedAt.Add(-100*time.Millisecond - time.Nanosecond)
	result.Jobs[0].TerminalObservationUncertainty = (100*time.Millisecond + time.Nanosecond).Nanoseconds()
	if err := result.ValidateFor(suite, SubmissionModeSequential); err == nil {
		t.Fatal("sequential result above the observation uncertainty floor was accepted")
	}

	// A job the adapter gave up waiting on is a recorded did-not-finish, and
	// like a client-reported failure it must carry its reason.
	result.Jobs[0].TerminalObservationLowerBound = completedAt.Add(-time.Millisecond)
	result.Jobs[0].TerminalObservationUncertainty = time.Millisecond.Nanoseconds()
	result.Jobs[0].TerminalStatus = "timed_out"
	if err := result.ValidateFor(suite, SubmissionModeSequential); err == nil {
		t.Fatal("timed-out result without a reason was accepted")
	}
	result.Jobs[0].TerminalError = "no terminal state within 20m0s of acceptance"
	if err := result.ValidateFor(suite, SubmissionModeSequential); err != nil {
		t.Fatalf("timed-out result with a reason was rejected: %v", err)
	}
	if queueJobOutcome(result.Jobs[0]) != "dnf" || !strings.Contains(terminalFailureDescription(result.Jobs[0]), "did not reach a terminal state") {
		t.Fatalf("timed-out job was not recorded as did-not-finish: %#v", result.Jobs[0])
	}
}

func TestQueueJobOutcome(t *testing.T) {
	for name, result := range map[string]QueueJobResult{
		"completed":        {TerminalStatus: "succeeded"},
		"terminal failure": {TerminalStatus: "failed", TerminalError: "Failed"},
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

func TestQueueArtifactDNFFailsTopLevelExecution(t *testing.T) {
	if !queueArtifactFailed(QueueArtifact{Status: "completed_with_dnf"}) {
		t.Fatal("completed_with_dnf must fail the top-level command")
	}
	if queueArtifactFailed(QueueArtifact{Status: "passed"}) {
		t.Fatal("passed artifact must not fail the top-level command")
	}
	// A client that did not finish is a recorded result, not a harness
	// failure; the two must stay distinguishable so a chained run can carry
	// on past the former and stop on the latter.
	if queueArtifactHarnessFailed(QueueArtifact{Status: "completed_with_dnf"}) {
		t.Fatal("completed_with_dnf is a client outcome, not a harness failure")
	}
	if !queueArtifactHarnessFailed(QueueArtifact{Status: "failed"}) {
		t.Fatal("failed must count as a harness failure")
	}
	var err error = &ClientDidNotFinishError{Suites: []string{"sequential-0001: 1 queue job(s) did not finish"}}
	var didNotFinish *ClientDidNotFinishError
	if !errors.As(err, &didNotFinish) || len(didNotFinish.Suites) != 1 || ExitStatusClientDidNotFinish == 1 || ExitStatusClientDidNotFinish == 0 {
		t.Fatalf("did-not-finish error is not distinguishable from a harness failure: %v", err)
	}
}

func TestQueueArtifactWriteFailurePropagates(t *testing.T) {
	artifact := QueueArtifact{Status: "passed"}
	persistQueueArtifact(t.TempDir(), &artifact)
	if artifact.Status != "failed" || !strings.Contains(artifact.Error, "write queue artifact") {
		t.Fatalf("artifact write failure was not propagated: %#v", artifact)
	}
}
