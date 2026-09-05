package main

import (
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func queueDrainTestArtifact(t *testing.T, copies int) (benchmark.QueueArtifact, map[string]benchmark.Run) {
	t.Helper()
	plan, err := benchmark.BuildPlan(benchmark.PlanOptions{
		FixtureIDs:        []string{"direct-mkv-200mb"},
		Clients:           []benchmark.Client{benchmark.Weaver},
		ArchiveToolchains: []benchmark.ArchiveToolchain{benchmark.VanillaArchiveToolchain},
		Transports:        []benchmark.Transport{benchmark.TLS},
		Targets:           []benchmark.ExecutionTarget{benchmark.DockerLinux},
		Profile:           benchmark.ProfileEquivalentThroughput,
		Repetitions:       copies,
		Seed:              3,
	})
	if err != nil {
		t.Fatal(err)
	}
	planned := make(map[string]benchmark.Run, len(plan.Runs))
	for _, run := range plan.Runs {
		planned[run.ID] = run
	}
	started := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	completed := started.Add(90 * time.Second)
	verified := completed.Add(2 * time.Second)
	first := plan.Runs[0]
	artifact := benchmark.QueueArtifact{
		SchemaVersion:                7,
		SuiteID:                      "queue-transition-0001",
		SubmissionMode:               benchmark.SubmissionModeQueueDrain,
		Runs:                         append([]benchmark.Run(nil), plan.Runs...),
		Status:                       "passed",
		QueueWallClockNanoseconds:    completed.Sub(started).Nanoseconds(),
		VerifiedWallClockNanoseconds: verified.Sub(started).Nanoseconds(),
		QueueVerifiedAt:              &verified,
		AdapterResult: &benchmark.QueueAdapterResult{
			SchemaVersion:            6,
			SuiteID:                  "queue-transition-0001",
			SubmissionMode:           benchmark.SubmissionModeQueueDrain,
			Client:                   first.Client,
			ArchiveToolchain:         first.ArchiveToolchain,
			ArchiveToolchainIdentity: "stock-test",
			ExecutionTarget:          first.ExecutionTarget,
			Transport:                first.Transport,
			TLSValidation:            first.TLSValidation,
			TransportLabel:           first.TransportLabel,
			ServerLink:               first.ServerLink,
			StorageProfile:           first.StorageProfile,
			QueueStartedAt:           started,
			QueueCompletedAt:         completed,
			ClientIdentity:           "sha256:test-weaver",
			ClientVersion:            "test",
			RenderedConfigSHA256:     strings.Repeat("a", 64),
		},
	}
	for _, run := range plan.Runs {
		job := benchmark.QueueJobResult{RunID: run.ID, TerminalStatus: "succeeded"}
		artifact.AdapterResult.Jobs = append(artifact.AdapterResult.Jobs, job)
		artifact.Jobs = append(artifact.Jobs, benchmark.QueueJobArtifact{
			Run:           run,
			Outcome:       "completed",
			Verification:  &benchmark.OutputVerification{FixtureID: run.FixtureID},
			AdapterResult: job,
		})
	}
	return artifact, planned
}

func TestQueueDrainLaneReportsVerifiedDrainWallClock(t *testing.T) {
	artifact, planned := queueDrainTestArtifact(t, 10)
	lane, err := queueDrainLaneFor(artifact, planned)
	if err != nil {
		t.Fatal(err)
	}
	if lane.Client != benchmark.Weaver || lane.Copies != 10 || lane.Status != "passed" || lane.CopiesDidNotFinish != 0 ||
		lane.QueueWallClockNanoseconds != (90*time.Second).Nanoseconds() || lane.VerifiedWallClockNanoseconds != (92*time.Second).Nanoseconds() ||
		lane.TransportLabel != "tls-ca-verified" || lane.FixtureID != "direct-mkv-200mb" || lane.Profile != benchmark.ProfileEquivalentThroughput {
		t.Fatalf("unexpected lane: %#v", lane)
	}

	// A copy that did not finish leaves the lane with its failure and no time.
	dnf := artifact
	dnf.Status = "completed_with_dnf"
	dnf.Error = "queue-transition job run-0004: client terminal failure: Failed"
	dnf.Jobs = append([]benchmark.QueueJobArtifact(nil), artifact.Jobs...)
	dnf.Jobs[3].Outcome = "dnf"
	dnf.Jobs[3].Error = "client terminal failure: Failed"
	lane, err = queueDrainLaneFor(dnf, planned)
	if err != nil {
		t.Fatal(err)
	}
	if lane.Status != "completed_with_dnf" || lane.CopiesDidNotFinish != 1 || lane.QueueWallClockNanoseconds != 0 || lane.VerifiedWallClockNanoseconds != 0 || lane.Error == "" {
		t.Fatalf("did-not-finish lane was reported with a drain time: %#v", lane)
	}

	// A passed lane whose copies were never verified, one whose runs are not
	// the plan's, and a harness failure are all refused.
	unverified := artifact
	unverified.Jobs = append([]benchmark.QueueJobArtifact(nil), artifact.Jobs...)
	unverified.Jobs[0].Verification = nil
	if _, err := queueDrainLaneFor(unverified, planned); err == nil {
		t.Fatal("lane with an unverified copy was accepted")
	}
	unbound := artifact
	unbound.Runs = append([]benchmark.Run(nil), artifact.Runs...)
	unbound.Runs[2].Repetition = 99
	if _, err := queueDrainLaneFor(unbound, planned); err == nil {
		t.Fatal("lane not bound to the snapshotted plan was accepted")
	}
	failed := artifact
	failed.Status = "failed"
	if _, err := queueDrainLaneFor(failed, planned); err == nil {
		t.Fatal("harness-failed lane was accepted")
	}
}
