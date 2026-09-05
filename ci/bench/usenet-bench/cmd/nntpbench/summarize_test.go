package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func TestBuildSummaryReportRequiresCompleteVerifiedPairs(t *testing.T) {
	artifacts := make([]benchmark.QueueArtifact, 0, 40)
	for repetition := 1; repetition <= 20; repetition++ {
		artifacts = append(artifacts,
			summaryTestArtifact(benchmark.Weaver, repetition, int64(100+repetition)),
			summaryTestArtifact(benchmark.SABnzbd, repetition, int64(80+repetition)),
		)
	}
	report, err := buildSummaryReport(artifacts, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Comparisons) != 1 || report.Comparisons[0].Summary == nil || report.Comparisons[0].Summary.Count != 20 || report.Comparisons[0].Completion.PairedBlocks != 20 {
		t.Fatalf("unexpected summary report: %#v", report)
	}

	artifacts[len(artifacts)-1].AdapterResult.RenderedConfigSHA256 = strings.Repeat("b", 64)
	if _, err := buildSummaryReport(artifacts, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000); err == nil {
		t.Fatal("summary accepted a rendered configuration change within one stratum")
	}
	artifacts[len(artifacts)-1].AdapterResult.RenderedConfigSHA256 = strings.Repeat("a", 64)
	artifacts[len(artifacts)-1].Jobs[0].Verification = nil
	if _, err := buildSummaryReport(artifacts, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000); err == nil {
		t.Fatal("summary accepted an unverified measurement")
	}
}

func TestBuildSummaryReportRejectsIncompletePair(t *testing.T) {
	artifacts := []benchmark.QueueArtifact{
		summaryTestArtifact(benchmark.Weaver, 1, 100),
		summaryTestArtifact(benchmark.SABnzbd, 2, 80),
	}
	if _, err := buildSummaryReport(artifacts, benchmark.Weaver, benchmark.SABnzbd, 2, 17, 100); err == nil {
		t.Fatal("summary accepted unpaired randomized blocks")
	}
}

func TestBuildSummaryReportCountsDidNotFinishBlocks(t *testing.T) {
	artifacts := make([]benchmark.QueueArtifact, 0, 40)
	for repetition := 1; repetition <= 20; repetition++ {
		artifacts = append(artifacts, summaryTestArtifact(benchmark.Weaver, repetition, int64(100+repetition)))
		if repetition <= 3 {
			artifacts = append(artifacts, summaryTestDidNotFinishArtifact(benchmark.SABnzbd, repetition))
		} else {
			artifacts = append(artifacts, summaryTestArtifact(benchmark.SABnzbd, repetition, int64(80+repetition)))
		}
	}
	report, err := buildSummaryReport(artifacts, benchmark.SABnzbd, benchmark.Weaver, 17, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Comparisons) != 1 {
		t.Fatalf("unexpected summary report: %#v", report)
	}
	comparison := report.Comparisons[0]
	if comparison.Summary == nil || comparison.Summary.Count != 17 || comparison.ComparisonWithheld != "" {
		t.Fatalf("did-not-finish blocks were not excluded from the paired summary: %#v", comparison)
	}
	want := completionCounts{BlocksObserved: 20, PairedBlocks: 17, BaselineDidNotFinish: 3, CandidateDidNotFinish: 0}
	if comparison.Completion != want {
		t.Fatalf("completion counts = %#v, want %#v", comparison.Completion, want)
	}

	// Below the minimum because of the failures: the stratum stays in the
	// report with its counts, and the comparison is withheld rather than the
	// whole summary failing.
	report, err = buildSummaryReport(artifacts, benchmark.SABnzbd, benchmark.Weaver, 18, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	comparison = report.Comparisons[0]
	if comparison.Summary != nil || comparison.ComparisonWithheld == "" || comparison.Completion != want {
		t.Fatalf("stratum short of pairs through failures was not withheld: %#v", comparison)
	}

	// A did-not-finish artifact must carry the recorded failure.
	artifacts[1].Jobs[0].Error = ""
	if _, err := buildSummaryReport(artifacts, benchmark.SABnzbd, benchmark.Weaver, 17, 17, 1_000); err == nil {
		t.Fatal("summary accepted a did-not-finish artifact without a recorded job failure")
	}
}

func TestBuildSummaryReportRejectsShapedArtifactWithoutAttestation(t *testing.T) {
	artifacts := []benchmark.QueueArtifact{
		summaryTestArtifact(benchmark.Weaver, 1, 100),
		summaryTestArtifact(benchmark.SABnzbd, 1, 80),
		summaryTestArtifact(benchmark.Weaver, 2, 101),
		summaryTestArtifact(benchmark.SABnzbd, 2, 81),
	}
	shaped := benchmark.ServerLinkProfile{ID: benchmark.Link1Gbit, EgressBitsPerSecond: 1_000_000_000, BurstBytes: 1_048_576}
	for index := range artifacts {
		artifacts[index].Runs[0].ServerLink = shaped
		artifacts[index].Jobs[0].Run.ServerLink = shaped
		artifacts[index].AdapterResult.ServerLink = shaped
	}
	if _, err := buildSummaryReport(artifacts, benchmark.Weaver, benchmark.SABnzbd, 2, 17, 100); err == nil {
		t.Fatal("summary accepted shaped artifacts without shaper attestation")
	}
}

func TestLoadSummaryExecutionContextBindsSnapshotDigests(t *testing.T) {
	root := t.TempDir()
	plan, err := benchmark.BuildPlan(benchmark.PlanOptions{
		FixtureIDs:  []string{"fixture-a"},
		Clients:     []benchmark.Client{benchmark.Weaver},
		Transports:  []benchmark.Transport{benchmark.Plaintext},
		Targets:     []benchmark.ExecutionTarget{benchmark.DockerLinux},
		Profile:     benchmark.ProfileStock,
		Repetitions: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(root, "source-plan.json")
	if err := benchmark.WritePlan(planPath, plan); err != nil {
		t.Fatal(err)
	}
	catalog := benchmark.AdapterCatalog{SchemaVersion: 4, Adapters: []benchmark.Adapter{{
		Client: benchmark.Weaver, ArchiveToolchain: benchmark.VanillaArchiveToolchain,
		Target: benchmark.DockerLinux, Command: []string{"adapter"},
	}}}
	catalogContents, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(root, "source-adapters.json")
	if err := os.WriteFile(catalogPath, catalogContents, 0o644); err != nil {
		t.Fatal(err)
	}
	artifactRoot := filepath.Join(root, "artifacts")
	if err := writeExecutionManifest(artifactRoot, "sequential", planPath, catalogPath, string(benchmark.DockerLinux), benchmark.ProfileStock, nil, mustRead(t, planPath), catalogContents); err != nil {
		t.Fatal(err)
	}
	plannedRuns, err := loadSummaryExecutionContext(artifactRoot)
	if err != nil || len(plannedRuns) != 2 {
		t.Fatalf("load bound execution context: runs=%d error=%v", len(plannedRuns), err)
	}
	if err := os.WriteFile(filepath.Join(artifactRoot, "plan.snapshot.json"), []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSummaryExecutionContext(artifactRoot); err == nil {
		t.Fatal("summary accepted a plan snapshot whose digest no longer matches the execution manifest")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

// summaryTestDidNotFinishArtifact mirrors what the controller writes when the
// client reaches a terminal failure: status completed_with_dnf, job outcome
// dnf with its error, no verification and no measurement.
func summaryTestDidNotFinishArtifact(client benchmark.Client, repetition int) benchmark.QueueArtifact {
	artifact := summaryTestArtifact(client, repetition, 0)
	artifact.Status = "completed_with_dnf"
	artifact.Error = "1 queue job(s) did not finish"
	artifact.AdapterResult.Jobs[0].TerminalStatus = "failed"
	artifact.AdapterResult.Jobs[0].TerminalError = "Failed"
	artifact.Jobs[0].AdapterResult = artifact.AdapterResult.Jobs[0]
	artifact.Jobs[0].Outcome = "dnf"
	artifact.Jobs[0].Verification = nil
	artifact.Jobs[0].Error = "client terminal failure: Failed"
	return artifact
}

func summaryTestArtifact(client benchmark.Client, repetition int, measurement int64) benchmark.QueueArtifact {
	run := benchmark.Run{
		ID:               fmt.Sprintf("run-%s-%d", client, repetition),
		FixtureID:        "fixture-a",
		Client:           client,
		ArchiveToolchain: benchmark.VanillaArchiveToolchain,
		ExecutionTarget:  benchmark.DockerLinux,
		Transport:        benchmark.Plaintext,
		TLSValidation:    benchmark.TLSNotApplicable,
		TransportLabel:   "plaintext",
		Profile:          benchmark.ProfileStock,
		ServerLink:       benchmark.DefaultServerLinkProfile(),
		StorageProfile:   benchmark.DefaultStorageProfile(),
		Repetition:       repetition,
	}
	adapterJob := benchmark.QueueJobResult{
		RunID:                           run.ID,
		SubmissionToTerminalNanoseconds: measurement,
		TerminalObservationUncertainty:  measurement / 200,
	}
	return benchmark.QueueArtifact{
		SchemaVersion:  7,
		SuiteID:        run.ID,
		SubmissionMode: benchmark.SubmissionModeSequential,
		Runs:           []benchmark.Run{run},
		Status:         "passed",
		AdapterResult: &benchmark.QueueAdapterResult{
			SchemaVersion:            6,
			SuiteID:                  run.ID,
			SubmissionMode:           benchmark.SubmissionModeSequential,
			Client:                   run.Client,
			ArchiveToolchain:         run.ArchiveToolchain,
			ArchiveToolchainIdentity: "stock-test",
			ExecutionTarget:          run.ExecutionTarget,
			Transport:                run.Transport,
			TLSValidation:            run.TLSValidation,
			TransportLabel:           run.TransportLabel,
			ServerLink:               run.ServerLink,
			StorageProfile:           run.StorageProfile,
			Jobs:                     []benchmark.QueueJobResult{adapterJob},
			ClientIdentity:           "sha256:test-" + string(run.Client),
			ClientVersion:            "test",
			RenderedConfigSHA256:     strings.Repeat("a", 64),
		},
		Jobs: []benchmark.QueueJobArtifact{{
			Run:           run,
			Outcome:       "completed",
			Verification:  &benchmark.OutputVerification{FixtureID: run.FixtureID},
			AdapterResult: adapterJob,
		}},
	}
}
