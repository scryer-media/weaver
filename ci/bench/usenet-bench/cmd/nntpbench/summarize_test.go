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
	if len(report.Comparisons) != 1 || report.Comparisons[0].Summary.Count != 20 {
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
		Repetition:       repetition,
	}
	adapterJob := benchmark.QueueJobResult{
		RunID:                           run.ID,
		SubmissionToTerminalNanoseconds: measurement,
		TerminalObservationUncertainty:  measurement / 200,
	}
	return benchmark.QueueArtifact{
		SchemaVersion:  6,
		SuiteID:        run.ID,
		SubmissionMode: benchmark.SubmissionModeSequential,
		Runs:           []benchmark.Run{run},
		Status:         "passed",
		AdapterResult: &benchmark.QueueAdapterResult{
			SchemaVersion:            5,
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
