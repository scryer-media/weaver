package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

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
	report, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Comparisons) != 1 || report.Comparisons[0].Summary == nil || report.Comparisons[0].Summary.Count != 20 || report.Comparisons[0].Completion.PairedBlocks != 20 {
		t.Fatalf("unexpected summary report: %#v", report)
	}

	artifacts[len(artifacts)-1].AdapterResult.RenderedConfigSHA256 = strings.Repeat("b", 64)
	if _, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000); err == nil {
		t.Fatal("summary accepted a rendered configuration change within one stratum")
	}
	artifacts[len(artifacts)-1].AdapterResult.RenderedConfigSHA256 = strings.Repeat("a", 64)
	artifacts[len(artifacts)-1].Jobs[0].Verification = nil
	if _, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000); err == nil {
		t.Fatal("summary accepted an unverified measurement")
	}
}

func TestBuildSummaryReportRejectsIncompletePair(t *testing.T) {
	artifacts := []benchmark.QueueArtifact{
		summaryTestArtifact(benchmark.Weaver, 1, 100),
		summaryTestArtifact(benchmark.SABnzbd, 2, 80),
	}
	if _, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 2, 17, 100); err == nil {
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
	report, err := buildSummaryReport(artifacts, nil, benchmark.SABnzbd, benchmark.Weaver, 17, 17, 1_000)
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
	want := completionCounts{BlocksObserved: 20, PairedBlocks: 17, BaselineDidNotFinish: 3, CandidateDidNotFinish: 0, BaselineExcluded: 0, CandidateExcluded: 0}

	if comparison.Completion != want {
		t.Fatalf("completion counts = %#v, want %#v", comparison.Completion, want)
	}

	// Below the minimum because of the failures: the stratum stays in the
	// report with its counts, and the comparison is withheld rather than the
	// whole summary failing.
	report, err = buildSummaryReport(artifacts, nil, benchmark.SABnzbd, benchmark.Weaver, 18, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	comparison = report.Comparisons[0]
	if comparison.Summary != nil || comparison.ComparisonWithheld == "" || comparison.Completion != want {
		t.Fatalf("stratum short of pairs through failures was not withheld: %#v", comparison)
	}

	// A did-not-finish artifact must carry the recorded failure.
	artifacts[1].Jobs[0].Error = ""
	if _, err := buildSummaryReport(artifacts, nil, benchmark.SABnzbd, benchmark.Weaver, 17, 17, 1_000); err == nil {
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
	if _, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 2, 17, 100); err == nil {
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
	execution, err := loadSummaryExecutionContext(artifactRoot, "sequential")
	if err != nil || len(execution.PlannedRuns) != 2 || execution.Command != "sequential" {
		t.Fatalf("load bound execution context: runs=%d error=%v", len(execution.PlannedRuns), err)
	}
	if _, err := loadSummaryExecutionContext(artifactRoot, "queue-transition"); err == nil {
		t.Fatal("a sequential artifact root was accepted as a queue-transition root")
	}
	if err := os.WriteFile(filepath.Join(artifactRoot, "plan.snapshot.json"), []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSummaryExecutionContext(artifactRoot, "sequential"); err == nil {
		t.Fatal("summary accepted a plan snapshot whose digest no longer matches the execution manifest")
	}
}

// TestBuildSummaryReportPairsAcrossClientTLSPolicies covers the SABnzbd case:
// its TLS runs are labelled tls-unverified while the others are
// tls-ca-verified. The pairing must not key on that, and the comparison must
// still say how each client validated.
func TestBuildSummaryReportPairsAcrossClientTLSPolicies(t *testing.T) {
	artifacts := make([]benchmark.QueueArtifact, 0, 4)
	for repetition := 1; repetition <= 2; repetition++ {
		weaver := summaryTestArtifact(benchmark.Weaver, repetition, int64(100+repetition))
		summaryTestSetTLS(&weaver, benchmark.TLSCAVerified, "tls-ca-verified")
		sab := summaryTestArtifact(benchmark.SABnzbd, repetition, int64(80+repetition))
		summaryTestSetTLS(&sab, benchmark.TLSDisabled, "tls-unverified")
		artifacts = append(artifacts, weaver, sab)
	}
	report, err := buildSummaryReport(artifacts, nil, benchmark.SABnzbd, benchmark.Weaver, 2, 17, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Comparisons) != 1 || report.Comparisons[0].Completion.PairedBlocks != 2 || report.Comparisons[0].Stratum.Transport != benchmark.TLS {
		t.Fatalf("TLS blocks with differing client validation were not paired: %#v", report.Comparisons)
	}
	want := []clientTransportPolicy{
		{Client: benchmark.SABnzbd, TLSValidation: benchmark.TLSDisabled, TransportLabel: "tls-unverified"},
		{Client: benchmark.Weaver, TLSValidation: benchmark.TLSCAVerified, TransportLabel: "tls-ca-verified"},
	}
	if got := report.Comparisons[0].TransportPolicies; !reflect.DeepEqual(got, want) {
		t.Fatalf("transport policies = %#v, want %#v", got, want)
	}

	// A client's TLS policy changing inside one stratum is still a refusal:
	// it would mean two differently configured products were pooled.
	summaryTestSetTLS(&artifacts[3], benchmark.TLSCAVerified, "tls-ca-verified")
	if _, err := buildSummaryReport(artifacts, nil, benchmark.SABnzbd, benchmark.Weaver, 2, 17, 100); err == nil {
		t.Fatal("summary accepted a TLS policy change within one stratum")
	}
}

// TestBuildSummaryReportRecordsPlanExclusionsAsDidNotFinish covers a client
// the plan deliberately did not run on a fixture: every block counts as that
// client not finishing, the reason is reported, and the comparison is
// withheld rather than the summary failing on an "incomplete pair".
func TestBuildSummaryReportRecordsPlanExclusionsAsDidNotFinish(t *testing.T) {
	artifacts := []benchmark.QueueArtifact{
		summaryTestArtifact(benchmark.Weaver, 1, 100),
		summaryTestArtifact(benchmark.Weaver, 2, 101),
		summaryTestArtifact(benchmark.Weaver, 3, 102),
	}
	exclusion := benchmark.ClientExclusion{Client: benchmark.SABnzbd, FixtureID: "fixture-a", Reason: "does not use recovery volumes"}

	if _, err := buildSummaryReport(artifacts, nil, benchmark.SABnzbd, benchmark.Weaver, 3, 17, 100); err == nil {
		t.Fatal("summary accepted blocks with no baseline observation and no plan exclusion")
	}
	report, err := buildSummaryReport(artifacts, []benchmark.ClientExclusion{exclusion}, benchmark.SABnzbd, benchmark.Weaver, 3, 17, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Comparisons) != 1 {
		t.Fatalf("unexpected summary report: %#v", report)
	}
	comparison := report.Comparisons[0]
	want := completionCounts{BlocksObserved: 3, PairedBlocks: 0, BaselineDidNotFinish: 3, BaselineExcluded: 3}
	if comparison.Completion != want {
		t.Fatalf("completion counts = %#v, want %#v", comparison.Completion, want)
	}
	if comparison.Summary != nil || comparison.ComparisonWithheld == "" {
		t.Fatalf("excluded stratum was not withheld: %#v", comparison)
	}
	if !reflect.DeepEqual(comparison.ClientExclusions, []benchmark.ClientExclusion{exclusion}) {
		t.Fatalf("exclusion was not reported: %#v", comparison.ClientExclusions)
	}
	if len(comparison.TransportPolicies) != 1 || comparison.TransportPolicies[0].Client != benchmark.Weaver {
		t.Fatalf("transport policies should cover only the observed client: %#v", comparison.TransportPolicies)
	}

	// An exclusion on another fixture changes nothing here.
	elsewhere := benchmark.ClientExclusion{Client: benchmark.SABnzbd, FixtureID: "fixture-b", Reason: "elsewhere"}
	if _, err := buildSummaryReport(artifacts, []benchmark.ClientExclusion{elsewhere}, benchmark.SABnzbd, benchmark.Weaver, 3, 17, 100); err == nil {
		t.Fatal("an exclusion on a different fixture excused the missing observations")
	}
}

func summaryTestSetTLS(artifact *benchmark.QueueArtifact, validation benchmark.TLSValidation, label string) {
	artifact.Runs[0].Transport = benchmark.TLS
	artifact.Runs[0].TLSValidation = validation
	artifact.Runs[0].TransportLabel = label
	artifact.Jobs[0].Run = artifact.Runs[0]
	artifact.AdapterResult.Transport = benchmark.TLS
	artifact.AdapterResult.TLSValidation = validation
	artifact.AdapterResult.TransportLabel = label
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
func TestBuildSummaryReportPairsContainerCPUTime(t *testing.T) {
	artifacts := make([]benchmark.QueueArtifact, 0, 40)
	for repetition := 1; repetition <= 20; repetition++ {
		artifacts = append(artifacts,
			summaryTestArtifactWithCPU(benchmark.Weaver, repetition, int64(100+repetition), "client_container", 400_000_000),
			summaryTestArtifactWithCPU(benchmark.SABnzbd, repetition, int64(80+repetition), "client_container", 2_000_000_000),
		)
	}
	report, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if report.SchemaVersion != 5 {
		t.Fatalf("schema version %d, want 4", report.SchemaVersion)
	}
	cpu := report.Comparisons[0].CPUTime
	if cpu.Metric != "cpu_time_nanoseconds" || cpu.ComparisonWithheld != "" || cpu.Summary == nil {
		t.Fatalf("cpu comparison not summarized: %#v", cpu)
	}
	if cpu.PairedBlocks != 20 || cpu.Summary.Count != 20 {
		t.Fatalf("cpu paired %d/%d blocks, want 20", cpu.PairedBlocks, cpu.Summary.Count)
	}
	// Candidate over baseline, like the timing summary: SABnzbd's 2 s over
	// weaver's 400 ms.
	if ratio := cpu.Summary.GeometricMeanRatio; ratio < 4.99 || ratio > 5.01 {
		t.Fatalf("cpu geometric mean ratio %v, want 5", ratio)
	}
	if len(cpu.Accounting) != 2 {
		t.Fatalf("accounting %#v", cpu.Accounting)
	}
	for _, accounting := range cpu.Accounting {
		if accounting.Scope != "client_container" || accounting.Collector != "cgroup-v2-cpu.stat" || accounting.MeasuredBlocks != 20 || accounting.UnavailableBlocks != 0 {
			t.Fatalf("accounting for %s: %#v", accounting.Client, accounting)
		}
	}
}

func TestBuildSummaryReportWithholdsCPUTimeAcrossScopes(t *testing.T) {
	artifacts := make([]benchmark.QueueArtifact, 0, 40)
	for repetition := 1; repetition <= 20; repetition++ {
		artifacts = append(artifacts,
			summaryTestArtifactWithCPU(benchmark.Weaver, repetition, int64(100+repetition), "client_process", 400_000_000),
			summaryTestArtifactWithCPU(benchmark.SABnzbd, repetition, int64(80+repetition), "client_container", 2_000_000_000),
		)
	}
	report, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if report.Comparisons[0].Summary == nil {
		t.Fatal("timing summary must not depend on the CPU comparison")
	}
	cpu := report.Comparisons[0].CPUTime
	if cpu.Summary != nil || !strings.Contains(cpu.ComparisonWithheld, "scopes differ") {
		t.Fatalf("cpu comparison across scopes was not withheld: %#v", cpu)
	}
	if cpu.PairedBlocks != 20 {
		t.Fatalf("paired blocks %d, want 20 even when withheld", cpu.PairedBlocks)
	}
}

func TestBuildSummaryReportSkipsUnavailableCPUBlocks(t *testing.T) {
	artifacts := make([]benchmark.QueueArtifact, 0, 40)
	for repetition := 1; repetition <= 20; repetition++ {
		artifacts = append(artifacts, summaryTestArtifactWithCPU(benchmark.Weaver, repetition, int64(100+repetition), "client_container", 400_000_000))
		if repetition <= 3 {
			// The lane recorded the counter as unavailable for three runs.
			artifacts = append(artifacts, summaryTestArtifact(benchmark.SABnzbd, repetition, int64(80+repetition)))
		} else {
			artifacts = append(artifacts, summaryTestArtifactWithCPU(benchmark.SABnzbd, repetition, int64(80+repetition), "client_container", 2_000_000_000))
		}
	}
	report, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if report.Comparisons[0].Summary == nil || report.Comparisons[0].Summary.Count != 20 {
		t.Fatal("timing summary must still pair all 20 blocks")
	}
	cpu := report.Comparisons[0].CPUTime
	if cpu.PairedBlocks != 17 || cpu.Summary == nil || cpu.Summary.Count != 17 || cpu.ComparisonWithheld != "" {
		t.Fatalf("cpu comparison should pair the 17 measured blocks: %#v", cpu)
	}
	if cpu.Accounting[1].MeasuredBlocks != 17 || cpu.Accounting[1].UnavailableBlocks != 3 || len(cpu.Accounting[1].UnavailableReasons) != 1 {
		t.Fatalf("candidate accounting: %#v", cpu.Accounting[1])
	}
	// Short of the run's minimum the comparison stays, with the shortfall stated.
	if len(cpu.Caveats) != 1 || !strings.Contains(cpu.Caveats[0], "17 paired CPU blocks is below the run's minimum of 20") {
		t.Fatalf("cpu caveats: %#v", cpu.Caveats)
	}

	// With a single measured pair there is nothing to summarize; that is
	// withheld, never failed, and the timing summary is untouched.
	for index := range artifacts {
		if artifacts[index].Runs[0].Client == benchmark.SABnzbd && artifacts[index].Runs[0].Repetition != 20 {
			artifacts[index].AdapterResult.Jobs[0].ResourceMetrics = nil
			artifacts[index].Jobs[0].AdapterResult.ResourceMetrics = nil
		}
	}
	report, err = buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if report.Comparisons[0].Summary == nil {
		t.Fatal("timing summary must not depend on the CPU comparison")
	}
	cpu = report.Comparisons[0].CPUTime
	if cpu.Summary != nil || cpu.PairedBlocks != 1 || !strings.Contains(cpu.ComparisonWithheld, "1 paired CPU blocks, need at least 2") {
		t.Fatalf("cpu comparison with one pair was not withheld: %#v", cpu)
	}
}

func TestBuildSummaryReportWithholdsCPUTimeWithoutResourceMetrics(t *testing.T) {
	artifacts := make([]benchmark.QueueArtifact, 0, 40)
	for repetition := 1; repetition <= 20; repetition++ {
		artifacts = append(artifacts,
			summaryTestArtifact(benchmark.Weaver, repetition, int64(100+repetition)),
			summaryTestArtifact(benchmark.SABnzbd, repetition, int64(80+repetition)),
		)
	}
	report, err := buildSummaryReport(artifacts, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	cpu := report.Comparisons[0].CPUTime
	if cpu.Summary != nil || cpu.PairedBlocks != 0 || !strings.Contains(cpu.ComparisonWithheld, "no measured CPU time") {
		t.Fatalf("cpu comparison without resource metrics was not withheld: %#v", cpu)
	}
	for _, accounting := range cpu.Accounting {
		if accounting.UnavailableBlocks != 20 || len(accounting.UnavailableReasons) != 1 {
			t.Fatalf("accounting for %s: %#v", accounting.Client, accounting)
		}
	}
}

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

// summaryTestArtifactWithCPU is summaryTestArtifact with a measured CPU
// counter at the given scope, the way the Docker lane (`client_container`)
// and the native lanes (`client_process`) record it.
func summaryTestArtifactWithCPU(client benchmark.Client, repetition int, measurement int64, scope string, cpuNanoseconds uint64) benchmark.QueueArtifact {
	artifact := summaryTestArtifact(client, repetition, measurement)
	metrics := &benchmark.ResourceMetrics{
		CPUTimeNanoseconds:  benchmark.MeasuredMeasurement(scope, "cgroup-v2-cpu.stat", "test", cpuNanoseconds),
		InstructionsRetired: benchmark.UnavailableMeasurement(scope, "none", "test", "not collected in tests"),
	}
	// The artifact carries the adapter result twice (top level and inside
	// the job) and the summarizer requires both copies to agree.
	artifact.AdapterResult.Jobs[0].ResourceMetrics = metrics
	artifact.Jobs[0].AdapterResult.ResourceMetrics = metrics
	return artifact
}

// summaryTestShapedArtifact puts a finished run behind the 1 Gbit shaper with a
// consistent before/after snapshot pair delivering `downstream` bytes, and a
// schema-3 article census when `census` is non-nil.
func summaryTestShapedArtifact(client benchmark.Client, repetition int, measurement int64, downstream uint64, census *benchmark.ShaperArticleCensus) benchmark.QueueArtifact {
	artifact := summaryTestArtifact(client, repetition, measurement)
	shaped := benchmark.ServerLinkProfile{ID: benchmark.Link1Gbit, EgressBitsPerSecond: 1_000_000_000, BurstBytes: 1_048_576}
	artifact.Runs[0].ServerLink = shaped
	artifact.Jobs[0].Run.ServerLink = shaped
	artifact.AdapterResult.ServerLink = shaped
	acquired := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	source := "172.18.0.9"
	before := benchmark.ShaperSnapshot{
		SchemaVersion: 2, Status: "ok", StartedAt: acquired.Add(-time.Hour),
		ConfiguredEgressBitsPerSecond: shaped.EgressBitsPerSecond, ConfiguredBurstBytes: shaped.BurstBytes,
		DownstreamConnections: 40, DownstreamBytes: 9_000, DownstreamSourceConnections: map[string]uint64{source: 40}, DownstreamSourceBytes: map[string]uint64{source: 9_000},
		ExecutionLeaseID: strings.Repeat("a", 64), ExecutionLeaseAcquiredAt: &acquired,
		Build: benchmark.ShaperBuildIdentity{ExecutableSHA256: strings.Repeat("0", 64), Version: "test"},
	}
	after := before
	after.DownstreamConnections += 8
	after.DownstreamBytes += downstream
	after.DownstreamSourceConnections = map[string]uint64{source: 48}
	after.DownstreamSourceBytes = map[string]uint64{source: 9_000 + downstream}
	if census != nil {
		before.SchemaVersion, after.SchemaVersion = 3, 3
		before.ArticleRequests, before.RepeatedArticleRequests = 500, 3
		after.ArticleRequests = before.ArticleRequests + census.ArticleRequests
		after.RepeatedArticleRequests = before.RepeatedArticleRequests + census.RepeatedArticleRequests
		after.DistinctArticleRequests = census.DistinctArticleRequests
	}
	artifact.ShaperBefore = &before
	artifact.ShaperAfter = &after
	artifact.ShaperDownstreamBytes = downstream
	artifact.ShaperArticleCensus = census
	return artifact
}

func TestBuildSummaryReportRecordsTransferEvidence(t *testing.T) {
	// SABnzbd pulls the same bytes every block with no repeats; weaver pulls
	// more on one block and the census attributes it to repeated requests.
	artifacts := []benchmark.QueueArtifact{
		summaryTestShapedArtifact(benchmark.SABnzbd, 1, 100, 6_000_000, &benchmark.ShaperArticleCensus{ArticleRequests: 125, DistinctArticleRequests: 125}),
		summaryTestShapedArtifact(benchmark.SABnzbd, 2, 101, 6_000_003, &benchmark.ShaperArticleCensus{ArticleRequests: 125, DistinctArticleRequests: 125}),
		summaryTestShapedArtifact(benchmark.Weaver, 1, 50, 6_000_010, &benchmark.ShaperArticleCensus{ArticleRequests: 125, DistinctArticleRequests: 125}),
		summaryTestShapedArtifact(benchmark.Weaver, 2, 51, 6_900_000, &benchmark.ShaperArticleCensus{ArticleRequests: 140, DistinctArticleRequests: 125, RepeatedArticleRequests: 15}),
	}
	report, err := buildSummaryReport(artifacts, nil, benchmark.SABnzbd, benchmark.Weaver, 2, 17, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Comparisons) != 1 {
		t.Fatalf("comparisons=%d, want 1", len(report.Comparisons))
	}
	transfer := report.Comparisons[0].Transfer
	if len(transfer) != 2 || transfer[0].Client != benchmark.SABnzbd || transfer[1].Client != benchmark.Weaver {
		t.Fatalf("transfer=%+v, want baseline then candidate", transfer)
	}
	sab, weaver := transfer[0], transfer[1]
	if sab.FinishedBlocks != 2 || sab.DownstreamBytesMin != 6_000_000 || sab.DownstreamBytesMedian != 6_000_003 || sab.DownstreamBytesMax != 6_000_003 {
		t.Fatalf("sabnzbd transfer=%+v", sab)
	}
	if sab.ArticleCensus == nil || sab.ArticleCensus.Blocks != 2 || sab.ArticleCensus.ArticleRequests != 250 || sab.ArticleCensus.DistinctArticleRequests != 250 || sab.ArticleCensus.RepeatedArticleRequests != 0 {
		t.Fatalf("sabnzbd census=%+v", sab.ArticleCensus)
	}
	if weaver.DownstreamBytesMin != 6_000_010 || weaver.DownstreamBytesMax != 6_900_000 || weaver.ArticleCensus == nil || weaver.ArticleCensus.RepeatedArticleRequests != 15 || weaver.ArticleCensus.ArticleRequests != 265 {
		t.Fatalf("weaver transfer=%+v census=%+v", weaver, weaver.ArticleCensus)
	}
}

func TestBuildSummaryReportTransferEvidenceWithoutCensus(t *testing.T) {
	// A schema-2 shaper: bytes are reported, the census is absent rather than
	// zero, and a client that did not finish contributes no bytes.
	artifacts := []benchmark.QueueArtifact{
		summaryTestShapedArtifact(benchmark.SABnzbd, 1, 100, 6_000_000, nil),
		summaryTestShapedArtifact(benchmark.SABnzbd, 2, 101, 6_000_000, nil),
		summaryTestShapedArtifact(benchmark.Weaver, 1, 50, 6_000_010, nil),
		summaryTestShapedArtifact(benchmark.Weaver, 2, 51, 6_000_020, nil),
	}
	report, err := buildSummaryReport(artifacts, nil, benchmark.SABnzbd, benchmark.Weaver, 2, 17, 100)
	if err != nil {
		t.Fatal(err)
	}
	transfer := report.Comparisons[0].Transfer
	if len(transfer) != 2 || transfer[0].ArticleCensus != nil || transfer[1].ArticleCensus != nil {
		t.Fatalf("transfer=%+v, want two entries without a census", transfer)
	}
	if transfer[1].DownstreamBytesMedian != 6_000_020 {
		t.Fatalf("weaver median=%d, want the upper of two (6000020)", transfer[1].DownstreamBytesMedian)
	}
	// Unshaped runs carry no transfer evidence at all.
	plain := []benchmark.QueueArtifact{
		summaryTestArtifact(benchmark.SABnzbd, 1, 100), summaryTestArtifact(benchmark.SABnzbd, 2, 101),
		summaryTestArtifact(benchmark.Weaver, 1, 50), summaryTestArtifact(benchmark.Weaver, 2, 51),
	}
	report, err = buildSummaryReport(plain, nil, benchmark.SABnzbd, benchmark.Weaver, 2, 17, 100)
	if err != nil {
		t.Fatal(err)
	}
	if report.Comparisons[0].Transfer != nil {
		t.Fatalf("unshaped transfer=%+v, want none", report.Comparisons[0].Transfer)
	}
}
