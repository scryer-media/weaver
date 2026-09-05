package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

type summaryReport struct {
	SchemaVersion int                    `json:"schema_version"`
	Metric        string                 `json:"metric"`
	Baseline      benchmark.Client       `json:"baseline_client"`
	Candidate     benchmark.Client       `json:"candidate_client"`
	MinimumBlocks int                    `json:"minimum_complete_blocks"`
	Comparisons   []stratifiedComparison `json:"comparisons"`
}

// comparisonStratum is the pairing key. Transport is part of it; how each
// client validated TLS is not, because that is a per-client property of the
// run (SABnzbd cannot verify the harness CA and is labelled tls-unverified)
// and keying on it would leave every SABnzbd TLS block unpaired. Each
// client's validation and label are carried on the comparison instead, so a
// reader sees what was compared without the pairing depending on it.
type comparisonStratum struct {
	FixtureID        string                     `json:"fixture_id"`
	Profile          string                     `json:"profile"`
	ExecutionTarget  benchmark.ExecutionTarget  `json:"execution_target"`
	Transport        benchmark.Transport        `json:"transport"`
	ArchiveToolchain benchmark.ArchiveToolchain `json:"archive_toolchain"`
	ServerLinkID     string                     `json:"server_link_id"`
	ServerEgressBPS  uint64                     `json:"server_egress_bits_per_second"`
	ServerBurstBytes uint64                     `json:"server_burst_bytes"`
	// StorageProfileID and its link join the stratum key. A local run and an
	// NFS run measure different questions, so they are never pooled — the same
	// rule that keeps transports and toolchains apart.
	StorageProfileID string `json:"storage_profile_id"`
	StorageNFSLinkID string `json:"storage_nfs_link_id"`
	StorageLinkBPS   uint64 `json:"storage_link_bits_per_second"`
	StorageRTTMicros uint64 `json:"storage_rtt_micros"`
}

type stratifiedComparison struct {
	Stratum comparisonStratum `json:"stratum"`
	// TransportPolicies records, per observed client, how it validated TLS in
	// this stratum. Plaintext strata carry not_applicable. A client the plan
	// excluded on this fixture has no observation and so no entry here; its
	// exclusion appears under ClientExclusions.
	TransportPolicies []clientTransportPolicy `json:"transport_policies"`
	// ClientExclusions lists the plan's exclusions of the baseline or the
	// candidate on this fixture: blocks the plan chose not to run, counted
	// under Completion as that client not finishing, with the reason why.
	ClientExclusions []benchmark.ClientExclusion `json:"client_exclusions,omitempty"`
	Completion       completionCounts            `json:"completion"`
	// Summary is absent when the stratum has fewer complete pairs than the
	// minimum because one client did not finish; ComparisonWithheld then says
	// so. A client that cannot finish a fixture is a result in itself, and
	// hiding the whole run behind it would hide that result.
	Summary            *benchmark.PairedSummary `json:"summary,omitempty"`
	ComparisonWithheld string                   `json:"comparison_withheld,omitempty"`
}

// completionCounts records, per stratum, how many randomized blocks each
// client finished. Blocks where either client did not finish (a terminal
// failure or an output that failed neutral verification) are excluded from the
// paired timing summary and counted here instead.
type completionCounts struct {
	BlocksObserved        int `json:"blocks_observed"`
	PairedBlocks          int `json:"paired_blocks"`
	BaselineDidNotFinish  int `json:"baseline_did_not_finish"`
	CandidateDidNotFinish int `json:"candidate_did_not_finish"`
	// BaselineExcluded and CandidateExcluded are the part of the did-not-finish
	// counts that the plan recorded instead of running: a client excluded on
	// the fixture is counted as not finishing every block, by declaration.
	BaselineExcluded  int `json:"baseline_excluded"`
	CandidateExcluded int `json:"candidate_excluded"`
}

type clientTransportPolicy struct {
	Client         benchmark.Client        `json:"client"`
	TLSValidation  benchmark.TLSValidation `json:"tls_validation"`
	TransportLabel string                  `json:"transport_label"`
}

type comparisonBlock struct {
	baseline     *float64
	candidate    *float64
	baselineDNF  bool
	candidateDNF bool
}

type summaryProductKey struct {
	Stratum comparisonStratum
	Client  benchmark.Client
}

type summaryProductIdentity struct {
	ClientIdentity           string
	ClientVersion            string
	ArchiveToolchainIdentity string
	RenderedConfigSHA256     string
	TLSValidation            benchmark.TLSValidation
	TransportLabel           string
}

// summaryExecutionContext is what the summarizer takes from an artifact
// root's immutable execution manifest and snapshotted plan.
type summaryExecutionContext struct {
	Command     string
	PlannedRuns map[string]benchmark.Run
	Exclusions  []benchmark.ClientExclusion
}

func summarize(args []string) error {
	flags := flag.NewFlagSet("summarize", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var artifactRoot, baselineName, candidateName, mode string
	var minimumBlocks, resamples int
	var seed int64
	flags.StringVar(&artifactRoot, "artifacts", "", "benchmark artifact root containing sequential queue.json files")
	flags.StringVar(&mode, "mode", "sequential", "sequential (paired per-fixture comparison) or queue-drain (per-lane drain wall clock of a queue-transition root; --baseline and --candidate are not used)")
	flags.StringVar(&baselineName, "baseline", "", "baseline client: weaver, sabnzbd, or nzbget")
	flags.StringVar(&candidateName, "candidate", "", "candidate client: weaver, sabnzbd, or nzbget")
	flags.IntVar(&minimumBlocks, "minimum-blocks", 20, "minimum complete paired randomized blocks per stratum")
	flags.IntVar(&resamples, "bootstrap-resamples", benchmark.DefaultBootstrapResamples, "fixed-seed paired bootstrap resamples")
	flags.Int64Var(&seed, "bootstrap-seed", 20260802, "deterministic paired bootstrap seed")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if mode == "queue-drain" {
		if artifactRoot == "" {
			return fmt.Errorf("--artifacts is required")
		}
		report, err := loadQueueDrainReport(artifactRoot)
		if err != nil {
			return err
		}
		return printJSON(report)
	}
	if mode != "sequential" {
		return fmt.Errorf("--mode must be sequential or queue-drain, got %q", mode)
	}
	if artifactRoot == "" || baselineName == "" || candidateName == "" {
		return fmt.Errorf("--artifacts, --baseline, and --candidate are required")
	}

	if minimumBlocks < 2 {
		return fmt.Errorf("--minimum-blocks must be at least 2")
	}
	baseline, err := parseSingleClient(baselineName)
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	candidate, err := parseSingleClient(candidateName)
	if err != nil {
		return fmt.Errorf("candidate: %w", err)
	}
	if baseline == candidate {
		return fmt.Errorf("baseline and candidate clients must differ")
	}
	artifacts, exclusions, err := loadSequentialArtifacts(artifactRoot)
	if err != nil {
		return err
	}
	report, err := buildSummaryReport(artifacts, exclusions, baseline, candidate, minimumBlocks, seed, resamples)
	if err != nil {
		return err
	}
	return printJSON(report)
}

// validateSummaryShaperEvidence fails a summary closed when a run on a shaped
// server link cannot prove, from the shaper's own before/after counters, that
// the link was in force and carried the bytes the artifact claims.
func validateSummaryShaperEvidence(artifact benchmark.QueueArtifact, link benchmark.ServerLinkProfile) error {
	if link.EgressBitsPerSecond == 0 {
		return nil
	}
	if artifact.ShaperBefore == nil || artifact.ShaperAfter == nil {
		return fmt.Errorf("shaped artifact %s lacks shaper attestations", artifact.SuiteID)
	}
	if err := artifact.ShaperBefore.ValidateFor(link); err != nil {
		return fmt.Errorf("shaped artifact %s before snapshot: %w", artifact.SuiteID, err)
	}
	if err := artifact.ShaperAfter.ValidateFor(link); err != nil {
		return fmt.Errorf("shaped artifact %s after snapshot: %w", artifact.SuiteID, err)
	}
	delivered, err := benchmark.ValidateShaperSnapshotPair(*artifact.ShaperBefore, *artifact.ShaperAfter)
	if err != nil {
		return fmt.Errorf("shaped artifact %s snapshot pair: %w", artifact.SuiteID, err)
	}
	if delivered == 0 || delivered != artifact.ShaperDownstreamBytes {
		return fmt.Errorf("shaped artifact %s has invalid shaper byte evidence", artifact.SuiteID)
	}
	return nil
}

// validateSummaryStorageEvidence fails a summary closed when a published NFS
// run cannot prove its shaped link, and when a local run carries storage
// evidence it should never have had.
func validateSummaryStorageEvidence(artifact benchmark.QueueArtifact, profile benchmark.StorageProfile) error {
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("sequential artifact %s has an invalid storage profile: %w", artifact.SuiteID, err)
	}
	if profile.Kind == benchmark.StorageLocal {
		if artifact.StorageAttestation != nil {
			return fmt.Errorf("local-storage artifact %s unexpectedly carries an NFS attestation", artifact.SuiteID)
		}
		return nil
	}
	if artifact.StorageAttestation == nil {
		return fmt.Errorf("storage-shaped artifact %s lacks its NFS attestation", artifact.SuiteID)
	}
	if artifact.StorageAttestation.Profile != profile {
		return fmt.Errorf("storage attestation for %s does not describe the planned storage profile", artifact.SuiteID)
	}
	if err := artifact.StorageAttestation.Validate(); err != nil {
		return fmt.Errorf("storage-shaped artifact %s: %w", artifact.SuiteID, err)
	}
	return nil
}

func parseSingleClient(value string) (benchmark.Client, error) {
	clients, err := parseClients(strings.TrimSpace(value))
	if err != nil {
		return "", err
	}
	if len(clients) != 1 {
		return "", fmt.Errorf("expected exactly one client")
	}
	return clients[0], nil
}

func loadSequentialArtifacts(root string) ([]benchmark.QueueArtifact, []benchmark.ClientExclusion, error) {
	execution, err := loadSummaryExecutionContext(root, "sequential")
	if err != nil {
		return nil, nil, err
	}
	plannedRuns := execution.PlannedRuns
	var artifacts []benchmark.QueueArtifact
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "queue.json" {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read summary artifact %s: %w", path, err)
		}
		var artifact benchmark.QueueArtifact
		if err := json.Unmarshal(contents, &artifact); err != nil {
			return fmt.Errorf("decode summary artifact %s: %w", path, err)
		}
		if artifact.SubmissionMode == benchmark.SubmissionModeSequential {
			for _, run := range artifact.Runs {
				planned, ok := plannedRuns[run.ID]
				if !ok || planned != run {
					return fmt.Errorf("sequential artifact %s is not bound to the snapshotted plan", path)
				}
			}
			if !summarizableSequentialStatus(artifact.Status) {
				return fmt.Errorf("sequential artifact %s is not publishable: status=%s error=%s", path, artifact.Status, artifact.Error)
			}
			artifacts = append(artifacts, artifact)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(artifacts) == 0 {
		return nil, nil, fmt.Errorf("artifact root %s contains no passed sequential queue artifacts", root)
	}
	return artifacts, execution.Exclusions, nil
}

// summarizableSequentialStatus admits the two statuses that describe a client
// outcome: "passed" (verified output) and "completed_with_dnf" (the client
// reached a terminal failure or its output failed verification). "failed"
// means the harness itself could not run the suite and stays inadmissible.
func summarizableSequentialStatus(status string) bool {
	return status == "passed" || status == "completed_with_dnf"
}

// loadSummaryExecutionContext binds an artifact root to the command that
// produced it, its snapshotted plan and its adapter catalog, refusing a root
// whose snapshots no longer match the immutable manifest.
func loadSummaryExecutionContext(root, command string) (summaryExecutionContext, error) {
	manifestPath := filepath.Join(root, "execution-manifest.json")
	contents, err := os.ReadFile(manifestPath)
	if err != nil {
		return summaryExecutionContext{}, fmt.Errorf("read summary execution manifest: %w", err)
	}
	var manifest executionManifest
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return summaryExecutionContext{}, fmt.Errorf("decode summary execution manifest: %w", err)
	}
	if manifest.SchemaVersion != 1 || manifest.ExecutionTarget == "" || manifest.Profile == "" || len(manifest.ExecutableSHA256) != 64 {
		return summaryExecutionContext{}, fmt.Errorf("summary execution manifest has unsupported or incomplete provenance")
	}
	if manifest.Command != command {
		return summaryExecutionContext{}, fmt.Errorf("summary execution manifest was written by %q, want %q", manifest.Command, command)
	}
	if manifest.PlanSnapshotPath != "plan.snapshot.json" || manifest.AdapterSnapshot != "adapter-catalog.snapshot.json" {
		return summaryExecutionContext{}, fmt.Errorf("summary execution manifest uses unexpected snapshot paths")
	}

	planSnapshot := filepath.Join(root, manifest.PlanSnapshotPath)
	adapterSnapshot := filepath.Join(root, manifest.AdapterSnapshot)
	planDigest, err := sha256File(planSnapshot)
	if err != nil {
		return summaryExecutionContext{}, fmt.Errorf("hash snapshotted plan: %w", err)
	}
	adapterDigest, err := sha256File(adapterSnapshot)
	if err != nil {
		return summaryExecutionContext{}, fmt.Errorf("hash snapshotted adapter catalog: %w", err)
	}
	if planDigest != manifest.PlanSHA256 || adapterDigest != manifest.AdapterSHA256 {
		return summaryExecutionContext{}, fmt.Errorf("summary execution snapshot digest does not match immutable manifest")
	}
	planned, err := benchmark.LoadPlan(planSnapshot)
	if err != nil {
		return summaryExecutionContext{}, fmt.Errorf("load snapshotted plan: %w", err)
	}
	target := benchmark.ExecutionTarget(manifest.ExecutionTarget)
	if planned.Profile != manifest.Profile {
		return summaryExecutionContext{}, fmt.Errorf("execution manifest profile %q does not match snapshotted plan %q", manifest.Profile, planned.Profile)
	}
	catalog, err := benchmark.LoadAdapterCatalog(adapterSnapshot)
	if err != nil {
		return summaryExecutionContext{}, fmt.Errorf("load snapshotted adapter catalog: %w", err)
	}
	if err := catalog.ValidateFor(planned, target); err != nil {
		return summaryExecutionContext{}, fmt.Errorf("validate snapshotted adapter catalog: %w", err)
	}
	plannedRuns := make(map[string]benchmark.Run)
	for _, run := range planned.Runs {
		if run.ExecutionTarget == target {
			plannedRuns[run.ID] = run
		}
	}
	if len(plannedRuns) == 0 {
		return summaryExecutionContext{}, fmt.Errorf("snapshotted plan has no runs for execution target %q", target)
	}
	return summaryExecutionContext{Command: manifest.Command, PlannedRuns: plannedRuns, Exclusions: planned.ClientExclusions}, nil
}

func buildSummaryReport(artifacts []benchmark.QueueArtifact, exclusions []benchmark.ClientExclusion, baseline, candidate benchmark.Client, minimumBlocks int, seed int64, resamples int) (summaryReport, error) {
	groups := make(map[comparisonStratum]map[int]*comparisonBlock)
	identities := make(map[summaryProductKey]summaryProductIdentity)
	// One summary describes one storage stratum. Local and NFS runs answer
	// different questions, so a directory holding both is an operator mistake
	// and is refused rather than silently split into two comparisons that look
	// like one report.
	var storageProfile *benchmark.StorageProfile
	for _, artifact := range artifacts {
		if artifact.SchemaVersion != 7 {
			return summaryReport{}, fmt.Errorf("summary input %s uses queue artifact schema %d, want 7", artifact.SuiteID, artifact.SchemaVersion)
		}
		if !summarizableSequentialStatus(artifact.Status) || artifact.SubmissionMode != benchmark.SubmissionModeSequential {
			return summaryReport{}, fmt.Errorf("summary input contains a non-passed sequential artifact %s", artifact.SuiteID)
		}
		if artifact.AdapterResult == nil || artifact.AdapterResult.SchemaVersion != 6 {
			return summaryReport{}, fmt.Errorf("sequential artifact %s lacks queue adapter result schema 6", artifact.SuiteID)
		}
		if len(artifact.Jobs) != 1 {
			return summaryReport{}, fmt.Errorf("sequential artifact %s contains %d jobs, want exactly one", artifact.SuiteID, len(artifact.Jobs))
		}
		job := artifact.Jobs[0]
		if len(artifact.Runs) != 1 || artifact.Runs[0].ID != job.Run.ID || artifact.AdapterResult.SuiteID != artifact.SuiteID || len(artifact.AdapterResult.Jobs) != 1 || artifact.AdapterResult.Jobs[0].RunID != job.Run.ID || !reflect.DeepEqual(artifact.AdapterResult.Jobs[0], job.AdapterResult) {
			return summaryReport{}, fmt.Errorf("sequential artifact %s has inconsistent run or adapter-result identity", artifact.SuiteID)
		}
		if artifact.AdapterResult.Client != job.Run.Client || artifact.AdapterResult.ArchiveToolchain != job.Run.ArchiveToolchain || artifact.AdapterResult.ExecutionTarget != job.Run.ExecutionTarget || artifact.AdapterResult.Transport != job.Run.Transport || artifact.AdapterResult.TLSValidation != job.Run.TLSValidation || artifact.AdapterResult.TransportLabel != job.Run.TransportLabel || artifact.AdapterResult.ServerLink != job.Run.ServerLink || artifact.AdapterResult.StorageProfile != job.Run.StorageProfile {
			return summaryReport{}, fmt.Errorf("sequential artifact %s has adapter metadata inconsistent with its planned run", artifact.SuiteID)
		}
		if err := validateSummaryShaperEvidence(artifact, job.Run.ServerLink); err != nil {
			return summaryReport{}, err
		}
		if err := validateSummaryStorageEvidence(artifact, job.Run.StorageProfile); err != nil {
			return summaryReport{}, err
		}
		if storageProfile == nil {
			profile := job.Run.StorageProfile
			storageProfile = &profile
		} else if *storageProfile != job.Run.StorageProfile {
			return summaryReport{}, fmt.Errorf("summary inputs mix storage profiles %q and %q; summarize each storage profile separately",
				storageProfile.ID, job.Run.StorageProfile.ID)
		}
		if job.Run.Client != baseline && job.Run.Client != candidate {
			continue
		}
		didNotFinish := artifact.Status == "completed_with_dnf"
		if didNotFinish {
			if job.Outcome != "dnf" || job.Error == "" {
				return summaryReport{}, fmt.Errorf("sequential artifact %s reports did-not-finish without a recorded job failure", artifact.SuiteID)
			}
		} else {
			if job.Outcome != "completed" || job.Verification == nil || job.AdapterResult.SubmissionToTerminalNanoseconds <= 0 {
				return summaryReport{}, fmt.Errorf("sequential artifact %s contains an unverified or invalid measurement", artifact.SuiteID)
			}
			if !benchmark.ObservationUncertaintyAcceptable(job.AdapterResult.TerminalObservationUncertainty, job.AdapterResult.SubmissionToTerminalNanoseconds) {
				return summaryReport{}, fmt.Errorf("sequential artifact %s exceeds the terminal-observation uncertainty limit (%s)", artifact.SuiteID, benchmark.ObservationUncertaintyRule)
			}
		}
		stratum := comparisonStratum{
			FixtureID:        job.Run.FixtureID,
			Profile:          job.Run.Profile,
			ExecutionTarget:  job.Run.ExecutionTarget,
			Transport:        job.Run.Transport,
			ArchiveToolchain: job.Run.ArchiveToolchain,
			ServerLinkID:     job.Run.ServerLink.ID,
			ServerEgressBPS:  job.Run.ServerLink.EgressBitsPerSecond,
			ServerBurstBytes: job.Run.ServerLink.BurstBytes,
			StorageProfileID: job.Run.StorageProfile.ID,
			StorageNFSLinkID: job.Run.StorageProfile.NFSLinkID,
			StorageLinkBPS:   job.Run.StorageProfile.LinkBitsPerSecond,
			StorageRTTMicros: job.Run.StorageProfile.RTTMicros,
		}
		if len(artifact.AdapterResult.RenderedConfigSHA256) != 64 {
			return summaryReport{}, fmt.Errorf("sequential artifact %s lacks a rendered-config SHA-256", artifact.SuiteID)
		}
		productKey := summaryProductKey{Stratum: stratum, Client: job.Run.Client}
		identity := summaryProductIdentity{
			ClientIdentity:           artifact.AdapterResult.ClientIdentity,
			ClientVersion:            artifact.AdapterResult.ClientVersion,
			ArchiveToolchainIdentity: artifact.AdapterResult.ArchiveToolchainIdentity,
			RenderedConfigSHA256:     artifact.AdapterResult.RenderedConfigSHA256,
			TLSValidation:            job.Run.TLSValidation,
			TransportLabel:           job.Run.TransportLabel,
		}
		if identity.ClientIdentity == "" || identity.ClientVersion == "" || identity.ArchiveToolchainIdentity == "" {
			return summaryReport{}, fmt.Errorf("sequential artifact %s lacks product identity evidence", artifact.SuiteID)
		}
		if previous, ok := identities[productKey]; ok && previous != identity {
			return summaryReport{}, fmt.Errorf("product identity or TLS policy changed within stratum %+v for client %s", stratum, job.Run.Client)
		}
		identities[productKey] = identity
		blocks := groups[stratum]
		if blocks == nil {
			blocks = make(map[int]*comparisonBlock)
			groups[stratum] = blocks
		}
		block := blocks[job.Run.Repetition]
		if block == nil {
			block = &comparisonBlock{}
			blocks[job.Run.Repetition] = block
		}
		measurement := float64(job.AdapterResult.SubmissionToTerminalNanoseconds)
		if job.Run.Client == baseline {
			if block.baseline != nil || block.baselineDNF {
				return summaryReport{}, fmt.Errorf("duplicate baseline observation for %+v repetition %d", stratum, job.Run.Repetition)
			}
			if didNotFinish {
				block.baselineDNF = true
			} else {
				block.baseline = &measurement
			}
		} else {
			if block.candidate != nil || block.candidateDNF {
				return summaryReport{}, fmt.Errorf("duplicate candidate observation for %+v repetition %d", stratum, job.Run.Repetition)
			}
			if didNotFinish {
				block.candidateDNF = true
			} else {
				block.candidate = &measurement
			}
		}
	}

	strata := make([]comparisonStratum, 0, len(groups))
	for stratum := range groups {
		strata = append(strata, stratum)
	}
	sort.Slice(strata, func(left, right int) bool { return fmt.Sprint(strata[left]) < fmt.Sprint(strata[right]) })
	report := summaryReport{
		SchemaVersion: 3,
		Metric:        benchmark.PrimaryMetric,
		Baseline:      baseline,
		Candidate:     candidate,
		MinimumBlocks: minimumBlocks,
		Comparisons:   make([]stratifiedComparison, 0, len(strata)),
	}
	for _, stratum := range strata {
		blocks := groups[stratum]
		repetitions := make([]int, 0, len(blocks))
		for repetition := range blocks {
			repetitions = append(repetitions, repetition)
		}
		sort.Ints(repetitions)
		samples := make([]benchmark.PairedSample, 0, len(repetitions))
		completion := completionCounts{BlocksObserved: len(repetitions)}
		// A client the plan excluded on this fixture has no artifact in any
		// block. Every block then counts as that client not finishing, by the
		// plan's own record rather than by observation.
		baselineExclusion, baselineExcluded := benchmark.ClientExclusionFor(exclusions, baseline, stratum.FixtureID)
		candidateExclusion, candidateExcluded := benchmark.ClientExclusionFor(exclusions, candidate, stratum.FixtureID)
		for _, repetition := range repetitions {
			block := blocks[repetition]
			if block.baseline == nil && !block.baselineDNF && baselineExcluded {
				block.baselineDNF = true
				completion.BaselineExcluded++
			}
			if block.candidate == nil && !block.candidateDNF && candidateExcluded {
				block.candidateDNF = true
				completion.CandidateExcluded++
			}
			if block.baselineDNF {
				completion.BaselineDidNotFinish++
			}
			if block.candidateDNF {
				completion.CandidateDidNotFinish++
			}
			if (block.baseline == nil && !block.baselineDNF) || (block.candidate == nil && !block.candidateDNF) {
				// A block with neither a measurement nor a recorded failure for a
				// client is a run that never happened: an aborted pass, not a
				// client outcome.
				return summaryReport{}, fmt.Errorf("incomplete client pair for %+v repetition %d", stratum, repetition)
			}
			if block.baseline == nil || block.candidate == nil {
				continue
			}
			samples = append(samples, benchmark.PairedSample{Baseline: *block.baseline, Candidate: *block.candidate})
		}
		completion.PairedBlocks = len(samples)
		comparison := stratifiedComparison{Stratum: stratum, Completion: completion}
		for _, client := range []benchmark.Client{baseline, candidate} {
			if identity, ok := identities[summaryProductKey{Stratum: stratum, Client: client}]; ok {
				comparison.TransportPolicies = append(comparison.TransportPolicies, clientTransportPolicy{Client: client, TLSValidation: identity.TLSValidation, TransportLabel: identity.TransportLabel})
			}
		}
		if baselineExcluded {
			comparison.ClientExclusions = append(comparison.ClientExclusions, baselineExclusion)
		}
		if candidateExcluded {
			comparison.ClientExclusions = append(comparison.ClientExclusions, candidateExclusion)
		}
		if len(samples) < minimumBlocks {
			if completion.BaselineDidNotFinish == 0 && completion.CandidateDidNotFinish == 0 {
				return summaryReport{}, fmt.Errorf("stratum %+v has %d complete blocks, want at least %d", stratum, len(samples), minimumBlocks)
			}
			comparison.ComparisonWithheld = fmt.Sprintf("%d paired blocks, want at least %d: %s did not finish %d of %d blocks (%d excluded by the plan), %s did not finish %d of %d (%d excluded by the plan)",
				len(samples), minimumBlocks, baseline, completion.BaselineDidNotFinish, completion.BlocksObserved, completion.BaselineExcluded, candidate, completion.CandidateDidNotFinish, completion.BlocksObserved, completion.CandidateExcluded)
			report.Comparisons = append(report.Comparisons, comparison)
			continue
		}

		summary, err := benchmark.SummarizePaired(samples, seed, resamples)
		if err != nil {
			return summaryReport{}, fmt.Errorf("summarize stratum %+v: %w", stratum, err)
		}
		comparison.Summary = &summary
		report.Comparisons = append(report.Comparisons, comparison)
	}
	if len(report.Comparisons) == 0 {
		return summaryReport{}, fmt.Errorf("no strata contain either requested client")
	}
	return report, nil
}
