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

type comparisonStratum struct {
	FixtureID        string                     `json:"fixture_id"`
	Profile          string                     `json:"profile"`
	ExecutionTarget  benchmark.ExecutionTarget  `json:"execution_target"`
	Transport        benchmark.Transport        `json:"transport"`
	TLSValidation    benchmark.TLSValidation    `json:"tls_validation"`
	TransportLabel   string                     `json:"transport_label"`
	ArchiveToolchain benchmark.ArchiveToolchain `json:"archive_toolchain"`
	ServerLinkID     string                     `json:"server_link_id"`
	ServerEgressBPS  uint64                     `json:"server_egress_bits_per_second"`
	ServerBurstBytes uint64                     `json:"server_burst_bytes"`
}

type stratifiedComparison struct {
	Stratum comparisonStratum       `json:"stratum"`
	Summary benchmark.PairedSummary `json:"summary"`
}

type comparisonBlock struct {
	baseline  *float64
	candidate *float64
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
}

func summarize(args []string) error {
	flags := flag.NewFlagSet("summarize", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var artifactRoot, baselineName, candidateName string
	var minimumBlocks, resamples int
	var seed int64
	flags.StringVar(&artifactRoot, "artifacts", "", "benchmark artifact root containing sequential queue.json files")
	flags.StringVar(&baselineName, "baseline", "", "baseline client: weaver, sabnzbd, or nzbget")
	flags.StringVar(&candidateName, "candidate", "", "candidate client: weaver, sabnzbd, or nzbget")
	flags.IntVar(&minimumBlocks, "minimum-blocks", 20, "minimum complete paired randomized blocks per stratum")
	flags.IntVar(&resamples, "bootstrap-resamples", benchmark.DefaultBootstrapResamples, "fixed-seed paired bootstrap resamples")
	flags.Int64Var(&seed, "bootstrap-seed", 20260802, "deterministic paired bootstrap seed")
	if err := flags.Parse(args); err != nil {
		return err
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
	artifacts, err := loadSequentialArtifacts(artifactRoot)
	if err != nil {
		return err
	}
	report, err := buildSummaryReport(artifacts, baseline, candidate, minimumBlocks, seed, resamples)
	if err != nil {
		return err
	}
	return printJSON(report)
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

func loadSequentialArtifacts(root string) ([]benchmark.QueueArtifact, error) {
	plannedRuns, err := loadSummaryExecutionContext(root)
	if err != nil {
		return nil, err
	}
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
			if artifact.Status != "passed" {
				return fmt.Errorf("sequential artifact %s is not publishable: status=%s error=%s", path, artifact.Status, artifact.Error)
			}
			artifacts = append(artifacts, artifact)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(artifacts) == 0 {
		return nil, fmt.Errorf("artifact root %s contains no passed sequential queue artifacts", root)
	}
	return artifacts, nil
}

func loadSummaryExecutionContext(root string) (map[string]benchmark.Run, error) {
	manifestPath := filepath.Join(root, "execution-manifest.json")
	contents, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read summary execution manifest: %w", err)
	}
	var manifest executionManifest
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode summary execution manifest: %w", err)
	}
	if manifest.SchemaVersion != 1 || manifest.Command != "sequential" || manifest.ExecutionTarget == "" || manifest.Profile == "" || len(manifest.ExecutableSHA256) != 64 {
		return nil, fmt.Errorf("summary execution manifest has unsupported or incomplete provenance")
	}
	if manifest.PlanSnapshotPath != "plan.snapshot.json" || manifest.AdapterSnapshot != "adapter-catalog.snapshot.json" {
		return nil, fmt.Errorf("summary execution manifest uses unexpected snapshot paths")
	}
	planSnapshot := filepath.Join(root, manifest.PlanSnapshotPath)
	adapterSnapshot := filepath.Join(root, manifest.AdapterSnapshot)
	planDigest, err := sha256File(planSnapshot)
	if err != nil {
		return nil, fmt.Errorf("hash snapshotted plan: %w", err)
	}
	adapterDigest, err := sha256File(adapterSnapshot)
	if err != nil {
		return nil, fmt.Errorf("hash snapshotted adapter catalog: %w", err)
	}
	if planDigest != manifest.PlanSHA256 || adapterDigest != manifest.AdapterSHA256 {
		return nil, fmt.Errorf("summary execution snapshot digest does not match immutable manifest")
	}
	planned, err := benchmark.LoadPlan(planSnapshot)
	if err != nil {
		return nil, fmt.Errorf("load snapshotted plan: %w", err)
	}
	target := benchmark.ExecutionTarget(manifest.ExecutionTarget)
	if planned.Profile != manifest.Profile {
		return nil, fmt.Errorf("execution manifest profile %q does not match snapshotted plan %q", manifest.Profile, planned.Profile)
	}
	catalog, err := benchmark.LoadAdapterCatalog(adapterSnapshot)
	if err != nil {
		return nil, fmt.Errorf("load snapshotted adapter catalog: %w", err)
	}
	if err := catalog.ValidateFor(planned, target); err != nil {
		return nil, fmt.Errorf("validate snapshotted adapter catalog: %w", err)
	}
	plannedRuns := make(map[string]benchmark.Run)
	for _, run := range planned.Runs {
		if run.ExecutionTarget == target {
			plannedRuns[run.ID] = run
		}
	}
	if len(plannedRuns) == 0 {
		return nil, fmt.Errorf("snapshotted plan has no runs for execution target %q", target)
	}
	return plannedRuns, nil
}

func buildSummaryReport(artifacts []benchmark.QueueArtifact, baseline, candidate benchmark.Client, minimumBlocks int, seed int64, resamples int) (summaryReport, error) {
	groups := make(map[comparisonStratum]map[int]*comparisonBlock)
	identities := make(map[summaryProductKey]summaryProductIdentity)
	for _, artifact := range artifacts {
		if artifact.SchemaVersion != 6 {
			return summaryReport{}, fmt.Errorf("summary input %s uses queue artifact schema %d, want 6", artifact.SuiteID, artifact.SchemaVersion)
		}
		if artifact.Status != "passed" || artifact.SubmissionMode != benchmark.SubmissionModeSequential {
			return summaryReport{}, fmt.Errorf("summary input contains a non-passed sequential artifact %s", artifact.SuiteID)
		}
		if artifact.AdapterResult == nil || artifact.AdapterResult.SchemaVersion != 5 {
			return summaryReport{}, fmt.Errorf("sequential artifact %s lacks queue adapter result schema 5", artifact.SuiteID)
		}
		if len(artifact.Jobs) != 1 {
			return summaryReport{}, fmt.Errorf("sequential artifact %s contains %d jobs, want exactly one", artifact.SuiteID, len(artifact.Jobs))
		}
		job := artifact.Jobs[0]
		if len(artifact.Runs) != 1 || artifact.Runs[0].ID != job.Run.ID || artifact.AdapterResult.SuiteID != artifact.SuiteID || len(artifact.AdapterResult.Jobs) != 1 || artifact.AdapterResult.Jobs[0].RunID != job.Run.ID || !reflect.DeepEqual(artifact.AdapterResult.Jobs[0], job.AdapterResult) {
			return summaryReport{}, fmt.Errorf("sequential artifact %s has inconsistent run or adapter-result identity", artifact.SuiteID)
		}
		if artifact.AdapterResult.Client != job.Run.Client || artifact.AdapterResult.ArchiveToolchain != job.Run.ArchiveToolchain || artifact.AdapterResult.ExecutionTarget != job.Run.ExecutionTarget || artifact.AdapterResult.Transport != job.Run.Transport || artifact.AdapterResult.TLSValidation != job.Run.TLSValidation || artifact.AdapterResult.TransportLabel != job.Run.TransportLabel || artifact.AdapterResult.ServerLink != job.Run.ServerLink {
			return summaryReport{}, fmt.Errorf("sequential artifact %s has adapter metadata inconsistent with its planned run", artifact.SuiteID)
		}
		if job.Run.ServerLink.EgressBitsPerSecond > 0 {
			if artifact.ShaperBefore == nil || artifact.ShaperAfter == nil {
				return summaryReport{}, fmt.Errorf("shaped sequential artifact %s lacks shaper attestations", artifact.SuiteID)
			}
			if err := artifact.ShaperBefore.ValidateFor(job.Run.ServerLink); err != nil {
				return summaryReport{}, fmt.Errorf("shaped sequential artifact %s before snapshot: %w", artifact.SuiteID, err)
			}
			if err := artifact.ShaperAfter.ValidateFor(job.Run.ServerLink); err != nil {
				return summaryReport{}, fmt.Errorf("shaped sequential artifact %s after snapshot: %w", artifact.SuiteID, err)
			}
			delivered, err := benchmark.ValidateShaperSnapshotPair(*artifact.ShaperBefore, *artifact.ShaperAfter)
			if err != nil {
				return summaryReport{}, fmt.Errorf("shaped sequential artifact %s snapshot pair: %w", artifact.SuiteID, err)
			}
			if delivered == 0 || delivered != artifact.ShaperDownstreamBytes {
				return summaryReport{}, fmt.Errorf("shaped sequential artifact %s has invalid shaper byte evidence", artifact.SuiteID)
			}
		}
		if job.Run.Client != baseline && job.Run.Client != candidate {
			continue
		}
		if job.Outcome != "completed" || job.Verification == nil || job.AdapterResult.SubmissionToTerminalNanoseconds <= 0 {
			return summaryReport{}, fmt.Errorf("sequential artifact %s contains an unverified or invalid measurement", artifact.SuiteID)
		}
		if job.AdapterResult.TerminalObservationUncertainty > job.AdapterResult.SubmissionToTerminalNanoseconds/100 {
			return summaryReport{}, fmt.Errorf("sequential artifact %s exceeds the 1%% terminal-observation uncertainty limit", artifact.SuiteID)
		}
		stratum := comparisonStratum{
			FixtureID:        job.Run.FixtureID,
			Profile:          job.Run.Profile,
			ExecutionTarget:  job.Run.ExecutionTarget,
			Transport:        job.Run.Transport,
			TLSValidation:    job.Run.TLSValidation,
			TransportLabel:   job.Run.TransportLabel,
			ArchiveToolchain: job.Run.ArchiveToolchain,
			ServerLinkID:     job.Run.ServerLink.ID,
			ServerEgressBPS:  job.Run.ServerLink.EgressBitsPerSecond,
			ServerBurstBytes: job.Run.ServerLink.BurstBytes,
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
		}
		if identity.ClientIdentity == "" || identity.ClientVersion == "" || identity.ArchiveToolchainIdentity == "" {
			return summaryReport{}, fmt.Errorf("sequential artifact %s lacks product identity evidence", artifact.SuiteID)
		}
		if previous, ok := identities[productKey]; ok && previous != identity {
			return summaryReport{}, fmt.Errorf("product identity changed within stratum %+v for client %s", stratum, job.Run.Client)
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
			if block.baseline != nil {
				return summaryReport{}, fmt.Errorf("duplicate baseline observation for %+v repetition %d", stratum, job.Run.Repetition)
			}
			block.baseline = &measurement
		} else {
			if block.candidate != nil {
				return summaryReport{}, fmt.Errorf("duplicate candidate observation for %+v repetition %d", stratum, job.Run.Repetition)
			}
			block.candidate = &measurement
		}
	}

	strata := make([]comparisonStratum, 0, len(groups))
	for stratum := range groups {
		strata = append(strata, stratum)
	}
	sort.Slice(strata, func(left, right int) bool { return fmt.Sprint(strata[left]) < fmt.Sprint(strata[right]) })
	report := summaryReport{
		SchemaVersion: 1,
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
		for _, repetition := range repetitions {
			block := blocks[repetition]
			if block.baseline == nil || block.candidate == nil {
				return summaryReport{}, fmt.Errorf("incomplete client pair for %+v repetition %d", stratum, repetition)
			}
			samples = append(samples, benchmark.PairedSample{Baseline: *block.baseline, Candidate: *block.candidate})
		}
		if len(samples) < minimumBlocks {
			return summaryReport{}, fmt.Errorf("stratum %+v has %d complete blocks, want at least %d", stratum, len(samples), minimumBlocks)
		}
		summary, err := benchmark.SummarizePaired(samples, seed, resamples)
		if err != nil {
			return summaryReport{}, fmt.Errorf("summarize stratum %+v: %w", stratum, err)
		}
		report.Comparisons = append(report.Comparisons, stratifiedComparison{Stratum: stratum, Summary: summary})
	}
	if len(report.Comparisons) == 0 {
		return summaryReport{}, fmt.Errorf("no strata contain either requested client")
	}
	return report, nil
}
