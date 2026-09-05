package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// QueueDrainMetric names what a queue-transition lane measures: one burst of
// identical submissions, timed from the first submission to the moment the
// last copy's output has been independently verified. There is no per-job
// score and no pairing; each lane is one observation of one client's queue.
const QueueDrainMetric = "first_submission_to_last_verified_terminal_output"

type queueDrainReport struct {
	SchemaVersion int              `json:"schema_version"`
	Metric        string           `json:"metric"`
	Lanes         []queueDrainLane `json:"lanes"`
}

type queueDrainLane struct {
	SuiteID          string                     `json:"suite_id"`
	Client           benchmark.Client           `json:"client"`
	ArchiveToolchain benchmark.ArchiveToolchain `json:"archive_toolchain"`
	ExecutionTarget  benchmark.ExecutionTarget  `json:"execution_target"`
	Transport        benchmark.Transport        `json:"transport"`
	TLSValidation    benchmark.TLSValidation    `json:"tls_validation"`
	TransportLabel   string                     `json:"transport_label"`
	Profile          string                     `json:"profile"`
	FixtureID        string                     `json:"fixture_id"`
	Copies           int                        `json:"copies"`
	ServerLinkID     string                     `json:"server_link_id"`
	StorageProfileID string                     `json:"storage_profile_id"`
	ClientIdentity   string                     `json:"client_identity"`
	ClientVersion    string                     `json:"client_version"`
	// Status is passed or completed_with_dnf. Only a passed lane carries a
	// drain time: a queue with a copy that did not finish has no "last
	// verified output" to time to, so its wall clocks are omitted and the
	// recorded failure is reported instead.
	Status                       string `json:"status"`
	QueueWallClockNanoseconds    int64  `json:"queue_wall_clock_nanoseconds,omitempty"`
	VerifiedWallClockNanoseconds int64  `json:"verified_wall_clock_nanoseconds,omitempty"`
	CopiesDidNotFinish           int    `json:"copies_did_not_finish"`
	Error                        string `json:"error,omitempty"`
}

// loadQueueDrainReport reads a queue-transition artifact root, binds every
// lane to the snapshotted plan, and reports each lane's drain wall clock.
func loadQueueDrainReport(root string) (queueDrainReport, error) {
	execution, err := loadSummaryExecutionContext(root, "queue-transition")
	if err != nil {
		return queueDrainReport{}, err
	}
	report := queueDrainReport{SchemaVersion: 1, Metric: QueueDrainMetric}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "queue.json" {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read queue-drain artifact %s: %w", path, err)
		}
		var artifact benchmark.QueueArtifact
		if err := json.Unmarshal(contents, &artifact); err != nil {
			return fmt.Errorf("decode queue-drain artifact %s: %w", path, err)
		}
		if artifact.SubmissionMode != benchmark.SubmissionModeQueueDrain {
			return nil
		}
		lane, err := queueDrainLaneFor(artifact, execution.PlannedRuns)
		if err != nil {
			return fmt.Errorf("queue-drain artifact %s: %w", path, err)
		}
		report.Lanes = append(report.Lanes, lane)
		return nil
	})
	if err != nil {
		return queueDrainReport{}, err
	}
	if len(report.Lanes) == 0 {
		return queueDrainReport{}, fmt.Errorf("artifact root %s contains no queue-transition artifacts", root)
	}
	sort.Slice(report.Lanes, func(left, right int) bool { return report.Lanes[left].SuiteID < report.Lanes[right].SuiteID })
	return report, nil
}

func queueDrainLaneFor(artifact benchmark.QueueArtifact, plannedRuns map[string]benchmark.Run) (queueDrainLane, error) {
	if artifact.SchemaVersion != 7 {
		return queueDrainLane{}, fmt.Errorf("uses queue artifact schema %d, want 7", artifact.SchemaVersion)
	}
	if !summarizableSequentialStatus(artifact.Status) {
		return queueDrainLane{}, fmt.Errorf("is not publishable: status=%s error=%s", artifact.Status, artifact.Error)
	}
	if artifact.AdapterResult == nil || artifact.AdapterResult.SchemaVersion != 6 || artifact.AdapterResult.SuiteID != artifact.SuiteID || artifact.AdapterResult.SubmissionMode != benchmark.SubmissionModeQueueDrain {
		return queueDrainLane{}, fmt.Errorf("lacks a matching queue adapter result of schema 6")
	}
	if len(artifact.Runs) < benchmark.QueueTransitionMinimumCopies || len(artifact.Jobs) != len(artifact.Runs) || len(artifact.AdapterResult.Jobs) != len(artifact.Runs) {
		return queueDrainLane{}, fmt.Errorf("has %d runs, %d jobs and %d adapter jobs; want one job per queued copy", len(artifact.Runs), len(artifact.Jobs), len(artifact.AdapterResult.Jobs))
	}
	first := artifact.Runs[0]
	for index, run := range artifact.Runs {
		planned, ok := plannedRuns[run.ID]
		if !ok || planned != run {
			return queueDrainLane{}, fmt.Errorf("run %s is not bound to the snapshotted plan", run.ID)
		}
		if run.FixtureID != first.FixtureID || run.Client != first.Client || run.ArchiveToolchain != first.ArchiveToolchain || run.Transport != first.Transport || run.TLSValidation != first.TLSValidation || run.TransportLabel != first.TransportLabel || run.ExecutionTarget != first.ExecutionTarget || run.ServerLink != first.ServerLink || run.StorageProfile != first.StorageProfile || run.Profile != first.Profile {
			return queueDrainLane{}, fmt.Errorf("run %s does not belong to the lane of %s", run.ID, first.ID)
		}
		if artifact.Jobs[index].Run != run || artifact.AdapterResult.Jobs[index].RunID != run.ID {
			return queueDrainLane{}, fmt.Errorf("job %d is out of step with run %s", index+1, run.ID)
		}
	}
	result := artifact.AdapterResult
	if result.Client != first.Client || result.ArchiveToolchain != first.ArchiveToolchain || result.ExecutionTarget != first.ExecutionTarget || result.Transport != first.Transport || result.TLSValidation != first.TLSValidation || result.TransportLabel != first.TransportLabel || result.ServerLink != first.ServerLink || result.StorageProfile != first.StorageProfile {
		return queueDrainLane{}, fmt.Errorf("has adapter metadata inconsistent with its planned runs")
	}
	if result.ClientIdentity == "" || result.ClientVersion == "" || result.ArchiveToolchainIdentity == "" || len(result.RenderedConfigSHA256) != 64 {
		return queueDrainLane{}, fmt.Errorf("lacks product identity evidence")
	}
	if err := validateSummaryShaperEvidence(artifact, first.ServerLink); err != nil {
		return queueDrainLane{}, err
	}
	if err := validateSummaryStorageEvidence(artifact, first.StorageProfile); err != nil {
		return queueDrainLane{}, err
	}
	lane := queueDrainLane{
		SuiteID:          artifact.SuiteID,
		Client:           first.Client,
		ArchiveToolchain: first.ArchiveToolchain,
		ExecutionTarget:  first.ExecutionTarget,
		Transport:        first.Transport,
		TLSValidation:    first.TLSValidation,
		TransportLabel:   first.TransportLabel,
		Profile:          first.Profile,
		FixtureID:        first.FixtureID,
		Copies:           len(artifact.Runs),
		ServerLinkID:     first.ServerLink.ID,
		StorageProfileID: first.StorageProfile.ID,
		ClientIdentity:   result.ClientIdentity,
		ClientVersion:    result.ClientVersion,
		Status:           artifact.Status,
	}
	for _, job := range artifact.Jobs {
		if job.Outcome == "dnf" {
			lane.CopiesDidNotFinish++
		}
	}
	if artifact.Status == "completed_with_dnf" {
		if lane.CopiesDidNotFinish == 0 || artifact.Error == "" {
			return queueDrainLane{}, fmt.Errorf("reports did-not-finish without a recorded copy failure")
		}
		lane.Error = artifact.Error
		return lane, nil
	}
	if lane.CopiesDidNotFinish != 0 || artifact.QueueVerifiedAt == nil || artifact.QueueWallClockNanoseconds <= 0 || artifact.VerifiedWallClockNanoseconds < artifact.QueueWallClockNanoseconds {
		return queueDrainLane{}, fmt.Errorf("passed without a verified drain wall clock")
	}
	for _, job := range artifact.Jobs {
		if job.Outcome != "completed" || job.Verification == nil {
			return queueDrainLane{}, fmt.Errorf("passed with an unverified copy %s", job.Run.ID)
		}
	}
	lane.QueueWallClockNanoseconds = artifact.QueueWallClockNanoseconds
	lane.VerifiedWallClockNanoseconds = artifact.VerifiedWallClockNanoseconds
	return lane, nil
}
