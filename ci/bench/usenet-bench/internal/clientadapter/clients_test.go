package clientadapter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func TestNZBGetHistoryCompleteAcceptsSuccessfulDetailSuffix(t *testing.T) {
	for _, status := range []string{"SUCCESS", "SUCCESS/UNPACK", "COMPLETED", "COMPLETE"} {
		if !nzbgetHistoryComplete(status) {
			t.Fatalf("successful NZBGet status %q was not terminal", status)
		}
	}
	for _, status := range []string{"QUEUED", "DOWNLOADING", "FAILURE"} {
		if nzbgetHistoryComplete(status) {
			t.Fatalf("non-terminal NZBGet status %q was accepted", status)
		}
	}
}

func TestClassifyLiveStatusSeparatesWaitingFromProcessing(t *testing.T) {
	for _, status := range []string{"QUEUED", "Paused"} {
		if got := classifyLiveStatus(status).state; got != jobQueued {
			t.Fatalf("status %q = %v, want queued", status, got)
		}
	}
	for _, status := range []string{"DOWNLOADING", "VERIFYING", "UNPACKING"} {
		if got := classifyLiveStatus(status).state; got != jobActive {
			t.Fatalf("status %q = %v, want active", status, got)
		}
	}
}

type scriptedQueueAPI struct {
	productAPI
	snapshots []map[string]jobObservation
	calls     int
}

func (api *scriptedQueueAPI) observe(context.Context, []string) (map[string]jobObservation, error) {
	if api.calls >= len(api.snapshots) {
		return api.snapshots[len(api.snapshots)-1], nil
	}
	snapshot := api.snapshots[api.calls]
	api.calls++
	return snapshot, nil
}

func TestMonitorQueueRecordsProcessingWallInsteadOfQueueWait(t *testing.T) {
	api := &scriptedQueueAPI{snapshots: []map[string]jobObservation{
		{"1": {state: jobActive, status: "DOWNLOADING"}, "2": {state: jobQueued, status: "QUEUED"}},
		{"1": {state: jobComplete, status: "SUCCESS"}, "2": {state: jobActive, status: "UNPACKING"}},
		{"2": {state: jobComplete, status: "SUCCESS"}},
	}}
	queuedAt := time.Now().UTC().Add(-time.Second)
	registrations := make(chan queuedJob, 2)
	registrations <- queuedJob{result: benchmark.QueueJobResult{RunID: "run-1", JobID: "1", QueuedAt: queuedAt}}
	registrations <- queuedJob{result: benchmark.QueueJobResult{RunID: "run-2", JobID: "2", QueuedAt: queuedAt}}
	close(registrations)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	jobs, err := monitorQueue(ctx, api, time.Millisecond, time.Minute, registrations)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("jobs = %d, want 2", len(jobs))
	}
	for _, job := range jobs {
		if job.ProcessingStartedAt.IsZero() || job.CompletionAt.IsZero() {
			t.Fatalf("job %s lacks processing timestamps: %#v", job.RunID, job)
		}
		if job.TerminalObservationLowerBound.IsZero() || job.TerminalObservedAt.IsZero() || !job.TerminalObservedAt.Equal(job.CompletionAt) || job.TerminalObservationUncertainty != job.TerminalObservedAt.Sub(job.TerminalObservationLowerBound).Nanoseconds() {
			t.Fatalf("job %s lacks bounded terminal observation timing: %#v", job.RunID, job)
		}
		if got := job.CompletionAt.Sub(job.ProcessingStartedAt).Nanoseconds(); got != job.ProcessingWallClockNanoseconds {
			t.Fatalf("job %s processing wall = %d, want %d", job.RunID, job.ProcessingWallClockNanoseconds, got)
		}
		if queueWall := job.CompletionAt.Sub(job.QueuedAt); queueWall <= time.Duration(job.ProcessingWallClockNanoseconds) {
			t.Fatalf("job %s did not preserve queue wait separately", job.RunID)
		}
	}
	if jobs[1].ProcessingStartedAt.Before(jobs[0].CompletionAt) {
		t.Fatal("second fixture processing started before the first fixture completed")
	}
}

func TestMonitorQueueMarksTerminalWithoutObservedProcessingUnavailable(t *testing.T) {
	api := &scriptedQueueAPI{snapshots: []map[string]jobObservation{
		{"1": {state: jobComplete, status: "SUCCESS"}},
	}}
	registrations := make(chan queuedJob, 1)
	registrations <- queuedJob{result: benchmark.QueueJobResult{RunID: "run-1", JobID: "1", QueuedAt: time.Now().UTC()}}
	close(registrations)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	jobs, err := monitorQueue(ctx, api, time.Millisecond, time.Minute, registrations)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].TerminalStatus != "succeeded" || jobs[0].ProcessingTimingAvailable || !strings.Contains(jobs[0].ProcessingTimingError, "before an active state") {
		t.Fatalf("unexpected terminal timing result: %#v", jobs)
	}
}

func TestMonitorQueueRecordsFailureAndContinuesOtherFixtures(t *testing.T) {
	api := &scriptedQueueAPI{snapshots: []map[string]jobObservation{
		{"1": {state: jobActive, status: "DOWNLOADING"}, "2": {state: jobActive, status: "DOWNLOADING"}},
		{"1": {state: jobFailed, status: "FAILED/UNPACK"}, "2": {state: jobActive, status: "UNPACKING"}},
		{"2": {state: jobComplete, status: "SUCCESS"}},
	}}
	queuedAt := time.Now().UTC()
	registrations := make(chan queuedJob, 2)
	registrations <- queuedJob{result: benchmark.QueueJobResult{RunID: "run-1", JobID: "1", QueuedAt: queuedAt}}
	registrations <- queuedJob{result: benchmark.QueueJobResult{RunID: "run-2", JobID: "2", QueuedAt: queuedAt}}
	close(registrations)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	jobs, err := monitorQueue(ctx, api, time.Millisecond, time.Minute, registrations)
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].TerminalStatus != "failed" || jobs[0].TerminalError != "FAILED/UNPACK" || !jobs[0].ProcessingTimingAvailable {
		t.Fatalf("failed fixture was not retained with timing: %#v", jobs[0])
	}
	if jobs[1].TerminalStatus != "succeeded" || !jobs[1].ProcessingTimingAvailable {
		t.Fatalf("later fixture did not continue to completion: %#v", jobs[1])
	}
}
