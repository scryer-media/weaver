package clientadapter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// A client that reports the job as active forever must be given up on after
// the job timeout and recorded as timed out, not waited on indefinitely.
func TestMonitorQueueRecordsJobsThatNeverFinishAsTimedOut(t *testing.T) {
	api := &scriptedQueueAPI{snapshots: []map[string]jobObservation{{
		"job-1": {state: jobActive, status: "Extracting"},
	}}}
	registrations := make(chan queuedJob, 1)
	acceptedAt := time.Now().Round(0)
	registrations <- queuedJob{result: benchmark.QueueJobResult{
		RunID:               "run-1",
		JobID:               "job-1",
		SubmissionStartedAt: acceptedAt.Add(-time.Millisecond),
		AcceptedAt:          acceptedAt,
		QueuedAt:            acceptedAt,
	}}
	close(registrations)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	jobs, err := monitorQueue(ctx, api, time.Millisecond, 30*time.Millisecond, registrations)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("monitor returned %d jobs, want 1", len(jobs))
	}
	job := jobs[0]
	if job.TerminalStatus != "timed_out" || !strings.Contains(job.TerminalError, `"Extracting"`) {
		t.Fatalf("job was not recorded as timed out with its last status: %#v", job)
	}
	if job.CompletionAt.Sub(acceptedAt) < 30*time.Millisecond {
		t.Fatalf("job was given up on before the timeout: %#v", job)
	}
	if job.TerminalObservedAt != job.CompletionAt || job.TerminalObservationLowerBound.IsZero() || job.SubmissionToTerminalNanoseconds != job.TerminalObservedAt.Sub(job.SubmissionStartedAt).Nanoseconds() {
		t.Fatalf("timed-out job carries inconsistent observation timing: %#v", job)
	}
}
