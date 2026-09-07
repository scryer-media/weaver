package nativeadapter

import (
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// nativeSequentialResult builds the suite result the native sequential lane
// writes, with one job whose terminal outcome the caller chooses.
func nativeSequentialResult(terminalStatus, terminalError string) benchmark.QueueAdapterResult {
	started := time.Date(2026, time.September, 7, 12, 0, 0, 0, time.UTC)
	accepted := started.Add(10 * time.Millisecond)
	lowerBound := accepted.Add(1900 * time.Millisecond)
	observed := lowerBound.Add(10 * time.Millisecond)
	metrics := benchmark.ResourceMetrics{
		CPUTimeNanoseconds:  benchmark.MeasuredMeasurement("client_process_tree", "windows-job-cycle-time", "nominal-3600MHz", 123),
		InstructionsRetired: benchmark.UnavailableMeasurement("client_process", "native-instructions", "windows", "not available"),
	}
	return benchmark.QueueAdapterResult{
		SchemaVersion:            6,
		SuiteID:                  "sequential-0001",
		SubmissionMode:           benchmark.SubmissionModeSequential,
		Client:                   benchmark.Weaver,
		ArchiveToolchain:         benchmark.VanillaArchiveToolchain,
		ArchiveToolchainIdentity: "stock",
		ExecutionTarget:          benchmark.WindowsNative,
		Transport:                benchmark.Plaintext,
		TLSValidation:            benchmark.TLSNotApplicable,
		TransportLabel:           "plaintext",
		ServerLink:               benchmark.DefaultServerLinkProfile(),
		StorageProfile:           benchmark.DefaultStorageProfile(),
		QueueStartedAt:           started,
		QueueCompletedAt:         observed,
		StatusPollIntervalNanos:  int64(100 * time.Millisecond),
		ClientIdentity:           "sha256:test",
		ClientVersion:            "test",
		RenderedConfigSHA256:     strings.Repeat("a", 64),
		ResourceMetrics:          metrics,
		Jobs: []benchmark.QueueJobResult{{
			RunID:                           "run-0001",
			JobID:                           "native-run-0001",
			SubmissionStartedAt:             started,
			AcceptedAt:                      accepted,
			QueuedAt:                        accepted,
			CompletionAt:                    observed,
			FixtureWallClockNanoseconds:     observed.Sub(accepted).Nanoseconds(),
			TerminalStatus:                  terminalStatus,
			TerminalError:                   terminalError,
			ProcessingTimingError:           "native public API does not expose active-processing transitions",
			TerminalObservationLowerBound:   lowerBound,
			TerminalObservedAt:              observed,
			TerminalObservationUncertainty:  observed.Sub(lowerBound).Nanoseconds(),
			SubmissionToTerminalNanoseconds: observed.Sub(started).Nanoseconds(),
			ResourceMetrics:                 &metrics,
		}},
	}
}

// A client that reported its own failure is a measured outcome. The native
// lane records it as a did-not-finish job so the suite still publishes: the
// summarizer refuses an artifact whose status is "failed", which would
// discard every other suite in the phase for one product failure.
func TestTheNativeLaneCanRecordAClientReportedFailure(t *testing.T) {
	result := nativeSequentialResult("failed", `client job 42 terminal status "FAILED"`)
	if err := validateNativeSequentialQueueResult(result); err != nil {
		t.Fatalf("a recorded did-not-finish job was rejected: %v", err)
	}
}

// The reason is the evidence; a did-not-finish job without one is not a
// record of anything.
func TestARecordedFailureMustCarryItsReason(t *testing.T) {
	result := nativeSequentialResult("failed", "")
	if err := validateNativeSequentialQueueResult(result); err == nil {
		t.Fatal("a did-not-finish job without a reason was accepted")
	}
}

// A job the adapter gave up waiting on has no terminal observation, so the
// native lane reports it as an error rather than inventing a record.
func TestTheNativeLaneRefusesATimedOutTerminalStatus(t *testing.T) {
	result := nativeSequentialResult("timed_out", "client job 42 did not reach a terminal state within 1h0m0s of acceptance")
	if err := validateNativeSequentialQueueResult(result); err == nil {
		t.Fatal("a timed-out job was accepted as a native sequential record")
	}
}
