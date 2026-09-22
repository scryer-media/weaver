package weaver

import (
	"strings"
	"testing"
)

const checkpointTarget = "weaver_server_core::pipeline::completion::finalize::check::completion"

func checkpointLine(ts, level, jobID, payload string) string {
	// Weaver writes its log with colour; the assertion has to read through it.
	return ts + "  \x1b[32m" + level + "\x1b[0m " + checkpointTarget + ": RAR completion checkpoint job_id=" + jobID + " " + payload
}

func TestLogAssertionForbidsTheSameAnnouncementTwiceRunning(t *testing.T) {
	assertion := &ScenarioLogAssertion{Lines: []ScenarioLogLineAssertion{{
		Message:                  "RAR completion checkpoint",
		Level:                    "INFO",
		ForbidConsecutiveRepeats: true,
	}}}

	moving := strings.Join([]string{
		checkpointLine("2026-09-22T10:00:00.000000-06:00", "INFO", "7", "status=Downloading complete_data_files=1"),
		checkpointLine("2026-09-22T10:00:01.000000-06:00", "INFO", "7", "status=Downloading complete_data_files=2"),
		// Another job repeating itself is that job's problem.
		checkpointLine("2026-09-22T10:00:02.000000-06:00", "INFO", "8", "status=Downloading complete_data_files=0"),
		checkpointLine("2026-09-22T10:00:03.000000-06:00", "INFO", "8", "status=Downloading complete_data_files=0"),
		// The diagnostic level may repeat freely; only the announcement is held.
		checkpointLine("2026-09-22T10:00:04.000000-06:00", "DEBUG", "7", "status=Downloading complete_data_files=2"),
		// Moving away and back announces the earlier state again, legitimately.
		checkpointLine("2026-09-22T10:00:05.000000-06:00", "INFO", "7", "status=Downloading complete_data_files=1"),
	}, "\n")
	if err := assertLogLines(moving, 7, assertion); err != nil {
		t.Fatalf("a checkpoint that only speaks when its content moves must pass: %v", err)
	}

	repeating := strings.Join([]string{
		checkpointLine("2026-09-22T10:00:00.000000-06:00", "INFO", "7", "status=Downloading complete_data_files=1"),
		checkpointLine("2026-09-22T10:00:01.000000-06:00", "INFO", "7", "status=Downloading complete_data_files=1"),
	}, "\n")
	err := assertLogLines(repeating, 7, assertion)
	if err == nil || !strings.Contains(err.Error(), "twice running") {
		t.Fatalf("the same announcement on two timer ticks must fail, got %v", err)
	}
}

func TestLogAssertionCountsWithinTheJobAndLevel(t *testing.T) {
	raw := strings.Join([]string{
		checkpointLine("2026-09-22T10:00:00.000000-06:00", "WARN", "7", "reason=owner_cannot_finish"),
		checkpointLine("2026-09-22T10:00:01.000000-06:00", "DEBUG", "7", "reason=owner_cannot_finish"),
		checkpointLine("2026-09-22T10:00:02.000000-06:00", "WARN", "9", "reason=owner_cannot_finish"),
	}, "\n")

	one := 1
	if err := assertLogLines(raw, 7, &ScenarioLogAssertion{Lines: []ScenarioLogLineAssertion{{
		Message: "RAR completion checkpoint", Level: "warn", MinCount: 1, MaxCount: &one,
	}}}); err != nil {
		t.Fatalf("one WARN for this job is within [1,1]: %v", err)
	}
	if err := assertLogLines(raw, 7, &ScenarioLogAssertion{Lines: []ScenarioLogLineAssertion{{
		Message: "RAR completion checkpoint", MinCount: 3,
	}}}); err == nil {
		t.Fatal("two lines for this job across every level cannot satisfy a minimum of three")
	}
	if err := assertLogLines(raw, 7, &ScenarioLogAssertion{Lines: []ScenarioLogLineAssertion{{
		Message: "", MinCount: 1,
	}}}); err == nil {
		t.Fatal("an assertion with no message must be refused, not vacuously passed")
	}
}
