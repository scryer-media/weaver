// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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

func TestWeaverJSONRecordReadsAsItsTextLine(t *testing.T) {
	record := `{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver_server_core::pipeline::graph","fields":{"message":"member delivered","job_id":7,"member":"My Show.mkv","bytes":18446744073709551615,"ok":true}}`
	want := `2026-11-03T08:10:00.123456-07:00  INFO weaver_server_core::pipeline::graph: member delivered job_id=7 member="My Show.mkv" bytes=18446744073709551615 ok=true`
	if got := cleanWeaverLogLine(record); got != want {
		t.Fatalf("record read as\n%s\nwant\n%s", got, want)
	}
	spans := `{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"WARN","target":"weaver::http","fields":{"message":"slow","name":"\"plain\""},"spans":[{"name":"request","fields":{"id":3}},{"name":"bare"}]}`
	if got := cleanWeaverLogLine(spans); got != `2026-11-03T08:10:00.123456-07:00  WARN request{id=3}:bare: weaver::http: slow name="plain"` {
		t.Fatalf("spans read as %q", got)
	}
	if got := cleanWeaverLogLine("\x1b[32mplain\x1b[0m words"); got != "plain words" {
		t.Fatalf("a text line read as %q", got)
	}
	if got := cleanWeaverLog(record + "\n{half\n"); got != want+"\n{half\n" {
		t.Fatalf("a log read as %q", got)
	}
}

func TestWeaverJSONRecordsFeedTheGraphAndLineAssertions(t *testing.T) {
	record := func(ts, level, jobID, message, rest string) string {
		return `{"timestamp":"` + ts + `","level":"` + level + `","target":"` + checkpointTarget + `","fields":{"message":"` + message + `","job_id":` + jobID + rest + `}}`
	}
	log := strings.Join([]string{
		record("2026-09-22T10:00:00.000000-06:00", "INFO", "7", "RAR completion checkpoint", `,"status":"Downloading","complete_data_files":1`),
		record("2026-09-22T10:00:01.000000-06:00", "INFO", "7", "RAR completion checkpoint", `,"status":"Downloading","complete_data_files":1`),
	}, "\n")
	assertion := &ScenarioLogAssertion{Lines: []ScenarioLogLineAssertion{{
		Message:                  "RAR completion checkpoint",
		Level:                    "INFO",
		ForbidConsecutiveRepeats: true,
	}}}
	if err := assertLogLines(log, 7, assertion); err == nil {
		t.Fatal("the same checkpoint twice running in JSON records must fail")
	}
	if directLogJobID(log[:strings.Index(log, "\n")]) != "7" {
		t.Fatal("a JSON record's job_id must be read")
	}

	graph := `{"timestamp":"2026-09-22T10:00:00.000000-06:00","level":"INFO","target":"` + graphLogTarget + `","fields":{"message":"graph member done","job_id":7,"member":"a b.rar","state":"done"}}`
	events := graphLogEventsOfJobs(graph, "graph member done", []int{7})
	if len(events) != 1 || events[0]["member"] != "a b.rar" || events[0]["state"] != "done" {
		t.Fatalf("graph events from a JSON record: %v", events)
	}
}
