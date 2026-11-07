// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"path/filepath"
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
	twoTokens := `{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver::jobs","fields":{"message":"two","name":"\"a\" b \"c\"","tail":"\"a\\\""}}`
	if got := cleanWeaverLogLine(twoTokens); got != `2026-11-03T08:10:00.123456-07:00  INFO weaver::jobs: two name="\"a\" b \"c\"" tail="\"a\\\""` {
		t.Fatalf("a string quoted at each end read as %q", got)
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

func TestDelayHookFoundInAJSONRecord(t *testing.T) {
	raw := `{"timestamp":"2026-11-03T13:27:38.145719-07:00","level":"WARN","target":"weaver_server_core::e2e_failpoint","fields":{"message":"tripping e2e delay hook","delay_ms":20000,"hook":"direct_store.post_repair_verify"}}`
	clean := cleanWeaverLog(raw)
	if !weaverLogHasField(clean, "hook", "direct_store.post_repair_verify") {
		t.Fatalf("the delay hook in a JSON record must be found: %q", clean)
	}
	if weaverLogHasField(clean, "hook", "direct_store.post_repair") {
		t.Fatalf("a hook whose name only begins the recorded one must not match: %q", clean)
	}
}

func TestDelayHookFoundInATextLine(t *testing.T) {
	raw := "2026-11-03T13:27:38.145719-07:00  \x1b[33mWARN\x1b[0m weaver_server_core::e2e_failpoint: tripping e2e delay hook delay_ms=20000 hook=\"direct_store.post_repair_verify\""
	clean := cleanWeaverLog(raw)
	if !weaverLogHasField(clean, "hook", "direct_store.post_repair_verify") {
		t.Fatalf("the delay hook in a text line must be found: %q", clean)
	}
	if weaverLogHasField(clean, "hook", "direct_store.post_repair") {
		t.Fatalf("a hook whose name only begins the recorded one must not match: %q", clean)
	}
}

func TestDelayHookFoundInALogFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "weaver.log")
	log := `{"timestamp":"2026-11-03T13:27:38.100000-07:00","level":"INFO","target":"weaver_server_core::jobs","fields":{"message":"job submitted","job_id":10000}}` + "\n" +
		`{"timestamp":"2026-11-03T13:27:38.145719-07:00","level":"WARN","target":"weaver_server_core::e2e_failpoint","fields":{"message":"tripping e2e delay hook","delay_ms":20000,"hook":"direct_store.post_repair_verify"}}` + "\n"
	if err := os.WriteFile(path, []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}
	if !logHasDelayHook(path, "direct_store.post_repair_verify") {
		t.Fatal("the delay hook must be found in the log file")
	}
	if logHasDelayHook(path, "repair.task_start") {
		t.Fatal("a hook that never tripped must not be found")
	}
}
