// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestAssertTerminalFixtureStatePassesWhenArchivedJobIsClean(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_history (job_id, status) VALUES (1, 'COMPLETED')`)

	if err := assertTerminalFixtureState(dbPath, 1, "COMPLETE"); err != nil {
		t.Fatalf("expected clean archived job state to pass, got %v", err)
	}
}

func TestAssertTerminalFixtureStateEventuallyWaitsForHistoryVisibility(t *testing.T) {
	previousTimeout := terminalFixtureStateTimeout
	previousPollInterval := terminalFixtureStatePollInterval
	terminalFixtureStateTimeout = 250 * time.Millisecond
	terminalFixtureStatePollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		terminalFixtureStateTimeout = previousTimeout
		terminalFixtureStatePollInterval = previousPollInterval
	})

	dbPath := newTestWeaverStateDB(t)

	errCh := make(chan error, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			errCh <- err
			return
		}
		defer db.Close()
		_, err = db.Exec(`INSERT INTO job_history (job_id, status) VALUES (2, 'COMPLETED')`)
		errCh <- err
	}()

	if err := assertTerminalFixtureStateEventually(dbPath, 2, "COMPLETE"); err != nil {
		t.Fatalf("expected eventual terminal state validation to pass, got %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("insert delayed job history row: %v", err)
	}
}

func TestAssertTerminalFixtureStateFailsWhenLingeringFileProgressRemains(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_history (job_id, status) VALUES (7, 'COMPLETED')`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO active_file_progress (job_id) VALUES (7)`)

	err := assertTerminalFixtureState(dbPath, 7, "COMPLETE")
	if err == nil {
		t.Fatal("expected lingering active file progress to fail state validation")
	}
	if !strings.Contains(err.Error(), "active_file_progress=1") {
		t.Fatalf("expected active file progress leak to be reported, got %v", err)
	}
}

func TestAssertTerminalFixtureStateFailsWhenLingeringExtractedRowsRemain(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_history (job_id, status) VALUES (8, 'COMPLETED')`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO active_extracted (job_id, member_name, output_path) VALUES (8, 'episode01.mkv', '/config/intermediate/show/episode01.mkv')`)

	err := assertTerminalFixtureState(dbPath, 8, "COMPLETE")
	if err == nil {
		t.Fatal("expected lingering active_extracted rows to fail state validation")
	}
	if !strings.Contains(err.Error(), "active_extracted=1") {
		t.Fatalf("expected active_extracted leak to be reported, got %v", err)
	}
}

func TestAssertTerminalFixtureStateFailsWhenLingeringFailedExtractionsRemain(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_history (job_id, status) VALUES (9, 'FAILED')`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO active_failed_extractions (job_id, member_name) VALUES (9, 'episode01.mkv')`)

	err := assertTerminalFixtureState(dbPath, 9, "FAILED")
	if err == nil {
		t.Fatal("expected lingering active_failed_extractions rows to fail state validation")
	}
	if !strings.Contains(err.Error(), "active_failed_extractions=1") {
		t.Fatalf("expected active_failed_extractions leak to be reported, got %v", err)
	}
}

func TestAssertRequiredJobEventsFailsWhenExpectedEventIsMissing(t *testing.T) {
	previousTimeout := requiredJobEventTimeout
	previousPollInterval := requiredJobEventPollInterval
	previousStabilityWindow := requiredJobEventStabilityWindow
	requiredJobEventTimeout = 10 * time.Millisecond
	requiredJobEventPollInterval = time.Millisecond
	requiredJobEventStabilityWindow = 0
	t.Cleanup(func() {
		requiredJobEventTimeout = previousTimeout
		requiredJobEventPollInterval = previousPollInterval
		requiredJobEventStabilityWindow = previousStabilityWindow
	})

	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_events (job_id, kind) VALUES (10, 'JobVerificationStarted')`)

	err := assertRequiredJobEvents(dbPath, 10, []string{"RepairComplete"})
	if err == nil {
		t.Fatal("expected missing required job event to fail validation")
	}
	if !strings.Contains(err.Error(), "RepairComplete") {
		t.Fatalf("expected missing event to be reported, got %v", err)
	}
}

func TestAssertRequiredJobEventsPassesWhenExpectedEventIsPresent(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_events (job_id, kind) VALUES (11, 'RepairComplete')`)

	if err := assertRequiredJobEvents(dbPath, 11, []string{"RepairComplete"}); err != nil {
		t.Fatalf("expected required event validation to pass: %v", err)
	}
}

func TestAssertRequiredJobEventsWaitsForStableSnapshotBeforeFailing(t *testing.T) {
	previousTimeout := requiredJobEventTimeout
	previousPollInterval := requiredJobEventPollInterval
	previousStabilityWindow := requiredJobEventStabilityWindow
	requiredJobEventTimeout = 150 * time.Millisecond
	requiredJobEventPollInterval = 5 * time.Millisecond
	requiredJobEventStabilityWindow = 250 * time.Millisecond
	t.Cleanup(func() {
		requiredJobEventTimeout = previousTimeout
		requiredJobEventPollInterval = previousPollInterval
		requiredJobEventStabilityWindow = previousStabilityWindow
	})

	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()
	mustExecWeaverStateSQL(t, db, `PRAGMA journal_mode = WAL`)

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_events (job_id, kind) VALUES (14, 'JobVerificationStarted')`)

	errCh := make(chan error, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		if _, err := db.Exec(`INSERT INTO job_events (job_id, kind) VALUES (14, 'JobVerificationComplete')`); err != nil {
			errCh <- err
			return
		}
		time.Sleep(170 * time.Millisecond)
		_, err := db.Exec(`INSERT INTO job_events (job_id, kind) VALUES (14, 'RepairComplete')`)
		errCh <- err
	}()

	if err := assertRequiredJobEvents(dbPath, 14, []string{"RepairComplete"}); err != nil {
		t.Fatalf("expected required event validation to wait for delayed repair event: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("insert delayed job events: %v", err)
	}
	if err := assertRequiredJobEvents(dbPath, 14, []string{"RepairComplete"}); err != nil {
		t.Fatalf("expected delayed repair event to remain visible after persistence: %v", err)
	}
}

func TestAssertForbiddenJobEventsFailsWhenForbiddenEventIsPresent(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_events (job_id, kind) VALUES (12, 'JobVerificationStarted')`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO job_events (job_id, kind) VALUES (12, 'ExtractionMemberFailed')`)

	err := assertForbiddenJobEvents(dbPath, 12, []string{"JobVerificationStarted", "JobVerificationComplete"})
	if err == nil {
		t.Fatal("expected forbidden job event validation to fail")
	}
	if !strings.Contains(err.Error(), "JobVerificationStarted") {
		t.Fatalf("expected forbidden event to be reported, got %v", err)
	}
}

func TestAssertForbiddenJobEventsPassesWhenForbiddenEventIsAbsent(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_events (job_id, kind) VALUES (13, 'ExtractionMemberFailed')`)

	if err := assertForbiddenJobEvents(dbPath, 13, []string{"JobVerificationStarted", "JobVerificationComplete"}); err != nil {
		t.Fatalf("expected forbidden event validation to pass: %v", err)
	}
}

func TestAssertMaxJobEventCountsRejectsRepeatedVerification(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO job_events (job_id, kind) VALUES (15, 'JobVerificationStarted')`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO job_events (job_id, kind) VALUES (15, 'JobVerificationStarted')`)

	err := assertMaxJobEventCounts(dbPath, 15, map[string]int{"JobVerificationStarted": 1})
	if err == nil {
		t.Fatal("expected repeated verification to fail the event-count assertion")
	}
	if !strings.Contains(err.Error(), "JobVerificationStarted") {
		t.Fatalf("expected repeated event to be reported, got %v", err)
	}
}

func TestAssertOutputBLAKE3ChecksCompletedOutput(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	outputDir := t.TempDir()
	relativePath := filepath.Join("work", "payload", "movie.mkv")
	outputPath := filepath.Join(outputDir, relativePath)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	if err := os.WriteFile(outputPath, []byte("verified output"), 0o644); err != nil {
		t.Fatalf("write output: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO job_history (job_id, status, output_dir) VALUES (16, 'COMPLETED', ?)`, outputDir); err != nil {
		t.Fatalf("insert completed job: %v", err)
	}
	if err := assertOutputBLAKE3(dbPath, 16, map[string]string{
		"work/payload/movie.mkv": "70921b7152a16c7f6f7c6af596235ac7e0bdb6bbe4ae66cfc42146bdb6b7d029",
	}); err != nil {
		t.Fatalf("expected matching output digest to pass: %v", err)
	}

	if err := assertOutputBLAKE3(dbPath, 16, map[string]string{"../outside": ""}); err == nil {
		t.Fatal("expected non-relative output digest path to fail")
	}
}

func TestAssertNoOrphanActiveStatePathFailsForOrphanRows(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `INSERT INTO active_file_progress (job_id) VALUES (99)`)

	err := assertNoOrphanActiveStatePath(dbPath)
	if err == nil {
		t.Fatal("expected orphan active state to fail validation")
	}
	if !strings.Contains(err.Error(), "active_file_progress=1") {
		t.Fatalf("expected orphaned active_file_progress row to be reported, got %v", err)
	}
}

func TestApplyRuntimeFileIdentityRewriteTerminalCheckFailsWhenJobCompletesBeforeObservation(t *testing.T) {
	status, errMsg, overridden := applyRuntimeFileIdentityRewriteTerminalCheck(
		"COMPLETE",
		&ScenarioFileIdentityRewriteAssertion{
			RequiredCurrentFilenames:     []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar"},
			ForbiddenCurrentFilenames:    []string{"51273aad56a8b904e96928935278a627.101"},
			RequiredClassificationSource: "par2",
		},
		false,
		fileIdentityRewriteObservation{
			RequiredClassificationSource: "par2",
			MissingCurrentFilenames:      []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar"},
			ForbiddenCurrentFilenames:    []string{"51273aad56a8b904e96928935278a627.101"},
		},
		"",
	)
	if !overridden {
		t.Fatal("expected runtime file identity rewrite terminal check to override terminal success")
	}
	if status != "RUNTIME_ASSERTION_ERROR" {
		t.Fatalf("expected runtime assertion error status, got %s", status)
	}
	if !strings.Contains(errMsg, "job reached COMPLETE before file identity rewrite oracle observed") {
		t.Fatalf("expected terminal failure message to mention the missed oracle, got %q", errMsg)
	}
	if !strings.Contains(errMsg, "archive.part1.rar") || !strings.Contains(errMsg, "51273aad56a8b904e96928935278a627.101") {
		t.Fatalf("expected terminal failure message to include missing and forbidden filenames, got %q", errMsg)
	}
}

func TestAssertLogLinesIgnoresDebugRingReplays(t *testing.T) {
	maxCount := 1
	raw := strings.Join([]string{
		"2026-01-02T03:04:05.000001+00:00  INFO weaver_server_core::pipeline::stage: direct unpack armed job_id=7 set=a",
		`2026-01-02T03:04:06.000001+00:00  WARN weaver_server_core::runtime::job_debug_ring: stall diagnostics line job_id=7 reason="dispatch stall: job not eligible" at=2026-01-02T03:04:05.000001+00:00 replayed_level=INFO replayed_target="weaver_server_core::pipeline::stage" replayed_message="direct unpack armed" replayed_fields="job_id=7 set=a"`,
	}, "\n")
	err := assertLogLines(raw, 7, &ScenarioLogAssertion{Lines: []ScenarioLogLineAssertion{{
		Message:  "direct unpack armed",
		MinCount: 1,
		MaxCount: &maxCount,
	}}})
	if err != nil {
		t.Fatalf("expected the replay not to count, got %v", err)
	}
}

func TestAssertPar2CleanSettlementRejectsUnknownVerificationMode(t *testing.T) {
	err := assertPar2CleanSettlement(49, &ScenarioPar2CleanSettlementAssertion{
		ExpectedSetSliceSizes: map[string]uint64{"typed-set": 64},
		ExpectedSetVerificationModes: map[string][]string{
			"typed-set": {"mystery"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), `unknown verification mode "mystery"`) {
		t.Fatalf("expected unknown verification mode failure, got %v", err)
	}
}

func newTestWeaverStateDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "weaver-state.sqlite")
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	mustExecWeaverStateSQL(t, db, `CREATE TABLE job_history (job_id INTEGER PRIMARY KEY, status TEXT NOT NULL, output_dir TEXT)`)
	mustExecWeaverStateSQL(t, db, `CREATE TABLE job_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id INTEGER NOT NULL,
		kind TEXT NOT NULL
	)`)
	mustExecWeaverStateSQL(t, db, `CREATE TABLE active_jobs (job_id INTEGER PRIMARY KEY)`)
	for _, table := range weaverActiveStateTables[1:] {
		if table.Name == "active_file_identities" {
			mustExecWeaverStateSQL(t, db, `CREATE TABLE active_file_identities (
				job_id INTEGER NOT NULL,
				file_index INTEGER NOT NULL,
				source_filename TEXT NOT NULL,
				current_filename TEXT NOT NULL,
				canonical_filename TEXT,
				classification_kind TEXT,
				classification_set_name TEXT,
				classification_volume_index INTEGER,
				classification_source TEXT NOT NULL DEFAULT 'declared',
				PRIMARY KEY (job_id, file_index)
			)`)
			continue
		}
		if table.Name == "active_files" {
			mustExecWeaverStateSQL(t, db, `CREATE TABLE active_files (
				job_id INTEGER NOT NULL,
				file_index INTEGER NOT NULL,
				filename TEXT NOT NULL,
				PRIMARY KEY (job_id, file_index)
			)`)
			continue
		}
		if table.Name == "active_extracted" {
			mustExecWeaverStateSQL(t, db, `CREATE TABLE active_extracted (
				job_id INTEGER NOT NULL,
				member_name TEXT NOT NULL,
				output_path TEXT NOT NULL,
				PRIMARY KEY (job_id, member_name)
			)`)
			continue
		}
		if table.Name == "active_failed_extractions" {
			mustExecWeaverStateSQL(t, db, `CREATE TABLE active_failed_extractions (
				job_id INTEGER NOT NULL,
				member_name TEXT NOT NULL,
				PRIMARY KEY (job_id, member_name)
			)`)
			continue
		}
		mustExecWeaverStateSQL(t, db, "CREATE TABLE "+table.Name+" (job_id INTEGER NOT NULL)")
	}

	return dbPath
}

func openTestWeaverStateDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db %s: %v", dbPath, err)
	}
	return db
}

func mustExecWeaverStateSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func TestMatchForbiddenOutputPathsMatchesNamesGlobsAndPaths(t *testing.T) {
	delivered := []string{
		"test-media.mkv",
		"test-media.mkv.001",
		"test-media.mkv.002",
		"Season 01/test-media.mkv.003",
		"notes.nfo",
	}

	cases := []struct {
		name     string
		patterns []string
		want     []string
	}{
		{
			name:     "a bare glob reaches a file wherever it landed",
			patterns: []string{"test-media.mkv.0*"},
			want:     []string{"Season 01/test-media.mkv.003", "test-media.mkv.001", "test-media.mkv.002"},
		},
		{
			name:     "a literal name is a glob with nothing to expand",
			patterns: []string{"test-media.mkv.002"},
			want:     []string{"test-media.mkv.002"},
		},
		{
			name:     "a pattern with a separator is anchored to the relative path",
			patterns: []string{"Season 01/*.003"},
			want:     []string{"Season 01/test-media.mkv.003"},
		},
		{
			name:     "the release itself is not claimed by a part glob",
			patterns: []string{"test-media.mkv.0*"},
			want:     []string{"Season 01/test-media.mkv.003", "test-media.mkv.001", "test-media.mkv.002"},
		},
		{
			name:     "overlapping patterns report each file once",
			patterns: []string{"test-media.mkv.001", "test-media.mkv.0*"},
			want:     []string{"Season 01/test-media.mkv.003", "test-media.mkv.001", "test-media.mkv.002"},
		},
		{
			name:     "nothing forbidden is nothing found",
			patterns: []string{"*.rar", "  "},
			want:     nil,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			found, err := matchForbiddenOutputPaths(test.patterns, delivered)
			if err != nil {
				t.Fatalf("match: %v", err)
			}
			if strings.Join(found, "|") != strings.Join(test.want, "|") {
				t.Fatalf("found = %v, want %v", found, test.want)
			}
		})
	}

	if _, err := matchForbiddenOutputPaths([]string{"[bad"}, delivered); err == nil {
		t.Fatal("a malformed pattern must be an error rather than a silent pass")
	}
}

func TestAssertForbiddenOutputPathsReadsTheDeliveredTree(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	outputDir := t.TempDir()
	writeStageTestFile(t, filepath.Join(outputDir, "test-media.mkv"), []byte("joined"))
	mustExecWeaverStateSQL(t, db,
		`INSERT INTO job_history (job_id, status, output_dir) VALUES (1, 'COMPLETED', '`+outputDir+`')`)

	if err := assertForbiddenOutputPaths(dbPath, 1, []string{"test-media.mkv.0*"}); err != nil {
		t.Fatalf("a delivered tree holding only the release must pass, got %v", err)
	}

	writeStageTestFile(t, filepath.Join(outputDir, "test-media.mkv.002"), []byte("part"))
	err := assertForbiddenOutputPaths(dbPath, 1, []string{"test-media.mkv.0*"})
	if err == nil {
		t.Fatal("a leftover split part must fail the assertion")
	}
	if !strings.Contains(err.Error(), "test-media.mkv.002") {
		t.Fatalf("the failure must name the leftover, got %v", err)
	}
	if !strings.Contains(err.Error(), "delivered: ") {
		t.Fatalf("the failure must list what was delivered, got %v", err)
	}
}

func TestPartialDeliveryExpectsPartialHistoryBehindFailedFacade(t *testing.T) {
	scenario := &Scenario{
		Slug:            "partial-delivery",
		ExpectedOutcome: outcomePartialDelivery,
	}

	if got := scenario.expectedHistoryStatus("FAILED"); got != "PARTIAL" {
		t.Fatalf("partial delivery history = %q, want PARTIAL", got)
	}
	if !scenario.deliveredOn("FAILED") {
		t.Fatal("a partial delivery must run the delivered-output checks behind a FAILED facade")
	}
	if got := scenario.expectedHistoryStatus("COMPLETE"); got != "COMPLETE" {
		t.Fatalf("a completed facade still expects COMPLETE history, got %q", got)
	}

	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()
	mustExecWeaverStateSQL(t, db, `INSERT INTO job_history (job_id, status) VALUES (11, 'partial')`)
	if err := assertTerminalFixtureState(dbPath, 11, scenario.expectedHistoryStatus("FAILED")); err != nil {
		t.Fatalf("partial history behind a FAILED facade must pass, got %v", err)
	}

	whole := &Scenario{Slug: "whole-delivery", ExpectedOutcome: "success"}
	if got := whole.expectedHistoryStatus("FAILED"); got != "FAILED" {
		t.Fatalf("a scenario that expects success keeps the facade status, got %q", got)
	}
	if whole.deliveredOn("FAILED") {
		t.Fatal("a FAILED job the scenario does not expect to deliver in part has no delivered outputs to check")
	}
	if err := assertTerminalFixtureState(dbPath, 11, whole.expectedHistoryStatus("FAILED")); err == nil {
		t.Fatal("a scenario that expects FAILED history must mismatch a partial row")
	}
	var nilScenario *Scenario
	if got := nilScenario.expectedHistoryStatus("FAILED"); got != "FAILED" {
		t.Fatalf("a scenario that failed to load expects the facade status, got %q", got)
	}
}

func TestFileCompleteReadsTheGraphCheckpoint(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "weaver.db")
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()
	mustExecWeaverStateSQL(t, db, `CREATE TABLE graph_members (job_id INTEGER NOT NULL, node_id INTEGER NOT NULL, member_id INTEGER NOT NULL, name TEXT NOT NULL, length INTEGER)`)
	mustExecWeaverStateSQL(t, db, `CREATE TABLE graph_coverage (job_id INTEGER NOT NULL, node_id INTEGER NOT NULL, member_id INTEGER NOT NULL, range_start INTEGER NOT NULL, range_end INTEGER NOT NULL)`)

	check := func(want bool) {
		t.Helper()
		got, err := fileComplete(db, weaverDatastoreSQLite, 7, "media.mkv")
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("complete = %v, want %v", got, want)
		}
	}

	// Nothing written yet.
	check(false)

	// A root member whose length is unknown is not complete.
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_members VALUES (7, 0, 1, 'media.mkv', NULL)`)
	check(false)

	// Known length, with a hole in its runs.
	mustExecWeaverStateSQL(t, db, `UPDATE graph_members SET length = 300`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_coverage VALUES (7, 0, 1, 0, 100), (7, 0, 1, 200, 300)`)
	check(false)

	// The same name inside an archive, or in another job, does not count.
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_members VALUES (7, 3, 1, 'media.mkv', 100), (8, 0, 1, 'media.mkv', 100)`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_coverage VALUES (7, 3, 1, 0, 100), (8, 0, 1, 0, 100)`)
	check(false)

	// Its runs cover every byte.
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_coverage VALUES (7, 0, 1, 100, 200)`)
	check(true)
}
