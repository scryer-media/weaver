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

func TestObserveActiveFileIdentityRewritePassesWhenCanonicalRowsObserved(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 42, 1, "51273...101", "archive.part1.rar", "par2")
	insertActiveFileIdentity(t, db, 42, 2, "51273...102", "archive.part2.rar", "par2")
	insertActiveFileIdentity(t, db, 42, 3, "51273...103", "archive.part3.rar", "par2")

	observation, err := observeActiveFileIdentityRewrite(dbPath, 42, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar"},
		ForbiddenCurrentFilenames:    []string{"51273aad56a8b904e96928935278a627.101"},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if !observation.Observed {
		t.Fatalf("expected rewrite observation to pass, got %#v", observation)
	}
}

func TestFileIdentityRewriteObserverReusesOpenDatabase(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 42, 1, "51273...101", "archive.part1.rar", "par2")
	insertActiveFileIdentity(t, db, 43, 1, "51273...201", "episode.mkv", "par2")

	observer, err := openFileIdentityRewriteObserver(dbPath)
	if err != nil {
		t.Fatalf("open reusable file identity rewrite observer: %v", err)
	}
	defer observer.Close()

	for _, tc := range []struct {
		jobID    int
		filename string
	}{
		{jobID: 42, filename: "archive.part1.rar"},
		{jobID: 43, filename: "episode.mkv"},
	} {
		observation, err := observer.Observe(tc.jobID, &ScenarioFileIdentityRewriteAssertion{
			RequiredCurrentFilenames:     []string{tc.filename},
			RequiredClassificationSource: "par2",
		})
		if err != nil {
			t.Fatalf("observe job %d through reusable observer: %v", tc.jobID, err)
		}
		if !observation.Observed {
			t.Fatalf("expected rewrite observation for job %d to pass, got %#v", tc.jobID, observation)
		}
	}
}

func TestObserveActiveFileIdentityRewriteFailsWhenObfuscatedRowsRemain(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 7, 1, "51273...101", "archive.part1.rar", "par2")
	insertActiveFileIdentity(t, db, 7, 2, "51273...102", "archive.part2.rar", "par2")
	insertActiveFileIdentity(t, db, 7, 3, "51273...103", "archive.part3.rar", "par2")
	insertActiveFileIdentity(t, db, 7, 4, "51273...104", "51273aad56a8b904e96928935278a627.101", "declared")

	observation, err := observeActiveFileIdentityRewrite(dbPath, 7, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar"},
		ForbiddenCurrentFilenames:    []string{"51273aad56a8b904e96928935278a627.101"},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if observation.Observed {
		t.Fatalf("expected rewrite observation to fail when obfuscated names remain, got %#v", observation)
	}
	if len(observation.ForbiddenCurrentFilenames) != 1 || observation.ForbiddenCurrentFilenames[0] != "51273aad56a8b904e96928935278a627.101" {
		t.Fatalf("expected forbidden obfuscated filename to be reported, got %#v", observation)
	}
}

func TestObserveActiveFileIdentityRewriteFailsWhenPar2ClassificationIsMissing(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 9, 1, "51273...101", "archive.part1.rar", "declared")
	insertActiveFileIdentity(t, db, 9, 2, "51273...102", "archive.part2.rar", "declared")
	insertActiveFileIdentity(t, db, 9, 3, "51273...103", "archive.part3.rar", "declared")

	observation, err := observeActiveFileIdentityRewrite(dbPath, 9, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar"},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if observation.Observed {
		t.Fatalf("expected rewrite observation to fail when classification_source=par2 is missing, got %#v", observation)
	}
	if len(observation.WrongClassificationSources) != 3 {
		t.Fatalf("expected all canonical rows to report source mismatches, got %#v", observation)
	}
	if !strings.Contains(observation.WrongClassificationSources[0], "found: declared") {
		t.Fatalf("expected source mismatch details to include the observed classification source, got %#v", observation)
	}
}

func TestObserveActiveFileIdentityRewriteUsesObserverRowsAfterActiveCleanup(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	if err := installFileIdentityRewriteObserver(dbPath); err != nil {
		t.Fatalf("install file identity rewrite observer: %v", err)
	}
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 17, 1, "archive.part1.rar", "archive.part1.rar", "par2")
	insertActiveFileIdentity(t, db, 17, 2, "51273...102", "51273aad56a8b904e96928935278a627.102", "declared")
	if _, err := db.Exec(
		`UPDATE active_file_identities
		 SET current_filename = ?,
		     canonical_filename = ?,
		     classification_source = ?
		 WHERE job_id = ? AND file_index = ?`,
		"archive.part2.rar",
		"archive.part2.rar",
		"par2",
		17,
		2,
	); err != nil {
		t.Fatalf("update active_file_identities row: %v", err)
	}
	deleteActiveFileIdentities(t, db, 17)

	observation, err := observeActiveFileIdentityRewrite(dbPath, 17, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar", "archive.part2.rar"},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if !observation.Observed {
		t.Fatalf("expected observer rows to satisfy rewrite assertion after active cleanup, got %#v", observation)
	}

	assertObservedFileIdentityRewriteRow(t, db, 17, "insert", "archive.part1.rar", "archive.part1.rar", "par2")
	assertObservedFileIdentityRewriteRow(t, db, 17, "update", "archive.part2.rar", "archive.part2.rar", "par2")
}

func TestObserveActiveFileIdentityRewriteUsesLatestObserverRowsAfterSupersededObfuscatedRows(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	if err := installFileIdentityRewriteObserver(dbPath); err != nil {
		t.Fatalf("install file identity rewrite observer: %v", err)
	}
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 19, 0, "51273...101", "51273aad56a8b904e96928935278a627.101", "declared")
	insertActiveFileIdentity(t, db, 19, 1, "51273...102", "51273aad56a8b904e96928935278a627.102", "declared")
	insertActiveFileIdentity(t, db, 19, 2, "51273...103", "51273aad56a8b904e96928935278a627.103", "declared")
	updateActiveFileIdentity(t, db, 19, 0, "51273aad56a8b904e96928935278a627.101", "51273aad56a8b904e96928935278a627.101", "probe")
	updateActiveFileIdentity(t, db, 19, 0, "archive.part1.rar", "archive.part1.rar", "par2")
	updateActiveFileIdentity(t, db, 19, 1, "51273aad56a8b904e96928935278a627.102", "51273aad56a8b904e96928935278a627.102", "probe")
	updateActiveFileIdentity(t, db, 19, 1, "archive.part2.rar", "archive.part2.rar", "par2")
	updateActiveFileIdentity(t, db, 19, 2, "51273aad56a8b904e96928935278a627.103", "51273aad56a8b904e96928935278a627.103", "probe")
	updateActiveFileIdentity(t, db, 19, 2, "archive.part3.rar", "archive.part3.rar", "par2")
	deleteActiveFileIdentities(t, db, 19)

	observation, err := observeActiveFileIdentityRewrite(dbPath, 19, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames: []string{
			"archive.part1.rar",
			"archive.part2.rar",
			"archive.part3.rar",
		},
		ForbiddenCurrentFilenames: []string{
			"51273aad56a8b904e96928935278a627.101",
			"51273aad56a8b904e96928935278a627.102",
			"51273aad56a8b904e96928935278a627.103",
		},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if !observation.Observed {
		t.Fatalf("expected latest observer rows to satisfy rewrite assertion, got %#v", observation)
	}
	for _, observed := range observation.ObservedCurrentFilenames {
		if strings.Contains(observed, "51273aad56a8b904e96928935278a627") {
			t.Fatalf("expected effective rows to omit superseded obfuscated filenames, got %#v", observation.ObservedCurrentFilenames)
		}
	}
}

func TestObserveActiveFileIdentityRewriteFailsWhenLatestObserverRowRemainsForbidden(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	if err := installFileIdentityRewriteObserver(dbPath); err != nil {
		t.Fatalf("install file identity rewrite observer: %v", err)
	}
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 20, 1, "51273...101", "archive.part1.rar", "par2")
	updateActiveFileIdentity(t, db, 20, 1, "51273aad56a8b904e96928935278a627.101", "51273aad56a8b904e96928935278a627.101", "declared")
	deleteActiveFileIdentities(t, db, 20)

	observation, err := observeActiveFileIdentityRewrite(dbPath, 20, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar"},
		ForbiddenCurrentFilenames:    []string{"51273aad56a8b904e96928935278a627.101"},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if observation.Observed {
		t.Fatalf("expected latest forbidden observer row to fail, got %#v", observation)
	}
	if len(observation.ForbiddenCurrentFilenames) != 1 || observation.ForbiddenCurrentFilenames[0] != "51273aad56a8b904e96928935278a627.101" {
		t.Fatalf("expected latest forbidden filename to be reported, got %#v", observation)
	}
}

func TestObserveActiveFileIdentityRewriteFailsWhenLatestObserverRowMissesPar2Classification(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	if err := installFileIdentityRewriteObserver(dbPath); err != nil {
		t.Fatalf("install file identity rewrite observer: %v", err)
	}
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 21, 1, "51273...101", "archive.part1.rar", "par2")
	updateActiveFileIdentity(t, db, 21, 1, "archive.part1.rar", "archive.part1.rar", "declared")
	deleteActiveFileIdentities(t, db, 21)

	observation, err := observeActiveFileIdentityRewrite(dbPath, 21, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar"},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if observation.Observed {
		t.Fatalf("expected latest observer row without classification_source=par2 to fail, got %#v", observation)
	}
	if len(observation.WrongClassificationSources) != 1 || !strings.Contains(observation.WrongClassificationSources[0], "found: declared") {
		t.Fatalf("expected latest source mismatch to be reported, got %#v", observation)
	}
}

func TestObserveActiveFileIdentityRewriteUsesLiveRowsBeforeObserverHistory(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	if err := installFileIdentityRewriteObserver(dbPath); err != nil {
		t.Fatalf("install file identity rewrite observer: %v", err)
	}
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 22, 1, "51273...101", "archive.part1.rar", "par2")
	deleteActiveFileIdentities(t, db, 22)
	insertActiveFileIdentity(t, db, 22, 1, "51273...101", "51273aad56a8b904e96928935278a627.101", "declared")

	observation, err := observeActiveFileIdentityRewrite(dbPath, 22, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar"},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if observation.Observed {
		t.Fatalf("expected live active rows to override historical observer rows, got %#v", observation)
	}
	if len(observation.MissingCurrentFilenames) != 1 || observation.MissingCurrentFilenames[0] != "archive.part1.rar" {
		t.Fatalf("expected live-row snapshot to report missing canonical filename, got %#v", observation)
	}
}

func TestObserveActiveFileIdentityRewriteFailsWhenObserverRowsMissPar2Classification(t *testing.T) {
	dbPath := newTestWeaverStateDB(t)
	if err := installFileIdentityRewriteObserver(dbPath); err != nil {
		t.Fatalf("install file identity rewrite observer: %v", err)
	}
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	insertActiveFileIdentity(t, db, 18, 1, "51273...101", "archive.part1.rar", "declared")
	insertActiveFileIdentity(t, db, 18, 2, "51273...102", "archive.part2.rar", "declared")
	deleteActiveFileIdentities(t, db, 18)

	observation, err := observeActiveFileIdentityRewrite(dbPath, 18, &ScenarioFileIdentityRewriteAssertion{
		RequiredCurrentFilenames:     []string{"archive.part1.rar", "archive.part2.rar"},
		RequiredClassificationSource: "par2",
	})
	if err != nil {
		t.Fatalf("observe active file identity rewrite: %v", err)
	}
	if observation.Observed {
		t.Fatalf("expected observer rows without classification_source=par2 to fail, got %#v", observation)
	}
	if len(observation.WrongClassificationSources) != 2 {
		t.Fatalf("expected both observer rows to report source mismatches, got %#v", observation)
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

func insertActiveFileIdentity(
	t *testing.T,
	db *sql.DB,
	jobID int,
	fileIndex int,
	sourceFilename string,
	currentFilename string,
	classificationSource string,
) {
	t.Helper()

	_, err := db.Exec(
		`INSERT INTO active_file_identities (
			job_id,
			file_index,
			source_filename,
			current_filename,
			canonical_filename,
			classification_kind,
			classification_set_name,
			classification_volume_index,
			classification_source
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobID,
		fileIndex,
		sourceFilename,
		currentFilename,
		currentFilename,
		"rar",
		"archive",
		fileIndex,
		classificationSource,
	)
	if err != nil {
		t.Fatalf("insert active_file_identities row: %v", err)
	}
}

func updateActiveFileIdentity(
	t *testing.T,
	db *sql.DB,
	jobID int,
	fileIndex int,
	currentFilename string,
	canonicalFilename string,
	classificationSource string,
) {
	t.Helper()

	_, err := db.Exec(
		`UPDATE active_file_identities
		 SET current_filename = ?,
		     canonical_filename = ?,
		     classification_source = ?
		 WHERE job_id = ? AND file_index = ?`,
		currentFilename,
		canonicalFilename,
		classificationSource,
		jobID,
		fileIndex,
	)
	if err != nil {
		t.Fatalf("update active_file_identities row: %v", err)
	}
}

func deleteActiveFileIdentities(t *testing.T, db *sql.DB, jobID int) {
	t.Helper()

	if _, err := db.Exec(`DELETE FROM active_file_identities WHERE job_id = ?`, jobID); err != nil {
		t.Fatalf("delete active_file_identities rows: %v", err)
	}
}

func assertObservedFileIdentityRewriteRow(
	t *testing.T,
	db *sql.DB,
	jobID int,
	operation string,
	currentFilename string,
	canonicalFilename string,
	classificationSource string,
) {
	t.Helper()

	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*)
		 FROM e2e_file_identity_rewrite_observations
		 WHERE job_id = ?
		   AND operation = ?
		   AND current_filename = ?
		   AND canonical_filename = ?
		   AND classification_source = ?`,
		jobID,
		operation,
		currentFilename,
		canonicalFilename,
		classificationSource,
	).Scan(&count); err != nil {
		t.Fatalf("query observed file identity rewrite rows: %v", err)
	}
	if count != 1 {
		t.Fatalf(
			"expected one %s observer row for %s (%s), got %d",
			operation,
			currentFilename,
			classificationSource,
			count,
		)
	}
}
