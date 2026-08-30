package weaver

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInferForcedRuntimeLanes(t *testing.T) {
	cases := []struct {
		status       string
		wantDownload string
		wantPost     string
		wantRun      string
	}{
		{status: "repairing", wantDownload: "complete", wantPost: "repairing", wantRun: "active"},
		{status: "queued_repair", wantDownload: "complete", wantPost: "queued_repair", wantRun: "active"},
		{status: "extracting", wantDownload: "downloading", wantPost: "extracting", wantRun: "active"},
		{status: "verifying", wantDownload: "complete", wantPost: "verifying", wantRun: "active"},
		{status: "moving", wantDownload: "complete", wantPost: "finalizing", wantRun: "active"},
	}

	for _, tc := range cases {
		gotDownload, gotPost, gotRun := inferForcedRuntimeLanes(tc.status)
		if gotDownload == nil || *gotDownload != tc.wantDownload {
			t.Fatalf("%s: expected download lane %q, got %#v", tc.status, tc.wantDownload, gotDownload)
		}
		if gotPost == nil || *gotPost != tc.wantPost {
			t.Fatalf("%s: expected post lane %q, got %#v", tc.status, tc.wantPost, gotPost)
		}
		if gotRun == nil || *gotRun != tc.wantRun {
			t.Fatalf("%s: expected run lane %q, got %#v", tc.status, tc.wantRun, gotRun)
		}
	}
}

func TestRestartCasesIncludeDirectStorePar2AliasRegression(t *testing.T) {
	const caseName = "direct_store_par2_alias_claimant_completes_after_restart"
	for _, tc := range restartCases() {
		if tc.Name != caseName {
			continue
		}
		if len(tc.Slugs) != 1 || tc.Slugs[0] != "direct-store-par2-alias-restart" {
			t.Fatalf("%s has unexpected fixtures: %v", caseName, tc.Slugs)
		}
		if tc.Run == nil {
			t.Fatalf("%s has no flow", caseName)
		}
		return
	}
	t.Fatalf("restart suite does not include %s", caseName)
}

func TestCapturePar2AliasStateReportsSplitOwnershipAndMissingVolumes(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
	root := t.TempDir()
	dbPath := filepath.Join(root, "weaver.sqlite")
	outputDir := filepath.Join(root, "intermediate")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()
	mustExecWeaverStateSQL(t, db, `CREATE TABLE active_jobs (job_id INTEGER PRIMARY KEY, output_dir TEXT NOT NULL)`)
	mustExecWeaverStateSQL(t, db, `CREATE TABLE active_file_identities (
		job_id INTEGER NOT NULL,
		file_index INTEGER NOT NULL,
		current_filename TEXT NOT NULL,
		canonical_filename TEXT,
		classification_kind TEXT,
		classification_set_name TEXT
	)`)
	mustExecWeaverStateSQL(t, db, `CREATE TABLE active_rar_volume_facts (
		job_id INTEGER NOT NULL,
		set_name TEXT NOT NULL,
		volume_index INTEGER NOT NULL,
		facts_blob BLOB NOT NULL
	)`)
	mustExecWeaverStateSQL(t, db, `CREATE TABLE active_extracted (job_id INTEGER NOT NULL)`)
	mustExecWeaverStateSQL(t, db, `CREATE TABLE active_extraction_chunks (job_id INTEGER NOT NULL)`)
	if _, err := db.Exec(`INSERT INTO active_jobs (job_id, output_dir) VALUES (?, ?)`, 11809, outputDir); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 4; index++ {
		current := fmt.Sprintf("archive.part%d.rar", index)
		canonical := fmt.Sprintf("%s.part%d.rar", restartPar2AliasSet, index)
		if _, err := db.Exec(`
			INSERT INTO active_file_identities
			(job_id, file_index, current_filename, canonical_filename, classification_kind, classification_set_name)
			VALUES (?, ?, ?, ?, 'rar', ?)
		`, 11809, index-1, current, canonical, restartPar2AliasSet); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`
			INSERT INTO active_rar_volume_facts (job_id, set_name, volume_index, facts_blob)
			VALUES (?, 'archive', ?, X'01')
		`, 11809, index-1); err != nil {
			t.Fatal(err)
		}
	}

	state, err := capturePar2AliasState(dbPath, 11809)
	if err != nil {
		t.Fatal(err)
	}
	if state.IdentityCount != 4 || state.AliasClaimants != 4 {
		t.Fatalf("unexpected alias identity counts: %+v", state)
	}
	if len(state.ClassificationSets) != 1 || state.ClassificationSets[0] != restartPar2AliasSet {
		t.Fatalf("unexpected classification sets: %v", state.ClassificationSets)
	}
	if len(state.FactSets) != 1 || state.FactSets["archive"] != 4 {
		t.Fatalf("unexpected durable RAR fact ownership: %v", state.FactSets)
	}
	if len(state.ExistingVolumePaths) != 0 {
		t.Fatalf("expected no materialized RAR volumes, got %v", state.ExistingVolumePaths)
	}
}

func TestNormalizeRestartDBStatusHandlesLegacyAndRestoreLogForms(t *testing.T) {
	cases := map[string]string{
		"queued_repair":  "QUEUED_REPAIR",
		"QueuedRepair":   "QUEUED_REPAIR",
		"queued_extract": "QUEUED_EXTRACT",
		"QueuedExtract":  "QUEUED_EXTRACT",
		"Repairing":      "REPAIRING",
		"extracting":     "EXTRACTING",
		"complete":       "COMPLETE",
	}

	for input, want := range cases {
		if got := normalizeRestartDBStatus(input); got != want {
			t.Fatalf("%s: expected %q, got %q", input, want, got)
		}
	}
}

func TestForcedRuntimeLaneValuesFallsBackFromLegacyStatuses(t *testing.T) {
	pausedResumeStatus := "queued_extract"
	downloadState, postState, runState, pausedResumeDownloadState, pausedResumePostState := forcedRuntimeLaneValues(
		forcedActiveRuntimeState{
			Status:             "paused",
			PausedResumeStatus: &pausedResumeStatus,
		},
	)

	if downloadState != nil {
		t.Fatalf("expected paused download lane fallback to remain nil, got %#v", downloadState)
	}
	if postState != nil {
		t.Fatalf("expected paused post lane fallback to remain nil, got %#v", postState)
	}
	if runState != "paused" {
		t.Fatalf("expected paused run lane, got %#v", runState)
	}
	if pausedResumeDownloadState != "complete" {
		t.Fatalf("expected paused resume download lane to infer complete, got %#v", pausedResumeDownloadState)
	}
	if pausedResumePostState != "queued_extract" {
		t.Fatalf("expected paused resume post lane to infer queued_extract, got %#v", pausedResumePostState)
	}
}

func TestRestartSuiteEncryptionKeyIsStableForRun(t *testing.T) {
	runRoot := t.TempDir()
	ctxA := &restartCaseContext{
		Profile: restartProfileCurrent,
		CaseDir: filepath.Join(runRoot, "01-first"),
	}
	ctxB := &restartCaseContext{
		Profile: restartProfileCurrent,
		CaseDir: filepath.Join(runRoot, "02-second"),
	}

	keyA := restartSuiteEncryptionKey(ctxA)
	keyB := restartSuiteEncryptionKey(ctxB)
	if keyA != keyB {
		t.Fatalf("expected one restart-suite key per run, got %q and %q", keyA, keyB)
	}
	decoded, err := base64.StdEncoding.DecodeString(keyA)
	if err != nil {
		t.Fatalf("restart key is not valid base64: %v", err)
	}
	if len(decoded) != 32 {
		t.Fatalf("expected 32-byte restart key, got %d bytes", len(decoded))
	}
}

func TestBodyFetchesSinceRestartPointIgnoresEarlierRefetches(t *testing.T) {
	before := restartNntpMetrics{BodyCounts: map[string]int{"already-repeated": 2, "stable": 1}}
	after := restartNntpMetrics{BodyCounts: map[string]int{"already-repeated": 2, "stable": 1}}

	ids, extra := bodyFetchesSinceRestartPoint(before, after)
	if ids != 0 || extra != 0 {
		t.Fatalf("unchanged restart-point metrics reported refetches: ids=%d extra=%d", ids, extra)
	}

	after.BodyCounts["stable"] = 2
	after.BodyCounts["new-after-restart"] = 1
	ids, extra = bodyFetchesSinceRestartPoint(before, after)
	if ids != 2 || extra != 2 {
		t.Fatalf("post-restart refetches were not counted: ids=%d extra=%d", ids, extra)
	}
}

func TestPreseededRestartSkipsFixturePosting(t *testing.T) {
	t.Setenv(nntpSeedImageActiveEnv, "1")
	if err := ensureRestartFixturesSeeded([]string{"fixture-that-does-not-exist"}); err != nil {
		t.Fatalf("preseeded restart fixtures should not be reposted: %v", err)
	}
}

func TestRestartSuiteEncryptionKeyReplacesDuplicateEnvEntries(t *testing.T) {
	ctx := &restartCaseContext{
		Profile: restartProfileCurrent,
		CaseDir: filepath.Join(t.TempDir(), "01-case"),
	}
	key := restartSuiteEncryptionKey(ctx)
	env := appendOrReplaceEnv([]string{
		"OTHER=value",
		"WEAVER_ENCRYPTION_KEY=stale",
		"WEAVER_ENCRYPTION_KEY=older",
	}, "WEAVER_ENCRYPTION_KEY", key)

	count := 0
	for _, entry := range env {
		if strings.HasPrefix(entry, "WEAVER_ENCRYPTION_KEY=") {
			count++
			if entry != "WEAVER_ENCRYPTION_KEY="+key {
				t.Fatalf("unexpected restart key env entry %q", entry)
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected one restart key env entry, got %d in %#v", count, env)
	}
}

func TestWaitForManagedWeaverGraphQLReadyFailsWhenProcessExits(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "weaver.log")
	if err := os.WriteFile(logPath, []byte("first\nsecond\nfatal: boom\n"), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	done := make(chan error, 1)
	done <- errors.New("exit status 1")
	close(done)
	ctx := &restartCaseContext{
		weaverURL: "http://localhost:1",
		weaver: &restartWeaverProcess{
			LogPath: logPath,
			done:    done,
		},
	}

	started := time.Now()
	err := ctx.waitForManagedWeaverGraphQLReady(30 * time.Second)
	if err == nil {
		t.Fatal("expected readiness wait to fail when managed process exits")
	}
	if time.Since(started) > time.Second {
		t.Fatalf("expected fail-fast readiness error, got after %s: %v", time.Since(started), err)
	}
	message := err.Error()
	if !strings.Contains(message, "managed weaver exited before GraphQL readiness") ||
		!strings.Contains(message, logPath) ||
		!strings.Contains(message, "fatal: boom") {
		t.Fatalf("readiness error did not include process/log context: %v", err)
	}
}
