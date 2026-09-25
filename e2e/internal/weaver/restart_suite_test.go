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

func TestRestartCasesIncludeConventional7zDamagedBlockRepair(t *testing.T) {
	const caseName = "conventional_7z_damaged_block_repairs_once"
	for _, tc := range restartCases() {
		if tc.Name != caseName {
			continue
		}
		if len(tc.Slugs) != 1 || tc.Slugs[0] != conventional7zRepairSlug {
			t.Fatalf("%s has unexpected fixtures: %v", caseName, tc.Slugs)
		}
		if tc.Run == nil {
			t.Fatalf("%s has no flow", caseName)
		}
		for _, slug := range restartFixtureSlugs {
			if slug == conventional7zRepairSlug {
				return
			}
		}
		t.Fatalf("%s is not seeded for the restart suite", conventional7zRepairSlug)
	}
	t.Fatalf("restart suite does not include %s", caseName)
}

// Log lines in the shape weaver writes them, for a job that settled on the
// strong-decode claim and then hit a damaged block in extraction.
func conventional7zRepairLogLines(jobID int, extractionError string, failed bool) string {
	lines := []string{
		fmt.Sprintf("2026-01-01T00:00:01Z  INFO weaver_server_core::pipeline::completion::finalize::check::completion: skipping authoritative PAR2 verify for clean exhausted strong-decode job job_id=%d", jobID),
		fmt.Sprintf("2026-01-01T00:00:02Z  WARN weaver_server_core::pipeline::extraction::rar::scheduler: set extraction failed job_id=%d set_name=archive.7z error=%s", jobID, extractionError),
	}
	if failed {
		lines = append(lines, fmt.Sprintf("2026-01-01T00:00:03Z ERROR weaver_server_core::pipeline::health: job failed job_id=%d reason=%s", jobID, extractionError))
	}
	return strings.Join(lines, "\n") + "\n"
}

func TestReadConventional7zRepairLog(t *testing.T) {
	const dataError = `7z block data error: BlockDecode { block_index: 0, packed_offset: 32, kind: Io, message: "Io(Custom { kind: InvalidData, error: \"Error during PPMd decoding\" }, \"\")" }`
	const terminalError = `7z extraction failed: BlockDecode { block_index: 0, packed_offset: 32, kind: Io, message: "Io(Custom { kind: InvalidData, error: \"Error during PPMd decoding\" }, \"\")" }`

	repaired := readConventional7zRepairLog(conventional7zRepairLogLines(28, dataError, false), 28)
	if !repaired.SkippedAuthoritativeVerify || !repaired.DataErrorAfterSkip || repaired.Failed {
		t.Fatalf("a recoverable data error after the skip reads as the repair path: %+v", repaired)
	}

	// The shape the contract rules out: the error worded as terminal, and
	// the job failed on it.
	ended := readConventional7zRepairLog(conventional7zRepairLogLines(28, terminalError, true), 28)
	if !ended.SkippedAuthoritativeVerify || ended.DataErrorAfterSkip || !ended.Failed {
		t.Fatalf("a terminal error and a failed job read as such: %+v", ended)
	}

	// Another job's lines, including one whose ID shares a prefix, say
	// nothing about this one.
	other := readConventional7zRepairLog(
		conventional7zRepairLogLines(280, terminalError, true)+conventional7zRepairLogLines(2, dataError, false),
		28,
	)
	if other != (conventional7zRepairLog{}) {
		t.Fatalf("other jobs' lines leaked into the reading: %+v", other)
	}

	// A data error with no skip before it is not the settled-clean shape.
	unsettled := readConventional7zRepairLog(
		fmt.Sprintf("x WARN set extraction failed job_id=28 set_name=archive.7z error=%s\n", dataError),
		28,
	)
	if unsettled.DataErrorAfterSkip {
		t.Fatalf("a data error without the skip before it was read as after it: %+v", unsettled)
	}
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

func TestDirectStorePartialFromSnapshotMatchesJobRootAndFileSet(t *testing.T) {
	const expectedPath = ".weaver-staging/10000/amber.trail.s01e02.mkv.f0.direct.partial"
	snapshot := restartFilesystemSnapshot{
		Complete: []restartTreeEntry{
			{Path: ".weaver-staging/10000/tmp/nested.f0.direct.partial", Size: 1},
			{Path: ".weaver-staging/10000/other.f1.direct.partial", Size: 2},
			{Path: ".weaver-staging/10001/other-job.f0.direct.partial", Size: 3},
			{Path: expectedPath, Size: 24 << 20},
		},
	}

	path, size, ok := directStorePartialFromSnapshot(snapshot, 10000, 0)
	if !ok {
		t.Fatal("expected direct-store partial to be found")
	}
	if path != expectedPath || size != 24<<20 {
		t.Fatalf("unexpected direct-store partial: path=%q size=%d", path, size)
	}
	if _, _, ok := directStorePartialFromSnapshot(snapshot, 10000, 2); ok {
		t.Fatal("unexpected match for absent direct-store file set")
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

	// A readiness timeout no run can reach: the wait can only return by
	// noticing the exit. A wait that ignored the exit would sit out the timeout
	// and the go test runner's own bound would end it.
	err := ctx.waitForManagedWeaverGraphQLReady(time.Hour)
	if err == nil {
		t.Fatal("expected readiness wait to fail when managed process exits")
	}
	message := err.Error()
	if !strings.Contains(message, "managed weaver exited before GraphQL readiness") ||
		!strings.Contains(message, logPath) ||
		!strings.Contains(message, "fatal: boom") {
		t.Fatalf("readiness error did not include process/log context: %v", err)
	}
}
