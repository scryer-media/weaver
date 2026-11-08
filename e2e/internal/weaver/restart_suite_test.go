// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
	"github.com/scryer-media/weaver/e2e/internal/fixturegen"
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

// The full run seeds the restart NNTP image once from the restart seed profile
// and the suite seeds nothing per case while that image is active, so a slug a
// case names but the profile lacks is missing from the full run alone.
func TestRestartSeedProfileCoversEveryRestartCaseSlug(t *testing.T) {
	seeded := map[string]bool{}
	for _, slug := range fixtureSlugsForSeedProfile("restart") {
		seeded[slug] = true
	}
	for _, tc := range restartCases() {
		if len(tc.Slugs) == 0 {
			t.Errorf("%s names no fixture", tc.Name)
		}
		for _, slug := range tc.Slugs {
			if !seeded[slug] {
				t.Errorf("%s names %s, which the restart seed profile does not seed", tc.Name, slug)
			}
		}
	}
}

// The corpus hydration profile is the other place the restart fixtures are
// listed. `corpus ensure --profile restart` fetches what it names, so it must
// name exactly what the cases need, and the release gate's union profile must
// cover it.
func TestCorpusRestartProfileIsTheRestartCaseSlugs(t *testing.T) {
	contents, err := os.ReadFile(repoPath("test-corpus", "profiles.json"))
	if err != nil {
		t.Fatal(err)
	}
	var table struct {
		Profiles map[string]struct {
			Include []string `json:"include"`
		} `json:"profiles"`
	}
	if err := json.Unmarshal(contents, &table); err != nil {
		t.Fatal(err)
	}
	want := make([]string, 0)
	for _, slug := range restartFixtureSlugs() {
		want = append(want, "testdata/"+slug+"/**")
	}
	got := append([]string(nil), table.Profiles["restart"].Include...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("profiles.json restart profile is %v, the restart cases name %v", got, want)
	}
	gate := map[string]bool{}
	for _, glob := range table.Profiles["release-gate"].Include {
		gate[glob] = true
	}
	for _, glob := range want {
		if !gate[glob] {
			t.Errorf("profiles.json release-gate profile lacks %s, which the restart cases name", glob)
		}
	}
}

// The rename restart case crashes as its payload moves to the name the PAR3
// set records. A payload posted under a numbered piece's name, `name.201`, is
// held by a split and born under the set's name, so nothing moves and the
// crash never comes. The payload must be posted under a name that leaves it a
// plain file on disk until the set renames it.
func TestRenameRestartFixtureMovesItsPayload(t *testing.T) {
	scenario, err := loadScenario(filepath.Join(testdataDir(), renameRestartSlug))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := scenario.ExpectedOutputBLAKE3[renameRestartTarget]; !ok {
		t.Fatalf("%s does not deliver %s, the name the crash waits on", renameRestartSlug, renameRestartTarget)
	}
	ledger, _, err := corpus.LoadLedger(e2eDir())
	if err != nil {
		t.Fatal(err)
	}
	posted := map[string]bool{}
	for _, path := range fixturegen.ScenarioPaths(ledger, renameRestartSlug) {
		posted[filepath.Base(path)] = true
	}
	for _, asset := range scenario.FixtureAssets {
		_, name, err := parseFixtureAssetSpec(asset)
		if err != nil {
			t.Fatal(err)
		}
		posted[name] = true
	}
	var payloads []string
	for name := range posted {
		if !strings.HasSuffix(strings.ToLower(name), ".par3") {
			payloads = append(payloads, name)
		}
	}
	if len(payloads) != 1 {
		t.Fatalf("%s posts payloads %v, want exactly one", renameRestartSlug, payloads)
	}
	name := payloads[0]
	if name == renameRestartTarget {
		t.Fatalf("%s posts its payload under the set's name %s, so nothing moves", renameRestartSlug, name)
	}
	// The sniffer's numbered piece: a stem, a dot, and a last extension of
	// digits only.
	if dot := strings.LastIndex(name, "."); dot > 0 && dot < len(name)-1 &&
		strings.Trim(name[dot+1:], "0123456789") == "" {
		t.Fatalf("%s posts its payload as numbered piece %s, which a split holds, so nothing moves", renameRestartSlug, name)
	}
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
		for _, slug := range fixtureSlugsForSeedProfile("restart") {
			if slug == conventional7zRepairSlug {
				return
			}
		}
		t.Fatalf("%s is not seeded for the restart suite", conventional7zRepairSlug)
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

func TestFacadeWaitTimeoutErrorReportsLastObservation(t *testing.T) {
	cases := []struct {
		name    string
		last    facadeItemSnapshot
		lastErr error
		want    string
	}{
		{
			name:    "query error",
			lastErr: errors.New("gql: boom"),
			want:    "last error: gql: boom",
		},
		{
			name: "not found",
			want: "last observed: not found",
		},
		{
			name: "observed state",
			last: facadeItemSnapshot{Found: true, InQueue: true, Status: "QUEUED", TotalBytes: 10},
			want: "state=QUEUED in_queue=true downloaded=0/10",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := facadeWaitTimeoutError(7, tc.last, tc.lastErr)
			if !strings.Contains(err.Error(), "job 7") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("timeout error %q does not report %q", err, tc.want)
			}
		})
	}
}
