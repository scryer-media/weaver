package weaver

import (
	"encoding/base64"
	"errors"
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
