// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//go:build darwin || linux

package weaver

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	fullRunLockHelperEnv     = "E2E_FULL_RUN_LOCK_TEST_HELPER"
	fullRunLockHelperPathEnv = "E2E_FULL_RUN_LOCK_TEST_PATH"
)

// useTestFullRunLock points the lock at a fresh file and gives the test a
// process that holds no lock.
func useTestFullRunLock(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "full-run.lock")
	previousPath := fullRunLockPath
	fullRunLockPath = func() (string, error) { return path, nil }
	releaseTestFullRunLock()
	t.Cleanup(func() {
		releaseTestFullRunLock()
		fullRunLockPath = previousPath
	})
	return path
}

func releaseTestFullRunLock() {
	fullRunLock.mu.Lock()
	defer fullRunLock.mu.Unlock()
	if fullRunLock.file != nil {
		_ = fullRunLock.file.Close()
	}
	fullRunLock.file = nil
	fullRunLock.owned = false
	fullRunLock.holder = fullRunLockHolder{}
}

// TestFullRunLockHelperProcess is the second process of the tests below. It
// takes the lock, reports the result on stdout and, when it took it, holds
// it until its stdin closes.
func TestFullRunLockHelperProcess(t *testing.T) {
	if os.Getenv(fullRunLockHelperEnv) != "1" {
		return
	}
	path := os.Getenv(fullRunLockHelperPathEnv)
	fullRunLockPath = func() (string, error) { return path, nil }
	if err := takeFullRunLock("release-gate", os.Getenv, time.Now()); err != nil {
		os.Stdout.WriteString("refused: " + err.Error() + "\n")
		os.Exit(0)
	}
	recordFullRunLockTempRoot("/helper/temp-root")
	os.Stdout.WriteString("locked\n")
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

type fullRunLockHelper struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	line  string
}

// startFullRunLockHelper starts the helper and waits for its first line.
func startFullRunLockHelper(t *testing.T, path string, prepare func(*exec.Cmd)) *fullRunLockHelper {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFullRunLockHelperProcess$")
	cmd.Env = mergeChildEnv(os.Environ(), map[string]string{
		fullRunLockHelperEnv:     "1",
		fullRunLockHelperPathEnv: path,
	})
	if prepare != nil {
		prepare(cmd)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	helper := &fullRunLockHelper{cmd: cmd, stdin: stdin}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read helper result: %v", err)
	}
	helper.line = strings.TrimSpace(line)
	return helper
}

func TestSecondFullRunIsRefusedWithTheHolderNamed(t *testing.T) {
	path := useTestFullRunLock(t)
	holder := startFullRunLockHelper(t, path, nil)
	if holder.line != "locked" {
		t.Fatalf("first run did not take the lock: %q", holder.line)
	}

	err := takeFullRunLock("full", os.Getenv, time.Now())
	if err == nil {
		t.Fatal("second full run took the lock while another held it")
	}
	recorded := readFullRunLockHolder(path)
	for _, want := range []string{
		"another e2e full run is in progress",
		"pid " + strconv.Itoa(holder.cmd.Process.Pid),
		"(release-gate)",
		"started " + recorded.StartedAt.Local().Format("Mon Jan 2 3:04:05 PM MST"),
		"temp root /helper/temp-root",
		path,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
}

func TestFullRunLockWhoseHolderIsGoneIsTaken(t *testing.T) {
	path := useTestFullRunLock(t)
	holder := startFullRunLockHelper(t, path, nil)
	if holder.line != "locked" {
		t.Fatalf("first run did not take the lock: %q", holder.line)
	}
	if err := holder.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.cmd.Wait()

	// The killed run's record is still in the file; only the lock counts.
	if stale := readFullRunLockHolder(path); stale.PID != holder.cmd.Process.Pid {
		t.Fatalf("lock file holder = %d, want the killed run's %d", stale.PID, holder.cmd.Process.Pid)
	}
	if err := takeFullRunLock("full", os.Getenv, time.Now()); err != nil {
		t.Fatalf("lock of a killed run blocked the next one: %v", err)
	}
	if got := readFullRunLockHolder(path); got.PID != os.Getpid() || got.Command != "full" {
		t.Fatalf("lock file holder = %+v, want this process running full", got)
	}
}

func TestChildOfTheFullRunLockHolderIsNotRefused(t *testing.T) {
	path := useTestFullRunLock(t)
	if err := takeFullRunLock("full", os.Getenv, time.Now()); err != nil {
		t.Fatal(err)
	}
	recordFullRunLockTempRoot("/parent/temp-root")

	child := startFullRunLockHelper(t, path, passFullRunLock)
	if child.line != "locked" {
		t.Fatalf("a phase the holder started was refused: %q", child.line)
	}
	// The child shares the lock and leaves the holder's record alone.
	if got := readFullRunLockHolder(path); got.PID != os.Getpid() || got.TempRoot != "/parent/temp-root" {
		t.Fatalf("lock file holder = %+v, want this process with its temp root", got)
	}
}

func TestFullRunLockDescriptorWithoutTheHoldersFileIsRefused(t *testing.T) {
	path := useTestFullRunLock(t)
	holder := startFullRunLockHelper(t, path, nil)
	if holder.line != "locked" {
		t.Fatalf("first run did not take the lock: %q", holder.line)
	}
	// A process that opens the lock file itself and names it as inherited is
	// not the holder's child, and the lock says so.
	own, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer own.Close()
	other := startFullRunLockHelper(t, path, func(cmd *exec.Cmd) {
		cmd.ExtraFiles = []*os.File{own}
		cmd.Env = mergeChildEnv(cmd.Env, map[string]string{fullRunLockFDEnv: "3"})
	})
	if !strings.HasPrefix(other.line, "refused: ") ||
		!strings.Contains(other.line, "pid "+strconv.Itoa(holder.cmd.Process.Pid)) {
		t.Fatalf("an unrelated process got past the lock: %q", other.line)
	}
}

func TestFullRunLockRefusalSaysWhenTheHolderHasExited(t *testing.T) {
	started := time.Date(2026, 11, 15, 21, 3, 4, 0, time.UTC)
	err := fullRunLockRefusal("/lock", fullRunLockHolder{PID: 42, Command: "full", StartedAt: started}, func(int) bool { return false })
	for _, want := range []string{
		"pid 42 (full)",
		"started " + started.Local().Format("Mon Jan 2 3:04:05 PM MST"),
		"temp root not created yet",
		"pid 42 has exited and a phase it started still holds the lock",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not say %q", err, want)
		}
	}
}

func TestCommandsThatTakeTheFullRunLock(t *testing.T) {
	for _, args := range [][]string{
		{"full"}, {"full", "real-usenet"}, {"functional"}, {"release-gate"}, {"release-gate", "datastore-matrix"},
		{"test-all"}, {"chaos-test"}, {"adversarial"}, {"filesystem"}, {"seed-all"},
	} {
		if !commandTakesFullRunLock(args) {
			t.Errorf("%v does not take the full-run lock", args)
		}
	}
	// Flows and finalizers run only under a gate; compose, doctor and the
	// console inspect what is running.
	for _, args := range [][]string{
		{"release-flow", "x", "sqlite"}, {"release-finalize"}, {"compose", "ps"}, {"doctor"}, {"release-console"}, {"scenarios"},
	} {
		if commandTakesFullRunLock(args) {
			t.Errorf("%v takes the full-run lock", args)
		}
	}
}
