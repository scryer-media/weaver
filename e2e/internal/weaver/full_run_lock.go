// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// One full run at a time. Two runs on one container engine starve each
// other's deadlines and take each other's subnets, so a full run, and every
// phase command a full run is made of, holds an exclusive OS file lock for
// the life of its process. The kernel drops the lock when the process ends,
// however it ends, so a killed or crashed run never blocks the next one.
//
// A phase the holder starts is handed the holder's open lock file. Taking
// the lock on that same open file succeeds while the holder has it, which is
// what lets the holder's own children through; anyone else is refused.

// fullRunLockFDEnv names the descriptor a child received the lock file on.
// It is not a way past the lock: the child still takes the lock on that
// descriptor, which only succeeds when it is the holder's own open file or
// nobody holds the lock.
const fullRunLockFDEnv = "E2E_FULL_RUN_LOCK_FD"

// fullRunLockHolder is what the lock file says about the process holding it.
type fullRunLockHolder struct {
	PID       int       `json:"pid"`
	Command   string    `json:"command"`
	StartedAt time.Time `json:"started_at"`
	TempRoot  string    `json:"temp_root,omitempty"`
}

var fullRunLock struct {
	mu     sync.Mutex
	file   *os.File
	owned  bool
	holder fullRunLockHolder
}

// fullRunLockPath is the lock file: one per user, outside the repository
// and outside every run's temp root. Tests replace it.
var fullRunLockPath = func() (string, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate the user cache directory for the full-run lock: %w", err)
	}
	return filepath.Join(cache, "weaver-e2e", "full-run.lock"), nil
}

// commandTakesFullRunLock reports whether a top-level command shares the
// engine the way a full run does: the full and functional suites, the
// real-usenet run, seeding, and every phase command of the full suite, so a
// phase run by hand and a full run refuse each other.
func commandTakesFullRunLock(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "full", "functional", "seed-all", "release-gate":
		return true
	}
	for _, def := range fullPhaseDefinitions {
		if def.command == args[0] {
			return true
		}
	}
	return false
}

// fullRunLockCommand names the command for the holder's record.
func fullRunLockCommand(args []string) string {
	if len(args) > 1 && args[0] == "full" && args[1] == "real-usenet" {
		return "full real-usenet"
	}
	return args[0]
}

// mustTakeFullRunLock takes the lock or ends the process with a message
// naming the holder.
func mustTakeFullRunLock(command string) {
	if err := takeFullRunLock(command, os.Getenv, time.Now()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// takeFullRunLock takes the lock for this process. A process started by the
// holder with the holder's lock file shares the holder's lock and leaves its
// record alone.
func takeFullRunLock(command string, getenv func(string) string, now time.Time) error {
	fullRunLock.mu.Lock()
	defer fullRunLock.mu.Unlock()
	if fullRunLock.file != nil {
		return nil
	}
	path, err := fullRunLockPath()
	if err != nil {
		return err
	}
	if inherited := inheritedFullRunLockFile(path, getenv); inherited != nil {
		if err := tryLockFile(inherited); err == nil {
			fullRunLock.file = inherited
			return nil
		}
		_ = inherited.Close()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create the full-run lock directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open the full-run lock %s: %w", path, err)
	}
	if err := tryLockFile(file); err != nil {
		_ = file.Close()
		if errors.Is(err, errFullRunLockHeld) {
			return fullRunLockRefusal(path, readFullRunLockHolder(path), processAlive)
		}
		return fmt.Errorf("take the full-run lock %s: %w", path, err)
	}
	fullRunLock.file = file
	fullRunLock.owned = true
	fullRunLock.holder = fullRunLockHolder{PID: os.Getpid(), Command: command, StartedAt: now}
	return writeFullRunLockHolderLocked()
}

// inheritedFullRunLockFile is the lock file a parent handed this process, or
// nil when the descriptor is missing or names some other file. The
// descriptor is closed on exec at once, so no engine CLI or daemon this
// process starts carries the lock beyond the run.
func inheritedFullRunLockFile(path string, getenv func(string) string) *os.File {
	fd, err := strconv.Atoi(strings.TrimSpace(getenv(fullRunLockFDEnv)))
	if err != nil || fd < 3 {
		return nil
	}
	closeOnExec(fd)
	if !fdNamesFile(fd, path) {
		// Not the lock file: leave the descriptor alone.
		return nil
	}
	return os.NewFile(uintptr(fd), path)
}

// markInheritedFullRunLockCloseOnExec keeps a descriptor handed down by the
// lock holder from leaking into the engine CLIs and daemons a command that
// does not take the lock starts.
func markInheritedFullRunLockCloseOnExec() {
	if fd, err := strconv.Atoi(strings.TrimSpace(os.Getenv(fullRunLockFDEnv))); err == nil && fd >= 3 {
		closeOnExec(fd)
	}
}

// recordFullRunLockTempRoot adds the run's temp root to the holder's record,
// once this process made it. A child sharing its parent's lock leaves the
// parent's record alone.
func recordFullRunLockTempRoot(tempRoot string) {
	fullRunLock.mu.Lock()
	defer fullRunLock.mu.Unlock()
	if fullRunLock.file == nil || !fullRunLock.owned {
		return
	}
	fullRunLock.holder.TempRoot = tempRoot
	if err := writeFullRunLockHolderLocked(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: record the temp root in the full-run lock: %v\n", err)
	}
}

func writeFullRunLockHolderLocked() error {
	body, err := json.Marshal(fullRunLock.holder)
	if err != nil {
		return err
	}
	if err := fullRunLock.file.Truncate(0); err != nil {
		return fmt.Errorf("write the full-run lock holder: %w", err)
	}
	if _, err := fullRunLock.file.WriteAt(append(body, '\n'), 0); err != nil {
		return fmt.Errorf("write the full-run lock holder: %w", err)
	}
	return nil
}

func readFullRunLockHolder(path string) fullRunLockHolder {
	var holder fullRunLockHolder
	if body, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(body, &holder)
	}
	return holder
}

// fullRunLockRefusal is the message a second run stops with.
func fullRunLockRefusal(path string, holder fullRunLockHolder, alive func(int) bool) error {
	if holder.PID <= 0 {
		return fmt.Errorf("another e2e full run is in progress (lock %s); this run will not start until it ends", path)
	}
	command := holder.Command
	if command == "" {
		command = "unknown command"
	}
	started := "at an unknown time"
	if !holder.StartedAt.IsZero() {
		started = holder.StartedAt.Local().Format("Mon Jan 2 3:04:05 PM MST")
	}
	tempRoot := holder.TempRoot
	if tempRoot == "" {
		tempRoot = "not created yet"
	}
	var gone string
	if !alive(holder.PID) {
		gone = fmt.Sprintf("; pid %d has exited and a phase it started still holds the lock", holder.PID)
	}
	return fmt.Errorf(
		"another e2e full run is in progress: pid %d (%s), started %s, temp root %s%s; this run will not start until it ends (lock %s)",
		holder.PID, command, started, tempRoot, gone, path,
	)
}

// passFullRunLock hands this process's lock file to a child it starts, so a
// phase of this run is not refused. Call it after cmd.Env is set.
func passFullRunLock(cmd *exec.Cmd) {
	fullRunLock.mu.Lock()
	file := fullRunLock.file
	fullRunLock.mu.Unlock()
	if file == nil {
		return
	}
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	fd := 3 + len(cmd.ExtraFiles)
	cmd.ExtraFiles = append(cmd.ExtraFiles, file)
	cmd.Env = mergeChildEnv(cmd.Env, map[string]string{fullRunLockFDEnv: strconv.Itoa(fd)})
}
