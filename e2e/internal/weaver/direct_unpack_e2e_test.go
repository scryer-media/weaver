// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func unpackJobLog(raw string, job int) string {
	var lines []string
	for _, line := range strings.Split(raw, "\n") {
		line = ansiEscape.ReplaceAllString(line, "")
		if !isWeaverDebugRingReplay(line) && directLogJobID(line) == strconv.Itoa(job) {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func startUnpackWeaver(t *testing.T, bin, root string, nntpPort int, extraEnv ...string) (string, string) {
	t.Helper()
	url, logPath, _ := startManagedUnpackWeaver(t, bin, root, "weaver.log", nntpPort, extraEnv...)
	return url, logPath
}

func startManagedUnpackWeaver(t *testing.T, bin, root, logName string, nntpPort int, extraEnv ...string) (string, string, func()) {
	t.Helper()
	databaseURL := nativeUnpackPostgresURL(t, root)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	config := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true
max_retries = 1
[[servers]]
id = 1
host = "127.0.0.1"
port = %d
tls = false
connections = 8
active = true
priority = 0
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), nntpPort)
	path := filepath.Join(root, "weaver.toml")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, logName)
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "--config", path, "serve", "--port", strconv.Itoa(port))
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "WEAVER_") && !strings.HasPrefix(entry, "RUST_LOG=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "WEAVER_FORCE_KEY_FILE=1", "RUST_LOG=info,weaver_server_core::pipeline::completion=debug", "NO_COLOR=1")
	cmd.Env = append(cmd.Env, extraEnv...)
	if databaseURL != "" {
		cmd.Env = append(cmd.Env, "WEAVER_DATABASE_URL="+databaseURL)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stop := sync.OnceFunc(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		logFile.Close()
	})
	t.Cleanup(stop)
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	return url, logPath, stop
}
