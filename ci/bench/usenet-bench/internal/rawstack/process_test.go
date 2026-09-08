package rawstack

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Reuse the test executable so child lifecycle checks work on Windows too.
func TestRawstackProcessFixture(t *testing.T) {
	switch os.Args[len(os.Args)-1] {
	case "rawstack-exit":
		os.Exit(23)
	case "rawstack-unhealthy":
		os.Exit(1)
	case "rawstack-healthy":
		os.Exit(0)
	case "rawstack-wait":
		for {
			time.Sleep(time.Hour)
		}
	}
}

func processFixtureConfig(t *testing.T, mode, health string) processConfig {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return processConfig{
		name: "fixture", path: executable,
		args:    []string{"-test.run=^TestRawstackProcessFixture$", "--", mode},
		health:  []string{executable, "-test.run=^TestRawstackProcessFixture$", "--", health},
		logPath: filepath.Join(t.TempDir(), "process.log"), timeout: 30 * time.Second,
	}
}

func TestStartupReportsChildExitBeforeHealthTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := start(ctx, processFixtureConfig(t, "rawstack-exit", "rawstack-unhealthy"))
	if err == nil || !strings.Contains(err.Error(), "exited during startup (exit status 23)") {
		t.Fatalf("want the child's exit status, got %v", err)
	}
}

func TestStopWaitsForTheStartupWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	running, err := start(ctx, processFixtureConfig(t, "rawstack-wait", "rawstack-healthy"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = running.stop() })
	if err := running.stop(); err != nil {
		t.Fatal(err)
	}
	if exited, _ := running.exited(); !exited {
		t.Fatal("stop returned without reaping the child")
	}
	if err := running.stop(); err != nil {
		t.Fatalf("stopping an exited child: %v", err)
	}
}
