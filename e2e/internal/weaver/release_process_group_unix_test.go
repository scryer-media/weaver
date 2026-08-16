//go:build darwin || linux

package weaver

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCanceledWeaverReleaseCommandKillsDescendantsBeforeFinalization(t *testing.T) {
	tmp := t.TempDir()
	readyPath := filepath.Join(tmp, "ready")
	leakPath := filepath.Join(tmp, "leaked")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		"/bin/sh",
		"-c",
		`printf ready > "$READY_PATH"; (sleep 0.25; printf leaked > "$LEAK_PATH") & wait`,
	)
	configureWeaverReleaseCommandCancellation(cmd)
	cmd.Env = append(
		os.Environ(),
		"READY_PATH="+readyPath,
		"LEAK_PATH="+leakPath,
	)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Run()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("release command did not report ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled release command unexpectedly succeeded")
	}

	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(leakPath); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			t.Fatal("release command descendant survived cancellation")
		}
		t.Fatalf("stat descendant leak marker: %v", err)
	}
}
