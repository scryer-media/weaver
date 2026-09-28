//go:build darwin || linux

package weaver

import (
	"bufio"
	"context"
	"os/exec"
	"testing"
)

// A canceled full-suite phase must get to run its own teardown. The phase
// here traps the interrupt, reports it, and exits with its own status; a kill
// would end it without either.
func TestCanceledFullPhaseCommandIsInterruptedNotKilled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		"/bin/sh",
		"-c",
		`trap 'kill "$child"; echo torn-down; exit 3' INT; sleep 1000 & child=$!; echo ready; wait "$child"`,
	)
	configureFullPhaseCommandCancellation(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(stdout)
	if !lines.Scan() || lines.Text() != "ready" {
		cancel()
		_ = cmd.Wait()
		t.Fatalf("phase did not report ready: %q, %v", lines.Text(), lines.Err())
	}

	cancel()
	if !lines.Scan() || lines.Text() != "torn-down" {
		_ = cmd.Wait()
		t.Fatalf("phase did not run its interrupt handler: %q, %v", lines.Text(), lines.Err())
	}
	_ = cmd.Wait()
	if code := cmd.ProcessState.ExitCode(); code != 3 {
		t.Fatalf("exit code = %d, want the handler's 3", code)
	}
}
