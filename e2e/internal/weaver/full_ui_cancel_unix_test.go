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
//
// The phase waits on a child that blocks reading a pipe the test holds open,
// so it runs until it is signalled however long that takes. A background job
// of a non-interactive shell reads /dev/null unless told otherwise, so the
// pipe is handed to it on a descriptor of its own; its output goes nowhere,
// so it never holds the phase's stdout open once the phase itself is gone.
func TestCanceledFullPhaseCommandIsInterruptedNotKilled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		"/bin/sh",
		"-c",
		`trap 'kill "$child"; echo torn-down; exit 3' INT; exec 3<&0; cat <&3 >/dev/null & child=$!; echo ready; wait "$child"`,
	)
	configureFullPhaseCommandCancellation(cmd)
	// Held open until Wait closes it after the phase exits.
	if _, err := cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
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
