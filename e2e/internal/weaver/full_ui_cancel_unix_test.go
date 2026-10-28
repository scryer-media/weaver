// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//go:build darwin || linux

package weaver

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"testing"
)

// A canceled full-suite phase must get to run its own teardown. The phase
// here handles the interrupt, reports it, and exits with its own status; a kill
// would end it without either.
func TestCanceledFullPhaseCommandIsInterruptedNotKilled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		os.Args[0],
		"-test.run=^TestFullPhaseInterruptHelper$",
	)
	cmd.Env = append(os.Environ(), "WEAVER_TEST_INTERRUPT_HELPER=1")
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

func TestFullPhaseInterruptHelper(t *testing.T) {
	if os.Getenv("WEAVER_TEST_INTERRUPT_HELPER") != "1" {
		return
	}
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	// Readiness means the handler is installed, independent of shell wait
	// semantics or whether a background child has started blocking yet.
	fmt.Println("ready")
	<-interrupts
	fmt.Println("torn-down")
	os.Exit(3)
}
