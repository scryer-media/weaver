// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"
)

// The lane's waits hold on the condition they assert and carry no deadline of
// their own, so a hostile archive that wedges Weaver would stall the run
// rather than fail it. The bound sits around the whole lane instead, the way
// the release gate bounds a flow: the lane runs as a child in its own process
// group, and when the bound passes that group, managed Weaver included, is
// killed and the lane fails. No case carries a deadline.

// adversarialTimeoutEnv sets the lane's bound in seconds.
const adversarialTimeoutEnv = "E2E_ADVERSARIAL_TIMEOUT_SEC"

// adversarialBoundedEnv marks the child that runs the lane under the bound.
const adversarialBoundedEnv = "E2E_ADVERSARIAL_BOUNDED"

// adversarialDefaultTimeout is far above a real run of every case; it only
// turns a hang into a failure.
const adversarialDefaultTimeout = 60 * time.Minute

// adversarialTimeout reads the lane's bound from getenv.
func adversarialTimeout(getenv func(string) string) (time.Duration, error) {
	value := getenv(adversarialTimeoutEnv)
	if value == "" {
		return adversarialDefaultTimeout, nil
	}
	var seconds int
	if _, err := fmt.Sscanf(value, "%d", &seconds); err != nil || seconds <= 0 || fmt.Sprint(seconds) != value {
		return 0, fmt.Errorf("invalid %s=%q: want a positive whole number of seconds", adversarialTimeoutEnv, value)
	}
	return time.Duration(seconds) * time.Second, nil
}

// runAdversarialBounded runs the lane as a child under the bound and returns
// the lane's failure, or the bound's when it passed first.
func runAdversarialBounded(args []string) error {
	timeout, err := adversarialTimeout(os.Getenv)
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate the e2e binary: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, exe, append([]string{"adversarial"}, args...)...)
	configureWeaverReleaseCommandCancellation(cmd)
	cmd.Dir = e2eDir()
	cmd.Env = mergeChildEnv(os.Environ(), map[string]string{adversarialBoundedEnv: "1"})
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("adversarial lane exceeded %s", timeout)
	}
	return err
}
