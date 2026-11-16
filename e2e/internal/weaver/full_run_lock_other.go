// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//go:build !darwin && !linux

package weaver

import (
	"errors"
	"os"
)

var errFullRunLockHeld = errors.New("full-run lock is held")

// The harness runs its stacks on macOS and Linux only; elsewhere a full run
// cannot prove it is the only one, so it does not start.
func tryLockFile(_ *os.File) error {
	return errors.New("the full-run lock needs macOS or Linux")
}

func closeOnExec(_ int) {}

func fdNamesFile(_ int, _ string) bool { return false }
