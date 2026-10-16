// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//go:build !darwin && !linux

package weaver

import "os/exec"

func configureWeaverReleaseCommandCancellation(_ *exec.Cmd) {
	// CommandContext's direct-child cancellation is the portable fallback.
}
