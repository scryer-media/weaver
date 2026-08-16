//go:build !darwin && !linux

package weaver

import "os/exec"

func configureWeaverReleaseCommandCancellation(_ *exec.Cmd) {
	// CommandContext's direct-child cancellation is the portable fallback.
}
