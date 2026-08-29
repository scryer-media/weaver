//go:build darwin || linux

package weaver

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// configureWeaverReleaseCommandCancellation isolates a release child and its
// descendants so canceling the parent cannot leave an orphaned docker compose
// command racing the exact-project finalizer.
func configureWeaverReleaseCommandCancellation(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
}
