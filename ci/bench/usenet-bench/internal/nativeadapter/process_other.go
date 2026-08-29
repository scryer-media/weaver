//go:build !darwin && !windows

package nativeadapter

import (
	"os"
	"os/exec"
)

// The native launcher rejects unsupported operating systems at configuration
// validation. These fallbacks keep cross-platform Go builds deterministic.
func configureNativeProcess(_ *exec.Cmd) {}

func interruptNativeProcessTree(process *os.Process) error {
	if process == nil {
		return os.ErrProcessDone
	}
	return process.Signal(os.Interrupt)
}

func killNativeProcessTree(process *os.Process) error {
	if process == nil {
		return os.ErrProcessDone
	}
	return process.Kill()
}
