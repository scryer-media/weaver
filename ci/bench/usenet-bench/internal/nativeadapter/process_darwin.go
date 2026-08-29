//go:build darwin

package nativeadapter

import (
	"os"
	"os/exec"
	"syscall"
)

// configureNativeProcess gives each benchmark client its own process group so
// controlled shutdown cannot leave a native unpacker behind for the next run.
func configureNativeProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func interruptNativeProcessTree(process *os.Process) error {
	if process == nil || process.Pid < 1 {
		return os.ErrProcessDone
	}
	return syscall.Kill(-process.Pid, syscall.SIGINT)
}

func killNativeProcessTree(process *os.Process) error {
	if process == nil || process.Pid < 1 {
		return os.ErrProcessDone
	}
	return syscall.Kill(-process.Pid, syscall.SIGKILL)
}
