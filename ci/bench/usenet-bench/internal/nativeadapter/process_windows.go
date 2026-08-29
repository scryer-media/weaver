//go:build windows

package nativeadapter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func configureNativeProcess(_ *exec.Cmd) {}

// taskkill /T is the Windows process-tree boundary. Native SABnzbd and NZBGet
// are required to run in the foreground, but any helper they leave behind
// must still be removed before the next isolated benchmark run.
func interruptNativeProcessTree(process *os.Process) error {
	return terminateWindowsProcessTree(process)
}

func killNativeProcessTree(process *os.Process) error {
	return terminateWindowsProcessTree(process)
}

func terminateWindowsProcessTree(process *os.Process) error {
	if process == nil || process.Pid < 1 {
		return os.ErrProcessDone
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "taskkill.exe", "/PID", strconv.Itoa(process.Pid), "/T", "/F").CombinedOutput()
	if err != nil {
		return fmt.Errorf("taskkill /T /F: %w: %s", err, output)
	}
	return nil
}
