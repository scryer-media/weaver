package weaver

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

// waitForDockerServiceReady blocks until the Compose service's container
// reports a healthy (or, without a healthcheck, running) state.
func waitForDockerServiceReady(service string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	lastStatus := "unknown"
	for time.Now().Before(deadline) {
		status, err := dockerServiceReadyStatus(service)
		if err == nil && (status == "healthy" || status == "running") {
			log.Printf("%s is ready (%s)", service, status)
			return nil
		}
		if err != nil {
			lastStatus = err.Error()
		} else if status != "" {
			lastStatus = status
		}
		if err := sleepWithSuspendDetection(time.Second, "waiting for docker service "+service); err != nil {
			return err
		}
	}
	return fmt.Errorf("timeout waiting for %s to become ready; last status: %s", service, lastStatus)
}

func dockerServiceReadyStatus(service string) (string, error) {
	containerID, err := dockerComposeServiceContainerID(service)
	if err != nil {
		return "", err
	}
	cmd := exec.Command("docker", "inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}{{else if .State.Running}}running{{else}}{{.State.Status}}{{end}}", containerID)
	cmd.Dir = e2eDir()
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("inspect %s container %s: %w", service, containerID, err)
	}
	return strings.TrimSpace(string(output)), nil
}

// captureDockerComposeOutput runs one Compose subcommand and writes its
// combined output to path for the artifact record, returning the run error.
func captureDockerComposeOutput(path string, args ...string) error {
	cmd := exec.Command("docker", dockerComposeArgs(args...)...)
	cmd.Dir = e2eDir()
	output, err := cmd.CombinedOutput()
	if writeErr := os.WriteFile(path, output, 0o644); writeErr != nil {
		return fmt.Errorf("write %s: %w", path, writeErr)
	}
	if err != nil {
		return fmt.Errorf("docker compose %s: %w", strings.Join(args, " "), err)
	}
	return nil
}
