package weaver

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// printRoundDiagnostics queries weaver for per-job health details and
// greps the log file for retry/failover events to show how weaver handled
// the chaos round.
func printRoundDiagnostics(weaverURL string, jobIDs []int, statuses []string, useDockerLogs bool) {
	var totalHealth float64
	var healthCount int
	var failedJobs, degradedJobs int

	for i, jobID := range jobIDs {
		if jobID == 0 {
			continue
		}
		snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
		if err != nil {
			continue
		}
		if !snapshot.Found {
			continue
		}
		h := float64(snapshot.Health)
		totalHealth += h
		healthCount++
		if statuses[i] == "FAILED" || statuses[i] == "TIMEOUT" {
			failedJobs++
		}
		if h < 100 && h > 0 {
			degradedJobs++
		}
	}

	avgHealth := 0.0
	if healthCount > 0 {
		avgHealth = totalHealth / float64(healthCount)
	}

	// Grep weaver log for key events.
	logStr := readWeaverLogForDiagnostics(useDockerLogs)

	retries := strings.Count(logStr, "decode failed \xe2\x80\x94 re-downloading")
	failovers := strings.Count(logStr, "transient error, trying next server") + strings.Count(logStr, "soft timeout, trying next server")
	permFails := strings.Count(logStr, "decode failed permanently")
	connResets := strings.Count(logStr, "connection reset") + strings.Count(logStr, "broken pipe")

	fmt.Printf("  Diagnostics: avg_health=%.1f%% degraded=%d failed=%d\n",
		avgHealth/10.0, degradedJobs, failedJobs)
	fmt.Printf("  Log events: decode_retries=%d server_failovers=%d perm_failures=%d conn_resets=%d\n",
		retries, failovers, permFails, connResets)

	if !useDockerLogs {
		_ = os.Truncate(localWeaverLogPath(), 0)
	}
}

func readWeaverLogForDiagnostics(useDockerLogs bool) string {
	if useDockerLogs {
		containerID, err := dockerComposeServiceContainerID("weaver")
		if err != nil {
			return ""
		}
		cmd := exec.Command("docker", "logs", "--tail", "5000", containerID)
		cmd.Dir = e2eDir()
		out, err := cmd.CombinedOutput()
		if err != nil && len(out) == 0 {
			return ""
		}
		return string(out)
	}

	logData, _ := os.ReadFile(localWeaverLogPath())
	return string(logData)
}

func pollJobOnce(weaverURL string, jobID int) string {
	snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
	if err != nil {
		return ""
	}
	if !snapshot.Found {
		return ""
	}
	return snapshot.Status
}
