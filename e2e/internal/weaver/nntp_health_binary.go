// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
)

var nntpHealthBuildMu sync.Mutex

// nntpHealthBuilt records, under nntpHealthBuildMu, that this process has the
// healthcheck binary; nntpHealthBuiltEnv hands that to the flow processes it
// starts, so each flow does not run its own go build.
var nntpHealthBuilt bool

const nntpHealthBuiltEnv = "E2E_NNTP_HEALTH_BINARY_BUILT"

// This small static binary is mounted into even the scratch-based pinned
// fixture image. The Go build cache handles reuse without a new dependency.
func ensureNNTPHealthBinary() error {
	nntpHealthBuildMu.Lock()
	defer nntpHealthBuildMu.Unlock()
	if nntpHealthBuilt {
		return nil
	}
	dir := filepath.Join(e2eDir(), ".bin")
	binary := filepath.Join(dir, "nntp-health")
	if envBool(nntpHealthBuiltEnv, false) {
		if info, err := os.Stat(binary); err == nil && info.Mode().IsRegular() {
			nntpHealthBuilt = true
			return nil
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	cmd := exec.Command("go", "build", "-o", binary, "./cmd/nntp-health")
	cmd.Dir = e2eDir()
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build control-aware NNTP healthcheck: %w\n%s", err, output)
	}
	nntpHealthBuilt = true
	setEnv(nntpHealthBuiltEnv, "1")
	return nil
}
