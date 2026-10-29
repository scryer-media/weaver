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

// This small static binary is mounted into even the scratch-based pinned
// fixture image. The Go build cache handles reuse without a new dependency.
func ensureNNTPHealthBinary() error {
	nntpHealthBuildMu.Lock()
	defer nntpHealthBuildMu.Unlock()
	dir := filepath.Join(e2eDir(), ".bin")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	cmd := exec.Command("go", "build", "-o", filepath.Join(dir, "nntp-health"), "./cmd/nntp-health")
	cmd.Dir = e2eDir()
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build control-aware NNTP healthcheck: %w\n%s", err, output)
	}
	return nil
}
