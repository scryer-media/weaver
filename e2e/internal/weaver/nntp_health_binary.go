// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
)

var nntpHealthBuildMu sync.Mutex

// nntpHealthBuilt records, under nntpHealthBuildMu, that this process has the
// healthcheck binary; nntpHealthBinaryEnv hands its path to the flow
// processes it starts and to Compose, so each flow does not run its own go
// build and every news-server container mounts that path.
var nntpHealthBuilt bool

// nntpHealthBinaryEnv is the published binary's path. The Compose files
// mount it at /nntp-health.
const nntpHealthBinaryEnv = "E2E_NNTP_HEALTH_BINARY"

// nntpHealthBuild runs the build into the given output path. Tests replace it.
var nntpHealthBuild = func(output string) error {
	cmd := exec.Command("go", "build", "-trimpath", "-o", output, "./cmd/nntp-health")
	cmd.Dir = e2eDir()
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build control-aware NNTP healthcheck: %w\n%s", err, out)
	}
	return nil
}

// This small static binary is mounted into even the scratch-based pinned
// fixture image. The Go build cache handles reuse without a new dependency.
//
// A running container's bind mount must never see its file change: a build
// that rewrote or renamed over one shared path left the news-server
// containers of a run still going unhealthy. So the binary is published under
// a name made from its content, .bin/nntp-health-<sha256 prefix>, and a
// published name is never written, renamed over or removed again. A rebuild
// with the same content finds its name taken and publishes nothing; a
// changed binary gets a new name, and containers already up keep the old one.
func ensureNNTPHealthBinary() error {
	nntpHealthBuildMu.Lock()
	defer nntpHealthBuildMu.Unlock()
	if nntpHealthBuilt {
		return nil
	}
	if inherited := os.Getenv(nntpHealthBinaryEnv); inherited != "" {
		if info, err := os.Stat(inherited); err == nil && info.Mode().IsRegular() {
			nntpHealthBuilt = true
			return nil
		}
	}
	binary, err := publishNNTPHealthBinary(filepath.Join(e2eDir(), ".bin"))
	if err != nil {
		return err
	}
	nntpHealthBuilt = true
	setEnv(nntpHealthBinaryEnv, binary)
	return nil
}

// publishNNTPHealthBinary builds into a private name in dir and links the
// result to its content name. A hard link never replaces an existing name,
// so a name already published, possibly mounted, is left exactly as it is.
func publishNNTPHealthBinary(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	scratch, err := os.MkdirTemp(dir, ".nntp-health-build-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(scratch)
	built := filepath.Join(scratch, "nntp-health")
	if err := nntpHealthBuild(built); err != nil {
		return "", err
	}
	sum, err := fileSHA256(built)
	if err != nil {
		return "", err
	}
	published := filepath.Join(dir, "nntp-health-"+sum[:16])
	if err := os.Link(built, published); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("publish NNTP healthcheck %s: %w", published, err)
	}
	return published, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
