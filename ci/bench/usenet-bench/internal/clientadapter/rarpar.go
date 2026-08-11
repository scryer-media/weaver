package clientadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validateRarparInput(binary, version, digest string) error {
	if strings.TrimSpace(binary) == "" || strings.TrimSpace(version) == "" || !sha256Pattern.MatchString(strings.ToLower(strings.TrimSpace(digest))) {
		return fmt.Errorf("Rarpar-backed runs require CLIENT_RARPAR_BINARY, CLIENT_RARPAR_VERSION, and a lowercase 64-character CLIENT_RARPAR_SHA256")
	}
	info, err := os.Stat(binary)
	if err != nil {
		return fmt.Errorf("inspect CLIENT_RARPAR_BINARY: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return fmt.Errorf("CLIENT_RARPAR_BINARY must be an executable regular file")
	}
	return nil
}

func (c Config) archiveToolchainIdentity() string {
	if c.ArchiveToolchain == benchmark.RarparArchiveToolchain {
		identity := "rarpar " + c.RarparVersion + " sha256:" + strings.ToLower(c.RarparSHA256)
		if c.Client == benchmark.NZBGet {
			return identity + " (UnRAR only; NZBGet built-in PAR2)"
		}
		return identity
	}
	return "stock"
}

// prepareRarparToolchain copies a verified published Rarpar release into the
// fresh per-run config directory. The client sees only this copy, so a user
// cannot accidentally swap the host binary during a timed run.
func prepareRarparToolchain(c Config) error {
	if c.ArchiveToolchain != benchmark.RarparArchiveToolchain {
		return nil
	}
	if err := validateRarparInput(c.RarparBinary, c.RarparVersion, c.RarparSHA256); err != nil {
		return err
	}
	contents, err := os.ReadFile(c.RarparBinary)
	if err != nil {
		return fmt.Errorf("read CLIENT_RARPAR_BINARY: %w", err)
	}
	digest := sha256.Sum256(contents)
	actual := hex.EncodeToString(digest[:])
	if actual != strings.ToLower(c.RarparSHA256) {
		return fmt.Errorf("CLIENT_RARPAR_SHA256 does not match CLIENT_RARPAR_BINARY (got %s)", actual)
	}
	dir := filepath.Join(c.ConfigDir, "toolchain")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create Rarpar toolchain directory: %w", err)
	}
	if err := writeToolchainFile(filepath.Join(dir, "rarpar"), contents, 0o755); err != nil {
		return err
	}
	if err := writeToolchainFile(filepath.Join(dir, "unrar"), rarparUnrarShim(), 0o755); err != nil {
		return err
	}
	// SABnzbd discovers its PAR2 executable by name. Stage the verified Rarpar
	// binary itself at that entry point so SAB's original argv reaches Rarpar's
	// native par2cmdline compatibility dispatcher without a translation wrapper.
	if err := writeToolchainFile(filepath.Join(dir, "par2"), contents, 0o755); err != nil {
		return err
	}
	passwords := []byte(c.ArchivePassword)
	if c.ArchivePassword != "" {
		passwords = append(passwords, '\n')
	}
	return writeToolchainFile(filepath.Join(dir, "archive-passwords"), passwords, 0o644)
}

func writeToolchainFile(path string, contents []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return fmt.Errorf("write Rarpar toolchain file %s: %w", filepath.Base(path), err)
	}
	if _, err := file.Write(contents); err != nil {
		_ = file.Close()
		return fmt.Errorf("write Rarpar toolchain file %s: %w", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close Rarpar toolchain file %s: %w", filepath.Base(path), err)
	}
	return nil
}

func rarparUnrarShim() []byte {
	return []byte("#!/bin/sh\nexec \"$(dirname \"$0\")/rarpar\" \"$@\"\n")
}
