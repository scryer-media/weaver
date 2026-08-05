package generator

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

type ToolchainLock struct {
	SchemaVersion int         `json:"schema_version"`
	Toolchains    []Toolchain `json:"toolchains"`
}

// Toolchain is a source-locked RARLAB command-line release. The hash is
// verified in Dockerfile before any executable is installed.
type Toolchain struct {
	ID       string `json:"id"`
	Image    string `json:"image"`
	Platform string `json:"platform"`
	URL      string `json:"url"`
	SHA256   string `json:"sha256"`
	Binary   string `json:"binary"`
}

// PAR2Toolchain pins the open-source parity generator used only while
// materializing repair fixtures. It is not a benchmarked client dependency.
type PAR2Toolchain struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Image         string `json:"image"`
	Platform      string `json:"platform"`
	URL           string `json:"url"`
	SHA256        string `json:"sha256"`
}

func LoadToolchainLock(path string) (ToolchainLock, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return ToolchainLock{}, fmt.Errorf("read toolchain lock %s: %w", path, err)
	}
	var lock ToolchainLock
	if err := json.Unmarshal(contents, &lock); err != nil {
		return ToolchainLock{}, fmt.Errorf("decode toolchain lock %s: %w", path, err)
	}
	if lock.SchemaVersion != 2 || len(lock.Toolchains) == 0 {
		return ToolchainLock{}, fmt.Errorf("toolchain lock %s is empty or has unsupported schema", path)
	}
	ids := map[string]bool{}
	for _, toolchain := range lock.Toolchains {
		if err := toolchain.Validate(); err != nil {
			return ToolchainLock{}, err
		}
		if ids[toolchain.ID] {
			return ToolchainLock{}, fmt.Errorf("toolchain lock has duplicate id %q", toolchain.ID)
		}
		ids[toolchain.ID] = true
	}
	return lock, nil
}

func LoadPAR2Toolchain(path string) (PAR2Toolchain, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return PAR2Toolchain{}, fmt.Errorf("read PAR2 toolchain %s: %w", path, err)
	}
	var toolchain PAR2Toolchain
	if err := json.Unmarshal(contents, &toolchain); err != nil {
		return PAR2Toolchain{}, fmt.Errorf("decode PAR2 toolchain %s: %w", path, err)
	}
	if err := toolchain.Validate(); err != nil {
		return PAR2Toolchain{}, err
	}
	return toolchain, nil
}

func (t PAR2Toolchain) Validate() error {
	if t.SchemaVersion != 1 {
		return fmt.Errorf("PAR2 toolchain %q has unsupported schema version %d", t.ID, t.SchemaVersion)
	}
	if strings.TrimSpace(t.ID) == "" || strings.TrimSpace(t.Image) == "" || strings.TrimSpace(t.Platform) == "" {
		return fmt.Errorf("PAR2 toolchain must include id, image, and platform")
	}
	parsed, err := url.Parse(t.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("PAR2 toolchain %q must use an https URL", t.ID)
	}
	if len(t.SHA256) != 64 || strings.Trim(t.SHA256, "0123456789abcdefABCDEF") != "" {
		return fmt.Errorf("PAR2 toolchain %q has invalid SHA-256", t.ID)
	}
	return nil
}

func (t Toolchain) Validate() error {
	if strings.TrimSpace(t.ID) == "" || strings.TrimSpace(t.Image) == "" || strings.TrimSpace(t.Platform) == "" {
		return fmt.Errorf("toolchain must include id, image, and platform")
	}
	binary := strings.TrimSpace(t.Binary)
	if binary == "" || binary == "." || binary == ".." || strings.ContainsAny(binary, "/\\") {
		return fmt.Errorf("toolchain %q has invalid archive binary %q", t.ID, t.Binary)
	}
	parsed, err := url.Parse(t.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("toolchain %q must use an https URL", t.ID)
	}
	if len(t.SHA256) != 64 || strings.Trim(t.SHA256, "0123456789abcdefABCDEF") != "" {
		return fmt.Errorf("toolchain %q has invalid SHA-256", t.ID)
	}
	return nil
}

func (l ToolchainLock) Find(id string) (Toolchain, bool) {
	for _, toolchain := range l.Toolchains {
		if toolchain.ID == id {
			return toolchain, true
		}
	}
	return Toolchain{}, false
}

func (t Toolchain) ManifestID() fixture.ToolchainID {
	return fixture.ToolchainID{
		ID:       t.ID,
		Image:    t.Image,
		URL:      t.URL,
		SHA256:   t.SHA256,
		Platform: t.Platform,
		Binary:   t.Binary,
	}
}
