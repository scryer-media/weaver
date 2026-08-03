package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const e2eNNTPModulePath = "github.com/scryer-media/e2e-nntp/cmd/e2e-nntp"

// E2EImageBuildOptions declares one reproducible source for the local NNTP
// image. The provenance file is an artifact, never a repository input.
type E2EImageBuildOptions struct {
	Version        string
	SourceDir      string
	Tag            string
	ProvenancePath string
	GoBinary       string
}

// E2EImageProvenance is the exact JSON emitted by e2e-nntp image build. It
// intentionally names local source only as source-directory, never by path.
type E2EImageProvenance struct {
	ModuleVersion string `json:"module_version"`
	Source        string `json:"source"`
	Platform      string `json:"platform"`
	Tag           string `json:"tag"`
	ImageID       string `json:"image_id"`
	BinarySHA256  string `json:"binary_sha256"`
}

// BuildE2EImage invokes the standalone e2e-nntp builder and stores its JSON
// provenance alongside benchmark artifacts. It never pulls an NNTP image.
func BuildE2EImage(ctx context.Context, options E2EImageBuildOptions) (E2EImageProvenance, error) {
	if err := validateE2EImageBuildOptions(options); err != nil {
		return E2EImageProvenance{}, err
	}
	if _, err := os.Stat(options.ProvenancePath); err == nil {
		return E2EImageProvenance{}, fmt.Errorf("NNTP image provenance already exists: %s", options.ProvenancePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return E2EImageProvenance{}, fmt.Errorf("inspect NNTP image provenance path: %w", err)
	}

	goBinary := options.GoBinary
	if goBinary == "" {
		goBinary = "go"
	}
	var arguments []string
	if options.Version != "" {
		arguments = []string{
			"run", e2eNNTPModulePath + "@" + options.Version,
			"image", "build", "--version", options.Version, "--tag", options.Tag,
		}
	} else {
		sourceDir, err := filepath.Abs(options.SourceDir)
		if err != nil {
			return E2EImageProvenance{}, fmt.Errorf("resolve e2e-nntp source directory: %w", err)
		}
		arguments = []string{
			"-C", sourceDir, "run", "./cmd/e2e-nntp",
			"image", "build", "--source-dir", sourceDir, "--tag", options.Tag,
		}
	}
	command := exec.CommandContext(ctx, goBinary, arguments...)
	output, err := command.Output()
	if err != nil {
		return E2EImageProvenance{}, fmt.Errorf("run e2e-nntp image builder: %w", err)
	}
	var provenance E2EImageProvenance
	if err := json.Unmarshal(output, &provenance); err != nil {
		return E2EImageProvenance{}, fmt.Errorf("decode e2e-nntp image provenance: %w", err)
	}
	if provenance.Tag != options.Tag || provenance.Source == "" || provenance.ImageID == "" || provenance.BinarySHA256 == "" {
		return E2EImageProvenance{}, errors.New("e2e-nntp image builder returned incomplete provenance")
	}
	file, err := os.OpenFile(options.ProvenancePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return E2EImageProvenance{}, fmt.Errorf("create NNTP image provenance: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(output); err != nil {
		return E2EImageProvenance{}, fmt.Errorf("write NNTP image provenance: %w", err)
	}
	return provenance, nil
}

func validateE2EImageBuildOptions(options E2EImageBuildOptions) error {
	if (options.Version == "") == (options.SourceDir == "") {
		return errors.New("exactly one of --version or --source-dir is required")
	}
	if !validLocalImageTag(options.Tag) {
		return errors.New("a local image tag is required")
	}
	if options.ProvenancePath == "" {
		return errors.New("--provenance is required")
	}
	if options.Version != "" && !validExactModuleVersion(options.Version) {
		return errors.New("--version must be an exact vX.Y.Z module version")
	}
	if options.SourceDir != "" {
		if _, err := os.Stat(filepath.Join(options.SourceDir, "go.mod")); err != nil {
			return fmt.Errorf("locate e2e-nntp source directory: %w", err)
		}
	}
	return nil
}

func validLocalImageTag(value string) bool {
	return strings.TrimSpace(value) != "" && !strings.ContainsAny(value, "\r\n")
}

func validExactModuleVersion(value string) bool {
	if !strings.HasPrefix(value, "v") || strings.ContainsAny(value, " \t\r\n@/") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(value, "v"), ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, character := range part {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}
