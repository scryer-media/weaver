// Package nntp delegates corpus publication to Nyuu, the same conventional
// yEnc/NZB poster used by the E2E harness. It does not implement a competing
// poster in Go.
package nntp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

const defaultSegmentBytes = 750 << 10

type NyuuImageConfig struct {
	DockerBinary string
	Dockerfile   string
	Image        string
	Platform     string
}

type NyuuSeedConfig struct {
	DockerBinary string
	Image        string
	Platform     string
	Network      string
	FixtureDir   string
	RunID        string
	NZBPath      string
	NNTPHost     string
	NNTPPort     string
	Username     string
	Password     string
	Group        string
	SegmentBytes int
}

type SeedResult struct {
	FixtureID string
	NZBPath   string
	Files     int
	Articles  int
}

func BuildNyuuImage(ctx context.Context, config NyuuImageConfig) error {
	if config.DockerBinary == "" {
		config.DockerBinary = "docker"
	}
	if config.Dockerfile == "" {
		config.Dockerfile = "docker/nyuu/Dockerfile"
	}
	if config.Image == "" {
		config.Image = "weaver-nntp-bench-nyuu:0.4.2"
	}
	if config.Platform == "" {
		config.Platform = "linux/amd64"
	}
	args := []string{"build", "--platform", config.Platform, "--tag", config.Image, "--file", config.Dockerfile, filepath.Dir(config.Dockerfile)}
	if err := runCommand(ctx, config.DockerBinary, args...); err != nil {
		return fmt.Errorf("build Nyuu image: %w", err)
	}
	return nil
}

// SeedWithNyuu posts the generated RAR volumes via Nyuu to the public test
// server's Docker network, then validates the NZB Nyuu emitted. The benchmark
// never measures this operation; its sole purpose is corpus preparation.
func SeedWithNyuu(ctx context.Context, config NyuuSeedConfig) (SeedResult, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return SeedResult{}, err
	}
	fixtureDir, err := filepath.Abs(config.FixtureDir)
	if err != nil {
		return SeedResult{}, fmt.Errorf("resolve fixture directory: %w", err)
	}
	manifest, err := fixture.LoadGeneratedManifest(filepath.Join(fixtureDir, "fixture-manifest.json"))
	if err != nil {
		return SeedResult{}, err
	}
	if err := verifyArchiveFiles(fixtureDir, manifest.ArchiveFiles); err != nil {
		return SeedResult{}, err
	}

	nzbPath := config.NZBPath
	if nzbPath == "" {
		nzbPath = filepath.Join(fixtureDir, manifest.Case.ID+".nzb")
	}
	nzbPath, err = filepath.Abs(nzbPath)
	if err != nil {
		return SeedResult{}, fmt.Errorf("resolve NZB output path: %w", err)
	}
	relativeNZB, err := filepath.Rel(fixtureDir, nzbPath)
	if err != nil || relativeNZB == "." || strings.HasPrefix(relativeNZB, ".."+string(os.PathSeparator)) || relativeNZB == ".." {
		return SeedResult{}, fmt.Errorf("NZB output path must be inside fixture directory %s", fixtureDir)
	}
	if _, err := os.Stat(nzbPath); err == nil {
		return SeedResult{}, fmt.Errorf("NZB output already exists: %s (use a new run id/path to preserve prior evidence)", nzbPath)
	} else if !os.IsNotExist(err) {
		return SeedResult{}, fmt.Errorf("inspect NZB output %s: %w", nzbPath, err)
	}

	args := []string{
		"run", "--rm", "--platform", config.Platform, "--network", config.Network,
		"--mount", "type=bind,src=" + fixtureDir + ",dst=/work",
		config.Image,
		"-h", config.NNTPHost,
		"-P", config.NNTPPort,
		"--ssl=false",
		"-u", config.Username,
		"-p", config.Password,
		"-n", "1",
		"-g", config.Group,
		"-f", "nntp-bench@example.invalid",
		"--keep-message-id",
		"--message-id", fmt.Sprintf("bench-%s-{0filenum}-{0part}@nntp-bench", safeID(config.RunID)),
		"-o", "/work/" + filepath.ToSlash(relativeNZB),
		"--check-connections", "0",
		"-a", strconv.Itoa(config.SegmentBytes),
	}
	if manifest.Case.RequiresPassword() {
		args = append(args, "--nzb-password", fixture.FixturePassword)
	}
	for _, archive := range manifest.ArchiveFiles {
		args = append(args, "/work/"+filepath.ToSlash(archive.Path))
	}
	if err := runCommand(ctx, config.DockerBinary, args...); err != nil {
		return SeedResult{}, fmt.Errorf("post fixture %q with Nyuu: %w", manifest.Case.ID, err)
	}
	nzbContents, err := os.ReadFile(nzbPath)
	if err != nil {
		return SeedResult{}, fmt.Errorf("read Nyuu NZB %s: %w", nzbPath, err)
	}
	document, err := UnmarshalNZB(nzbContents)
	if err != nil {
		return SeedResult{}, fmt.Errorf("parse Nyuu NZB %s: %w", nzbPath, err)
	}
	if len(document.Files) != len(manifest.ArchiveFiles) {
		return SeedResult{}, fmt.Errorf("Nyuu NZB has %d files, expected %d archive volumes", len(document.Files), len(manifest.ArchiveFiles))
	}
	articles := 0
	for _, file := range document.Files {
		articles += len(file.Segments)
	}
	if articles == 0 {
		return SeedResult{}, fmt.Errorf("Nyuu NZB contains no article segments")
	}
	return SeedResult{FixtureID: manifest.Case.ID, NZBPath: nzbPath, Files: len(document.Files), Articles: articles}, nil
}

func (c NyuuSeedConfig) withDefaults() NyuuSeedConfig {
	if c.DockerBinary == "" {
		c.DockerBinary = "docker"
	}
	if c.Image == "" {
		c.Image = "weaver-nntp-bench-nyuu:0.4.2"
	}
	if c.Platform == "" {
		c.Platform = "linux/amd64"
	}
	if c.Group == "" {
		c.Group = "alt.binaries.test"
	}
	if c.NNTPHost == "" {
		c.NNTPHost = "nntp"
	}
	if c.NNTPPort == "" {
		c.NNTPPort = "119"
	}
	if c.SegmentBytes == 0 {
		c.SegmentBytes = defaultSegmentBytes
	}
	return c
}

func (c NyuuSeedConfig) validate() error {
	if c.FixtureDir == "" || c.RunID == "" || c.Network == "" {
		return fmt.Errorf("fixture directory, run id, and Docker network are required")
	}
	if c.Username == "" || c.Password == "" {
		return fmt.Errorf("NNTP username and password are required for Nyuu")
	}
	if c.SegmentBytes < 1024 {
		return fmt.Errorf("segment bytes must be at least 1024")
	}
	return nil
}

func runCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func safeID(value string) string {
	var output strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			output.WriteRune(r)
		default:
			output.WriteByte('-')
		}
	}
	if output.Len() == 0 {
		return "run"
	}
	return output.String()
}

func verifyArchiveFiles(fixtureDir string, files []fixture.FileDigest) error {
	ordered := append([]fixture.FileDigest(nil), files...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Path < ordered[j].Path })
	for _, file := range ordered {
		path := filepath.Join(fixtureDir, filepath.FromSlash(file.Path))
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("fixture archive file %s: %w", file.Path, err)
		}
		if info.Size() != file.Size {
			return fmt.Errorf("fixture archive file %s has size %d, expected %d", file.Path, info.Size(), file.Size)
		}
		actual, err := hashFile(path)
		if err != nil {
			return err
		}
		if actual != file.SHA256 {
			return fmt.Errorf("fixture archive file %s hash mismatch", file.Path)
		}
	}
	return nil
}

func hashFile(path string) (string, error) {
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
