// Package nntp delegates corpus publication to Nyuu, the same conventional
// yEnc/NZB poster used by the E2E harness. It does not implement a competing
// poster in Go.
package nntp

import (
	"context"
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
	"github.com/zeebo/blake3"
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
	// NZBOrder and NZBOrderSeed record the posting-order axis this seed
	// realized, so a run artifact can state it without re-reading the fixture.
	NZBOrder     fixture.NZBOrder `json:",omitempty"`
	NZBOrderSeed uint64           `json:",omitempty"`
	// NZBFileOrder is the file order present in the emitted NZB, which for a
	// withheld-volume fixture includes the files that were never posted.
	NZBFileOrder []string `json:",omitempty"`
	// WithheldFiles are listed in the NZB but were never posted, so every
	// article a client requests for them is refused.
	WithheldFiles []string `json:",omitempty"`
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
	// Withheld volumes are never posted, but their recorded size decides how
	// many articles the NZB claims for them, so the bytes on disk still have
	// to match the manifest.
	if err := verifyArchiveFiles(fixtureDir, manifest.WithheldFiles); err != nil {
		return SeedResult{}, err
	}
	plan, err := newPostingPlan(manifest)
	if err != nil {
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
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
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
		"--message-id", messageID(config.RunID, manifest.Case.ID),
		"-o", "/work/" + filepath.ToSlash(relativeNZB),
		"--check-connections", "0",
		"-a", strconv.Itoa(config.SegmentBytes),
	}
	if manifest.Case.RequiresPassword() {
		args = append(args, "--nzb-password", fixture.FixturePassword)
	}
	// Nyuu posts in argv order and writes the NZB in that order, so the
	// declared posting order is expressed here rather than by rewriting the
	// document afterwards.
	for _, posted := range plan.Posted {
		args = append(args, "/work/"+filepath.ToSlash(posted))
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
	if err := assertNZBFileOrder(document, plan.Posted); err != nil {
		return SeedResult{}, fmt.Errorf("Nyuu NZB %s: %w", nzbPath, err)
	}
	if len(plan.Withheld) > 0 {
		if document, err = spliceWithheldFiles(document, plan, config.RunID, manifest.Case.ID, config.SegmentBytes); err != nil {
			return SeedResult{}, fmt.Errorf("describe withheld volumes for %q: %w", manifest.Case.ID, err)
		}
		rewritten, err := MarshalNZB(document.Files)
		if err != nil {
			return SeedResult{}, fmt.Errorf("rewrite NZB %s: %w", nzbPath, err)
		}
		if err := os.WriteFile(nzbPath, rewritten, 0o644); err != nil {
			return SeedResult{}, fmt.Errorf("write NZB %s: %w", nzbPath, err)
		}
		if document, err = UnmarshalNZB(rewritten); err != nil {
			return SeedResult{}, fmt.Errorf("parse rewritten NZB %s: %w", nzbPath, err)
		}
		if err := assertNZBFileOrder(document, plan.Order); err != nil {
			return SeedResult{}, fmt.Errorf("rewritten NZB %s: %w", nzbPath, err)
		}
	}
	articles := 0
	for _, file := range document.Files {
		articles += len(file.Segments)
	}
	if articles == 0 {
		return SeedResult{}, fmt.Errorf("Nyuu NZB contains no article segments")
	}
	return SeedResult{
		FixtureID:     manifest.Case.ID,
		NZBPath:       nzbPath,
		Files:         len(document.Files),
		Articles:      articles,
		NZBOrder:      manifest.Case.NZBOrder,
		NZBOrderSeed:  manifest.NZBOrderSeed,
		NZBFileOrder:  plan.Order,
		WithheldFiles: sortedPaths(manifest.WithheldFiles),
	}, nil
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
		return fmt.Errorf("%s: %w\n%s", redactedCommand(name, args), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func redactedCommand(name string, args []string) string {
	preview := append([]string(nil), args...)
	for index, argument := range preview {
		switch {
		case argument == "-p" || argument == "--password" || argument == "--nzb-password":
			if index+1 < len(preview) {
				preview[index+1] = "<redacted>"
			}
		case strings.HasPrefix(argument, "-hp") && len(argument) > len("-hp"):
			preview[index] = "-hp<redacted>"
		case strings.HasPrefix(argument, "-p") && len(argument) > len("-p"):
			preview[index] = "-p<redacted>"
		case strings.HasPrefix(argument, "--password="):
			preview[index] = "--password=<redacted>"
		case strings.HasPrefix(argument, "--nzb-password="):
			preview[index] = "--nzb-password=<redacted>"
		}
	}
	return name + " " + strings.Join(preview, " ")
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

func messageID(runID, fixtureID string) string {
	return MessageIDTemplate(runID, fixtureID)
}

// MessageIDTemplate is the poster's per-article identifier scheme. It is
// exported because it decides what every article on the server is called, so
// anything that caches an already-seeded server has to fingerprint it.
func MessageIDTemplate(runID, fixtureID string) string {
	return fmt.Sprintf("bench-%s-%s-{0filenum}-{0part}@nntp-bench", safeID(runID), safeID(fixtureID))
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
		if actual != file.BLAKE3 {
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
	hash := blake3.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
