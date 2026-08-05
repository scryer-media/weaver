// Package generator creates large, deterministic RAR fixture sets without
// putting their binary output in Git.
package generator

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

const (
	defaultBytesPerFile         int64 = 64 << 20
	defaultBluRayLargeFile      int64 = 1 << 30
	defaultBluRaySmallFile      int64 = 128 << 10
	defaultBluRaySmallFileCount       = 512
)

var canonicalFileTime = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

type Config struct {
	MatrixPath           string
	ToolchainsPath       string
	DockerfilePath       string
	PAR2ToolchainPath    string
	PAR2DockerfilePath   string
	OutputDir            string
	DockerBinary         string
	BytesPerFile         int64
	BluRayLargeFileBytes int64
	BluRaySmallFileBytes int64
	BluRaySmallFileCount int
	CaseIDs              map[string]bool
	BuildImages          bool
}

func (c Config) withDefaults() Config {
	if c.MatrixPath == "" {
		c.MatrixPath = "fixtures/matrix.json"
	}
	if c.ToolchainsPath == "" {
		c.ToolchainsPath = "docker/rarlab/toolchains.json"
	}
	if c.DockerfilePath == "" {
		c.DockerfilePath = "docker/rarlab/Dockerfile"
	}
	if c.PAR2ToolchainPath == "" {
		c.PAR2ToolchainPath = "docker/par2/toolchain.json"
	}
	if c.PAR2DockerfilePath == "" {
		c.PAR2DockerfilePath = "docker/par2/Dockerfile"
	}
	if c.OutputDir == "" {
		c.OutputDir = "generated"
	}
	if c.DockerBinary == "" {
		c.DockerBinary = "docker"
	}
	if c.BytesPerFile == 0 {
		c.BytesPerFile = defaultBytesPerFile
	}
	if c.BluRayLargeFileBytes == 0 {
		c.BluRayLargeFileBytes = defaultBluRayLargeFile
	}
	if c.BluRaySmallFileBytes == 0 {
		c.BluRaySmallFileBytes = defaultBluRaySmallFile
	}
	if c.BluRaySmallFileCount == 0 {
		c.BluRaySmallFileCount = defaultBluRaySmallFileCount
	}
	return c
}

func (c Config) Validate() error {
	if c.BytesPerFile <= 0 {
		return fmt.Errorf("bytes per file must be positive, got %d", c.BytesPerFile)
	}
	if c.BluRayLargeFileBytes <= 0 || c.BluRaySmallFileBytes <= 0 || c.BluRaySmallFileCount < 1 {
		return fmt.Errorf("Blu-ray layout sizes and file count must be positive")
	}
	if strings.TrimSpace(c.OutputDir) == "" {
		return fmt.Errorf("output directory is required")
	}
	return nil
}

// Generate builds the requested pinned RARLAB images, creates the expanded
// matrix, verifies every archive using RARLAB, and writes a durable manifest.
// It never overwrites an existing case directory.
func Generate(ctx context.Context, config Config) ([]fixture.GeneratedManifest, error) {
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	matrix, err := fixture.LoadMatrix(config.MatrixPath)
	if err != nil {
		return nil, err
	}
	cases, err := matrix.Expand()
	if err != nil {
		return nil, err
	}
	lock, err := LoadToolchainLock(config.ToolchainsPath)
	if err != nil {
		return nil, err
	}
	var par2Toolchain *PAR2Toolchain
	if selectedCasesRequirePAR2(cases, config.CaseIDs) {
		loaded, err := LoadPAR2Toolchain(config.PAR2ToolchainPath)
		if err != nil {
			return nil, err
		}
		if config.BuildImages {
			if err := buildPAR2Image(ctx, config, loaded); err != nil {
				return nil, err
			}
		}
		par2Toolchain = &loaded
	}

	if err := os.MkdirAll(config.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	built := map[string]bool{}
	manifests := make([]fixture.GeneratedManifest, 0, len(cases))
	for _, archiveCase := range cases {
		if len(config.CaseIDs) > 0 && !config.CaseIDs[archiveCase.ID] {
			continue
		}
		toolchain, ok := lock.Find(archiveCase.GeneratorToolchain)
		if !ok {
			return nil, fmt.Errorf("fixture %q references unknown toolchain %q", archiveCase.ID, archiveCase.GeneratorToolchain)
		}
		if config.BuildImages && !built[toolchain.ID] {
			if err := buildImage(ctx, config, toolchain); err != nil {
				return nil, err
			}
			built[toolchain.ID] = true
		}
		manifest, err := generateCase(ctx, config, archiveCase, toolchain, par2Toolchain)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, manifest)
	}
	if len(manifests) == 0 {
		return nil, fmt.Errorf("fixture selection did not match any cases")
	}
	return manifests, nil
}

func buildImage(ctx context.Context, config Config, toolchain Toolchain) error {
	args := []string{
		"build", "--platform", toolchain.Platform,
		"--tag", toolchain.Image,
		"--file", config.DockerfilePath,
		"--build-arg", "RAR_URL=" + toolchain.URL,
		"--build-arg", "RAR_SHA256=" + toolchain.SHA256,
		"--build-arg", "RAR_BINARY=" + toolchain.Binary,
		filepath.Dir(config.DockerfilePath),
	}
	if err := runCommand(ctx, config.DockerBinary, args...); err != nil {
		return fmt.Errorf("build RARLAB image %s: %w", toolchain.ID, err)
	}
	return nil
}

func generateCase(ctx context.Context, config Config, archiveCase fixture.ArchiveCase, toolchain Toolchain, par2Toolchain *PAR2Toolchain) (fixture.GeneratedManifest, error) {
	caseDir, err := filepath.Abs(filepath.Join(config.OutputDir, archiveCase.ID))
	if err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("resolve fixture directory: %w", err)
	}
	if _, err := os.Stat(caseDir); err == nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("fixture directory already exists: %s (use a new output directory to preserve prior evidence)", caseDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fixture.GeneratedManifest{}, fmt.Errorf("inspect fixture directory %s: %w", caseDir, err)
	}
	inputDir := filepath.Join(caseDir, "input")
	archiveDir := filepath.Join(caseDir, "archive")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("create input directory: %w", err)
	}
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("create archive directory: %w", err)
	}

	expected, inputs, recipe, err := writePayloadFiles(inputDir, archiveCase, config)
	if err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("write fixture %q payload: %w", archiveCase.ID, err)
	}
	args, err := archiveCase.RARArgs(filepath.ToSlash(filepath.Join("archive", "fixture.rar")), inputs)
	if err != nil {
		return fixture.GeneratedManifest{}, err
	}
	if err := runRAR(ctx, config.DockerBinary, toolchain, caseDir, args...); err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("create fixture %q: %w", archiveCase.ID, err)
	}

	archives, firstVolume, err := digestArchiveFiles(archiveDir, caseDir)
	if err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("inspect fixture %q archive: %w", archiveCase.ID, err)
	}
	if len(archives) < 2 {
		return fixture.GeneratedManifest{}, fmt.Errorf("fixture %q did not create a multi-volume archive; increase bytes per file or reduce volume size", archiveCase.ID)
	}
	testArgs := []string{"t", "-idq", "-y"}
	if archiveCase.RequiresPassword() {
		testArgs = append(testArgs, "-p"+fixture.FixturePassword)
	}
	testArgs = append(testArgs, filepath.ToSlash(firstVolume))
	if err := runRAR(ctx, config.DockerBinary, toolchain, caseDir, testArgs...); err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("verify fixture %q with RARLAB: %w", archiveCase.ID, err)
	}
	repair, postedFiles, err := applyRepairProfile(ctx, config, archiveCase, toolchain, par2Toolchain, caseDir, archives, firstVolume)
	if err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("apply repair profile to fixture %q: %w", archiveCase.ID, err)
	}

	manifest := fixture.GeneratedManifest{
		SchemaVersion:      4,
		Case:               archiveCase,
		Toolchain:          toolchain.ManifestID(),
		PayloadRecipe:      recipe,
		ExpectedFiles:      expected,
		SourceArchiveFiles: archives,
		ArchiveFiles:       postedFiles,
		Repair:             repair,
	}
	if err := writeManifest(filepath.Join(caseDir, "fixture-manifest.json"), manifest); err != nil {
		return fixture.GeneratedManifest{}, err
	}
	// Input data is only a deterministic staging source. The expected digests
	// above are the verification oracle, so retaining it would double fixture
	// storage without increasing reproducibility.
	if err := os.RemoveAll(inputDir); err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("remove fixture staging input: %w", err)
	}
	return manifest, nil
}

func runRAR(ctx context.Context, dockerBinary string, toolchain Toolchain, caseDir string, rarArgs ...string) error {
	args := []string{
		"run", "--rm", "--platform", toolchain.Platform,
		"--user", callerDockerUser(),
		"--mount", "type=bind,src=" + caseDir + ",dst=/work",
		"--workdir", "/work",
		toolchain.Image,
	}
	args = append(args, rarArgs...)
	return runCommand(ctx, dockerBinary, args...)
}

func callerDockerUser() string {
	return fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
}

func runCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func writePayloadFiles(dir string, archiveCase fixture.ArchiveCase, config Config) ([]fixture.FileDigest, []string, fixture.PayloadRecipe, error) {
	layout := archiveCase.PayloadLayout
	if layout == "" {
		layout = fixture.UniformPayloadLayout
	}
	switch layout {
	case fixture.UniformPayloadLayout:
		return writeUniformPayloadFiles(dir, archiveCase, config.BytesPerFile)
	case fixture.BluRayDiscPayloadLayout:
		return writeBluRayDiscPayloadFiles(dir, archiveCase, config)
	default:
		return nil, nil, fixture.PayloadRecipe{}, fmt.Errorf("fixture %q has unsupported payload layout %q", archiveCase.ID, layout)
	}
}

func writeUniformPayloadFiles(dir string, archiveCase fixture.ArchiveCase, bytesPerFile int64) ([]fixture.FileDigest, []string, fixture.PayloadRecipe, error) {
	digests := make([]fixture.FileDigest, 0, archiveCase.FileCount)
	inputs := make([]string, 0, archiveCase.FileCount)
	for index := 1; index <= archiveCase.FileCount; index++ {
		name := fmt.Sprintf("payload-%02d.bin", index)
		digest, err := writePayload(filepath.Join(dir, name), archiveCase.Payload, bytesPerFile, uint64(index))
		if err != nil {
			return nil, nil, fixture.PayloadRecipe{}, err
		}
		digests = append(digests, fixture.FileDigest{Path: filepath.ToSlash(name), Size: bytesPerFile, SHA256: digest})
		inputs = append(inputs, filepath.ToSlash(filepath.Join("input", name)))
	}
	return digests, inputs, fixture.PayloadRecipe{
		Layout:              fixture.UniformPayloadLayout,
		UniformBytesPerFile: bytesPerFile,
	}, nil
}

// writeBluRayDiscPayloadFiles makes an intentionally declared disc-layout
// workload: one large media stream plus many small playlist, clip-info, and
// metadata-shaped files. It does not claim to be a byte-for-byte Blu-ray image.
func writeBluRayDiscPayloadFiles(dir string, archiveCase fixture.ArchiveCase, config Config) ([]fixture.FileDigest, []string, fixture.PayloadRecipe, error) {
	digests := make([]fixture.FileDigest, 0, config.BluRaySmallFileCount+1)
	inputs := make([]string, 0, config.BluRaySmallFileCount+1)
	for index := 1; index <= config.BluRaySmallFileCount; index++ {
		relative := bluRaySmallPath(index)
		digest, err := writePayloadAt(dir, relative, fixture.CompressiblePayload, config.BluRaySmallFileBytes, uint64(10_000+index))
		if err != nil {
			return nil, nil, fixture.PayloadRecipe{}, err
		}
		digests = append(digests, fixture.FileDigest{Path: relative, Size: config.BluRaySmallFileBytes, SHA256: digest})
		inputs = append(inputs, filepath.ToSlash(filepath.Join("input", relative)))
	}
	largeRelative := "BDMV/STREAM/00000.m2ts"
	largeDigest, err := writePayloadAt(dir, largeRelative, archiveCase.Payload, config.BluRayLargeFileBytes, 1)
	if err != nil {
		return nil, nil, fixture.PayloadRecipe{}, err
	}
	digests = append(digests, fixture.FileDigest{Path: largeRelative, Size: config.BluRayLargeFileBytes, SHA256: largeDigest})
	inputs = append(inputs, filepath.ToSlash(filepath.Join("input", largeRelative)))
	return digests, inputs, fixture.PayloadRecipe{
		Layout:         fixture.BluRayDiscPayloadLayout,
		LargeFileBytes: config.BluRayLargeFileBytes,
		SmallFileCount: config.BluRaySmallFileCount,
		SmallFileBytes: config.BluRaySmallFileBytes,
	}, nil
}

func bluRaySmallPath(index int) string {
	directories := []string{"BDMV/PLAYLIST", "BDMV/CLIPINF", "BDMV/JAR", "CERTIFICATE/BACKUP"}
	extensions := []string{"mpls", "clpi", "bdjo", "bdmv"}
	slot := (index - 1) % len(directories)
	return fmt.Sprintf("%s/%06d.%s", directories[slot], index, extensions[slot])
}

func writePayloadAt(root, relative string, kind fixture.PayloadKind, size int64, stream uint64) (string, error) {
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	return writePayload(path, kind, size, stream)
}

func writePayload(path string, kind fixture.PayloadKind, size int64, stream uint64) (string, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	writer := io.MultiWriter(file, hash)
	var writeErr error
	switch kind {
	case fixture.IncompressiblePayload:
		writeErr = writeIncompressible(writer, size, stream)
	case fixture.CompressiblePayload:
		writeErr = writeModeratelyCompressible(writer, size, stream)
	default:
		writeErr = fmt.Errorf("unsupported payload kind %q", kind)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return "", writeErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if err := os.Chtimes(path, canonicalFileTime, canonicalFileTime); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func writeIncompressible(writer io.Writer, size int64, stream uint64) error {
	var counter uint64
	var written int64
	for written < size {
		var seed [16]byte
		binary.BigEndian.PutUint64(seed[:8], stream)
		binary.BigEndian.PutUint64(seed[8:], counter)
		block := sha256.Sum256(seed[:])
		remaining := size - written
		chunk := block[:]
		if int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}
		if _, err := writer.Write(chunk); err != nil {
			return err
		}
		written += int64(len(chunk))
		counter++
	}
	return nil
}

// writeModeratelyCompressible emits a fresh pseudorandom 32 KiB block followed
// by an exact copy. It is visibly compressed by RAR while still producing
// multi-volume fixtures at the default size.
func writeModeratelyCompressible(writer io.Writer, size int64, stream uint64) error {
	const halfBlock = 32 << 10
	var counter uint64
	var written int64
	for written < size {
		block := make([]byte, halfBlock)
		for offset := 0; offset < len(block); offset += sha256.Size {
			var seed [16]byte
			binary.BigEndian.PutUint64(seed[:8], stream)
			binary.BigEndian.PutUint64(seed[8:], counter)
			digest := sha256.Sum256(seed[:])
			copy(block[offset:], digest[:])
			counter++
		}
		for repeat := 0; repeat < 2 && written < size; repeat++ {
			chunk := block
			if remaining := size - written; int64(len(chunk)) > remaining {
				chunk = chunk[:remaining]
			}
			if _, err := writer.Write(chunk); err != nil {
				return err
			}
			written += int64(len(chunk))
		}
	}
	return nil
}

func digestArchiveFiles(archiveDir, caseDir string) ([]fixture.FileDigest, string, error) {
	var paths []string
	if err := filepath.WalkDir(archiveDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if strings.EqualFold(filepath.Ext(entry.Name()), ".rar") {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		return nil, "", err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("RARLAB did not produce a .rar file")
	}
	digests := make([]fixture.FileDigest, 0, len(paths))
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, "", err
		}
		digest, err := hashFile(path)
		if err != nil {
			return nil, "", err
		}
		relative, err := filepath.Rel(caseDir, path)
		if err != nil {
			return nil, "", err
		}
		digests = append(digests, fixture.FileDigest{
			Path:   filepath.ToSlash(relative),
			Size:   info.Size(),
			SHA256: digest,
		})
	}
	first, err := filepath.Rel(caseDir, paths[0])
	if err != nil {
		return nil, "", err
	}
	return digests, first, nil
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

func writeManifest(path string, manifest fixture.GeneratedManifest) error {
	contents, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		return fmt.Errorf("write fixture manifest %s: %w", path, err)
	}
	return nil
}
