// Package generator creates large, deterministic RAR fixture sets without
// putting their binary output in Git.
package generator

import (
	"context"
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
	"sync"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
	"github.com/zeebo/blake3"
)

const (
	// Ordinary RAR fixtures contain one substantial movie. The one fixture
	// with multiple input movies uses the separate 48 MiB size below.
	defaultBytesPerFile            int64 = 150 << 20
	defaultMultiVolumeBytesPerFile int64 = 48 << 20
	defaultBluRayLargeFile         int64 = 5 << 30
	defaultBluRaySmallFile         int64 = 128 << 10
	defaultBluRaySmallFileCount          = 512
	defaultGenerationWorkers             = 4
)

var canonicalFileTime = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

type Config struct {
	MatrixPath              string
	ToolchainsPath          string
	DockerfilePath          string
	PAR2ToolchainPath       string
	PAR2DockerfilePath      string
	OutputDir               string
	DockerBinary            string
	BytesPerFile            int64
	MultiVolumeBytesPerFile int64
	BluRayLargeFileBytes    int64
	BluRaySmallFileBytes    int64
	BluRaySmallFileCount    int
	Workers                 int
	CaseIDs                 map[string]bool
	BuildImages             bool
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
	if c.MultiVolumeBytesPerFile == 0 {
		c.MultiVolumeBytesPerFile = defaultMultiVolumeBytesPerFile
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
	if c.Workers == 0 {
		c.Workers = defaultGenerationWorkers
	}
	return c
}

func (c Config) Validate() error {
	if c.BytesPerFile <= 0 || c.MultiVolumeBytesPerFile <= 0 {
		return fmt.Errorf("movie sizes must be positive")
	}
	if c.BluRayLargeFileBytes <= 0 || c.BluRaySmallFileBytes <= 0 || c.BluRaySmallFileCount < 1 {
		return fmt.Errorf("Blu-ray layout sizes and file count must be positive")
	}
	if c.Workers < 1 {
		return fmt.Errorf("generator workers must be positive")
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
	type generationJob struct {
		index       int
		archiveCase fixture.ArchiveCase
		toolchain   Toolchain
	}
	jobs := make([]generationJob, 0, len(cases))
	for _, archiveCase := range cases {
		if len(config.CaseIDs) > 0 && !config.CaseIDs[archiveCase.ID] {
			continue
		}
		toolchain, ok := lock.Find(archiveCase.GeneratorToolchain)
		if !ok {
			return nil, fmt.Errorf("fixture %q references unknown toolchain %q", archiveCase.ID, archiveCase.GeneratorToolchain)
		}
		jobs = append(jobs, generationJob{index: len(jobs), archiveCase: archiveCase, toolchain: toolchain})
	}
	if len(jobs) == 0 {
		return nil, fmt.Errorf("fixture selection did not match any cases")
	}
	if config.BuildImages {
		built := make(map[string]bool)
		for _, job := range jobs {
			if built[job.toolchain.ID] {
				continue
			}
			if err := buildImage(ctx, config, job.toolchain); err != nil {
				return nil, err
			}
			built[job.toolchain.ID] = true
		}
	}

	workers := config.Workers
	if workers > len(jobs) {
		workers = len(jobs)
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan generationJob)
	results := make([]fixture.GeneratedManifest, len(jobs))
	errs := make(chan error, 1)
	var workerGroup sync.WaitGroup
	worker := func() {
		defer workerGroup.Done()
		for job := range queue {
			manifest, err := generateCase(workCtx, config, job.archiveCase, job.toolchain, par2Toolchain)
			if err != nil {
				select {
				case errs <- fmt.Errorf("generate fixture %q: %w", job.archiveCase.ID, err):
					cancel()
				default:
				}
				return
			}
			results[job.index] = manifest
		}
	}
	workerGroup.Add(workers)
	for range workers {
		go worker()
	}
enqueue:
	for _, job := range jobs {
		select {
		case queue <- job:
		case <-workCtx.Done():
			break enqueue
		}
	}
	close(queue)
	workerGroup.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
	}
	return results, nil
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

	expected, inputs, recipe, err := writePayloadFiles(ctx, inputDir, caseDir, archiveCase, config, toolchain)
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
	if requiresMultiVolumeArchive(archiveCase) && len(archives) < 2 {
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
		SchemaVersion:      5,
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

func requiresMultiVolumeArchive(archiveCase fixture.ArchiveCase) bool {
	return archiveCase.FileCount > 1
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

func writePayloadFiles(ctx context.Context, dir, caseDir string, archiveCase fixture.ArchiveCase, config Config, toolchain Toolchain) ([]fixture.FileDigest, []string, fixture.PayloadRecipe, error) {
	layout := archiveCase.PayloadLayout
	if layout == "" {
		layout = fixture.UniformPayloadLayout
	}
	switch layout {
	case fixture.UniformPayloadLayout:
		return writeUniformPayloadFiles(ctx, caseDir, archiveCase, config, toolchain)
	case fixture.BluRayDiscPayloadLayout:
		return writeBluRayDiscPayloadFiles(ctx, dir, caseDir, archiveCase, config, toolchain)
	default:
		return nil, nil, fixture.PayloadRecipe{}, fmt.Errorf("fixture %q has unsupported payload layout %q", archiveCase.ID, layout)
	}
}

func writeUniformPayloadFiles(ctx context.Context, caseDir string, archiveCase fixture.ArchiveCase, config Config, toolchain Toolchain) ([]fixture.FileDigest, []string, fixture.PayloadRecipe, error) {
	bytesPerFile := uniformMovieBytes(archiveCase, config)
	digests := make([]fixture.FileDigest, 0, archiveCase.FileCount)
	inputs := make([]string, 0, archiveCase.FileCount)
	for index := 1; index <= archiveCase.FileCount; index++ {
		name := fmt.Sprintf("payload-%02d%s", index, videoExtension(archiveCase.Payload))
		digest, err := renderVideo(ctx, config, toolchain, caseDir, filepath.ToSlash(filepath.Join("input", name)), archiveCase.Payload, bytesPerFile, uint64(index))
		if err != nil {
			return nil, nil, fixture.PayloadRecipe{}, err
		}
		digests = append(digests, fixture.FileDigest{Path: filepath.ToSlash(name), Size: digest.Size, BLAKE3: digest.BLAKE3})
		inputs = append(inputs, filepath.ToSlash(filepath.Join("input", name)))
	}
	return digests, inputs, fixture.PayloadRecipe{
		Layout:              fixture.UniformPayloadLayout,
		UniformBytesPerFile: bytesPerFile,
	}, nil
}

func uniformMovieBytes(archiveCase fixture.ArchiveCase, config Config) int64 {
	if archiveCase.FileCount > 1 {
		return config.MultiVolumeBytesPerFile
	}
	return config.BytesPerFile
}

// writeBluRayDiscPayloadFiles makes an intentionally declared disc-layout
// workload: one large media stream, a few small menu/extra streams, and many
// tiny playlist, clip-info, BD-J, and presentation members. It does not claim
// to be a byte-for-byte authored Blu-ray image.
func writeBluRayDiscPayloadFiles(ctx context.Context, dir, caseDir string, archiveCase fixture.ArchiveCase, config Config, toolchain Toolchain) ([]fixture.FileDigest, []string, fixture.PayloadRecipe, error) {
	digests := make([]fixture.FileDigest, 0, config.BluRaySmallFileCount+1)
	var smallStream fixture.FileDigest
	for index := 1; index <= config.BluRaySmallFileCount; index++ {
		relative := bluRaySmallPath(index)
		if isTransportStream(relative) {
			if smallStream.Path == "" {
				digest, err := renderVideo(ctx, config, toolchain, caseDir, filepath.ToSlash(filepath.Join("input", relative)), fixture.CompressiblePayload, config.BluRaySmallFileBytes, 10_001)
				if err != nil {
					return nil, nil, fixture.PayloadRecipe{}, err
				}
				smallStream = fixture.FileDigest{Path: relative, Size: digest.Size, BLAKE3: digest.BLAKE3}
			} else if err := copyVideoFile(filepath.Join(dir, filepath.FromSlash(smallStream.Path)), filepath.Join(dir, filepath.FromSlash(relative))); err != nil {
				return nil, nil, fixture.PayloadRecipe{}, err
			}
			digest := smallStream
			digest.Path = relative
			digests = append(digests, digest)
			continue
		}
		if _, err := writePayloadAt(dir, relative, fixture.CompressiblePayload, bluRayMetadataBytes(relative, config.BluRaySmallFileBytes), uint64(index)); err != nil {
			return nil, nil, fixture.PayloadRecipe{}, err
		}
		digest, err := digestFile(relative, filepath.Join(dir, filepath.FromSlash(relative)))
		if err != nil {
			return nil, nil, fixture.PayloadRecipe{}, err
		}
		digests = append(digests, digest)
	}
	largeRelative := "BDMV/STREAM/00000.m2ts"
	largeDigest, err := renderVideo(ctx, config, toolchain, caseDir, filepath.ToSlash(filepath.Join("input", largeRelative)), archiveCase.Payload, config.BluRayLargeFileBytes, 1)
	if err != nil {
		return nil, nil, fixture.PayloadRecipe{}, err
	}
	digests = append(digests, fixture.FileDigest{Path: largeRelative, Size: largeDigest.Size, BLAKE3: largeDigest.BLAKE3})
	return digests, bluRayArchiveInputRoots(), fixture.PayloadRecipe{
		Layout:         fixture.BluRayDiscPayloadLayout,
		LargeFileBytes: config.BluRayLargeFileBytes,
		SmallFileCount: config.BluRaySmallFileCount,
		SmallFileBytes: config.BluRaySmallFileBytes,
	}, nil
}

// bluRayArchiveInputRoots preserves the archive's disc tree. RAR's -ep1
// removes the input/ prefix while retaining BDMV/ and CERTIFICATE/ beneath
// these roots. Passing individual nested files would instead flatten each
// file to its basename and make duplicate legitimate member names collide.
func bluRayArchiveInputRoots() []string {
	return []string{"input/BDMV", "input/CERTIFICATE"}
}

func bluRaySmallPath(index int) string {
	switch {
	case index <= 4:
		return fmt.Sprintf("BDMV/STREAM/%05d.m2ts", index)
	case index <= 164:
		return fmt.Sprintf("BDMV/PLAYLIST/%05d.mpls", index-5)
	case index <= 324:
		return fmt.Sprintf("BDMV/CLIPINF/%05d.clpi", index-165)
	case index <= 388:
		return fmt.Sprintf("BDMV/BDJO/%05d.bdjo", index-325)
	case index <= 452:
		return fmt.Sprintf("BDMV/META/DL/Composite%03d_BT2020_HDR.png", index-389)
	case index <= 484:
		return fmt.Sprintf("BDMV/META/DL/metadata-%03d.xml", index-453)
	case index == 485:
		return "BDMV/index.bdmv"
	case index == 486:
		return "BDMV/MovieObject.bdmv"
	case index == 487:
		return "BDMV/JAR/00000.jar"
	case index == 488:
		return "BDMV/AUXDATA/00000.otf"
	case index == 489:
		return "CERTIFICATE/id.bdmv"
	case index == 490:
		return "CERTIFICATE/BACKUP/id.bdmv"
	case index == 491:
		return "CERTIFICATE/backup/00000.cer"
	default:
		return fmt.Sprintf("BDMV/META/DL/locale-%03d.txt", index-492)
	}
}

func bluRayMetadataBytes(relative string, limit int64) int64 {
	requested := int64(8 << 10)
	switch {
	case strings.HasSuffix(relative, ".clpi"):
		requested = 16 << 10
	case strings.HasSuffix(relative, ".bdjo"):
		requested = 24 << 10
	case strings.HasSuffix(relative, ".png"):
		requested = 48 << 10
	case strings.HasSuffix(relative, ".jar"):
		requested = 64 << 10
	case strings.HasSuffix(relative, ".otf"):
		requested = 96 << 10
	}
	if requested > limit {
		return limit
	}
	return requested
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
	hash := blake3.New()
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
		block := blake3.Sum256(seed[:])
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

// writeModeratelyCompressible emits four fresh pseudorandom 32 KiB blocks and
// repeats one. The resulting 20% redundancy is enough to exercise RAR
// compression while keeping a 192 MiB fixture near the intended 150–200 MiB
// on-wire scale.
func writeModeratelyCompressible(writer io.Writer, size int64, stream uint64) error {
	const (
		blockSize    = 32 << 10
		uniqueBlocks = 4
	)
	var counter uint64
	var written int64
	for written < size {
		blocks := make([][]byte, uniqueBlocks)
		for index := range blocks {
			block := make([]byte, blockSize)
			for offset := 0; offset < len(block); offset += 32 {
				var seed [16]byte
				binary.BigEndian.PutUint64(seed[:8], stream)
				binary.BigEndian.PutUint64(seed[8:], counter)
				digest := blake3.Sum256(seed[:])
				copy(block[offset:], digest[:])
				counter++
			}
			blocks[index] = block
		}
		for index := 0; index <= uniqueBlocks && written < size; index++ {
			chunk := blocks[index%uniqueBlocks]
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
			BLAKE3: digest,
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
	hash := blake3.New()
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
