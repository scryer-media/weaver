package generator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

// sevenZipArchiveName is the base name every 7z fixture is written under.
// 7-Zip appends .001, .002, ... for multi-volume output, so the posted set is
// fixture.7z.001 upward rather than the RAR lane's fixture.partNN.rar.
const sevenZipArchiveName = "fixture.7z"

func selectedCasesRequireSevenZip(cases []fixture.ArchiveCase, selected map[string]bool) bool {
	for _, archiveCase := range cases {
		if len(selected) > 0 && !selected[archiveCase.ID] {
			continue
		}
		if archiveCase.ArchiveFormat == fixture.SevenZip {
			return true
		}
	}
	return false
}

func buildSevenZipImage(ctx context.Context, config Config, toolchain SevenZipToolchain) error {
	args := []string{
		"build", "--platform", toolchain.Platform,
		"--tag", toolchain.Image,
		"--file", config.SevenZipDockerfilePath,
		"--build-arg", "SEVENZIP_URL=" + toolchain.URL,
		"--build-arg", "SEVENZIP_SHA256=" + toolchain.SHA256,
		"--build-arg", "SEVENZIP_BINARY=" + toolchain.Binary,
		filepath.Dir(config.SevenZipDockerfilePath),
	}
	if err := runCommand(ctx, config.DockerBinary, args...); err != nil {
		return fmt.Errorf("build 7-Zip image %s: %w", toolchain.ID, err)
	}
	return nil
}

// runSevenZip executes the pinned 7-Zip console binary with workdir set to
// workdir inside the mounted case directory. 7-Zip stores member names
// relative to its working directory, so archiving runs from the staging input
// directory and the stored names carry no input/ prefix — the same result the
// RAR lane gets from -ep1.
func runSevenZip(ctx context.Context, config Config, toolchain SevenZipToolchain, caseDir, workdir string, sevenZipArgs ...string) error {
	args := []string{
		"run", "--rm", "--platform", toolchain.Platform,
		"--user", callerDockerUser(),
		"--mount", "type=bind,src=" + caseDir + ",dst=/work",
		"--workdir", "/work/" + filepath.ToSlash(workdir),
		toolchain.Image,
	}
	args = append(args, sevenZipArgs...)
	return runCommand(ctx, config.DockerBinary, args...)
}

// createSevenZipArchive writes the fixture's 7z volumes. inputs are the same
// case-relative staging paths the RAR lane receives, so the caller does not
// need to know which writer will be used.
func createSevenZipArchive(
	ctx context.Context,
	config Config,
	archiveCase fixture.ArchiveCase,
	toolchain SevenZipToolchain,
	caseDir string,
	inputs []string,
) error {
	relative, err := inputRelativePaths(inputs)
	if err != nil {
		return fmt.Errorf("create fixture %q: %w", archiveCase.ID, err)
	}
	args, err := archiveCase.SevenZipArgs("../archive/"+sevenZipArchiveName, relative)
	if err != nil {
		return err
	}
	if err := runSevenZip(ctx, config, toolchain, caseDir, "input", args...); err != nil {
		return fmt.Errorf("create fixture %q: %w", archiveCase.ID, err)
	}
	return nil
}

// inputRelativePaths strips the staging prefix the payload writers use, so
// 7-Zip run from input/ sees BDMV or payload-01.mkv rather than input/BDMV.
func inputRelativePaths(inputs []string) ([]string, error) {
	relative := make([]string, 0, len(inputs))
	for _, input := range inputs {
		cleaned := filepath.ToSlash(filepath.Clean(input))
		trimmed := strings.TrimPrefix(cleaned, "input/")
		if trimmed == cleaned || trimmed == "" {
			return nil, fmt.Errorf("7z input %q is not inside the staging input directory", input)
		}
		relative = append(relative, trimmed)
	}
	return relative, nil
}

// verifySevenZipArchive extracts the archive with the pinned 7-Zip build and
// checks every expected payload file against its BLAKE3 digest. The RAR lanes
// get this assurance from RARLAB's own `rar t`; 7-Zip's equivalent test does
// not prove the extracted bytes match the oracle, so the 7z lane extracts.
func verifySevenZipArchive(
	ctx context.Context,
	config Config,
	archiveCase fixture.ArchiveCase,
	toolchain SevenZipToolchain,
	caseDir, firstVolume string,
	expected []fixture.FileDigest,
) error {
	extractDir, err := os.MkdirTemp(caseDir, "sevenzip-verification-")
	if err != nil {
		return fmt.Errorf("create 7-Zip verification directory: %w", err)
	}
	defer os.RemoveAll(extractDir)
	relativeExtract, err := filepath.Rel(caseDir, extractDir)
	if err != nil {
		return fmt.Errorf("resolve 7-Zip verification directory: %w", err)
	}
	args := []string{"x", "-y", "-bso0", "-bsp0"}
	args = append(args, archiveCase.SevenZipPasswordArgs()...)
	args = append(args, "-o/work/"+filepath.ToSlash(relativeExtract), filepath.ToSlash(firstVolume))
	if err := runSevenZip(ctx, config, toolchain, caseDir, ".", args...); err != nil {
		return fmt.Errorf("extract 7z archive: %w", err)
	}
	for _, file := range expected {
		path := filepath.Join(extractDir, filepath.FromSlash(file.Path))
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("extracted 7z member %s: %w", file.Path, err)
		}
		if info.Size() != file.Size {
			return fmt.Errorf("extracted 7z member %s has size %d, expected %d", file.Path, info.Size(), file.Size)
		}
		digest, err := hashFile(path)
		if err != nil {
			return err
		}
		if digest != file.BLAKE3 {
			return fmt.Errorf("extracted 7z member %s does not match the payload oracle", file.Path)
		}
	}
	return nil
}
