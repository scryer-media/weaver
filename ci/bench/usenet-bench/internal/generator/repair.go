package generator

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

const (
	par2BlockSize     = 1 << 20
	lightCorruptBytes = 128
)

func selectedCasesRequirePAR2(cases []fixture.ArchiveCase, selected map[string]bool) bool {
	for _, archiveCase := range cases {
		if len(selected) > 0 && !selected[archiveCase.ID] {
			continue
		}
		switch archiveCase.RepairProfile {
		case fixture.PAR2LightRepairProfile, fixture.PAR2HeavyRepairProfile:
			return true
		}
	}
	return false
}

func buildPAR2Image(ctx context.Context, config Config, toolchain PAR2Toolchain) error {
	args := []string{
		"build", "--platform", toolchain.Platform,
		"--tag", toolchain.Image,
		"--file", config.PAR2DockerfilePath,
		"--build-arg", "PAR2_URL=" + toolchain.URL,
		"--build-arg", "PAR2_SHA256=" + toolchain.SHA256,
		filepath.Dir(config.PAR2DockerfilePath),
	}
	if err := runCommand(ctx, config.DockerBinary, args...); err != nil {
		return fmt.Errorf("build PAR2 image %s: %w", toolchain.ID, err)
	}
	return nil
}

func applyRepairProfile(
	ctx context.Context,
	config Config,
	archiveCase fixture.ArchiveCase,
	rarToolchain Toolchain,
	par2Toolchain *PAR2Toolchain,
	caseDir string,
	sourceArchives []fixture.FileDigest,
	firstVolume string,
) (fixture.RepairDetails, []fixture.FileDigest, error) {
	profile := archiveCase.RepairProfile
	if profile == "" {
		profile = fixture.CleanRepairProfile
	}
	details := fixture.RepairDetails{Profile: profile}

	switch profile {
	case fixture.CleanRepairProfile:
		posted, err := digestPostedFiles(filepath.Join(caseDir, "archive"), caseDir)
		return details, posted, err
	case fixture.PAR2LightRepairProfile, fixture.PAR2HeavyRepairProfile:
		if par2Toolchain == nil {
			return fixture.RepairDetails{}, nil, fmt.Errorf("PAR2 repair fixture requires a PAR2 toolchain")
		}
		redundancy := 10
		missingVolumes := 0
		if profile == fixture.PAR2HeavyRepairProfile {
			redundancy = 35
			missingVolumes = 2
		}
		if err := createPAR2(ctx, config.DockerBinary, *par2Toolchain, caseDir, sourceArchives, redundancy); err != nil {
			return fixture.RepairDetails{}, nil, err
		}
		details.PAR2RedundancyPercent = redundancy
		if missingVolumes == 0 {
			targets, err := repairTargets(sourceArchives, 1)
			if err != nil {
				return fixture.RepairDetails{}, nil, err
			}
			fault, err := flipArchiveBytes(caseDir, targets[0])
			if err != nil {
				return fixture.RepairDetails{}, nil, err
			}
			details.Corruptions = []fixture.CorruptionDetail{fault}
		} else {
			targets, err := repairTargets(sourceArchives, missingVolumes)
			if err != nil {
				return fixture.RepairDetails{}, nil, err
			}
			faults, err := removeArchiveFiles(caseDir, targets)
			if err != nil {
				return fixture.RepairDetails{}, nil, err
			}
			details.Corruptions = faults
		}
		if err := verifyPAR2Repair(ctx, config.DockerBinary, *par2Toolchain, rarToolchain, archiveCase, caseDir, firstVolume); err != nil {
			return fixture.RepairDetails{}, nil, err
		}
	case fixture.RARRecoveryVolumeLightProfile, fixture.RARRecoveryVolumeHeavyProfile:
		recoveryVolumes := 1
		if profile == fixture.RARRecoveryVolumeHeavyProfile {
			recoveryVolumes = 2
		}
		recoveryArgs := []string{fmt.Sprintf("rv%d", recoveryVolumes), "-idq", "-y"}
		if archiveCase.RequiresPassword() {
			recoveryArgs = append(recoveryArgs, "-p"+fixture.FixturePassword)
		}
		recoveryArgs = append(recoveryArgs, firstVolume)
		if err := runRAR(ctx, config.DockerBinary, rarToolchain, caseDir, recoveryArgs...); err != nil {
			return fixture.RepairDetails{}, nil, fmt.Errorf("create %d RAR recovery volumes: %w", recoveryVolumes, err)
		}
		targets, err := repairTargets(sourceArchives, recoveryVolumes)
		if err != nil {
			return fixture.RepairDetails{}, nil, err
		}
		faults, err := removeArchiveFiles(caseDir, targets)
		if err != nil {
			return fixture.RepairDetails{}, nil, err
		}
		details.RARRecoveryVolumes = recoveryVolumes
		details.Corruptions = faults
		if err := verifyRARRecoveryVolumes(ctx, config.DockerBinary, rarToolchain, archiveCase, caseDir, firstVolume); err != nil {
			return fixture.RepairDetails{}, nil, err
		}
	default:
		return fixture.RepairDetails{}, nil, fmt.Errorf("unsupported repair profile %q", profile)
	}

	posted, err := digestPostedFiles(filepath.Join(caseDir, "archive"), caseDir)
	if err != nil {
		return fixture.RepairDetails{}, nil, err
	}
	return details, posted, nil
}

func createPAR2(ctx context.Context, dockerBinary string, toolchain PAR2Toolchain, caseDir string, sources []fixture.FileDigest, redundancy int) error {
	args := []string{
		"create", "-q", fmt.Sprintf("-r%d", redundancy), fmt.Sprintf("-s%d", par2BlockSize),
		"archive/fixture.par2",
	}
	for _, source := range sources {
		args = append(args, source.Path)
	}
	if err := runPAR2(ctx, dockerBinary, toolchain, caseDir, args...); err != nil {
		return fmt.Errorf("create PAR2 recovery material: %w", err)
	}
	return nil
}

func runPAR2(ctx context.Context, dockerBinary string, toolchain PAR2Toolchain, caseDir string, par2Args ...string) error {
	args := []string{
		"run", "--rm", "--platform", toolchain.Platform,
		"--user", callerDockerUser(),
		"--mount", "type=bind,src=" + caseDir + ",dst=/work",
		"--workdir", "/work",
		toolchain.Image,
	}
	args = append(args, par2Args...)
	return runCommand(ctx, dockerBinary, args...)
}

func repairTargets(sources []fixture.FileDigest, count int) ([]fixture.FileDigest, error) {
	if count < 1 || len(sources) < count+1 {
		return nil, fmt.Errorf("need at least %d non-leading RAR volumes, have %d", count, len(sources)-1)
	}
	start := len(sources) / 2
	if start+count > len(sources) {
		start = len(sources) - count
	}
	if start == 0 {
		start = 1
	}
	return append([]fixture.FileDigest(nil), sources[start:start+count]...), nil
}

func flipArchiveBytes(caseDir string, target fixture.FileDigest) (fixture.CorruptionDetail, error) {
	path := filepath.Join(caseDir, filepath.FromSlash(target.Path))
	if target.Size <= lightCorruptBytes {
		return fixture.CorruptionDetail{}, fmt.Errorf("cannot corrupt short archive volume %s", target.Path)
	}
	offset := int64(64 << 10)
	if offset+lightCorruptBytes > target.Size {
		offset = target.Size - lightCorruptBytes
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fixture.CorruptionDetail{}, fmt.Errorf("open archive volume %s for corruption: %w", target.Path, err)
	}
	defer file.Close()
	contents := make([]byte, lightCorruptBytes)
	if _, err := io.ReadFull(io.NewSectionReader(file, offset, lightCorruptBytes), contents); err != nil {
		return fixture.CorruptionDetail{}, fmt.Errorf("read archive volume %s for corruption: %w", target.Path, err)
	}
	for index := range contents {
		contents[index] ^= byte(0xa5 + index)
	}
	if _, err := file.WriteAt(contents, offset); err != nil {
		return fixture.CorruptionDetail{}, fmt.Errorf("write archive corruption %s: %w", target.Path, err)
	}
	return fixture.CorruptionDetail{Kind: "byte-flip", Path: target.Path, Offset: offset, Length: lightCorruptBytes}, nil
}

func removeArchiveFiles(caseDir string, targets []fixture.FileDigest) ([]fixture.CorruptionDetail, error) {
	faults := make([]fixture.CorruptionDetail, 0, len(targets))
	for _, target := range targets {
		path := filepath.Join(caseDir, filepath.FromSlash(target.Path))
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove archive volume %s: %w", target.Path, err)
		}
		faults = append(faults, fixture.CorruptionDetail{Kind: "missing-volume", Path: target.Path})
	}
	return faults, nil
}

func verifyPAR2Repair(ctx context.Context, dockerBinary string, par2Toolchain PAR2Toolchain, rarToolchain Toolchain, archiveCase fixture.ArchiveCase, caseDir, firstVolume string) error {
	verifyDir, err := copyArchiveForRepairVerification(caseDir)
	if err != nil {
		return err
	}
	defer os.RemoveAll(verifyDir)
	if err := runPAR2(ctx, dockerBinary, par2Toolchain, verifyDir, "repair", "-q", "archive/fixture.par2"); err != nil {
		return fmt.Errorf("PAR2 repair verification: %w", err)
	}
	if err := testRARArchive(ctx, dockerBinary, rarToolchain, archiveCase, verifyDir, firstVolume); err != nil {
		return fmt.Errorf("verify PAR2-repaired RAR archive: %w", err)
	}
	return nil
}

func verifyRARRecoveryVolumes(ctx context.Context, dockerBinary string, toolchain Toolchain, archiveCase fixture.ArchiveCase, caseDir, firstVolume string) error {
	verifyDir, err := copyArchiveForRepairVerification(caseDir)
	if err != nil {
		return err
	}
	defer os.RemoveAll(verifyDir)
	reconstructArgs := []string{"rc", "-idq", "-y"}
	if archiveCase.RequiresPassword() {
		reconstructArgs = append(reconstructArgs, "-p"+fixture.FixturePassword)
	}
	reconstructArgs = append(reconstructArgs, firstVolume)
	if err := runRAR(ctx, dockerBinary, toolchain, verifyDir, reconstructArgs...); err != nil {
		return fmt.Errorf("RAR recovery-volume verification: %w", err)
	}
	if err := testRARArchive(ctx, dockerBinary, toolchain, archiveCase, verifyDir, firstVolume); err != nil {
		return fmt.Errorf("verify RAR recovery-volume archive: %w", err)
	}
	return nil
}

func testRARArchive(ctx context.Context, dockerBinary string, toolchain Toolchain, archiveCase fixture.ArchiveCase, caseDir, firstVolume string) error {
	args := []string{"t", "-idq", "-y"}
	if archiveCase.RequiresPassword() {
		args = append(args, "-p"+fixture.FixturePassword)
	}
	args = append(args, firstVolume)
	return runRAR(ctx, dockerBinary, toolchain, caseDir, args...)
}

func copyArchiveForRepairVerification(caseDir string) (string, error) {
	verifyDir := filepath.Join(caseDir, "repair-verification")
	if err := os.Mkdir(verifyDir, 0o755); err != nil {
		return "", fmt.Errorf("create repair verification directory: %w", err)
	}
	archiveSource := filepath.Join(caseDir, "archive")
	archiveDestination := filepath.Join(verifyDir, "archive")
	if err := copyDirectory(archiveSource, archiveDestination); err != nil {
		_ = os.RemoveAll(verifyDir)
		return "", err
	}
	return verifyDir, nil
}

func copyDirectory(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported repair verification input %s", path)
		}
		return copyFile(path, target)
	})
}

func copyFile(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func digestPostedFiles(archiveDir, caseDir string) ([]fixture.FileDigest, error) {
	var paths []string
	if err := filepath.WalkDir(archiveDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("repair fixture has no posted files")
	}
	digests := make([]fixture.FileDigest, 0, len(paths))
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		digest, err := hashFile(path)
		if err != nil {
			return nil, err
		}
		relative, err := filepath.Rel(caseDir, path)
		if err != nil {
			return nil, err
		}
		digests = append(digests, fixture.FileDigest{Path: filepath.ToSlash(relative), Size: info.Size(), SHA256: digest})
	}
	return digests, nil
}
