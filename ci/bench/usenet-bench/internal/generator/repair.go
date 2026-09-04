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
		if archiveCase.RepairProfile.UsesPAR2() {
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

// repairInputs is everything a repair profile needs. It is a struct because
// the 7z lane added a second writer toolchain and the extraction oracle, and a
// nine-argument positional call is easy to transpose silently.
type repairInputs struct {
	Config            Config
	Case              fixture.ArchiveCase
	RARToolchain      Toolchain
	PAR2Toolchain     *PAR2Toolchain
	SevenZipToolchain *SevenZipToolchain
	CaseDir           string
	SourceArchives    []fixture.FileDigest
	FirstVolume       string
	ExpectedFiles     []fixture.FileDigest
}

// applyRepairProfile injects the declared fault and returns the repair
// metadata, the files that are actually posted, and the files that are listed
// in the NZB but deliberately never posted.
func applyRepairProfile(ctx context.Context, in repairInputs) (fixture.RepairDetails, []fixture.FileDigest, []fixture.FileDigest, error) {
	config := in.Config
	archiveCase := in.Case
	caseDir := in.CaseDir
	profile := archiveCase.RepairProfile
	if profile == "" {
		profile = fixture.CleanRepairProfile
	}
	details := fixture.RepairDetails{Profile: profile}
	var withheld []fixture.FileDigest

	switch profile {
	case fixture.CleanRepairProfile:
		posted, err := digestPostedFiles(filepath.Join(caseDir, "archive"), caseDir)
		return details, posted, nil, err
	case fixture.PAR2LightRepairProfile, fixture.PAR2HeavyRepairProfile, fixture.PAR2HeavyWithheldProfile:
		if in.PAR2Toolchain == nil {
			return fixture.RepairDetails{}, nil, nil, fmt.Errorf("PAR2 repair fixture requires a PAR2 toolchain")
		}
		redundancy, missingVolumes := par2RepairParameters(profile)
		if err := createPAR2(ctx, config.DockerBinary, *in.PAR2Toolchain, caseDir, in.SourceArchives, redundancy); err != nil {
			return fixture.RepairDetails{}, nil, nil, err
		}
		details.PAR2RedundancyPercent = redundancy
		if missingVolumes == 0 {
			targets, err := repairTargets(in.SourceArchives, 1)
			if err != nil {
				return fixture.RepairDetails{}, nil, nil, err
			}
			fault, err := flipArchiveBytes(caseDir, targets[0])
			if err != nil {
				return fixture.RepairDetails{}, nil, nil, err
			}
			details.Corruptions = []fixture.CorruptionDetail{fault}
		} else {
			targets, err := repairTargets(in.SourceArchives, missingVolumes)
			if err != nil {
				return fixture.RepairDetails{}, nil, nil, err
			}
			if profile == fixture.PAR2HeavyWithheldProfile {
				faults, held, err := withholdArchiveFiles(caseDir, targets)
				if err != nil {
					return fixture.RepairDetails{}, nil, nil, err
				}
				details.Corruptions = faults
				withheld = held
			} else {
				faults, err := removeArchiveFiles(caseDir, targets)
				if err != nil {
					return fixture.RepairDetails{}, nil, nil, err
				}
				details.Corruptions = faults
			}
		}
		if err := verifyPAR2Repair(ctx, in, withheld); err != nil {
			return fixture.RepairDetails{}, nil, nil, err
		}
	case fixture.RARRecoveryVolumeLightProfile, fixture.RARRecoveryVolumeHeavyProfile:
		if archiveCase.ArchiveFormat == fixture.SevenZip {
			return fixture.RepairDetails{}, nil, nil, fmt.Errorf("repair profile %q is a RAR container feature", profile)
		}
		recoveryVolumes := 1
		if profile == fixture.RARRecoveryVolumeHeavyProfile {
			recoveryVolumes = 2
		}
		recoveryArgs := []string{fmt.Sprintf("rv%d", recoveryVolumes), "-idq", "-y"}
		if archiveCase.RequiresPassword() {
			recoveryArgs = append(recoveryArgs, "-p"+fixture.FixturePassword)
		}
		recoveryArgs = append(recoveryArgs, in.FirstVolume)
		if err := runRAR(ctx, config.DockerBinary, in.RARToolchain, caseDir, recoveryArgs...); err != nil {
			return fixture.RepairDetails{}, nil, nil, fmt.Errorf("create %d RAR recovery volumes: %w", recoveryVolumes, err)
		}
		targets, err := repairTargets(in.SourceArchives, recoveryVolumes)
		if err != nil {
			return fixture.RepairDetails{}, nil, nil, err
		}
		faults, err := removeArchiveFiles(caseDir, targets)
		if err != nil {
			return fixture.RepairDetails{}, nil, nil, err
		}
		details.RARRecoveryVolumes = recoveryVolumes
		details.Corruptions = faults
		if err := verifyRARRecoveryVolumes(ctx, config.DockerBinary, in.RARToolchain, archiveCase, caseDir, in.FirstVolume); err != nil {
			return fixture.RepairDetails{}, nil, nil, err
		}
	default:
		return fixture.RepairDetails{}, nil, nil, fmt.Errorf("unsupported repair profile %q", profile)
	}

	posted, err := digestPostedFiles(filepath.Join(caseDir, "archive"), caseDir)
	if err != nil {
		return fixture.RepairDetails{}, nil, nil, err
	}
	posted = excludeWithheld(posted, withheld)
	return details, posted, withheld, nil
}

func par2RepairParameters(profile fixture.RepairProfile) (redundancy, missingVolumes int) {
	switch profile {
	case fixture.PAR2HeavyRepairProfile, fixture.PAR2HeavyWithheldProfile:
		// A 150 MiB movie split into 32 MiB volumes has five volumes. Two
		// missing volumes exceed 35% recovery capacity, so heavy repair is
		// one complete missing volume; light repair remains a byte flip.
		return 35, 1
	default:
		return 10, 0
	}
}

// excludeWithheld removes the withheld volumes from the posted set. Their
// bytes stay on disk so the PAR2 target remains auditable and so the seeder
// can size their NZB entries, but they are never handed to the poster.
func excludeWithheld(posted, withheld []fixture.FileDigest) []fixture.FileDigest {
	if len(withheld) == 0 {
		return posted
	}
	held := make(map[string]bool, len(withheld))
	for _, file := range withheld {
		held[file.Path] = true
	}
	kept := make([]fixture.FileDigest, 0, len(posted))
	for _, file := range posted {
		if held[file.Path] {
			continue
		}
		kept = append(kept, file)
	}
	return kept
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

// withholdArchiveFiles marks volumes as posted-but-never-sent. Unlike
// removeArchiveFiles it keeps the bytes on disk: the seeder needs the volume's
// size to write its NZB entry, and the retained file keeps the repair target
// independently auditable. A client sees exactly the same thing either way —
// the volume never arrives — but a withheld volume is still requested, and
// every one of its articles is refused, which is what a real short post does.
func withholdArchiveFiles(caseDir string, targets []fixture.FileDigest) ([]fixture.CorruptionDetail, []fixture.FileDigest, error) {
	faults := make([]fixture.CorruptionDetail, 0, len(targets))
	withheld := make([]fixture.FileDigest, 0, len(targets))
	for _, target := range targets {
		path := filepath.Join(caseDir, filepath.FromSlash(target.Path))
		if _, err := os.Stat(path); err != nil {
			return nil, nil, fmt.Errorf("withhold archive volume %s: %w", target.Path, err)
		}
		faults = append(faults, fixture.CorruptionDetail{Kind: "withheld-volume", Path: target.Path})
		withheld = append(withheld, target)
	}
	return faults, withheld, nil
}

func verifyPAR2Repair(ctx context.Context, in repairInputs, withheld []fixture.FileDigest) error {
	verifyDir, err := copyArchiveForRepairVerification(in.CaseDir, withheld)
	if err != nil {
		return err
	}
	defer os.RemoveAll(verifyDir)
	if err := runPAR2(ctx, in.Config.DockerBinary, *in.PAR2Toolchain, verifyDir, "repair", "-q", "archive/fixture.par2"); err != nil {
		return fmt.Errorf("PAR2 repair verification: %w", err)
	}
	if in.Case.ArchiveFormat == fixture.SevenZip {
		if in.SevenZipToolchain == nil {
			return fmt.Errorf("7z repair verification requires the pinned 7-Zip toolchain")
		}
		if err := verifySevenZipArchive(ctx, in.Config, in.Case, *in.SevenZipToolchain, verifyDir, in.FirstVolume, in.ExpectedFiles); err != nil {
			return fmt.Errorf("verify PAR2-repaired 7z archive: %w", err)
		}
		return nil
	}
	if err := testRARArchive(ctx, in.Config.DockerBinary, in.RARToolchain, in.Case, verifyDir, in.FirstVolume); err != nil {
		return fmt.Errorf("verify PAR2-repaired RAR archive: %w", err)
	}
	return nil
}

func verifyRARRecoveryVolumes(ctx context.Context, dockerBinary string, toolchain Toolchain, archiveCase fixture.ArchiveCase, caseDir, firstVolume string) error {
	verifyDir, err := copyArchiveForRepairVerification(caseDir, nil)
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

// copyArchiveForRepairVerification stages exactly what a client would receive.
// Withheld volumes stay on disk in the fixture but are never posted, so they
// are excluded here: repairing a set that still contains them would prove
// nothing about the declared fault.
func copyArchiveForRepairVerification(caseDir string, withheld []fixture.FileDigest) (string, error) {
	verifyDir := filepath.Join(caseDir, "repair-verification")
	if err := os.Mkdir(verifyDir, 0o755); err != nil {
		return "", fmt.Errorf("create repair verification directory: %w", err)
	}
	skip := make(map[string]bool, len(withheld))
	for _, file := range withheld {
		skip[filepath.Join(caseDir, filepath.FromSlash(file.Path))] = true
	}
	archiveSource := filepath.Join(caseDir, "archive")
	archiveDestination := filepath.Join(verifyDir, "archive")
	if err := copyDirectory(archiveSource, archiveDestination, skip); err != nil {
		_ = os.RemoveAll(verifyDir)
		return "", err
	}
	return verifyDir, nil
}

func copyDirectory(source, destination string, skip map[string]bool) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if skip[path] {
			return nil
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
		digests = append(digests, fixture.FileDigest{Path: filepath.ToSlash(relative), Size: info.Size(), BLAKE3: digest})
	}
	return digests, nil
}
