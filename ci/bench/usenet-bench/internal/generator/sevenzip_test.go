package generator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

func TestCheckedInSevenZipToolchainIsPinned(t *testing.T) {
	toolchain, err := LoadSevenZipToolchain("../../docker/sevenzip/toolchain.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(toolchain.URL, "https://") {
		t.Fatalf("7-Zip toolchain URL is not https: %q", toolchain.URL)
	}
	if len(toolchain.SHA256) != 64 {
		t.Fatalf("7-Zip toolchain sha256 is not a full digest: %q", toolchain.SHA256)
	}
	if toolchain.Version == "" || toolchain.Binary == "" {
		t.Fatalf("7-Zip toolchain is under-specified: %#v", toolchain)
	}
	if strings.Contains(toolchain.Image, ":latest") || !strings.Contains(toolchain.Image, ":") {
		t.Fatalf("7-Zip image tag is not pinned: %q", toolchain.Image)
	}
	id := toolchain.ManifestID()
	if id.Version != toolchain.Version || id.SHA256 != toolchain.SHA256 {
		t.Fatalf("manifest toolchain id does not carry the pin: %#v", id)
	}
}

func TestSevenZipToolchainRejectsUnpinnedInputs(t *testing.T) {
	base := SevenZipToolchain{
		SchemaVersion: 1,
		ID:            "sevenzip-test",
		Image:         "weaver-nntp-bench-7zip:test",
		Platform:      "linux/amd64",
		URL:           "https://example.invalid/7z.tar.xz",
		SHA256:        strings.Repeat("a", 64),
		Binary:        "7zz",
		Version:       "26.02",
	}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for label, mutate := range map[string]func(*SevenZipToolchain){
		"plaintext url":  func(s *SevenZipToolchain) { s.URL = "http://example.invalid/7z.tar.xz" },
		"short digest":   func(s *SevenZipToolchain) { s.SHA256 = "abc" },
		"missing digest": func(s *SevenZipToolchain) { s.SHA256 = "" },
		"path traversal": func(s *SevenZipToolchain) { s.Binary = "../7zz" },
		"absent version": func(s *SevenZipToolchain) { s.Version = "" },
		"wrong schema":   func(s *SevenZipToolchain) { s.SchemaVersion = 2 },
	} {
		candidate := base
		mutate(&candidate)
		if err := candidate.Validate(); err == nil {
			t.Fatalf("%s was accepted", label)
		}
	}
}

func TestLoadSevenZipToolchainRejectsMalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "toolchain.json")
	if err := os.WriteFile(path, []byte("{\"schema_version\":1}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSevenZipToolchain(path); err == nil {
		t.Fatal("an incomplete 7-Zip toolchain was accepted")
	}
}

func TestIsArchiveVolumeIsFormatAware(t *testing.T) {
	if !isArchiveVolume("fixture.part01.rar", fixture.RAR5) || isArchiveVolume("fixture.par2", fixture.RAR5) {
		t.Fatal("RAR volume detection is wrong")
	}
	if !isArchiveVolume("fixture.7z.001", fixture.SevenZip) || !isArchiveVolume("fixture.7z", fixture.SevenZip) {
		t.Fatal("7z volume detection missed a volume")
	}
	if isArchiveVolume("fixture.7z.001.par2", fixture.RAR4) {
		t.Fatal("RAR volume detection accepted a 7z volume")
	}
	if isArchiveVolume("fixture.par2", fixture.SevenZip) || isArchiveVolume("fixture.vol000+01.par2", fixture.SevenZip) {
		t.Fatal("7z volume detection accepted repair material")
	}
}

func TestInputRelativePathsStripTheStagingPrefix(t *testing.T) {
	got, err := inputRelativePaths([]string{"input/payload-01.mkv", "input/BDMV"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "payload-01.mkv" || got[1] != "BDMV" {
		t.Fatalf("relative inputs = %v", got)
	}
	if _, err := inputRelativePaths([]string{"archive/fixture.7z"}); err == nil {
		t.Fatal("an input outside the staging directory was accepted")
	}
}

func TestSelectedCasesRequireSevenZip(t *testing.T) {
	cases := []fixture.ArchiveCase{
		{ID: "rar", ArchiveFormat: fixture.RAR5},
		{ID: "sevenzip", ArchiveFormat: fixture.SevenZip},
	}
	if !selectedCasesRequireSevenZip(cases, nil) {
		t.Fatal("a corpus containing a 7z case did not require the 7-Zip writer")
	}
	if selectedCasesRequireSevenZip(cases, map[string]bool{"rar": true}) {
		t.Fatal("a RAR-only selection pulled in the 7-Zip writer")
	}
	if !selectedCasesRequireSevenZip(cases, map[string]bool{"sevenzip": true}) {
		t.Fatal("a 7z selection did not require the 7-Zip writer")
	}
}

func TestExcludeWithheldRemovesOnlyWithheldVolumes(t *testing.T) {
	posted := []fixture.FileDigest{
		{Path: "archive/fixture.part01.rar"},
		{Path: "archive/fixture.part02.rar"},
		{Path: "archive/fixture.par2"},
	}
	kept := excludeWithheld(posted, []fixture.FileDigest{{Path: "archive/fixture.part02.rar"}})
	if len(kept) != 2 {
		t.Fatalf("kept = %v", kept)
	}
	for _, file := range kept {
		if file.Path == "archive/fixture.part02.rar" {
			t.Fatalf("withheld volume stayed in the posted set: %v", kept)
		}
	}
	if len(excludeWithheld(posted, nil)) != len(posted) {
		t.Fatal("excludeWithheld dropped files with nothing withheld")
	}
}

func TestWithholdArchiveFilesKeepsBytesOnDisk(t *testing.T) {
	caseDir := t.TempDir()
	archive := filepath.Join(caseDir, "archive")
	if err := os.MkdirAll(archive, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(archive, "fixture.part02.rar")
	if err := os.WriteFile(path, []byte("volume"), 0o644); err != nil {
		t.Fatal(err)
	}
	faults, withheld, err := withholdArchiveFiles(caseDir, []fixture.FileDigest{{Path: "archive/fixture.part02.rar", Size: 6}})
	if err != nil {
		t.Fatal(err)
	}
	if len(faults) != 1 || faults[0].Kind != "withheld-volume" {
		t.Fatalf("faults = %#v", faults)
	}
	if len(withheld) != 1 || withheld[0].Size != 6 {
		t.Fatalf("withheld = %#v", withheld)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("withholding removed the volume from disk: %v", err)
	}
	if _, _, err := withholdArchiveFiles(caseDir, []fixture.FileDigest{{Path: "archive/absent.rar"}}); err == nil {
		t.Fatal("withholding a volume that does not exist was accepted")
	}
}

func TestPAR2ParametersTreatWithheldHeavyLikeHeavy(t *testing.T) {
	heavyRedundancy, heavyMissing := par2RepairParameters(fixture.PAR2HeavyRepairProfile)
	withheldRedundancy, withheldMissing := par2RepairParameters(fixture.PAR2HeavyWithheldProfile)
	if heavyRedundancy != withheldRedundancy || heavyMissing != withheldMissing {
		t.Fatalf("withheld-heavy diverges from heavy: %d/%d vs %d/%d", withheldRedundancy, withheldMissing, heavyRedundancy, heavyMissing)
	}
	lightRedundancy, lightMissing := par2RepairParameters(fixture.PAR2LightRepairProfile)
	if lightMissing != 0 || lightRedundancy >= heavyRedundancy {
		t.Fatalf("light repair parameters = %d/%d", lightRedundancy, lightMissing)
	}
}

func TestCopyArchiveForRepairVerificationExcludesWithheldVolumes(t *testing.T) {
	caseDir := t.TempDir()
	archive := filepath.Join(caseDir, "archive")
	if err := os.MkdirAll(archive, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"fixture.part01.rar", "fixture.part02.rar", "fixture.par2"} {
		if err := os.WriteFile(filepath.Join(archive, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	verifyDir, err := copyArchiveForRepairVerification(caseDir, []fixture.FileDigest{{Path: "archive/fixture.part02.rar"}})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(verifyDir)
	if _, err := os.Stat(filepath.Join(verifyDir, "archive", "fixture.part01.rar")); err != nil {
		t.Fatalf("posted volume was not staged: %v", err)
	}
	if _, err := os.Stat(filepath.Join(verifyDir, "archive", "fixture.part02.rar")); !os.IsNotExist(err) {
		t.Fatalf("withheld volume was staged, so repair capacity would not be exercised: %v", err)
	}
}

func TestBluRayMediumPathsDoNotCollide(t *testing.T) {
	seen := map[string]bool{"BDMV/STREAM/00000.m2ts": true}
	for index := 1; index <= 4; index++ {
		seen[bluRaySmallPath(index)] = true
	}
	for index := 1; index <= 8; index++ {
		path := bluRayMediumPath(index)
		if seen[path] {
			t.Fatalf("medium stream %d collides with an existing member: %s", index, path)
		}
		seen[path] = true
	}
}

func TestGeneratorConfigRequiresBluRayMediumSizes(t *testing.T) {
	base := Config{
		MatrixPath:              "fixtures/matrix.json",
		ToolchainsPath:          "docker/rarlab/toolchains.json",
		DockerfilePath:          "docker/rarlab/Dockerfile",
		OutputDir:               "generated",
		BytesPerFile:            1 << 20,
		MultiVolumeBytesPerFile: 1 << 20,
		BluRayLargeFileBytes:    1 << 20,
		BluRayMediumFileBytes:   1 << 20,
		BluRayMediumFileCount:   8,
		BluRaySmallFileBytes:    1 << 10,
		BluRaySmallFileCount:    4,
		Workers:                 1,
	}
	if err := base.withDefaults().Validate(); err != nil {
		t.Fatal(err)
	}
	// A zero is a "not supplied" marker that withDefaults fills in, so the
	// rejection path is exercised with values a caller had to state.
	negativeBytes := base
	negativeBytes.BluRayMediumFileBytes = -1
	if err := negativeBytes.withDefaults().Validate(); err == nil {
		t.Fatal("a bluray fixture with a negative extra-stream size was accepted")
	}
	negativeCount := base
	negativeCount.BluRayMediumFileCount = -1
	if err := negativeCount.withDefaults().Validate(); err == nil {
		t.Fatal("a bluray fixture with a negative extra-stream count was accepted")
	}
	if defaults := (Config{}).withDefaults(); defaults.BluRayMediumFileCount < 1 || defaults.BluRayMediumFileBytes <= 0 {
		t.Fatalf("extra-stream defaults are not set: %#v", defaults)
	}
}

func TestSevenZipToolchainJSONRoundTrips(t *testing.T) {
	contents, err := os.ReadFile("../../docker/sevenzip/toolchain.json")
	if err != nil {
		t.Fatal(err)
	}
	var decoded SevenZipToolchain
	if err := json.Unmarshal(contents, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
}

// A reduced-size local run asks for far fewer small files than a full disc,
// so the CERTIFICATE members are never written and the archiver must not be
// pointed at a directory that does not exist.
func TestPresentBluRayInputRootsSkipsRootsWithNoMembers(t *testing.T) {
	full := []fixture.FileDigest{
		{Path: "BDMV/STREAM/00000.m2ts"},
		{Path: "CERTIFICATE/id.bdmv"},
	}
	if got := presentBluRayInputRoots(full); len(got) != 2 || got[0] != "input/BDMV" || got[1] != "input/CERTIFICATE" {
		t.Fatalf("full disc roots = %v", got)
	}
	reduced := []fixture.FileDigest{
		{Path: "BDMV/STREAM/00000.m2ts"},
		{Path: "BDMV/PLAYLIST/00000.mpls"},
	}
	if got := presentBluRayInputRoots(reduced); len(got) != 1 || got[0] != "input/BDMV" {
		t.Fatalf("reduced disc roots = %v", got)
	}
	if got := presentBluRayInputRoots(nil); len(got) != 0 {
		t.Fatalf("an empty payload produced roots: %v", got)
	}
}
