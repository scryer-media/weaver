package generator

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

func TestRepairTargetsNeverSelectTheFirstVolume(t *testing.T) {
	sources := []fixture.FileDigest{
		{Path: "archive/fixture.part1.rar"},
		{Path: "archive/fixture.part2.rar"},
		{Path: "archive/fixture.part3.rar"},
		{Path: "archive/fixture.part4.rar"},
	}
	targets, err := repairTargets(sources, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(targets), 2; got != want {
		t.Fatalf("targets = %d, want %d", got, want)
	}
	for _, target := range targets {
		if target.Path == sources[0].Path {
			t.Fatalf("repair target included the leading volume: %#v", targets)
		}
	}
}

func TestFlipArchiveBytesIsDeterministicAndBounded(t *testing.T) {
	caseDir := t.TempDir()
	path := filepath.Join(caseDir, "archive", "fixture.part2.rar")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	original := bytes.Repeat([]byte{0x3c}, 128<<10)
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatal(err)
	}
	fault, err := flipArchiveBytes(caseDir, fixture.FileDigest{Path: "archive/fixture.part2.rar", Size: int64(len(original))})
	if err != nil {
		t.Fatal(err)
	}
	if fault.Kind != "byte-flip" || fault.Offset != 64<<10 || fault.Length != lightCorruptBytes {
		t.Fatalf("fault = %#v", fault)
	}
	changed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(original, changed) {
		t.Fatal("archive volume was not changed")
	}
	if !bytes.Equal(original[:fault.Offset], changed[:fault.Offset]) || !bytes.Equal(original[fault.Offset+int64(fault.Length):], changed[fault.Offset+int64(fault.Length):]) {
		t.Fatal("corruption escaped its declared bounds")
	}
}

func TestSelectedCasesRequirePAR2OnlyForPAR2Profiles(t *testing.T) {
	cases := []fixture.ArchiveCase{{ID: "rar", RepairProfile: fixture.RARRecoveryVolumeLightProfile}}
	if selectedCasesRequirePAR2(cases, nil) {
		t.Fatal("RAR recovery volumes must not require the PAR2 image")
	}
	cases = append(cases, fixture.ArchiveCase{ID: "par2", RepairProfile: fixture.PAR2HeavyRepairProfile})
	if !selectedCasesRequirePAR2(cases, map[string]bool{"par2": true}) {
		t.Fatal("selected PAR2 profile did not require the PAR2 image")
	}
}

func TestPAR2HeavyRepairStaysWithinOneVolume(t *testing.T) {
	if redundancy, missing := par2RepairParameters(fixture.PAR2LightRepairProfile); redundancy != 10 || missing != 0 {
		t.Fatalf("light PAR2 repair = %d%%, %d missing volumes", redundancy, missing)
	}
	if redundancy, missing := par2RepairParameters(fixture.PAR2HeavyRepairProfile); redundancy != 35 || missing != 1 {
		t.Fatalf("heavy PAR2 repair = %d%%, %d missing volumes", redundancy, missing)
	}
}
