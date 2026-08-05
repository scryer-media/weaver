package fixture

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadGeneratedManifestV4RequiresMatchingRepairMetadata(t *testing.T) {
	manifest := GeneratedManifest{
		SchemaVersion:      4,
		Case:               ArchiveCase{ID: "repair", RepairProfile: PAR2LightRepairProfile},
		ExpectedFiles:      []FileDigest{{Path: "payload.bin", Size: 1, SHA256: "a"}},
		SourceArchiveFiles: []FileDigest{{Path: "archive/fixture.part01.rar", Size: 1, SHA256: "b"}},
		ArchiveFiles:       []FileDigest{{Path: "archive/fixture.part01.rar", Size: 1, SHA256: "c"}},
		Repair: RepairDetails{
			Profile:               PAR2LightRepairProfile,
			PAR2RedundancyPercent: 10,
			Corruptions:           []CorruptionDetail{{Kind: "byte-flip", Path: "archive/fixture.part01.rar", Offset: 1, Length: 1}},
		},
	}
	path := writeTestManifest(t, manifest)
	loaded, err := LoadGeneratedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Repair.Profile != PAR2LightRepairProfile || len(loaded.SourceArchiveFiles) != 1 {
		t.Fatalf("loaded repair manifest = %#v", loaded)
	}

	manifest.Repair.Profile = RARRecoveryVolumeLightProfile
	path = writeTestManifest(t, manifest)
	if _, err := LoadGeneratedManifest(path); err == nil {
		t.Fatal("mismatched case and repair profile was accepted")
	}
}

func TestLoadGeneratedManifestUpgradesLegacyManifestAsClean(t *testing.T) {
	path := writeTestManifest(t, GeneratedManifest{
		SchemaVersion: 3,
		Case:          ArchiveCase{ID: "clean"},
		ExpectedFiles: []FileDigest{{Path: "payload.bin", Size: 1, SHA256: "a"}},
		ArchiveFiles:  []FileDigest{{Path: "archive/fixture.part01.rar", Size: 1, SHA256: "b"}},
	})
	loaded, err := LoadGeneratedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Repair.Profile != CleanRepairProfile || len(loaded.SourceArchiveFiles) != 1 {
		t.Fatalf("legacy manifest was not normalized as clean: %#v", loaded)
	}
}

func writeTestManifest(t *testing.T, manifest GeneratedManifest) string {
	t.Helper()
	contents, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fixture-manifest.json")
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
