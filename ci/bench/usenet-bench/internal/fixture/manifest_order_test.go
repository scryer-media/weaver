package fixture

import "testing"

func TestLoadGeneratedManifestV6RoundTripsPostingOrderAndWithheldFiles(t *testing.T) {
	manifest := GeneratedManifest{
		SchemaVersion: GeneratedManifestSchemaVersion,
		Case: ArchiveCase{
			ID:            "withheld",
			RepairProfile: PAR2HeavyWithheldProfile,
			NZBOrder:      ScatteredNZBOrder,
		},
		ExpectedFiles:      []FileDigest{{Path: "payload.bin", Size: 1, BLAKE3: "a"}},
		SourceArchiveFiles: []FileDigest{{Path: "archive/fixture.part01.rar", Size: 1, BLAKE3: "b"}},
		ArchiveFiles: []FileDigest{
			{Path: "archive/fixture.part01.rar", Size: 1, BLAKE3: "b"},
			{Path: "archive/fixture.par2", Size: 1, BLAKE3: "d"},
		},
		WithheldFiles: []FileDigest{{Path: "archive/fixture.part02.rar", Size: 1, BLAKE3: "c"}},
		NZBFileOrder: []string{
			"archive/fixture.par2",
			"archive/fixture.part02.rar",
			"archive/fixture.part01.rar",
		},
		NZBOrderSeed: 42,
		Repair: RepairDetails{
			Profile:               PAR2HeavyWithheldProfile,
			PAR2RedundancyPercent: 35,
			Corruptions:           []CorruptionDetail{{Kind: "withheld-volume", Path: "archive/fixture.part02.rar"}},
		},
	}
	loaded, err := LoadGeneratedManifest(writeTestManifest(t, manifest))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Case.NZBOrder != ScatteredNZBOrder || loaded.NZBOrderSeed != 42 {
		t.Fatalf("posting order did not round-trip: %#v", loaded)
	}
	if len(loaded.PostedFiles()) != 3 {
		t.Fatalf("PostedFiles() = %v, want three entries", loaded.PostedFiles())
	}
	if !loaded.IsWithheld("archive/fixture.part02.rar") || loaded.IsWithheld("archive/fixture.part01.rar") {
		t.Fatalf("withheld membership is wrong: %#v", loaded.WithheldFiles)
	}
}

func TestLoadGeneratedManifestV6RejectsAnOrderThatOmitsAPostedFile(t *testing.T) {
	manifest := GeneratedManifest{
		SchemaVersion:      GeneratedManifestSchemaVersion,
		Case:               ArchiveCase{ID: "ordered", RepairProfile: CleanRepairProfile, NZBOrder: SequentialNZBOrder},
		ExpectedFiles:      []FileDigest{{Path: "payload.bin", Size: 1, BLAKE3: "a"}},
		SourceArchiveFiles: []FileDigest{{Path: "archive/fixture.part01.rar", Size: 1, BLAKE3: "b"}},
		ArchiveFiles: []FileDigest{
			{Path: "archive/fixture.part01.rar", Size: 1, BLAKE3: "b"},
			{Path: "archive/fixture.part02.rar", Size: 1, BLAKE3: "c"},
		},
		NZBFileOrder: []string{"archive/fixture.part01.rar"},
		Repair:       RepairDetails{Profile: CleanRepairProfile},
	}
	if _, err := LoadGeneratedManifest(writeTestManifest(t, manifest)); err == nil {
		t.Fatal("an NZB order missing a posted file was accepted")
	}
}

func TestLoadGeneratedManifestBeforeV6DefaultsToSequentialOrder(t *testing.T) {
	loaded, err := LoadGeneratedManifest(writeTestManifest(t, GeneratedManifest{
		SchemaVersion:      5,
		Case:               ArchiveCase{ID: "legacy-order", RepairProfile: CleanRepairProfile},
		ExpectedFiles:      []FileDigest{{Path: "payload.bin", Size: 1, BLAKE3: "a"}},
		SourceArchiveFiles: []FileDigest{{Path: "archive/fixture.part01.rar", Size: 1, BLAKE3: "b"}},
		ArchiveFiles: []FileDigest{
			{Path: "archive/fixture.part01.rar", Size: 1, BLAKE3: "b"},
			{Path: "archive/fixture.part02.rar", Size: 1, BLAKE3: "c"},
		},
		Repair: RepairDetails{Profile: CleanRepairProfile},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Case.NZBOrder != SequentialNZBOrder || loaded.NZBOrderSeed != 0 {
		t.Fatalf("pre-order manifest was not normalized: %#v", loaded)
	}
	if len(loaded.NZBFileOrder) != 2 || loaded.NZBFileOrder[0] != "archive/fixture.part01.rar" {
		t.Fatalf("pre-order manifest lost its posting order: %v", loaded.NZBFileOrder)
	}
}
