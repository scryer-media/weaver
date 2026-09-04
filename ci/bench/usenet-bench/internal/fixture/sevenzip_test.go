package fixture

import (
	"strings"
	"testing"
)

func sevenZipCase(compression Compression, solid bool, encryption Encryption) ArchiveCase {
	return ArchiveCase{
		ID:            "sevenzip-case",
		ArchiveFormat: SevenZip,
		Compression:   compression,
		Solid:         solid,
		Encryption:    encryption,
		VolumeSize:    "32m",
	}
}

func TestSevenZipArgsStoreIsExplicitCopyMode(t *testing.T) {
	args, err := sevenZipCase(Store, false, NoEncryption).SevenZipArgs("../archive/fixture.7z", []string{"payload-01.mkv"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	for _, expected := range []string{"a", "-t7z", "-mx0", "-m0=Copy", "-ms=off", "-v32m", "../archive/fixture.7z", "payload-01.mkv"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("7z store args lack %q: %v", expected, args)
		}
	}
	if strings.Contains(joined, "-p") {
		t.Fatalf("unencrypted 7z case passed a password: %v", args)
	}
}

func TestSevenZipArgsCoverSolidAndEncryptionAxes(t *testing.T) {
	solid, err := sevenZipCase(Normal, true, NoEncryption).SevenZipArgs("a.7z", []string{"in"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(solid, " "), "-ms=on") || !strings.Contains(strings.Join(solid, " "), "-mx5") {
		t.Fatalf("solid LZMA2 args = %v", solid)
	}

	data, err := sevenZipCase(Store, false, DataEncryption).SevenZipArgs("a.7z", []string{"in"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(data, " ")
	if !strings.Contains(joined, "-p"+FixturePassword) {
		t.Fatalf("data-encrypted args lack the password: %v", data)
	}
	if strings.Contains(joined, "-mhe=on") {
		t.Fatalf("data-only encryption must not encrypt headers: %v", data)
	}

	headers, err := sevenZipCase(Store, false, HeaderEncryption).SevenZipArgs("a.7z", []string{"in"})
	if err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(headers, " ")
	if !strings.Contains(joined, "-p"+FixturePassword) || !strings.Contains(joined, "-mhe=on") {
		t.Fatalf("header-encrypted args = %v", headers)
	}
}

func TestSevenZipPasswordArgsOnlyForEncryptedCases(t *testing.T) {
	if got := sevenZipCase(Store, false, NoEncryption).SevenZipPasswordArgs(); len(got) != 0 {
		t.Fatalf("unencrypted case wants no password args, got %v", got)
	}
	if got := sevenZipCase(Store, false, HeaderEncryption).SevenZipPasswordArgs(); len(got) != 1 || got[0] != "-p"+FixturePassword {
		t.Fatalf("encrypted case password args = %v", got)
	}
}

func TestArchiveArgsRejectTheWrongWriter(t *testing.T) {
	if _, err := sevenZipCase(Store, false, NoEncryption).RARArgs("a.rar", []string{"in"}); err == nil {
		t.Fatal("RARArgs accepted a 7z case")
	}
	rarCase := ArchiveCase{ID: "rar", ArchiveFormat: RAR5, Compression: Store, VolumeSize: "32m"}
	if _, err := rarCase.SevenZipArgs("a.7z", []string{"in"}); err == nil {
		t.Fatal("SevenZipArgs accepted a RAR case")
	}
}

func TestMatrixRejectsSevenZipWithRARRecoveryVolumes(t *testing.T) {
	matrix := Matrix{SchemaVersion: 2, Sets: []FixtureSet{{
		ID:                 "sevenzip-recovery",
		WriterEra:          "7-Zip",
		GeneratorToolchain: "rarlab-7.23",
		ArchiveWriter:      "sevenzip-26.02",
		ArchiveFormat:      SevenZip,
		Compressions:       []Compression{Store},
		Solid:              []bool{false},
		Encryptions:        []Encryption{NoEncryption},
		Payloads:           []PayloadKind{IncompressiblePayload},
		RepairProfiles:     []RepairProfile{RARRecoveryVolumeLightProfile},
		FileCount:          1,
		VolumeSize:         "32m",
	}}}
	_, err := matrix.Expand()
	if err == nil {
		t.Fatal("a 7z set with RAR recovery volumes was accepted")
	}
	if !strings.Contains(err.Error(), "not a 7z feature") {
		t.Fatalf("unhelpful rejection: %v", err)
	}
}

func TestMatrixAcceptsSevenZipWithPAR2(t *testing.T) {
	matrix := Matrix{SchemaVersion: 2, Sets: []FixtureSet{{
		ID:                 "sevenzip-par2",
		WriterEra:          "7-Zip",
		GeneratorToolchain: "rarlab-7.23",
		ArchiveWriter:      "sevenzip-26.02",
		ArchiveFormat:      SevenZip,
		Compressions:       []Compression{Store},
		Solid:              []bool{false},
		Encryptions:        []Encryption{NoEncryption},
		Payloads:           []PayloadKind{IncompressiblePayload},
		RepairProfiles:     []RepairProfile{PAR2LightRepairProfile},
		NZBOrder:           ScatteredNZBOrder,
		FileCount:          1,
		VolumeSize:         "32m",
	}}}
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 1 {
		t.Fatalf("expected one expanded case, got %d", len(cases))
	}
	if cases[0].ArchiveWriter != "sevenzip-26.02" || cases[0].ArchiveFormat != SevenZip {
		t.Fatalf("expanded case lost its writer or format: %#v", cases[0])
	}
	if cases[0].NZBOrder != ScatteredNZBOrder {
		t.Fatalf("expanded case lost its nzb_order: %#v", cases[0])
	}
}

func TestExpandDefaultsWriterAndOrder(t *testing.T) {
	matrix := Matrix{SchemaVersion: 2, Sets: []FixtureSet{{
		ID:                 "defaults",
		WriterEra:          "RAR 7.23",
		GeneratorToolchain: "rarlab-7.23",
		ArchiveFormat:      RAR5,
		Compressions:       []Compression{Store},
		Solid:              []bool{false},
		Encryptions:        []Encryption{NoEncryption},
		Payloads:           []PayloadKind{IncompressiblePayload},
		FileCount:          1,
		VolumeSize:         "32m",
	}}}
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatal(err)
	}
	if cases[0].ArchiveWriter != "rarlab-7.23" {
		t.Fatalf("archive writer did not default to the generator toolchain: %#v", cases[0])
	}
	if cases[0].NZBOrder != SequentialNZBOrder {
		t.Fatalf("nzb order did not default to sequential: %#v", cases[0])
	}
}

func TestCheckedInMatrixAndCorpusAgree(t *testing.T) {
	matrix, err := LoadMatrix("../../fixtures/matrix.json")
	if err != nil {
		t.Fatal(err)
	}
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatal(err)
	}
	known := make(map[string]bool, len(cases))
	for _, archiveCase := range cases {
		known[archiveCase.ID] = true
	}
	corpus, err := LoadCorpus("../../fixtures/corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range corpus.FixtureIDs {
		// The direct MKV fixture is generated outside the matrix.
		if id == "direct-mkv-200mb" {
			continue
		}
		if !known[id] {
			t.Fatalf("corpus names %q, which the matrix does not expand to", id)
		}
	}
	for _, archiveCase := range cases {
		found := false
		for _, id := range corpus.FixtureIDs {
			if id == archiveCase.ID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("matrix expands %q, which the declared corpus omits", archiveCase.ID)
		}
	}
}
