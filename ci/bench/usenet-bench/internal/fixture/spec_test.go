package fixture

import (
	"strings"
	"testing"
)

func TestExpandCoversEveryAxis(t *testing.T) {
	matrix := Matrix{
		SchemaVersion: 2,
		Sets: []FixtureSet{{
			ID:                 "modern-rar5",
			WriterEra:          "RAR 5.x-7.x compatibility",
			GeneratorToolchain: "rarlab-7.23",
			ArchiveFormat:      RAR5,
			Compressions:       []Compression{Store, Normal},
			Solid:              []bool{false, true},
			Encryptions:        []Encryption{NoEncryption, DataEncryption, HeaderEncryption},
			Payloads:           []PayloadKind{IncompressiblePayload, CompressiblePayload},
			FileCount:          4,
			VolumeSize:         "32m",
		}},
	}
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatalf("Expand() error = %v", err)
	}
	if got, want := len(cases), 24; got != want {
		t.Fatalf("len(Expand()) = %d, want %d", got, want)
	}
	seen := map[string]bool{}
	for _, c := range cases {
		seen[c.ID] = true
	}
	if !seen["modern-rar5-normal-solid-headers-compressible"] {
		t.Fatalf("expanded matrix omitted solid encrypted compressed fixture")
	}
}

func TestRARArgsAreExplicit(t *testing.T) {
	c := ArchiveCase{
		ID:            "case",
		ArchiveFormat: RAR5,
		Compression:   Normal,
		Solid:         true,
		Encryption:    HeaderEncryption,
		VolumeSize:    "32m",
	}
	args, err := c.RARArgs("archive/fixture.rar", []string{"input/one.bin", "input/two.bin"})
	if err != nil {
		t.Fatalf("RARArgs() error = %v", err)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"-ma5", "-qo-", "-m5", "-md256m", "-s", "-hp" + FixturePassword, "-v32m"} {
		if !strings.Contains(joined, want) {
			t.Errorf("RARArgs() = %q, missing %q", joined, want)
		}
	}
}

func TestRAR4ReleaseCompressionUsesItsMaximumDictionary(t *testing.T) {
	c := ArchiveCase{
		ID:            "rar4-release",
		ArchiveFormat: RAR4,
		Compression:   Normal,
		Solid:         true,
		Encryption:    NoEncryption,
		VolumeSize:    "32m",
	}
	args, err := c.RARArgs("archive/fixture.rar", []string{"input/one.bin"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"-m5", "-md4096", "-s"} {
		if !strings.Contains(joined, want) {
			t.Errorf("RAR4 args = %q, missing %q", joined, want)
		}
	}
	if strings.Contains(joined, "-qo-") {
		t.Fatalf("RAR4 args must not use RAR5-only quick-open control: %q", joined)
	}
}

func TestLegacyRAR4ArgsUseTheLockedWriterDefault(t *testing.T) {
	c := ArchiveCase{
		ID:            "legacy-case",
		ArchiveFormat: RAR4,
		Compression:   Store,
		Solid:         false,
		Encryption:    DataEncryption,
		VolumeSize:    "32m",
	}
	args, err := c.RARArgs("archive/fixture.rar", []string{"input/one.bin"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "-ma") {
		t.Fatalf("legacy RAR4 must not claim a newer -ma format selector: %q", joined)
	}
	for _, want := range []string{"-m0", "-s-", "-p" + FixturePassword, "-v32m"} {
		if !strings.Contains(joined, want) {
			t.Errorf("legacy RAR4 args = %q, missing %q", joined, want)
		}
	}
}

func TestBluRayLayoutDoesNotRequireUniformFileCount(t *testing.T) {
	matrix := Matrix{SchemaVersion: 2, Sets: []FixtureSet{{
		ID:                 "modern-rar5-bluray",
		WriterEra:          "RAR 5.x-7.x compatibility",
		GeneratorToolchain: "rarlab-7.23",
		ArchiveFormat:      RAR5,
		Compressions:       []Compression{Normal},
		Solid:              []bool{true},
		Encryptions:        []Encryption{NoEncryption},
		Payloads:           []PayloadKind{IncompressiblePayload},
		PayloadLayout:      BluRayDiscPayloadLayout,
		VolumeSize:         "32m",
	}}}
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(cases), 1; got != want {
		t.Fatalf("cases = %d, want %d", got, want)
	}
	if cases[0].PayloadLayout != BluRayDiscPayloadLayout {
		t.Fatalf("layout = %q", cases[0].PayloadLayout)
	}
}

func TestRepairProfilesAreExplicitFixtureCases(t *testing.T) {
	matrix := Matrix{SchemaVersion: 2, Sets: []FixtureSet{{
		ID:                 "repair-rar5",
		WriterEra:          "RAR 7.23",
		GeneratorToolchain: "rarlab-7.23",
		ArchiveFormat:      RAR5,
		Compressions:       []Compression{Normal},
		Solid:              []bool{true},
		Encryptions:        []Encryption{NoEncryption},
		Payloads:           []PayloadKind{IncompressiblePayload},
		RepairProfiles:     []RepairProfile{PAR2LightRepairProfile, RARRecoveryVolumeHeavyProfile},
		FileCount:          4,
		VolumeSize:         "32m",
	}}}
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(cases), 2; got != want {
		t.Fatalf("cases = %d, want %d", got, want)
	}
	if got, want := cases[0].ID, "repair-rar5-par2-light-normal-solid-none-incompressible"; got != want {
		t.Fatalf("case id = %q, want %q", got, want)
	}
	if cases[1].RepairProfile != RARRecoveryVolumeHeavyProfile {
		t.Fatalf("second repair profile = %q", cases[1].RepairProfile)
	}
}
