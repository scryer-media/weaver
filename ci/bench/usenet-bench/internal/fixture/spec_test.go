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
			Class:              BreadthFixtureClass,
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

func TestQuickOpenLaneKeepsTheRAR5QuickOpenRecords(t *testing.T) {
	c := ArchiveCase{
		ID:            "quick-open-case",
		ArchiveFormat: RAR5,
		Compression:   Store,
		Solid:         false,
		Encryption:    NoEncryption,
		VolumeSize:    "32m",
		QuickOpen:     true,
	}
	args, err := c.RARArgs("archive/fixture.rar", []string{"input/one.bin"})
	if err != nil {
		t.Fatalf("RARArgs() error = %v", err)
	}
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "-qo-") {
		t.Fatalf("quick-open lane must not suppress the records: %q", joined)
	}
	if !strings.Contains(joined, "-ma5 -qo+") {
		t.Fatalf("quick-open lane must request a record for every header: %q", joined)
	}
}

func TestQuickOpenIsRejectedOutsideRAR5(t *testing.T) {
	for _, format := range []ArchiveFormat{RAR4, SevenZip} {
		set := FixtureSet{
			ID: "quick-open-" + string(format), WriterEra: "era", GeneratorToolchain: "toolchain",
			Class:         BreadthFixtureClass,
			ArchiveFormat: format, Compressions: []Compression{Store}, Solid: []bool{false},
			Encryptions: []Encryption{NoEncryption}, Payloads: []PayloadKind{IncompressiblePayload},
			FileCount: 1, VolumeSize: "32m", QuickOpen: true,
		}
		if err := set.validate(); err == nil || !strings.Contains(err.Error(), "quick_open") {
			t.Errorf("%s set with quick_open validated, want a quick_open error, got %v", format, err)
		}
	}
}

func TestExpandCarriesQuickOpenOntoEveryCase(t *testing.T) {
	matrix := Matrix{SchemaVersion: 2, Sets: []FixtureSet{{
		ID: "rar5-7-quickopen", WriterEra: "RAR 7.23", GeneratorToolchain: "rarlab-7.23",
		Class:         BreadthFixtureClass,
		ArchiveFormat: RAR5, Compressions: []Compression{Store}, Solid: []bool{false},
		Encryptions: []Encryption{NoEncryption}, Payloads: []PayloadKind{IncompressiblePayload},
		FileCount: 4, VolumeSize: "32m", QuickOpen: true,
	}}}
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 1 || !cases[0].QuickOpen || cases[0].ID != "rar5-7-quickopen-store-nonsolid-none-incompressible" {
		t.Fatalf("Expand() = %+v, want one quick-open case", cases)
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
		Class:              BreadthFixtureClass,
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
		Class:              BreadthFixtureClass,
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

func TestFixtureSetRequiresAClass(t *testing.T) {
	matrix := Matrix{
		SchemaVersion: 2,
		Sets: []FixtureSet{{
			ID:                 "unclassed",
			WriterEra:          "RAR 7.23",
			GeneratorToolchain: "rarlab-7.23",
			ArchiveFormat:      RAR5,
			Compressions:       []Compression{Store},
			Solid:              []bool{false},
			Encryptions:        []Encryption{HeaderEncryption},
			Payloads:           []PayloadKind{IncompressiblePayload},
			FileCount:          1,
			VolumeSize:         "32m",
		}},
	}
	if _, err := matrix.Expand(); err == nil || !strings.Contains(err.Error(), "class") {
		t.Fatalf("matrix without a class expanded: %v", err)
	}
	matrix.Sets[0].Class = FixtureClass("popular")
	if _, err := matrix.Expand(); err == nil {
		t.Fatal("matrix with an unknown class expanded")
	}
	matrix.Sets[0].Class = HeadlineFixtureClass
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 1 || cases[0].Class != HeadlineFixtureClass {
		t.Fatalf("expanded case did not carry the set's class: %+v", cases)
	}
}

func TestCheckedInMatrixClassesTheHeadlineAsStoredRARWithPAR2(t *testing.T) {
	matrix, err := LoadMatrix("../../fixtures/matrix.json")
	if err != nil {
		t.Fatal(err)
	}
	cases, err := matrix.Expand()
	if err != nil {
		t.Fatal(err)
	}
	headline := 0
	for _, c := range cases {
		if c.Class != HeadlineFixtureClass {
			continue
		}
		headline++
		if c.Compression != Store || c.ArchiveFormat == SevenZip || c.Payload != IncompressiblePayload || c.FileCount != 1 {
			t.Fatalf("headline fixture %s is not a stored, single-movie RAR of incompressible media: %+v", c.ID, c)
		}
		switch c.RepairProfile {
		case CleanRepairProfile, PAR2LightRepairProfile, PAR2HeavyWithheldProfile:
		default:
			t.Fatalf("headline fixture %s carries repair profile %q; only clean and PAR2 profiles are the common case", c.ID, c.RepairProfile)
		}
	}
	if headline == 0 {
		t.Fatal("checked-in matrix declares no headline fixtures")
	}
	// Every RARLAB writer era must have a clean headline lane in each posting
	// form: most real posts are not encrypted, and the encrypted forms must
	// still be measured beside the clear one rather than instead of it.
	for _, era := range []string{"rarlab-3.93", "rarlab-4.20", "rarlab-5.00", "rarlab-6.24", "rarlab-7.23"} {
		for _, encryption := range []Encryption{NoEncryption, HeaderEncryption, DataEncryption} {
			found := false
			for _, c := range cases {
				if c.Class == HeadlineFixtureClass && c.GeneratorToolchain == era && c.RepairProfile == CleanRepairProfile && c.Encryption == encryption {
					found = true
				}
			}
			if !found {
				t.Fatalf("no clean headline fixture for writer %s with %s encryption", era, encryption)
			}
		}
	}
	// PAR2 repair is part of the common case, not a compatibility extra: the
	// stored RAR from the 4.20, 5.00 and 7.23 writers must carry a headline
	// lane for light damage and for a withheld volume, in each of the three
	// postings.
	for _, era := range []string{"rarlab-4.20", "rarlab-5.00", "rarlab-7.23"} {
		for _, profile := range []RepairProfile{PAR2LightRepairProfile, PAR2HeavyWithheldProfile} {
			for _, encryption := range []Encryption{NoEncryption, HeaderEncryption, DataEncryption} {
				found := false
				for _, c := range cases {
					if c.Class == HeadlineFixtureClass && c.GeneratorToolchain == era && c.RepairProfile == profile && c.Encryption == encryption {
						found = true
					}
				}
				if !found {
					t.Fatalf("no headline %s fixture for writer %s with %s encryption", profile, era, encryption)
				}
			}
		}
	}
}
