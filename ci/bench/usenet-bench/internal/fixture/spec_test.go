package fixture

import (
	"strings"
	"testing"
)

func TestExpandCoversEveryAxis(t *testing.T) {
	matrix := Matrix{
		SchemaVersion: 1,
		Sets: []FixtureSet{{
			ID:                 "modern-rar5",
			WriterEra:          "RAR 5.x-7.x compatibility",
			GeneratorToolchain: "rarlab-7.23",
			RARFormat:          RAR5,
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
		ID:          "case",
		RARFormat:   RAR5,
		Compression: Normal,
		Solid:       true,
		Encryption:  HeaderEncryption,
		VolumeSize:  "32m",
	}
	args, err := c.RARArgs("archive/fixture.rar", []string{"input/one.bin", "input/two.bin"})
	if err != nil {
		t.Fatalf("RARArgs() error = %v", err)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"-ma5", "-m5", "-s", "-hp" + FixturePassword, "-v32m"} {
		if !strings.Contains(joined, want) {
			t.Errorf("RARArgs() = %q, missing %q", joined, want)
		}
	}
}

func TestRAR3ArgsUseTheLockedLegacyWriterDefault(t *testing.T) {
	c := ArchiveCase{
		ID:          "legacy-case",
		RARFormat:   RAR3,
		Compression: Store,
		Solid:       false,
		Encryption:  DataEncryption,
		VolumeSize:  "32m",
	}
	args, err := c.RARArgs("archive/fixture.rar", []string{"input/one.bin"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "-ma") {
		t.Fatalf("RAR3 must not claim a newer -ma format selector: %q", joined)
	}
	for _, want := range []string{"-m0", "-s-", "-p" + FixturePassword, "-v32m"} {
		if !strings.Contains(joined, want) {
			t.Errorf("RAR3 args = %q, missing %q", joined, want)
		}
	}
}

func TestBluRayLayoutDoesNotRequireUniformFileCount(t *testing.T) {
	matrix := Matrix{SchemaVersion: 1, Sets: []FixtureSet{{
		ID:                 "modern-rar5-bluray",
		WriterEra:          "RAR 5.x-7.x compatibility",
		GeneratorToolchain: "rarlab-7.23",
		RARFormat:          RAR5,
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
