package corpus

import (
	"bytes"
	"strings"
	"testing"
)

func manifestFixture(t *testing.T) (*Ledger, *Profiles, ToolchainLock) {
	t.Helper()
	root := writeTree(t, nil)
	ledger := newLedger(
		entry("testdata/two/b.par2", "bbb", blockedSource("generator pending: par2 set")),
		entry("testdata/one/a.rar", "aaa", generatedSource()),
		entry("testdata/shared/clip.mkv", "ccc", blockedSource("generator pending: shared clip")),
	)
	profiles := newProfiles(map[string][]string{
		"one": {"testdata/one/**", "testdata/shared/**"},
		"all": {"testdata/**"},
	})
	return ledger, profiles, loadToolchains(t, root)
}

// The manifest is a pure function of the three checked-in documents, so the
// same inputs must always encode to the same bytes — that is what makes its
// digest the corpus address.
func TestManifestEncodingIsCanonicalAndStable(t *testing.T) {
	ledger, profiles, toolchains := manifestFixture(t)
	first, err := BuildManifest(ledger, profiles, toolchains)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := first.Encode()
	if err != nil {
		t.Fatal(err)
	}
	// Re-derive from the same inputs several times: Go map iteration order is
	// randomised per run, so a non-canonical encoder shows up here.
	for attempt := 0; attempt < 20; attempt++ {
		again, err := BuildManifest(ledger, profiles, toolchains)
		if err != nil {
			t.Fatal(err)
		}
		againBytes, err := again.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(firstBytes, againBytes) {
			t.Fatalf("manifest encoding is not canonical:\n%s\n%s", firstBytes, againBytes)
		}
	}
	if !bytes.HasSuffix(firstBytes, []byte("\n")) {
		t.Fatal("canonical bytes end with a newline")
	}
	if bytes.Contains(firstBytes, []byte("\n  ")) {
		t.Fatal("canonical bytes are compact, not indented")
	}
	// Files are sorted by path regardless of ledger order.
	if !bytes.Contains(firstBytes, []byte(`"path":"testdata/one/a.rar"`)) {
		t.Fatalf("manifest does not look like compact JSON: %s", firstBytes)
	}
	paths := make([]string, 0, len(first.Files))
	for _, file := range first.Files {
		paths = append(paths, file.Path)
	}
	if strings.Join(paths, ",") != "testdata/one/a.rar,testdata/shared/clip.mkv,testdata/two/b.par2" {
		t.Fatalf("files are not sorted by path: %v", paths)
	}
	if !IsDigest(DigestBytes(firstBytes)) {
		t.Fatal("the manifest digest should be a blake3 hex")
	}
}

// A ledger edit must move the digest: that is what makes `verify` fail closed
// when someone changes the corpus without republishing.
func TestManifestDigestMovesWithTheLedger(t *testing.T) {
	ledger, profiles, toolchains := manifestFixture(t)
	before := encodeManifest(t, ledger, profiles, toolchains)
	ledger.Files[0].BLAKE3 = DigestBytes([]byte("different"))
	after := encodeManifest(t, ledger, profiles, toolchains)
	if before == after {
		t.Fatal("changing a fixture digest must change the manifest digest")
	}
}

func TestManifestDigestMovesWithTheToolchainLock(t *testing.T) {
	ledger, profiles, toolchains := manifestFixture(t)
	before := encodeManifest(t, ledger, profiles, toolchains)
	toolchains.BLAKE3 = DigestBytes([]byte("a different lock"))
	if before == encodeManifest(t, ledger, profiles, toolchains) {
		t.Fatal("a toolchain lock change is a corpus revision and must move the manifest digest")
	}
}

func TestDecodeManifestRejectsMalformedDocuments(t *testing.T) {
	ledger, profiles, toolchains := manifestFixture(t)
	manifest, err := BuildManifest(ledger, profiles, toolchains)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := manifest.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManifest(contents)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.File("testdata/one/a.rar"); !ok {
		t.Fatal("decoded manifest lost a file")
	}
	if _, ok := decoded.File("testdata/nope"); ok {
		t.Fatal("decoded manifest invented a file")
	}
	for _, broken := range []string{
		`{"schema_version":2,"digest_algorithm":"blake3","files":[]}`,
		`{"schema_version":1,"digest_algorithm":"sha256","files":[]}`,
		`{"schema_version":1,"digest_algorithm":"blake3","files":[{"path":"../x","size":1,"blake3":"` + zeroDigest + `","source_kind":"blocked"}]}`,
		`{"schema_version":1,"digest_algorithm":"blake3","files":[],"profiles":{"one":[]}}`,
		`{"schema_version":1,"digest_algorithm":"blake3","files":[],"profiles":{"one":["testdata/absent"]}}`,
	} {
		if _, err := DecodeManifest([]byte(broken)); err == nil {
			t.Errorf("expected %s to be rejected", broken)
		}
	}
}

func encodeManifest(t *testing.T, ledger *Ledger, profiles *Profiles, toolchains ToolchainLock) string {
	t.Helper()
	manifest, err := BuildManifest(ledger, profiles, toolchains)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := manifest.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return DigestBytes(contents)
}
