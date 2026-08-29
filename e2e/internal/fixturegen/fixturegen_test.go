package fixturegen

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// repoRoot is the harness root as seen from this package.
const repoRoot = "../.."

func TestPayloadIsDeterministicAndSeedDependent(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "a.bin")
	second := filepath.Join(dir, "b.bin")
	third := filepath.Join(dir, "c.bin")
	for _, spec := range []struct {
		path, seed string
		size       int64
	}{
		{first, "silver-horizon", 100_000},
		{second, "silver-horizon", 100_000},
		{third, "amber-trail", 100_000},
	} {
		if err := WritePRNG(spec.path, spec.seed, spec.size); err != nil {
			t.Fatalf("WritePRNG(%s): %v", spec.seed, err)
		}
	}
	left, right, other := read(t, first), read(t, second), read(t, third)
	if !bytes.Equal(left, right) {
		t.Fatal("the same seed produced different bytes; payloads must be reproducible from the recipe alone")
	}
	if bytes.Equal(left, other) {
		t.Fatal("different seeds produced the same bytes")
	}
	if int64(len(left)) != 100_000 {
		t.Fatalf("payload is %d bytes, want 100000", len(left))
	}
	// A payload an archiver is told to store must not be compressible: check
	// that no 32-byte block repeats, which a counter-fed digest stream cannot
	// produce.
	seen := map[string]struct{}{}
	for offset := 0; offset+32 <= len(left); offset += 32 {
		key := string(left[offset : offset+32])
		if _, repeat := seen[key]; repeat {
			t.Fatalf("payload repeats a 32-byte block at offset %d", offset)
		}
		seen[key] = struct{}{}
	}
}

func TestWriteTextIsExactLength(t *testing.T) {
	dir := t.TempDir()
	for _, size := range []int{0, 1, 29, 34, 39} {
		path := filepath.Join(dir, fmt.Sprintf("t%d.txt", size))
		if err := WriteText(path, "solid run member one, e2e and then some", size); err != nil {
			t.Fatalf("WriteText(%d): %v", size, err)
		}
		contents := read(t, path)
		if len(contents) != size {
			t.Fatalf("WriteText(%d) wrote %d bytes", size, len(contents))
		}
		if size > 0 && contents[size-1] != '\n' {
			t.Fatalf("WriteText(%d) did not end in a newline", size)
		}
	}
}

func TestSplitConcatRoundTrip(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "whole.bin")
	if err := WritePRNG(source, "split", 700_000); err != nil {
		t.Fatal(err)
	}
	parts, err := SplitFile(source, 256<<10, func(index int) string {
		return filepath.Join(dir, fmt.Sprintf("whole.bin.%03d", index+1))
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatalf("split 700000 bytes at 256 KiB into %d parts, want 3", len(parts))
	}
	for index, part := range parts[:2] {
		if size, _ := FileSize(part); size != 256<<10 {
			t.Fatalf("part %d is %d bytes, want a full 256 KiB part", index+1, size)
		}
	}
	rejoined := filepath.Join(dir, "rejoined.bin")
	if err := ConcatFiles(rejoined, parts...); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read(t, source), read(t, rejoined)) {
		t.Fatal("splitting and rejoining changed the bytes")
	}
}

func TestDamageOperations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "archive.bin")
	if err := WritePRNG(path, "damage", 4<<20); err != nil {
		t.Fatal(err)
	}
	clean := read(t, path)

	if err := ZeroRange(path, 1<<20, 64<<10); err != nil {
		t.Fatal(err)
	}
	damaged := read(t, path)
	if len(damaged) != len(clean) {
		t.Fatal("zeroing a range changed the file length")
	}
	if !bytes.Equal(damaged[:1<<20], clean[:1<<20]) {
		t.Fatal("zeroing damaged bytes before the requested offset")
	}
	if !bytes.Equal(damaged[1<<20:(1<<20)+(64<<10)], make([]byte, 64<<10)) {
		t.Fatal("the requested window was not zeroed")
	}
	if !bytes.Equal(damaged[(1<<20)+(64<<10):], clean[(1<<20)+(64<<10):]) {
		t.Fatal("zeroing damaged bytes after the requested window")
	}

	if err := ZeroRange(path, int64(len(clean))-16, 64); err == nil {
		t.Fatal("zeroing past the end of the file should fail rather than extend it")
	}

	if err := TruncateBy(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	if size, _ := FileSize(path); size != 3<<20 {
		t.Fatalf("truncation left %d bytes, want %d", size, 3<<20)
	}
	if err := TruncateBy(path, 1<<30); err == nil {
		t.Fatal("truncating a file to nothing should fail")
	}

	overwrite := PatternBytes("par2-repair", 100)
	if bytes.Equal(overwrite, make([]byte, 100)) {
		t.Fatal("the overwrite pattern must not be zeros; a zeroed run is indistinguishable from sparse payload")
	}
	if !bytes.Equal(overwrite, PatternBytes("par2-repair", 100)) {
		t.Fatal("the overwrite pattern is not deterministic")
	}
}

func TestObfuscatedNamesAndSwap(t *testing.T) {
	if got := obfuscatedNames(10); !equal(got, []string{
		"51273aad56a8b904e96928935278a627.10",
		"51273aad56a8b904e96928935278a627.11",
		"51273aad56a8b904e96928935278a627.12",
	}) {
		t.Fatalf("obfuscatedNames(10) = %v", got)
	}
	if got := obfuscatedNames(100); got[2] != "51273aad56a8b904e96928935278a627.102" {
		t.Fatalf("obfuscatedNames(100) = %v", got)
	}

	work := t.TempDir()
	env := &Env{Work: work}
	if err := os.MkdirAll(filepath.Join(work, outputDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteText(env.OutputPath("part2.rar"), "two", 8); err != nil {
		t.Fatal(err)
	}
	if err := WriteText(env.OutputPath("part3.rar"), "three", 8); err != nil {
		t.Fatal(err)
	}
	if err := swapOutputs(env, "part2.rar", "part3.rar"); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(read(t, env.OutputPath("part2.rar")))); got != "three" {
		t.Fatalf("part2.rar holds %q after the swap", got)
	}
	if got := strings.TrimSpace(string(read(t, env.OutputPath("part3.rar")))); got != "two" {
		t.Fatalf("part3.rar holds %q after the swap", got)
	}
}

func TestRARArgumentsCarryTheDeclaredShape(t *testing.T) {
	cases := []struct {
		name     string
		spec     RARSpec
		contains []string
		absent   []string
	}{
		{
			name: "rar5 compressed non-solid",
			spec: RARSpec{Toolchain: "rarlab-7.23", Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Members: []string{"work/sample.mkv"}},
			contains: []string{"a", "-ma5", "-m1", "-md32m", "-s-", "-ed", "../out/archive.rar", "work/sample.mkv"},
			absent:   []string{"-ma4", "-s"},
		},
		{
			name: "rar4 stored on a 4.x writer takes no format selector",
			spec: RARSpec{Toolchain: "rarlab-4.20", Format: RAR4, Archive: "archive.rar",
				Method: "-m0", Members: []string{"work/x.mkv"}},
			contains: []string{"-m0", "-s-"},
			absent:   []string{"-ma4", "-ma5"},
		},
		{
			name: "rar4 on a modern writer selects the format",
			spec: RARSpec{Toolchain: "rarlab-6.24", Format: RAR4, Archive: "archive.rar",
				Method: "-m1", Members: []string{"x.mkv"}},
			contains: []string{"-ma4"},
			absent:   []string{"-ma5"},
		},
		{
			name: "solid encrypted multivolume",
			spec: RARSpec{Toolchain: "rarlab-7.23", Format: RAR5, Archive: "archive.rar",
				Method: "-m3", Solid: true, Password: "secret", VolumeSize: "22m", Members: []string{"m.mkv"}},
			contains: []string{"-s", "-psecret", "-v22m"},
			absent:   []string{"-s-", "-hpsecret"},
		},
		{
			name: "header encryption is a different switch",
			spec: RARSpec{Toolchain: "rarlab-7.23", Format: RAR5, Archive: "archive.rar",
				Method: "-m1", HeaderPassword: "secret", Members: []string{"m.mkv"}},
			contains: []string{"-hpsecret"},
			absent:   []string{"-psecret"},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			args := test.spec.arguments("../" + outputDir)
			for _, want := range test.contains {
				if !contains(args, want) {
					t.Errorf("arguments %v do not contain %q", args, want)
				}
			}
			for _, unwanted := range test.absent {
				if contains(args, unwanted) {
					t.Errorf("arguments %v unexpectedly contain %q", args, unwanted)
				}
			}
			if args[len(args)-len(test.spec.Members)-1] != "../"+outputDir+"/"+test.spec.Archive {
				t.Errorf("the archive name must come immediately before the members: %v", args)
			}
		})
	}
}

func TestDirectStoreShapeConstraints(t *testing.T) {
	for _, recipe := range DirectStoreRecipes() {
		if !strings.HasPrefix(recipe.Slug, "direct-store-") {
			t.Errorf("%s is not a direct-store slug", recipe.Slug)
		}
		if recipe.ExpectedOutputs == nil {
			t.Errorf("%s must pin the BLAKE3 of its extracted member", recipe.Slug)
		}
	}
	// The RAR5 writer takes -qo-; the 4.x writer predates it and exits with
	// "Unknown option".
	rar5 := directStore{format: RAR5}
	if !equal(rar5.extra(), []string{"-qo-"}) {
		t.Errorf("RAR5 direct-store sets should suppress the quick-open record, got %v", rar5.extra())
	}
	if got := (directStore{format: RAR4}).extra(); len(got) != 0 {
		t.Errorf("RAR4 direct-store sets must pass no -qo-, got %v", got)
	}
}

func TestZipCryptoMatchesTheSpecifiedCipher(t *testing.T) {
	plaintext := []byte("silver horizon zip member payload, long enough to cross the header")
	checksum := crc32.ChecksumIEEE(plaintext)
	ciphertext := zipCryptoEncrypt(plaintext, "e2e-test-password", checksum, "sample.mkv")
	if len(ciphertext) != len(plaintext)+12 {
		t.Fatalf("ZipCrypto output is %d bytes, want %d", len(ciphertext), len(plaintext)+12)
	}
	if bytes.Equal(ciphertext[12:], plaintext) {
		t.Fatal("the member was not encrypted")
	}
	decrypted := zipCryptoDecryptForTest(ciphertext, "e2e-test-password")
	if !bytes.Equal(decrypted[12:], plaintext) {
		t.Fatal("decrypting with the same password did not recover the payload")
	}
	if decrypted[11] != byte(checksum>>24) {
		t.Fatal("the encryption header's check byte must be the high byte of the member CRC")
	}
	if !bytes.Equal(ciphertext, zipCryptoEncrypt(plaintext, "e2e-test-password", checksum, "sample.mkv")) {
		t.Fatal("ZipCrypto output is not deterministic")
	}
}

// zipCryptoDecryptForTest is the inverse of the cipher under test, written out
// in full so the test does not simply re-run the implementation.
func zipCryptoDecryptForTest(ciphertext []byte, password string) []byte {
	keys := [3]uint32{0x12345678, 0x23456789, 0x34567890}
	update := func(value byte) {
		keys[0] = crcStep(keys[0], value)
		keys[1] += keys[0] & 0xff
		keys[1] = keys[1]*134775813 + 1
		keys[2] = crcStep(keys[2], byte(keys[1]>>24))
	}
	for index := 0; index < len(password); index++ {
		update(password[index])
	}
	plain := make([]byte, 0, len(ciphertext))
	for _, cipher := range ciphertext {
		temp := keys[2] | 2
		value := cipher ^ byte((temp*(temp^1))>>8)
		update(value)
		plain = append(plain, value)
	}
	return plain
}

func TestGoZipAndTarWritersAreReproducible(t *testing.T) {
	dir := t.TempDir()
	payload := filepath.Join(dir, "sample.mkv")
	if err := WritePRNG(payload, "container", 300_000); err != nil {
		t.Fatal(err)
	}
	members := []Member{{Name: "sample.mkv", Source: payload}}

	for index, name := range []string{"a.zip", "b.zip"} {
		if err := WriteZip(filepath.Join(dir, name), members, ""); err != nil {
			t.Fatalf("zip %d: %v", index, err)
		}
	}
	if !bytes.Equal(read(t, filepath.Join(dir, "a.zip")), read(t, filepath.Join(dir, "b.zip"))) {
		t.Fatal("the zip writer is not byte-reproducible")
	}
	archive, err := zip.OpenReader(filepath.Join(dir, "a.zip"))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if len(archive.File) != 1 || archive.File[0].Name != "sample.mkv" || archive.File[0].Method != zip.Store {
		t.Fatalf("unexpected zip shape: %+v", archive.File)
	}

	for index, name := range []string{"a.tar", "b.tar"} {
		if err := WriteTar(filepath.Join(dir, name), []string{"./"}, members); err != nil {
			t.Fatalf("tar %d: %v", index, err)
		}
	}
	if !bytes.Equal(read(t, filepath.Join(dir, "a.tar")), read(t, filepath.Join(dir, "b.tar"))) {
		t.Fatal("the tar writer is not byte-reproducible")
	}
	size, _ := FileSize(filepath.Join(dir, "a.tar"))
	if size%tarBlockFactor != 0 {
		t.Fatalf("tar is %d bytes, which is not a whole number of 10 KiB blocks", size)
	}
}

func TestPAR2SliceSizeReadsTheMainPacket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "archive.par2")
	if err := os.WriteFile(path, syntheticPAR2Index(418384), 0o644); err != nil {
		t.Fatal(err)
	}
	slice, err := PAR2SliceSize(path)
	if err != nil {
		t.Fatal(err)
	}
	if slice != 418384 {
		t.Fatalf("slice size %d, want 418384", slice)
	}
	if err := os.WriteFile(path, []byte("not a par2 file at all"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := PAR2SliceSize(path); err == nil {
		t.Fatal("a file with no main packet should be an error, not a zero slice size")
	}
}

// syntheticPAR2Index builds a creator packet followed by a main packet, so the
// parser has to skip a packet to find the one it wants.
func syntheticPAR2Index(slice uint64) []byte {
	packet := func(kind string, body []byte) []byte {
		out := make([]byte, 0, 64+len(body))
		out = append(out, par2Magic...)
		length := make([]byte, 8)
		binary.LittleEndian.PutUint64(length, uint64(64+len(body)))
		out = append(out, length...)
		out = append(out, make([]byte, 32)...) // packet and set hashes
		out = append(out, []byte(kind)...)
		return append(out, body...)
	}
	main := make([]byte, 12)
	binary.LittleEndian.PutUint64(main, slice)
	binary.LittleEndian.PutUint32(main[8:], 1)
	return append(packet("PAR 2.0\x00Creator\x00", []byte("fixturegen-test\x00")),
		packet("PAR 2.0\x00Main\x00\x00\x00\x00", main)...)
}

func TestScenarioDigestRewriteTouchesNothingElse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scenario.json")
	original := `{
  "slug": "direct-store-single",
  "title": "Silver.Horizon.S01E01.2026.1080p.WEB-DL.H264-TESTGRP",
  "description": "Single-volume RAR5 store-method set.",
  "category": "2000",
  "expected_outcome": "success",
  "expectedOutputBLAKE3": {
    "silver.horizon.s01e01.mkv": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "password": "weaver-e2e-direct-password"
}
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("b", 64)
	if err := RewriteScenarioDigests(path, map[string]string{"silver.horizon.s01e01.mkv": digest}); err != nil {
		t.Fatal(err)
	}
	updated := string(read(t, path))
	if !strings.Contains(updated, digest) {
		t.Fatal("the new digest was not written")
	}
	if strings.Contains(updated, strings.Repeat("a", 64)) {
		t.Fatal("the old digest survived")
	}
	for _, line := range []string{
		`  "slug": "direct-store-single",`,
		`  "title": "Silver.Horizon.S01E01.2026.1080p.WEB-DL.H264-TESTGRP",`,
		`  "description": "Single-volume RAR5 store-method set.",`,
		`  "category": "2000",`,
		`  "expected_outcome": "success",`,
		`  "password": "weaver-e2e-direct-password"`,
	} {
		if !strings.Contains(updated, line) {
			t.Fatalf("rewriting the digests disturbed %q", line)
		}
	}
	if !strings.HasSuffix(updated, "}\n") {
		t.Fatal("the trailing newline was lost")
	}
	if strings.Count(updated, "\n") != strings.Count(original, "\n") {
		t.Fatal("the rewrite changed the line count")
	}

	if err := os.WriteFile(path, []byte("{\n  \"slug\": \"x\"\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RewriteScenarioDigests(path, map[string]string{"a": digest}); err == nil {
		t.Fatal("rewriting a scenario with no digest block should fail loudly")
	}
}

func TestEveryScenarioDirectoryHasARecipeOrIsDeclaredScenarioOnly(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join(repoRoot, "testdata"))
	if err != nil {
		t.Fatal(err)
	}
	recipes := map[string]Recipe{}
	for _, recipe := range Recipes() {
		if _, duplicate := recipes[recipe.Slug]; duplicate {
			t.Fatalf("%s has two recipes", recipe.Slug)
		}
		recipes[recipe.Slug] = recipe
	}
	var uncovered []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		slug := entry.Name()
		_, hasRecipe := recipes[slug]
		reason, scenarioOnly := ScenarioOnly[slug]
		switch {
		case hasRecipe && scenarioOnly:
			t.Errorf("%s is both a recipe and declared scenario-only", slug)
		case !hasRecipe && !scenarioOnly:
			uncovered = append(uncovered, slug)
		case scenarioOnly && strings.TrimSpace(reason) == "":
			t.Errorf("%s is declared scenario-only with no reason", slug)
		}
		delete(recipes, slug)
	}
	if len(uncovered) > 0 {
		sort.Strings(uncovered)
		t.Errorf("no recipe and no scenario-only declaration for: %s", strings.Join(uncovered, ", "))
	}
	for slug := range recipes {
		t.Errorf("recipe %s has no testdata/%s directory", slug, slug)
	}
}

func TestEveryLedgeredFixtureIsOwnedByARecipe(t *testing.T) {
	ledger, _, err := corpus.LoadLedger(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	owned := map[string]struct{}{}
	for _, recipe := range Recipes() {
		owned[recipe.Slug] = struct{}{}
	}
	inputs := map[string]struct{}{}
	for _, recipe := range Recipes() {
		for _, input := range recipe.Inputs {
			inputs[input] = struct{}{}
		}
	}
	ledgered := map[string]struct{}{}
	for _, file := range ledger.Files {
		ledgered[file.Path] = struct{}{}
		slug := strings.SplitN(strings.TrimPrefix(file.Path, "testdata/"), "/", 2)[0]
		if _, ok := owned[slug]; !ok {
			t.Errorf("%s is ledgered but no recipe produces it", file.Path)
		}
	}
	for input := range inputs {
		if _, ok := ledgered[input]; !ok {
			t.Errorf("a recipe declares input %q, which is not a ledger path", input)
		}
	}
	for slug := range ScenarioOnly {
		for path := range ledgered {
			if strings.HasPrefix(path, "testdata/"+slug+"/") {
				t.Errorf("%s is declared scenario-only but the ledger lists %s", slug, path)
			}
		}
	}
}

// The ledger's `inputs` are written from the owning recipe's Inputs, so a
// recipe whose declaration is corrected without the ledger being rewritten
// leaves the published provenance naming bytes the fixture was never built
// from. Nothing else notices: the digests still match, and the stale path is
// still a valid ledger path.
func TestLedgerInputsMatchTheOwningRecipe(t *testing.T) {
	ledger, _, err := corpus.LoadLedger(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string][]string{}
	for _, recipe := range Recipes() {
		declared[recipe.Slug] = recipe.Inputs
	}
	for _, file := range ledger.Files {
		slug := strings.SplitN(strings.TrimPrefix(file.Path, "testdata/"), "/", 2)[0]
		want, ok := declared[slug]
		if !ok {
			continue
		}
		if strings.Join(file.Source.Inputs, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s records inputs %v, but recipe %s declares %v",
				file.Path, file.Source.Inputs, slug, want)
		}
	}
}

func TestRecipesAreWellFormed(t *testing.T) {
	for _, recipe := range Recipes() {
		if strings.TrimSpace(recipe.Family) == "" {
			t.Errorf("%s declares no family", recipe.Slug)
		}
		if len(strings.TrimSpace(recipe.Notes)) < 20 {
			t.Errorf("%s needs notes a reviewer can use", recipe.Slug)
		}
		if recipe.Build == nil {
			t.Errorf("%s has no build step", recipe.Slug)
		}
		for _, input := range recipe.Inputs {
			if !strings.HasPrefix(input, "testdata/") {
				t.Errorf("%s declares input %q, which is not a root-relative ledger path", recipe.Slug, input)
			}
		}
	}
}

func TestArtifactTableIsClosedOverItsToolchains(t *testing.T) {
	lock, err := LoadLock(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	pinned := map[string]struct{}{}
	for _, id := range lock.IDs() {
		pinned[id] = struct{}{}
	}
	for name, artifact := range Artifacts() {
		if artifact.Name != name {
			t.Errorf("artifact %q is keyed as %q", artifact.Name, name)
		}
		if len(artifact.Files) == 0 {
			t.Errorf("artifact %s declares no files", name)
		}
		if artifact.Build == nil {
			t.Errorf("artifact %s has no build step", name)
		}
		for _, id := range artifact.Toolchains {
			if _, ok := pinned[id]; !ok {
				t.Errorf("artifact %s names unpinned toolchain %q", name, id)
			}
		}
	}
	for _, id := range []string{RAR5Writer, RAR4Writer, DirectStoreRAR5Writer, DirectStoreRAR4Writer,
		SevenZipToolchain, PAR2Toolchain, VideoToolchain} {
		if _, ok := pinned[id]; !ok {
			t.Errorf("the generator names %q, which the lock does not pin", id)
		}
	}
}

func TestGoWriterAttributionFollowsTheExtension(t *testing.T) {
	lock, err := LoadLock(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]string{
		"archive.zip":            {"go-fixture-bytes", "go-archive-zip"},
		"archive.tar":            {"go-fixture-bytes", "go-archive-tar"},
		"archive.tar.gz":         {"go-fixture-bytes", "go-archive-tar", "go-compress-gzip"},
		"archive.tbz2":           {"go-fixture-bytes", "go-archive-tar", "go-dsnet-bzip2@v0.0.1"},
		"test-media.mkv.zst":     {"go-fixture-bytes", "go-klauspost-zstd@v1.19.2"},
		"test-media.mkv.br":      {"go-fixture-bytes", "go-andybalholm-brotli@v1.2.2"},
		"test-media.mkv.deflate": {"go-fixture-bytes", "go-compress-flate"},
		"archive.rar":            {"go-fixture-bytes"},
	}
	for file, want := range cases {
		got := goWriterIDs(lock, []string{file})
		if !equal(got, want) {
			t.Errorf("goWriterIDs(%q) = %v, want %v", file, got, want)
		}
	}
}

func TestLockRejectsAnUnpinnedOracle(t *testing.T) {
	valid := Toolchain{ID: "x", Image: "img:1", Platform: "linux/amd64",
		URL: "https://example.invalid/x.tar.gz", SHA256: hex.EncodeToString(make([]byte, 32)),
		Binary: "x", Dockerfile: "internal/fixturegen/docker/rarlab/Dockerfile"}
	if err := valid.validate(); err != nil {
		t.Fatalf("a fully pinned toolchain should validate: %v", err)
	}
	for name, mutate := range map[string]func(*Toolchain){
		"no digest":       func(t *Toolchain) { t.SHA256 = "" },
		"short digest":    func(t *Toolchain) { t.SHA256 = "abc" },
		"plaintext url":   func(t *Toolchain) { t.URL = "http://example.invalid/x.tar.gz" },
		"no dockerfile":   func(t *Toolchain) { t.Dockerfile = "" },
		"no image":        func(t *Toolchain) { t.Image = "" },
		"no platform":     func(t *Toolchain) { t.Platform = "" },
		"url but no repo": func(t *Toolchain) { t.URL = "not a url at all" },
	} {
		candidate := valid
		mutate(&candidate)
		if err := candidate.validate(); err == nil {
			t.Errorf("%s should not validate", name)
		}
	}
	digestPinned := Toolchain{ID: "y", Image: "repo/img@sha256:" + hex.EncodeToString(make([]byte, 32)), Platform: "linux/amd64"}
	if err := digestPinned.validate(); err != nil {
		t.Errorf("an image pinned by digest needs no source archive: %v", err)
	}
}

func TestLockLoadsAndPinsEveryOracleTheRecipesName(t *testing.T) {
	lock, err := LoadLock(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(lock.RARWriters) == 0 || len(lock.Archivers) == 0 || len(lock.GoWriters) == 0 {
		t.Fatal("the lock must pin RAR writers, archivers and Go writers")
	}
	for _, writer := range lock.RARWriters {
		if !strings.Contains(writer.URL, "rarlab.com") {
			t.Errorf("RAR writer %s is not sourced from RARLAB: %s", writer.ID, writer.URL)
		}
	}
	for _, archiver := range lock.Archivers {
		if strings.Contains(strings.ToLower(archiver.URL), "p7zip") {
			t.Errorf("archiver %s must be the official 7-Zip release, not a p7zip fork", archiver.ID)
		}
	}
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func equal(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
