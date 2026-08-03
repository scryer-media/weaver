package generator

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

func TestPayloadIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	first, err := writePayload(filepath.Join(dir, "first.bin"), fixture.IncompressiblePayload, 4097, 7)
	if err != nil {
		t.Fatal(err)
	}
	second, err := writePayload(filepath.Join(dir, "second.bin"), fixture.IncompressiblePayload, 4097, 7)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("same deterministic payload hashes differ: %s != %s", first, second)
	}
	firstBytes, err := os.ReadFile(filepath.Join(dir, "first.bin"))
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(filepath.Join(dir, "second.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("same deterministic payload bytes differ")
	}
}

func TestModeratelyCompressiblePayloadContainsRepeatedBlocks(t *testing.T) {
	dir := t.TempDir()
	_, err := writePayload(filepath.Join(dir, "payload.bin"), fixture.CompressiblePayload, 64<<10, 2)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(dir, "payload.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents[:32<<10], contents[32<<10:]) {
		t.Fatal("moderately compressible payload should repeat each 32 KiB source block")
	}
}

func TestBluRayDiscPayloadHasOneLargeStreamAndManySmallFiles(t *testing.T) {
	dir := t.TempDir()
	archiveCase := fixture.ArchiveCase{
		ID:            "bluray",
		Payload:       fixture.IncompressiblePayload,
		PayloadLayout: fixture.BluRayDiscPayloadLayout,
	}
	config := Config{BluRayLargeFileBytes: 4096, BluRaySmallFileBytes: 32, BluRaySmallFileCount: 12}
	digests, inputs, recipe, err := writePayloadFiles(dir, archiveCase, config)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(digests), 13; got != want {
		t.Fatalf("digests = %d, want %d", got, want)
	}
	if got, want := len(inputs), 13; got != want {
		t.Fatalf("inputs = %d, want %d", got, want)
	}
	if recipe.Layout != fixture.BluRayDiscPayloadLayout || recipe.LargeFileBytes != 4096 || recipe.SmallFileCount != 12 || recipe.SmallFileBytes != 32 {
		t.Fatalf("recipe = %#v", recipe)
	}
	large := digests[len(digests)-1]
	if large.Path != "BDMV/STREAM/00000.m2ts" || large.Size != 4096 {
		t.Fatalf("large file = %#v", large)
	}
	for _, digest := range digests[:len(digests)-1] {
		if digest.Size != 32 || !strings.Contains(digest.Path, "BDMV/") && !strings.Contains(digest.Path, "CERTIFICATE/") {
			t.Fatalf("unexpected small disc file %#v", digest)
		}
	}
}

func TestToolchainRequiresHTTPSAndHash(t *testing.T) {
	toolchain := Toolchain{ID: "x", Image: "x", Platform: "linux/amd64", URL: "http://example.test/rar.tar.gz", SHA256: "abcd"}
	if err := toolchain.Validate(); err == nil {
		t.Fatal("expected invalid URL/hash to fail validation")
	}
}
