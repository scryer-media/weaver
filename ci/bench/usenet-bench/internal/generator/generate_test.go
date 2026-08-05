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
	_, err := writePayload(filepath.Join(dir, "payload.bin"), fixture.CompressiblePayload, 160<<10, 2)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(dir, "payload.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents[:32<<10], contents[128<<10:]) {
		t.Fatal("moderately compressible payload should repeat one 32 KiB block per 160 KiB window")
	}
}

func TestBluRayDiscUsesVideoStreamPaths(t *testing.T) {
	if got, want := bluRaySmallPath(1), "BDMV/STREAM/10001.m2ts"; got != want {
		t.Fatalf("small path = %q, want %q", got, want)
	}
	if !isTransportStream(bluRaySmallPath(12)) || !isTransportStream("BDMV/STREAM/00000.m2ts") {
		t.Fatal("Blu-ray payload paths must be transport streams")
	}
}

func TestVideoFormatsMatchPayloadKinds(t *testing.T) {
	if got := videoExtension(fixture.IncompressiblePayload); got != ".mkv" {
		t.Fatalf("incompressible extension = %q", got)
	}
	if got := videoExtension(fixture.CompressiblePayload); got != ".avi" {
		t.Fatalf("compressible extension = %q", got)
	}
	incompressible := strings.Join(ffmpegRenderArgs(fixture.IncompressiblePayload, 4096, 1, "/work/payload.mkv"), " ")
	if !strings.Contains(incompressible, "libx264") || !strings.Contains(incompressible, "-t 0.001628 /work/payload.mkv") {
		t.Fatalf("unexpected H.264 command: %s", incompressible)
	}
	compressed := strings.Join(ffmpegRenderArgs(fixture.CompressiblePayload, 4096, 1, "/work/payload.avi"), " ")
	if !strings.Contains(compressed, "rawvideo") || !strings.Contains(compressed, "-f avi") {
		t.Fatalf("unexpected AVI command: %s", compressed)
	}
}

func TestMediaDuration(t *testing.T) {
	if got, want := mediaDuration(2_516_000, 20_128_000), "1.000000"; got != want {
		t.Fatalf("duration = %q, want %q", got, want)
	}
}

func TestUniformMovieSizeUsesMultiInputOverride(t *testing.T) {
	config := Config{BytesPerFile: 150 << 20, MultiVolumeBytesPerFile: 48 << 20}
	if got, want := uniformMovieBytes(fixture.ArchiveCase{FileCount: 1}, config), int64(150<<20); got != want {
		t.Fatalf("ordinary movie bytes = %d, want %d", got, want)
	}
	if got, want := uniformMovieBytes(fixture.ArchiveCase{FileCount: 4}, config), int64(48<<20); got != want {
		t.Fatalf("multi-input movie bytes = %d, want %d", got, want)
	}
}

func TestOnlyMultiInputFixtureRequiresMultipleVolumes(t *testing.T) {
	if requiresMultiVolumeArchive(fixture.ArchiveCase{FileCount: 1}) {
		t.Fatal("single-movie fixture must allow a single RAR volume")
	}
	if !requiresMultiVolumeArchive(fixture.ArchiveCase{FileCount: 4}) {
		t.Fatal("multi-input fixture must require multiple RAR volumes")
	}
}

func TestToolchainRequiresHTTPSAndHash(t *testing.T) {
	toolchain := Toolchain{ID: "x", Image: "x", Platform: "linux/amd64", URL: "http://example.test/rar.tar.gz", SHA256: "abcd"}
	if err := toolchain.Validate(); err == nil {
		t.Fatal("expected invalid URL/hash to fail validation")
	}
}
