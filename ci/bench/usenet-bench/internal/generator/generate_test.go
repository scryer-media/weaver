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

func TestBluRayDiscUsesBluRayShapedMemberMix(t *testing.T) {
	tests := map[int]string{
		1:   "BDMV/STREAM/00001.m2ts",
		5:   "BDMV/PLAYLIST/00000.mpls",
		165: "BDMV/CLIPINF/00000.clpi",
		325: "BDMV/BDJO/00000.bdjo",
		389: "BDMV/META/DL/Composite000_BT2020_HDR.png",
		453: "BDMV/META/DL/metadata-000.xml",
		485: "BDMV/index.bdmv",
		487: "BDMV/JAR/00000.jar",
		489: "CERTIFICATE/id.bdmv",
		512: "BDMV/META/DL/locale-020.txt",
	}
	for index, want := range tests {
		if got := bluRaySmallPath(index); got != want {
			t.Errorf("small path %d = %q, want %q", index, got, want)
		}
	}

	seen := make(map[string]bool, defaultBluRaySmallFileCount)
	for index := 1; index <= defaultBluRaySmallFileCount; index++ {
		path := bluRaySmallPath(index)
		if seen[path] {
			t.Fatalf("duplicate Blu-ray path %q", path)
		}
		seen[path] = true
	}
	if !isTransportStream(bluRaySmallPath(1)) || !isTransportStream("BDMV/STREAM/00000.m2ts") {
		t.Fatal("Blu-ray stream members must be transport streams")
	}
	if isTransportStream(bluRaySmallPath(5)) {
		t.Fatal("Blu-ray metadata members must not be modeled as transport streams")
	}
}

func TestBluRayArchiveInputRootsPreserveDiscDirectories(t *testing.T) {
	got := bluRayArchiveInputRoots()
	want := []string{"input/BDMV", "input/CERTIFICATE"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("Blu-ray archive roots = %#v, want %#v", got, want)
	}
}

func TestBluRayMetadataSizesStaySmall(t *testing.T) {
	limit := int64(128 << 10)
	for path, want := range map[string]int64{
		"BDMV/PLAYLIST/00000.mpls":                 8 << 10,
		"BDMV/CLIPINF/00000.clpi":                  16 << 10,
		"BDMV/BDJO/00000.bdjo":                     24 << 10,
		"BDMV/META/DL/Composite000_BT2020_HDR.png": 48 << 10,
		"BDMV/JAR/00000.jar":                       64 << 10,
		"BDMV/AUXDATA/00000.otf":                   96 << 10,
	} {
		if got := bluRayMetadataBytes(path, limit); got != want {
			t.Errorf("metadata size for %q = %d, want %d", path, got, want)
		}
	}
	if got := bluRayMetadataBytes("BDMV/AUXDATA/00000.otf", 32<<10); got != 32<<10 {
		t.Fatalf("metadata size must honor configured cap, got %d", got)
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

func TestBluRayTransportStreamCarriesHighEntropyVideo(t *testing.T) {
	command := strings.Join(ffmpegRenderArgs(fixture.IncompressiblePayload, 5<<30, 1, "/work/BDMV/STREAM/00000.m2ts"), " ")
	for _, expected := range []string{"noise=", "-c:v libx264", "nal-hrd=cbr:force-cfr=1", "-f mpegts"} {
		if !strings.Contains(command, expected) {
			t.Fatalf("Blu-ray transport command lacks %q: %s", expected, command)
		}
	}
	if strings.Contains(command, "mpeg2video") || strings.Contains(command, "-muxrate") {
		t.Fatalf("Blu-ray transport command must not use compressible MPEG-TS padding: %s", command)
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

func TestGenerationWorkersDefaultAndValidate(t *testing.T) {
	if got, want := (Config{}).withDefaults().Workers, defaultGenerationWorkers; got != want {
		t.Fatalf("default workers = %d, want %d", got, want)
	}
	config := Config{
		OutputDir:               t.TempDir(),
		BytesPerFile:            1,
		MultiVolumeBytesPerFile: 1,
		BluRayLargeFileBytes:    1,
		BluRaySmallFileBytes:    1,
		BluRaySmallFileCount:    1,
		Workers:                 -1,
	}
	if err := config.Validate(); err == nil {
		t.Fatal("negative worker count must fail validation")
	}
}

func TestToolchainRequiresHTTPSAndHash(t *testing.T) {
	toolchain := Toolchain{ID: "x", Image: "x", Platform: "linux/amd64", URL: "http://example.test/rar.tar.gz", SHA256: "abcd"}
	if err := toolchain.Validate(); err == nil {
		t.Fatal("expected invalid URL/hash to fail validation")
	}
}
