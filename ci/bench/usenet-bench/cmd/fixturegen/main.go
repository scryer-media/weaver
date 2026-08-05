// fixturegen creates the generated RAR corpus. It deliberately contains no
// network download of fixture data: Docker obtains only the pinned RARLAB tool
// archive while the deterministic payload is made locally.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/generator"
)

type repeatedFlag []string

func (values *repeatedFlag) String() string { return strings.Join(*values, ",") }

func (values *repeatedFlag) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("fixture id cannot be empty")
	}
	*values = append(*values, value)
	return nil
}

func main() {
	var config generator.Config
	var fixtureIDs repeatedFlag
	var bytesPerFile, multiVolumeBytesPerFile, bluRayLargeFile, bluRaySmallFile, directMKVBytes string
	var list, directMKV bool

	flag.StringVar(&config.MatrixPath, "matrix", "fixtures/matrix.json", "fixture matrix JSON path")
	flag.StringVar(&config.ToolchainsPath, "toolchains", "docker/rarlab/toolchains.json", "pinned RARLAB toolchain JSON path")
	flag.StringVar(&config.DockerfilePath, "dockerfile", "docker/rarlab/Dockerfile", "RARLAB image Dockerfile path")
	flag.StringVar(&config.PAR2ToolchainPath, "par2-toolchain", "docker/par2/toolchain.json", "source-locked PAR2 generator JSON path")
	flag.StringVar(&config.PAR2DockerfilePath, "par2-dockerfile", "docker/par2/Dockerfile", "PAR2 generator image Dockerfile path")
	flag.StringVar(&config.OutputDir, "output", "generated", "directory for generated fixtures (never overwritten)")
	flag.StringVar(&config.DockerBinary, "docker", "docker", "Docker executable")
	flag.StringVar(&bytesPerFile, "bytes-per-file", "150MiB", "target size for each ordinary movie file")
	flag.StringVar(&multiVolumeBytesPerFile, "multi-volume-bytes-per-file", "48MiB", "target size for each movie in the multi-input fixture")
	flag.StringVar(&bluRayLargeFile, "bluray-large-file-bytes", "5GiB", "large media-stream size for bluray-disc fixtures")
	flag.StringVar(&bluRaySmallFile, "bluray-small-file-bytes", "128KiB", "small metadata-file size for bluray-disc fixtures")
	flag.IntVar(&config.BluRaySmallFileCount, "bluray-small-file-count", 512, "small metadata files for bluray-disc fixtures")
	flag.BoolVar(&directMKV, "direct-mkv", false, "generate only the direct 200MiB MKV fixture without Docker")
	flag.StringVar(&directMKVBytes, "direct-mkv-bytes", "200MiB", "payload size for --direct-mkv")
	flag.Var(&fixtureIDs, "fixture", "one expanded fixture id to generate (repeatable; defaults to all)")
	flag.BoolVar(&config.BuildImages, "build-images", true, "build source-locked RARLAB and selected PAR2 images before generation")
	flag.BoolVar(&list, "list", false, "print expanded fixture cases and exit")
	flag.Parse()

	if directMKV {
		size, err := parseBytes(directMKVBytes)
		if err != nil {
			fatal(fmt.Errorf("parse --direct-mkv-bytes: %w", err))
		}
		manifest, err := generator.GenerateDirectMKV(context.Background(), config, size)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("generated %s (%d direct bytes)\n", manifest.Case.ID, manifest.ArchiveFiles[0].Size)
		return
	}

	matrix, err := fixture.LoadMatrix(config.MatrixPath)
	if err != nil {
		fatal(err)
	}
	if list {
		cases, err := matrix.Expand()
		if err != nil {
			fatal(err)
		}
		contents, err := json.MarshalIndent(cases, "", "  ")
		if err != nil {
			fatal(err)
		}
		fmt.Println(string(contents))
		return
	}
	config.BytesPerFile, err = parseBytes(bytesPerFile)
	if err != nil {
		fatal(fmt.Errorf("parse --bytes-per-file: %w", err))
	}
	config.MultiVolumeBytesPerFile, err = parseBytes(multiVolumeBytesPerFile)
	if err != nil {
		fatal(fmt.Errorf("parse --multi-volume-bytes-per-file: %w", err))
	}
	config.BluRayLargeFileBytes, err = parseBytes(bluRayLargeFile)
	if err != nil {
		fatal(fmt.Errorf("parse --bluray-large-file-bytes: %w", err))
	}
	config.BluRaySmallFileBytes, err = parseBytes(bluRaySmallFile)
	if err != nil {
		fatal(fmt.Errorf("parse --bluray-small-file-bytes: %w", err))
	}
	if len(fixtureIDs) > 0 {
		config.CaseIDs = make(map[string]bool, len(fixtureIDs))
		for _, id := range fixtureIDs {
			config.CaseIDs[id] = true
		}
	}

	manifests, err := generator.Generate(context.Background(), config)
	if err != nil {
		fatal(err)
	}
	for _, manifest := range manifests {
		fmt.Printf("generated %s (%d archive volumes, %d expected files)\n", manifest.Case.ID, len(manifest.ArchiveFiles), len(manifest.ExpectedFiles))
	}
}

func parseBytes(value string) (int64, error) {
	normalized := strings.TrimSpace(strings.ToLower(value))
	multiplier := int64(1)
	for _, unit := range []struct {
		suffix string
		factor int64
	}{
		{"gib", 1 << 30},
		{"mib", 1 << 20},
		{"kib", 1 << 10},
		{"gb", 1 << 30},
		{"mb", 1 << 20},
		{"kb", 1 << 10},
		{"g", 1 << 30},
		{"m", 1 << 20},
		{"k", 1 << 10},
		{"b", 1},
	} {
		suffix, factor := unit.suffix, unit.factor
		if strings.HasSuffix(normalized, suffix) {
			normalized = strings.TrimSuffix(normalized, suffix)
			multiplier = factor
			break
		}
	}
	if normalized == "" {
		return 0, fmt.Errorf("missing byte count")
	}
	parsed, err := strconv.ParseInt(normalized, 10, 64)
	if err != nil || parsed <= 0 || parsed > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("invalid size %q", value)
	}
	return parsed * multiplier, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "fixturegen:", err)
	os.Exit(1)
}
