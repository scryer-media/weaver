package generator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

const (
	// DirectMKVFixtureID is deliberately not an archive: it exercises each
	// client's direct-download path and is the queue-transition workload.
	DirectMKVFixtureID = "direct-mkv-200mb"
	directMKVPath      = "archive/direct-200mb.mkv"
)

// GenerateDirectMKV creates one valid H.264/AAC Matroska direct-download
// fixture with the pinned FFmpeg embedded in the RARLAB generator image.
func GenerateDirectMKV(ctx context.Context, config Config, size int64) (fixture.GeneratedManifest, error) {
	if size <= 0 {
		return fixture.GeneratedManifest{}, fmt.Errorf("direct MKV size must be positive, got %d", size)
	}
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return fixture.GeneratedManifest{}, err
	}
	lock, err := LoadToolchainLock(config.ToolchainsPath)
	if err != nil {
		return fixture.GeneratedManifest{}, err
	}
	toolchain, ok := lock.Find("rarlab-7.23")
	if !ok {
		return fixture.GeneratedManifest{}, fmt.Errorf("direct MKV generator requires the rarlab-7.23 FFmpeg image")
	}
	if config.BuildImages {
		if err := buildImage(ctx, config, toolchain); err != nil {
			return fixture.GeneratedManifest{}, err
		}
	}
	if err := os.MkdirAll(config.OutputDir, 0o755); err != nil {
		return fixture.GeneratedManifest{}, fmt.Errorf("create direct MKV output root: %w", err)
	}
	caseDir := filepath.Join(config.OutputDir, DirectMKVFixtureID)
	if err := os.Mkdir(caseDir, 0o755); err != nil {
		if os.IsExist(err) {
			return fixture.GeneratedManifest{}, fmt.Errorf("fixture directory %s already exists", caseDir)
		}
		return fixture.GeneratedManifest{}, fmt.Errorf("create direct MKV fixture directory: %w", err)
	}
	digest, err := renderVideo(ctx, config, toolchain, caseDir, directMKVPath, fixture.IncompressiblePayload, size, 20_000)
	if err != nil {
		return fixture.GeneratedManifest{}, err
	}
	file := digest
	manifest := fixture.GeneratedManifest{
		SchemaVersion: 5,
		Case: fixture.ArchiveCase{
			ID:                 DirectMKVFixtureID,
			SetID:              "direct-media",
			WriterEra:          "not-applicable",
			GeneratorToolchain: toolchain.ID,
			Payload:            fixture.IncompressiblePayload,
			PayloadLayout:      fixture.UniformPayloadLayout,
			RepairProfile:      fixture.CleanRepairProfile,
		},
		Toolchain:          toolchain.ManifestID(),
		PayloadRecipe:      fixture.PayloadRecipe{Layout: fixture.UniformPayloadLayout, LargeFileBytes: size},
		ExpectedFiles:      []fixture.FileDigest{{Path: filepath.Base(directMKVPath), Size: file.Size, BLAKE3: file.BLAKE3}},
		SourceArchiveFiles: []fixture.FileDigest{file},
		ArchiveFiles:       []fixture.FileDigest{file},
		Repair:             fixture.RepairDetails{Profile: fixture.CleanRepairProfile},
	}
	if err := writeManifest(filepath.Join(caseDir, "fixture-manifest.json"), manifest); err != nil {
		return fixture.GeneratedManifest{}, err
	}
	return manifest, nil
}
