package fixturegen

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// Payload sizes the corpus is built from. The 1080p clip is the payload almost
// every archive family wraps; the 5 MiB preview is what the stream codecs and
// the small PAR2 sets compress; the three 25 MiB episode members are byte
// ranges of the 1080p clip, so a multi-member set costs one encode rather than
// four.
const (
	SamplePayloadFloor  int64 = 85_693_671
	PreviewPayloadBytes int64 = 5 << 20
	EpisodePayloadBytes int64 = 25 << 20
	SmallPayloadFloor   int64 = 379_393
)

// Artifacts is the shared build cache: payloads, and the archives that several
// scenarios must agree on byte for byte. PAR2 sidecars are parity over exact
// bytes, and scenarios such as par2-rar-placement-normalization stage another
// scenario's volumes, so those sets are built once here rather than once per
// scenario.
func Artifacts() map[string]Artifact {
	table := map[string]Artifact{}
	add := func(artifact Artifact) {
		table[artifact.Name] = artifact
	}

	add(Artifact{
		Name:       "clip-sample",
		Files:      []string{"sample.mkv"},
		Toolchains: []string{VideoToolchain},
		Notes:      "120 s of synthetic 1080p H.264 with an AAC tone, the payload the RAR, 7z, zip and tar families wrap.",
		Build: func(ctx context.Context, env *Env) error {
			return env.Clip(ctx, ClipSpec{
				Name: "sample.mkv", Seconds: 120, Width: 1920, Height: 1080,
				VideoCodec: "libx264", AudioCodec: "aac", ConstantBitrate: true, Noise: true,
				VideoBitrate: 5_600_000, AudioBitrate: 128_000, SampleRate: 48_000,
				Seed: 1, MinBytes: SamplePayloadFloor,
			})
		},
	})

	add(Artifact{
		Name:       "clip-preview",
		Files:      []string{"preview.mkv"},
		Toolchains: []string{VideoToolchain},
		Notes: "A noisy 1080p preview trimmed to exactly 5 MiB, so the stream-codec fixtures have a fixed-size, genuinely incompressible input. " +
			"It is deliberately variable-bitrate: a constant-bitrate encode at this scale pads with filler NAL units, and filler is zeros, which every stream codec then compresses away.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Clip(ctx, ClipSpec{
				Name: "preview.mkv", Seconds: 120, Width: 1920, Height: 1080,
				VideoCodec: "libx264", AudioCodec: "aac", Noise: true,
				VideoBitrate: 600_000, AudioBitrate: 32_000, SampleRate: 48_000,
				Seed: 2, MinBytes: PreviewPayloadBytes,
			}); err != nil {
				return err
			}
			path := env.OutputPath("preview.mkv")
			size, err := FileSize(path)
			if err != nil {
				return err
			}
			return TruncateBy(path, size-PreviewPayloadBytes)
		},
	})

	add(Artifact{
		Name:       "clip-episodes",
		Files:      []string{"episode1.mkv", "episode2.mkv", "episode3.mkv"},
		Toolchains: []string{VideoToolchain},
		Notes:      "Three 25 MiB members carved from the 1080p clip as byte ranges: distinct content, fixed sizes, one encode.",
		Build: func(ctx context.Context, env *Env) error {
			source, err := env.ArtifactPath(ctx, "clip-sample")
			if err != nil {
				return err
			}
			for index := 0; index < 3; index++ {
				name := fmt.Sprintf("episode%d.mkv", index+1)
				offset := int64(index) * EpisodePayloadBytes
				if err := SliceFile(source, env.OutputPath(name), offset, EpisodePayloadBytes); err != nil {
					return err
				}
			}
			return nil
		},
	})

	add(Artifact{
		Name:       "clip-small",
		Files:      []string{"small.mkv"},
		Toolchains: []string{VideoToolchain},
		Notes:      "A short 720p clip, the payload for the plain split fixture.",
		Build: func(ctx context.Context, env *Env) error {
			return env.Clip(ctx, ClipSpec{
				Name: "small.mkv", Seconds: 60, Width: 1280, Height: 720,
				VideoCodec: "libx264", AudioCodec: "aac", ConstantBitrate: true, Noise: true,
				VideoBitrate: 44_000, AudioBitrate: 9_000, SampleRate: 44_100,
				Seed: 3, MinBytes: SmallPayloadFloor,
			})
		},
	})

	add(Artifact{
		Name:       "rar5-work-sample",
		Files:      []string{"archive.rar"},
		Toolchains: []string{RAR5Writer, VideoToolchain},
		Notes:      "RAR5, one member at work/sample.mkv, -m1. Published verbatim by rar5-single and obfuscated-rar, and the PAR2 target of par2-repair.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "work/sample.mkv"); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Members: []string{"work/sample.mkv"},
			})
		},
	})

	add(Artifact{
		Name:       "rar5-root-sample",
		Files:      []string{"archive.rar"},
		Toolchains: []string{RAR5Writer, VideoToolchain},
		Notes:      "RAR5, one member at the archive root, -m1. The mixed-archive payload, the base the corrupted RAR5 fixture damages, and the innermost archive of the nesting chains.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "sample.mkv"); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Members: []string{"sample.mkv"},
			})
		},
	})

	add(Artifact{
		Name:       "rar5-par2-base",
		Files:      []string{"archive.rar"},
		Toolchains: []string{RAR5Writer, VideoToolchain},
		Notes:      "RAR5 at payload/sample.mkv. One base archive carries both the heavy-damage and the insufficient-parity sets, exactly as the corpus has always had it.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "payload/sample.mkv"); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Members: []string{"payload/sample.mkv"},
			})
		},
	})

	add(Artifact{
		Name:       "rar5-small",
		Files:      []string{"archive.rar"},
		Toolchains: []string{RAR5Writer, VideoToolchain},
		Notes:      "A ~5 MiB RAR5 over the preview clip: the payload of the five small-repair PAR2 scenarios and of the multiserver backup repair.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-preview", "sample-preview.mkv"); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Members: []string{"sample-preview.mkv"},
			})
		},
	})

	add(Artifact{
		Name:       "rar5-multivolume",
		Files:      []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar"},
		Toolchains: []string{RAR5Writer, VideoToolchain},
		Notes:      "Three RAR5 volumes of 30 000 KiB. Five scenarios stage these exact bytes — under their own names, under obfuscated numeric names, and beside PAR2 sidecars computed over them — so they are built once.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "work/sample.mkv"); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", VolumeSize: "30000k",
				Members: []string{"work/sample.mkv"},
			}); err != nil {
				return err
			}
			return expectParts(env, "archive.part%d.rar", 3)
		},
	})

	add(Artifact{
		Name:       "rar5-par2-multivolume",
		Files:      []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar", "archive.part4.rar"},
		Toolchains: []string{RAR5Writer, VideoToolchain},
		Notes:      "Four 21 MiB RAR5 volumes, the multi-file PAR2 recovery target. The volume size carries headroom: a four-volume set needs the archive to land between 63 and 88 MiB, and a re-encoded payload moves within that band.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "work/sample.mkv"); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", VolumeSize: "21m",
				Members: []string{"work/sample.mkv"},
			}); err != nil {
				return err
			}
			return expectParts(env, "archive.part%d.rar", 4)
		},
	})

	add(Artifact{
		Name:       "rar4-work-sample",
		Files:      []string{"archive.rar"},
		Toolchains: []string{RAR4Writer, VideoToolchain},
		Notes:      "RAR4 store at work/sample.mkv: the plain RAR4 fixture and the base the corrupted RAR4 fixture damages.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "work/sample.mkv"); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR4Writer, Format: RAR4, Archive: "archive.rar",
				Method: "-m0", Members: []string{"work/sample.mkv"},
			})
		},
	})

	add(Artifact{
		Name:       "rar4-root-sample",
		Files:      []string{"archive.rar"},
		Toolchains: []string{RAR4Writer, VideoToolchain},
		Notes:      "RAR4 store at the archive root, the RAR4 PAR2 recovery target.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "sample.mkv"); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR4Writer, Format: RAR4, Archive: "archive.rar",
				Method: "-m0", Members: []string{"sample.mkv"},
			})
		},
	})

	add(Artifact{
		Name: "rar5-lz-volumes",
		Files: []string{"fixture_rar5_lz_plain.part1.rar", "fixture_rar5_lz_plain.part2.rar", "fixture_rar5_lz_plain.part3.rar",
			"fixture_rar5_lz_plain.part4.rar", "fixture_rar5_lz_plain.part5.rar", "fixture_rar5_lz_plain.part6.rar"},
		Toolchains: []string{RAR5Writer},
		Notes:      "Six small RAR5 volumes of 192 KiB over a deterministic payload. The multi-swap placement fixture stages these under swapped names beside canonical PAR2 sidecars.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WritePRNG(env.StagePath("payload.bin"), "rar5-lz-plain", 1_082_880); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "fixture_rar5_lz_plain.rar",
				Method: "-m1", Dictionary: "-md32m", VolumeSize: "192k",
				Members: []string{"payload.bin"},
			}); err != nil {
				return err
			}
			return expectParts(env, "fixture_rar5_lz_plain.part%d.rar", 6)
		},
	})

	add(Artifact{
		Name:       "sevenzip-single",
		Files:      []string{"archive.7z"},
		Toolchains: []string{SevenZipToolchain, VideoToolchain},
		Notes:      "One LZMA2 7z over the 1080p clip. Published whole, split into three volumes, renamed to numeric extensions, nested, truncated and PAR2-protected by seven scenarios.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "sample.mkv"); err != nil {
				return err
			}
			return env.SevenZip(ctx, SevenZipSpec{Archive: "archive.7z", Members: []string{"sample.mkv"}})
		},
	})

	add(Artifact{
		Name:       "sevenzip-encrypted",
		Files:      []string{"archive.7z"},
		Toolchains: []string{SevenZipToolchain, VideoToolchain},
		Notes:      "The same 7z with member data encrypted and headers left readable, published whole and split.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "sample.mkv"); err != nil {
				return err
			}
			return env.SevenZip(ctx, SevenZipSpec{
				Archive: "archive.7z", Members: []string{"sample.mkv"}, Password: CorpusPassword,
			})
		},
	})

	// ------------------------------------------- direct-unpack codec matrix
	//
	// One 7z per coder chain the pinned 7-Zip can WRITE and weaver can DECODE.
	// Built over the short clip rather than the 1080p one: these exercise decode
	// paths, not volume, and PPMd or BZip2 over 85 MiB of video would cost
	// minutes apiece for nothing.
	//
	// Deflate64 is deliberately absent — 7-Zip writes it, weaver has no decoder
	// arm for it. Zstd, Brotli and LZ4 are absent because the official 7-Zip
	// console binary cannot write them at all; weaver decodes all three, and
	// their coverage stays in the in-process matrix in
	// `pipeline::direct_unpack::read_pattern_tests`.
	for _, codec := range sevenZipCodecMatrix() {
		add(Artifact{
			Name:       "direct-unpack-" + codec.Slug,
			Files:      []string{"archive.7z"},
			Toolchains: []string{SevenZipToolchain, VideoToolchain},
			Notes:      codec.Notes,
			Build:      codec.Build,
		})
	}

	add(Artifact{
		Name:       "direct-unpack-repair-source",
		Files:      []string{"archive.7z"},
		Toolchains: []string{SevenZipToolchain},
		Notes:      "An 8 MiB PPMd 7z over derived bytes: the repair-resume fixture's source, sized and coded so decoding it outlasts downloading it.",
		Build:      BuildDirectUnpackRepairSource,
	})

	add(Artifact{
		Name: "par2-heavy-set",
		Files: []string{"archive.rar", "archive.rar.par2", "archive.rar.vol00+01.par2", "archive.rar.vol01+02.par2",
			"archive.rar.vol03+04.par2", "archive.rar.vol07+08.par2", "archive.rar.vol15+15.par2"},
		Toolchains: []string{RAR5Writer, PAR2Toolchain, VideoToolchain},
		Notes:      "200 source blocks, 30 recovery blocks in par2's default doubling layout, and three destroyed blocks. Four scenario slugs publish this one set.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Publish(ctx, "rar5-par2-base", "archive.rar"); err != nil {
				return err
			}
			if err := env.PAR2(ctx, PAR2Spec{
				Base: "archive.rar.par2", BlockCount: 200, RecoveryBlocks: 30, Sources: []string{"archive.rar"},
			}); err != nil {
				return err
			}
			return ZeroRange(env.OutputPath("archive.rar"), CorruptOffset, CorruptLength)
		},
	})

	add(Artifact{
		Name:       "par2-small-set",
		Files:      []string{"archive.rar", "archive_repair.par2", "archive_repair.vol00+24.par2"},
		Toolchains: []string{RAR5Writer, PAR2Toolchain, VideoToolchain},
		Notes:      "A 5 MiB RAR5 with ten of its 79 blocks destroyed and 24 recovery blocks in one file. Six scenarios stage these bytes.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Publish(ctx, "rar5-small", "archive.rar"); err != nil {
				return err
			}
			if err := env.PAR2(ctx, PAR2Spec{
				Base: "archive_repair.par2", SliceSize: 65536, RecoveryBlocks: 24, RecoveryFiles: 1,
				Sources: []string{"archive.rar"},
			}); err != nil {
				return err
			}
			return ZeroRange(env.OutputPath("archive.rar"), 10*65536, 10*65536)
		},
	})

	add(Artifact{
		Name:      "clip-shared",
		Resumable: true,
		Files: []string{
			"band-long-1500s-720p-av1.mkv",
			"band-short-120s-720p-av1.mkv",
			"series-movie-2400s-720p-av1.mkv",
			"short-720p-av1.mkv",
			"subtitle-sync-720p-h264.mkv",
		},
		Toolchains: []string{VideoToolchain},
		Notes:      "The five source clips generators encode their payloads from: four synthetic 720p AV1 clips with Opus audio at the durations the media suites expect, and one very low bitrate H.264 clip for subtitle timing.",
		Build: func(ctx context.Context, env *Env) error {
			for _, clip := range sharedClips() {
				if err := env.Clip(ctx, clip); err != nil {
					return err
				}
			}
			return nil
		},
	})

	return table
}

// sharedClips is the shared-clip table: duration, geometry and the bitrate
// constant each clip is encoded at, with the size floor the seeder needs.
func sharedClips() []ClipSpec {
	return []ClipSpec{
		{
			Name: "short-720p-av1.mkv", Seconds: 420, Width: 1280, Height: 720,
			VideoCodec: "libsvtav1", AudioCodec: "libopus",
			VideoBitrate: 1_430_000, AudioBitrate: 48_000, SampleRate: 48_000,
			Seed: 11, MinBytes: 77_514_027,
		},
		{
			Name: "band-long-1500s-720p-av1.mkv", Seconds: 1500, Width: 1280, Height: 720,
			VideoCodec: "libsvtav1", AudioCodec: "libopus",
			VideoBitrate: 410_000, AudioBitrate: 48_000, SampleRate: 48_000,
			Seed: 12, MinBytes: 85_620_041,
		},
		{
			Name: "band-short-120s-720p-av1.mkv", Seconds: 120, Width: 1280, Height: 720,
			VideoCodec: "libsvtav1", AudioCodec: "libopus",
			VideoBitrate: 4_790_000, AudioBitrate: 48_000, SampleRate: 48_000,
			Seed: 13, MinBytes: 72_595_940,
		},
		{
			Name: "series-movie-2400s-720p-av1.mkv", Seconds: 2400, Width: 1280, Height: 720,
			VideoCodec: "libsvtav1", AudioCodec: "libopus",
			VideoBitrate: 197_000, AudioBitrate: 48_000, SampleRate: 48_000,
			Seed: 14, MinBytes: 73_501_687,
		},
		{
			Name: "subtitle-sync-720p-h264.mkv", Seconds: 420, Width: 1280, Height: 720,
			VideoCodec: "libx264", AudioCodec: "aac",
			VideoBitrate: 8_000, AudioBitrate: 6_000, SampleRate: 44_100,
			Seed: 15, MinBytes: 490_260,
		},
	}
}

// expectParts fails unless RARLAB produced exactly count volumes, so a payload
// size that drifts across a writer upgrade is caught at generation time rather
// than by a scenario that silently loses a volume.
func expectParts(env *Env, pattern string, count int) error {
	produced, err := env.Outputs()
	if err != nil {
		return err
	}
	wanted := map[string]bool{}
	for index := 1; index <= count; index++ {
		wanted[fmt.Sprintf(pattern, index)] = true
	}
	for _, name := range produced {
		if !wanted[name] {
			return fmt.Errorf("expected exactly %d volumes matching %q, but %s was also written", count, pattern, name)
		}
		delete(wanted, name)
	}
	if len(wanted) != 0 {
		return fmt.Errorf("expected %d volumes matching %q, got %d", count, pattern, count-len(wanted))
	}
	return nil
}

// removeOutput deletes a file the recipe wrote but does not publish.
func removeOutput(env *Env, name string) error {
	return os.Remove(filepath.Join(env.Work, outputDir, filepath.FromSlash(name)))
}
