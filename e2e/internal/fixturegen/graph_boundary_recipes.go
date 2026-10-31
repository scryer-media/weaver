// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

const boundaryPayloadSize = 128 << 10
const boundaryUnicodeName = "灯火.bin"

// GraphBoundaryRecipes keep the graph's format and ownership boundaries small
// enough to exercise in the ordinary functional and restart suites.
func GraphBoundaryRecipes() []Recipe {
	var recipes []Recipe
	for _, codec := range []string{"gzip", "bzip2", "zstd"} {
		recipes = append(recipes, boundaryStreamRecipe("split-"+codec, codec, true, "", false))
	}
	for _, codec := range []string{"xz", "gzip"} {
		recipes = append(recipes, boundaryStreamRecipe("split-"+codec+"-damaged", codec, true, "damaged", false))
	}
	recipes = append(recipes,
		boundaryStreamRecipe("split-xz-missing", "xz", true, "missing", false),
		boundaryStreamRecipe("split-xz-missing-article", "xz", true, "missing-article", false),
		boundaryStreamRecipe("split-xz-missing-par2", "xz", true, "missing", true),
		boundaryStreamRecipe("split-xz-damaged-par2", "xz", true, "damaged", true),
	)
	for _, codec := range []string{"xz", "bzip2", "zstd"} {
		recipes = append(recipes, boundaryStreamRecipe(codec+"-corrupted", codec, false, "damaged", false))
	}
	for _, partial := range []bool{false, true} {
		recipes = append(recipes, boundaryTarXZRecipe(partial))
	}
	for _, kind := range []string{"plain", "rar", "stream"} {
		recipes = append(recipes, boundaryHeldRecipe(kind))
	}
	recipes = append(recipes, boundaryFeedVerdictRecipe())
	recipes = append(recipes, trailingNameStreamRecipe())
	return append(recipes, graphRecoveryRecipes()...)
}

func boundaryStreamExtension(codec string) string {
	switch codec {
	case "gzip":
		return ".gz"
	case "bzip2":
		return ".bz2"
	case "zstd":
		return ".zst"
	default:
		return ".xz"
	}
}

func boundaryCompress(ctx context.Context, env *Env, codec, name, source string) error {
	// Numbered pieces and UU article bodies no longer carry the codec
	// suffix when provenance is collected, so record it before splitting.
	for _, id := range goWriterIDs(env.Lock, []string{name, source}) {
		env.usedToolchain(id)
	}
	if codec == "xz" {
		return env.SevenZip(ctx, SevenZipSpec{Format: "xz", Archive: name, Members: []string{source}})
	}
	return CompressFile(codec, env.OutputPath(name), env.StagePath(source))
}

func boundaryFlip(path string, offset int64) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	var one [1]byte
	if _, err := file.ReadAt(one[:], offset); err != nil {
		return err
	}
	one[0] ^= 0x80
	_, err = file.WriteAt(one[:], offset)
	return err
}

func boundarySplit(env *Env, name string) ([]string, error) {
	info, err := os.Stat(env.OutputPath(name))
	if err != nil {
		return nil, err
	}
	return SplitFile(env.OutputPath(name), (info.Size()+2)/3, func(index int) string {
		return env.OutputPath(fmt.Sprintf("%s.%03d", name, index+1))
	})
}

func boundaryStreamRecipe(slug, codec string, split bool, damage string, recovery bool) Recipe {
	name := boundaryUnicodeName
	if codec == "gzip" {
		// RFC 1952 FNAME is Latin-1; XZ supplies the non-ASCII naming case.
		name = "lantern.bin"
	}
	archive := name + boundaryStreamExtension(codec)
	recipe := Recipe{
		Slug: slug, Family: "graph stream boundary", ByteReproducible: true,
		Notes: "A 128 KiB deterministic stream; numbered inputs join before decoding. Recovery protects the original numbered pieces, including their non-ASCII names.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WritePRNG(env.StagePath(name), "graph-boundary/stream", boundaryPayloadSize); err != nil {
				return err
			}
			if err := boundaryCompress(ctx, env, codec, archive, name); err != nil {
				return err
			}
			var pieces []string
			if split {
				var err error
				pieces, err = boundarySplit(env, archive)
				if err != nil {
					return err
				}
			}
			if recovery {
				var sources []string
				for _, part := range pieces {
					sources = append(sources, filepath.Base(part))
				}
				if err := env.PAR2(ctx, PAR2Spec{Base: "回復.par2", Sources: sources, SliceSize: 4096, RecoveryBlocks: 16, RecoveryFiles: 1}); err != nil {
					return err
				}
			}
			switch damage {
			case "damaged":
				if split {
					if err := boundaryFlip(pieces[1], 1024); err != nil {
						return err
					}
					// The retained joined file must contain the posted damaged bytes.
					if err := ConcatFiles(env.StagePath("retained"), pieces...); err != nil {
						return err
					}
				} else {
					if err := boundaryFlip(env.OutputPath(archive), 8192); err != nil {
						return err
					}
					if err := CopyFile(env.OutputPath(archive), env.StagePath("retained")); err != nil {
						return err
					}
				}
			case "missing":
				for _, index := range []int{0, 2} {
					if err := CopyFile(pieces[index], env.StagePath(fmt.Sprintf("retained-%03d", index+1))); err != nil {
						return err
					}
				}
				if err := os.Remove(pieces[1]); err != nil {
					return err
				}
			case "missing-article":
				{
					// The scenario deletes the second 16 KiB article of .002.
					// Its first article anchors the exact length and offset; the
					// identity join leaves only that declared range as zeroes.
					joined, err := os.ReadFile(env.OutputPath(archive))
					if err != nil {
						return err
					}
					part, err := os.Stat(pieces[0])
					if err != nil {
						return err
					}
					clear(joined[part.Size()+16384 : part.Size()+32768])
					if err := os.WriteFile(env.StagePath("retained"), joined, 0o644); err != nil {
						return err
					}
				}
			}
			if split {
				return os.Remove(env.OutputPath(archive))
			}
			return nil
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			if damage == "" || recovery {
				return map[string]string{name: env.StagePath(name)}, nil
			}
			if damage == "missing" {
				// With an entire middle piece absent, its length is unknown.
				// Preserve the remaining posted pieces without inventing offsets.
				return map[string]string{archive + ".001": env.StagePath("retained-001"), archive + ".003": env.StagePath("retained-003")}, nil
			}
			return map[string]string{archive: env.StagePath("retained")}, nil
		},
	}
	return recipe
}

func boundaryTarXZRecipe(partial bool) Recipe {
	slug := "tarxz-corrupted-empty"
	if partial {
		slug = "tarxz-corrupted-partial"
	}
	return Recipe{
		Slug: slug, Family: "graph stream boundary", ByteReproducible: true,
		Notes: "Valid XZ around a TAR with a damaged first or second header; a whole earlier member makes Partial, otherwise Failed.",
		Build: func(ctx context.Context, env *Env) error {
			var members []Member
			for _, name := range []string{"first.bin", "second.bin"} {
				if err := WritePRNG(env.StagePath(name), slug+"/"+name, 32768); err != nil {
					return err
				}
				members = append(members, Member{Name: name, Source: env.StagePath(name)})
			}
			if err := WriteTar(env.StagePath("bundle.tar"), nil, members); err != nil {
				return err
			}
			offset := int64(0)
			if partial {
				offset = 512 + 32768
			}
			if err := boundaryFlip(env.StagePath("bundle.tar"), offset); err != nil {
				return err
			}
			return boundaryCompress(ctx, env, "xz", "bundle.tar.xz", "bundle.tar")
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			if partial {
				return map[string]string{"first.bin": env.StagePath("first.bin"), "bundle.tar": env.StagePath("bundle.tar")}, nil
			}
			return map[string]string{"bundle.tar": env.StagePath("bundle.tar")}, nil
		},
	}
}

func boundaryHeldRecipe(kind string) Recipe {
	slug := "held-" + kind + "-uu"
	name := "held-" + kind + ".bin"
	if kind == "stream" {
		name = boundaryUnicodeName
	}
	return Recipe{
		Slug: slug, Family: "graph held parts", ByteReproducible: true,
		Notes: "UUDeview multipart encoding at 200 lines per article permits holding the first BODY while later unanchored parts durably spool.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WritePRNG(env.StagePath(name), slug, boundaryPayloadSize); err != nil {
				return err
			}
			var specs []UUSpec
			switch kind {
			case "plain":
				specs = []UUSpec{{Source: env.StagePath(name), Name: name, LinesPerPart: 200}}
			case "rar":
				if err := env.RAR(ctx, RARSpec{Toolchain: RAR5Writer, Format: RAR5, Archive: "held.rar", Members: []string{name}, Method: "-m0"}); err != nil {
					return err
				}
				specs = []UUSpec{{Source: env.OutputPath("held.rar"), Name: "held.rar", LinesPerPart: 200}}
			case "stream":
				archive := name + ".xz"
				if err := boundaryCompress(ctx, env, "xz", archive, name); err != nil {
					return err
				}
				pieces, err := boundarySplit(env, archive)
				if err != nil {
					return err
				}
				for _, part := range pieces {
					specs = append(specs, UUSpec{Source: part, Name: filepath.Base(part), LinesPerPart: 200})
				}
				if err := os.Remove(env.OutputPath(archive)); err != nil {
					return err
				}
			}
			if err := EncodeUU(ctx, env, specs); err != nil {
				return err
			}
			// Only the encoded articles belong in this posting, never a second
			// yEnc copy of the source archive or numbered pieces.
			if kind != "plain" {
				for _, spec := range specs {
					if err := os.Remove(spec.Source); err != nil {
						return err
					}
				}
			}
			return nil
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			return map[string]string{name: env.StagePath(name)}, nil
		},
	}
}

func boundaryFeedVerdictRecipe() Recipe {
	return Recipe{
		Slug: "feed-verdict-restart", Family: "graph feed restart", ByteReproducible: true,
		Notes: "Two plain 64 KiB files. The restart case withholds every broken-file article and holds the survivor until the abandoned-file verdict commits. Its oracle derives the retained zero-filled damaged placeholder from the NZB's encoded sizes; only the survivor is usable.",
		Build: func(_ context.Context, env *Env) error {
			for _, name := range []string{"broken.bin", "survivor.bin"} {
				if err := WritePRNG(env.OutputPath(name), "feed-verdict/"+name, 65536); err != nil {
					return err
				}
			}
			return nil
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			return map[string]string{"survivor.bin": env.OutputPath("survivor.bin")}, nil
		},
	}
}
