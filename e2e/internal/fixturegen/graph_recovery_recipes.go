// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

func graphRecoveryRecipes() []Recipe {
	var recipes []Recipe
	for _, format := range []RARFormat{RAR4, RAR5} {
		for _, volumes := range []bool{false, true} {
			recipes = append(recipes, boundaryRARRecordRecipe(format, volumes))
		}
	}
	for _, version := range []int{2, 3} {
		recipes = append(recipes, boundaryUnicodeRecoveryRecipe(version))
	}
	return append(recipes, boundaryUnrelatedPAR2Recipe(), boundaryUnpostedPAR2Recipe(), boundaryCaseCollisionRecipe(), boundaryNamespaceRestartRecipe(), boundaryRenameChainRecipe())
}

func boundaryRARRecordRecipe(format RARFormat, volumes bool) Recipe {
	slug := string(format) + "-record-repair"
	if volumes {
		slug = string(format) + "-record-and-volume-repair"
	}
	writer := RAR5Writer
	if format == RAR4 {
		// The existing pinned 6.24 writer still supports -ma4; the 7.x
		// writer must not silently stand in for the historical format.
		writer = RAR4Writer
	}
	return Recipe{
		// RAR4 stamps the recovery header with the current time; RAR5's
		// recovery data varies even with identical payloads and timestamps.
		// Recovery volumes inherit those differences from the archives.
		Slug: slug, Family: "RAR recovery record", ByteReproducible: false,
		Notes: "One damaged data block covered by RARLAB's own recovery record. The volume variant also withholds a second volume and supplies only one .rev, requiring record repair before volume recovery can finish.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WritePRNG(env.StagePath("record.bin"), "graph-boundary/rar-record", boundaryPayloadSize); err != nil {
				return err
			}
			spec := RARSpec{Toolchain: writer, Format: format, Archive: "record.rar", Members: []string{"record.bin"}, Method: "-m0", RecoveryRecord: "20p"}
			damaged := "record.rar"
			if volumes {
				spec.VolumeSize = "64k"
				spec.RecoveryVolumes = 1
				damaged = "record.part1.rar"
			}
			if err := env.RAR(ctx, spec); err != nil {
				return err
			}
			if err := boundaryFlip(env.OutputPath(damaged), 8192); err != nil {
				return err
			}
			if volumes {
				return os.Remove(env.OutputPath("record.part2.rar"))
			}
			return nil
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			return map[string]string{"record.bin": env.StagePath("record.bin")}, nil
		},
	}
}

func boundaryUnicodeRecoveryRecipe(version int) Recipe {
	slug := fmt.Sprintf("par%d-unicode-names", version)
	name := "灯火-café-данные.bin"
	return Recipe{
		Slug: slug, Family: "recovery identity", ByteReproducible: true,
		Notes: "A non-ASCII posted payload and non-ASCII recovery-set name; a corrupted block must recover to the safe described name and exact bytes.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WritePRNG(env.StagePath(name), slug, boundaryPayloadSize); err != nil {
				return err
			}
			if err := CopyFile(env.StagePath(name), env.OutputPath(name)); err != nil {
				return err
			}
			if version == 2 {
				if err := env.PAR2(ctx, PAR2Spec{Base: "回復.par2", Sources: []string{name}, SliceSize: 4096, RecoveryBlocks: 4, RecoveryFiles: 1}); err != nil {
					return err
				}
			} else {
				if err := env.PAR3(ctx, PAR3Spec{Index: "回復.par3", Sources: []string{name}, BlockSize: 4096, RecoveryBlocks: 4, RecoveryFiles: 1, ECC: 1}); err != nil {
					return err
				}
				if err := par3Snapshot(name)(ctx, env); err != nil {
					return err
				}
			}
			if err := boundaryFlip(env.OutputPath(name), 8192); err != nil {
				return err
			}
			if version == 3 {
				if err := par3ProveRepair(PAR3Proof{Index: "回復.par3", Restored: []string{name}})(ctx, env); err != nil {
					return err
				}
			}
			return validateUnicodeRecoveryOutputs(env, version, name)
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			return map[string]string{name: env.StagePath(name)}, nil
		},
	}
}

func validateUnicodeRecoveryOutputs(env *Env, version int, payload string) error {
	index := fmt.Sprintf("回復.par%d", version)
	volume := fmt.Sprintf("回復.vol0+4.par%d", version)
	want := map[string]bool{payload: true, index: true, volume: true}
	files, err := env.Outputs()
	if err != nil {
		return err
	}
	for _, name := range files {
		if !want[name] {
			return fmt.Errorf("unexpected Unicode recovery fixture output %q; preserve tool diagnostics outside the posted corpus", name)
		}
		delete(want, name)
	}
	if len(want) != 0 {
		return fmt.Errorf("Unicode recovery fixture is missing intended outputs: %v", want)
	}
	return nil
}

func boundaryUnrelatedPAR2Recipe() Recipe {
	return Recipe{
		// The RAR inherits the recovery record's variable bytes. The loose
		// payload and its unrelated PAR2 set remain digest-pinned.
		Slug: "par2-unrelated-set", Family: "recovery identity", ByteReproducible: false,
		Notes: "A damaged RAR protected only by its embedded recovery record, beside an unrelated PAR2 set over a healthy loose file. The unrelated set must neither claim the RAR nor delay its record repair.",
		Build: func(ctx context.Context, env *Env) error {
			if err := boundaryRARRecordRecipe(RAR5, false).Build(ctx, env); err != nil {
				return err
			}
			if err := WritePRNG(env.StagePath("unrelated.bin"), "unrelated-par2", 8192); err != nil {
				return err
			}
			if err := CopyFile(env.StagePath("unrelated.bin"), env.OutputPath("unrelated.bin")); err != nil {
				return err
			}
			return env.PAR2(ctx, PAR2Spec{Base: "unrelated.par2", Sources: []string{"unrelated.bin"}, SliceSize: 4096, RecoveryBlocks: 2, RecoveryFiles: 1})
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			return map[string]string{"record.bin": env.StagePath("record.bin"), "unrelated.bin": env.StagePath("unrelated.bin")}, nil
		},
	}
}

func boundaryCaseCollisionRecipe() Recipe {
	return Recipe{
		Slug: "rar-par2-case-collision", Family: "recovery identity", ByteReproducible: true,
		Notes: "A loose PAR2-described beacon.bin and a distinct RAR member BEACON.BIN collide across sources. Both byte sequences must survive under the existing namespace suffix policy.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WritePRNG(env.StagePath("BEACON.BIN"), "case-collision/archive", 8192); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar", Members: []string{"BEACON.BIN"}, Method: "-m0"}); err != nil {
				return err
			}
			// Keep the two source names in different directories, so the
			// recipe itself is valid on case-insensitive filesystems.
			if err := WritePRNG(env.OutputPath("beacon.bin"), "case-collision/loose", 12288); err != nil {
				return err
			}
			if err := CopyFile(env.OutputPath("beacon.bin"), env.StagePath("loose-source")); err != nil {
				return err
			}
			return env.PAR2(ctx, PAR2Spec{Base: "set.par2", Sources: []string{"beacon.bin"}, SliceSize: 4096, RecoveryBlocks: 1, RecoveryFiles: 1})
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			return map[string]string{"beacon.bin": env.StagePath("loose-source"), "BEACON (1).BIN": env.StagePath("BEACON.BIN")}, nil
		},
	}
}

func boundaryNamespaceRestartRecipe() Recipe {
	var base Recipe
	for _, recipe := range GraphGapRecipes() {
		if recipe.Slug == "par2-exact-name-content-match" {
			base = recipe
		}
	}
	return Recipe{
		Slug: "par2-namespace-restart", Family: "recovery identity restart", ByteReproducible: true,
		Notes: "The exact-name content match case plus an independent healthy file. The restart harness commits that file before releasing the content-matched leaf to its rename crash boundary.",
		Build: func(ctx context.Context, env *Env) error {
			if err := base.Build(ctx, env); err != nil {
				return err
			}
			return WritePRNG(env.OutputPath("00-keep.bin"), "namespace-restart/survivor", 8192)
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			outputs, err := base.ExpectedOutputs(ctx, env)
			if err != nil {
				return nil, err
			}
			outputs["00-keep.bin"] = env.OutputPath("00-keep.bin")
			return outputs, nil
		},
	}
}

func boundaryRenameChainRecipe() Recipe {
	return Recipe{
		Slug: "par3-rename-chain", Family: "recovery identity restart", ByteReproducible: true,
		Notes: "PAR3 names an obfuscated payload beacon.bin while the posted BEACON.BIN belongs to lantern.bin. The occupied source must move away before the other file moves in; an independent first file supplies the restart durability barrier.",
		Build: func(ctx context.Context, env *Env) error {
			for index, name := range []string{"beacon.bin", "lantern.bin"} {
				if err := WritePRNG(env.StagePath(name), "rename-chain/"+name, int64(8192*(index+1))); err != nil {
					return err
				}
				if err := CopyFile(env.StagePath(name), env.OutputPath(name)); err != nil {
					return err
				}
				if err := par3Snapshot(name)(ctx, env); err != nil {
					return err
				}
			}
			if err := env.PAR3(ctx, PAR3Spec{Index: "set.par3", Sources: []string{"beacon.bin", "lantern.bin"}, BlockSize: 4096, RecoveryBlocks: 4, RecoveryFiles: 1, ECC: 1}); err != nil {
				return err
			}
			if err := os.Rename(env.OutputPath("beacon.bin"), env.OutputPath("a07d5e19c4b26f83")); err != nil {
				return err
			}
			if err := os.Rename(env.OutputPath("lantern.bin"), env.OutputPath("BEACON.BIN")); err != nil {
				return err
			}
			if err := par3ProveRepair(PAR3Proof{Index: "set.par3", Restored: []string{"beacon.bin", "lantern.bin"}, Extra: []string{"a07d5e19c4b26f83", "BEACON.BIN"}})(ctx, env); err != nil {
				return err
			}
			return WritePRNG(env.OutputPath("00-keep.bin"), "namespace-restart/survivor", 8192)
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			return map[string]string{"beacon.bin": env.StagePath("beacon.bin"), "lantern.bin": env.StagePath("lantern.bin"), "00-keep.bin": env.OutputPath("00-keep.bin")}, nil
		},
	}
}

func boundaryUnpostedPAR2Recipe() Recipe {
	return Recipe{
		Slug: "par2-describes-unposted", Family: "recovery identity", ByteReproducible: true,
		Notes: "A PAR2 set describes only an unposted file with far too little parity to reconstruct it, beside a recognizable XZ with a bad stream-header checksum. Nothing usable exists: Failed with a Repair reason, retaining only posted bytes.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WritePRNG(env.StagePath("ember.bin"), "unrelated-set/posted", boundaryPayloadSize); err != nil {
				return err
			}
			if err := boundaryCompress(ctx, env, "xz", "ember.bin.xz", "ember.bin"); err != nil {
				return err
			}
			if err := corruptXZHeaderChecksum(env.OutputPath("ember.bin.xz")); err != nil {
				return err
			}
			if err := WritePRNG(env.OutputPath("tinder.bin"), "unrelated-set/unposted", boundaryPayloadSize); err != nil {
				return err
			}
			if err := env.PAR2(ctx, PAR2Spec{Base: "tinder.par2", Sources: []string{"tinder.bin"}, SliceSize: 4096, RecoveryBlocks: 2, RecoveryFiles: 1}); err != nil {
				return err
			}
			return os.Remove(env.OutputPath("tinder.bin"))
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			files, err := env.Outputs()
			if err != nil {
				return nil, err
			}
			posted := make(map[string]string, len(files))
			for _, name := range files {
				posted[name] = env.OutputPath(name)
			}
			return posted, nil
		},
	}
}

func corruptXZHeaderChecksum(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	var header [12]byte
	_, readErr := file.ReadAt(header[:], 0)
	closeErr := file.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if !bytes.Equal(header[:6], []byte("\xfd7zXZ\x00")) || binary.LittleEndian.Uint32(header[8:]) != crc32.ChecksumIEEE(header[6:8]) {
		return fmt.Errorf("expected an intact XZ stream header before introducing checksum damage")
	}
	// Keep the signature so the graph opens an XZ object; corrupting its
	// magic would instead post a complete opaque file with a valid yEnc CRC.
	return boundaryFlip(path, 8)
}
