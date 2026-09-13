package fixturegen

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// The PAR3 family. Every recovery packet comes from the pinned par3cmdline
// reference, unmodified: Go writes the payloads and the ZIP containers, RARLAB
// and 7-Zip write their archives, and after that Go only damages, renames or
// withholds whole files. Every damaged fixture is repaired by the reference
// itself at generation time, so a shape that stops being repairable fails the
// generator instead of the e2e run.
//
// Payloads are PRNG streams rather than encoded clips, so no recipe here needs
// the video encoder and each one builds in seconds.

const (
	// par3BlockSize is the block size the loose and archive sets use unless a
	// geometry fixture says otherwise. Big enough that a 6 MiB payload is under
	// a hundred blocks, small enough that a few damaged blocks stay a small
	// fraction of the file.
	par3BlockSize = 65536
	// par3PayloadBytes is the ordinary loose payload: eight articles at the
	// harness's default article size, so the posting is genuinely multi-part.
	par3PayloadBytes = 6 << 20
)

// par3Payload is one deterministic input file.
type par3Payload struct {
	name  string
	write func(path string) error
}

// prngPayload is an incompressible payload derived from its seed.
func prngPayload(name, seed string, size int64) par3Payload {
	return par3Payload{name: name, write: func(path string) error { return WritePRNG(path, seed, size) }}
}

// repeatingPayload is `lead` PRNG bytes followed by `repeats` passes over the
// same `distinct` chunks of `chunk` bytes. It is what deduplication is for: an
// aligned lead of zero lets `-d1` find every repeat on the block grid, and an
// odd lead moves every repeat off it so only `-d2`'s sliding search can.
func repeatingPayload(name, seed string, lead, chunk int64, distinct, repeats int) par3Payload {
	return par3Payload{name: name, write: func(path string) error {
		return writeFile(path, func(writer io.Writer) error {
			if err := streamPRNG(writer, seed+"/lead", lead); err != nil {
				return err
			}
			chunks := make([][]byte, distinct)
			for index := range chunks {
				chunks[index] = PatternBytes(fmt.Sprintf("%s/chunk-%d", seed, index), int(chunk))
			}
			for pass := 0; pass < repeats; pass++ {
				for _, contents := range chunks {
					if _, err := writer.Write(contents); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}}
}

// par3WritePayloads writes each payload straight into the output directory.
func par3WritePayloads(payloads ...par3Payload) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		for _, payload := range payloads {
			if err := payload.write(env.OutputPath(payload.name)); err != nil {
				return err
			}
		}
		return nil
	}
}

// par3StagePayloads writes each payload into the stage, for an archiver to pack.
func par3StagePayloads(payloads ...par3Payload) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		for _, payload := range payloads {
			if err := payload.write(env.StagePath(payload.name)); err != nil {
				return err
			}
		}
		return nil
	}
}

// par3Expected pins the delivered bytes of each payload under its delivered
// name. The bytes are re-derived from the payload's recipe, never read back.
func par3Expected(payloads ...par3Payload) func(context.Context, *Env) (map[string]string, error) {
	return func(_ context.Context, env *Env) (map[string]string, error) {
		outputs := make(map[string]string, len(payloads))
		for _, payload := range payloads {
			path := env.StagePath("expected/" + sanitizeName(payload.name))
			if err := payload.write(path); err != nil {
				return nil, err
			}
			outputs[payload.name] = path
		}
		return outputs, nil
	}
}

func par3Create(spec PAR3Spec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error { return env.PAR3(ctx, spec) }
}

func par3Insert(archive string, redundancyPercent int) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error { return env.PAR3Insert(ctx, archive, redundancyPercent) }
}

// scrambleOutput overwrites a range with deterministic noise rather than
// zeros, so the damage cannot be mistaken for a sparse or unwritten region.
func scrambleOutput(name string, offset int64, length int) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		return OverwriteRange(env.OutputPath(name), offset, PatternBytes(fmt.Sprintf("par3-damage/%s/%d", name, offset), length))
	}
}

// zeroBlocks zeroes whole blocks of a file, each window given as a
// (first block, block count) pair.
func zeroBlocks(name string, blockSize int64, windows ...[2]int64) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		for _, window := range windows {
			if err := ZeroRange(env.OutputPath(name), window[0]*blockSize, window[1]*blockSize); err != nil {
				return err
			}
		}
		return nil
	}
}

func renameOutput(from, to string) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		return os.Rename(env.OutputPath(from), env.OutputPath(to))
	}
}

// dropOutputsMatching withholds every output the predicate selects.
func dropOutputsMatching(drop func(name string) bool) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		outputs, err := env.Outputs()
		if err != nil {
			return err
		}
		for _, name := range outputs {
			if drop(name) {
				if err := removeOutput(env, name); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// par3SetFiles lists the outputs a set wrote, by the index's stem.
func par3SetFiles(env *Env, stem string) ([]string, error) {
	outputs, err := env.Outputs()
	if err != nil {
		return nil, err
	}
	var files []string
	for _, name := range outputs {
		if strings.HasPrefix(name, stem+".") && strings.HasSuffix(name, ".par3") {
			files = append(files, name)
		}
	}
	return files, nil
}

// PAR3Recipes is the PAR3 fixture family.
func PAR3Recipes() []Recipe {
	var recipes []Recipe
	add := func(recipe Recipe) {
		recipe.Family = "PAR3"
		recipes = append(recipes, recipe)
	}

	// ------------------------------------------------------ loose payloads

	clean := prngPayload("amber.lattice.s01e03.mkv", "par3-amber-lattice", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-clean", ByteReproducible: true,
		Notes: "A loose payload under a Cauchy PAR3 set with nothing damaged: verification must pass on the input alone and deliver without repairing.",
		Build: sequence(
			par3WritePayloads(clean),
			par3Create(PAR3Spec{Index: "amber.lattice.s01e03.par3", Sources: []string{clean.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
		),
		ExpectedOutputs: par3Expected(clean),
	})

	cauchy := prngPayload("cobalt.drift.s02e05.mkv", "par3-cobalt-drift", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-direct-repair", ByteReproducible: true,
		Notes: "A loose payload under a Cauchy PAR3 set with two 100-byte overwrites in different blocks. The bytes are damaged before posting, so the yEnc checksums match the damage and only PAR3's own block hashes can find it.",
		Build: sequence(
			par3WritePayloads(cauchy),
			par3Create(PAR3Spec{Index: "cobalt.drift.s02e05.par3", Sources: []string{cauchy.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot(cauchy.name),
			scrambleOutput(cauchy.name, 1_000_000, 100),
			scrambleOutput(cauchy.name, 4_000_000, 100),
			par3ProveRepair(PAR3Proof{Index: "cobalt.drift.s02e05.par3", Restored: []string{cauchy.name}}),
		),
		ExpectedOutputs: par3Expected(cauchy),
	})

	fft := prngPayload("quartz.fathom.s01e09.mkv", "par3-quartz-fathom", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-fft-repair", ByteReproducible: true,
		Notes: "The same repair under FFT-based Reed-Solomon (`-e8`), the other erasure code the reference writes, with two adjacent blocks zeroed.",
		Build: sequence(
			par3WritePayloads(fft),
			par3Create(PAR3Spec{Index: "quartz.fathom.s01e09.par3", Sources: []string{fft.name},
				ECC: 8, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot(fft.name),
			zeroBlocks(fft.name, par3BlockSize, [2]int64{40, 2}),
			par3ProveRepair(PAR3Proof{Index: "quartz.fathom.s01e09.par3", Restored: []string{fft.name}}),
		),
		ExpectedOutputs: par3Expected(fft),
	})

	gf16 := prngPayload("ember.vantage.s03e01.mkv", "par3-ember-vantage", 3<<20+123)
	add(Recipe{
		Slug: "par3-gf16-repair", ByteReproducible: true,
		Notes: "1 KiB blocks over a 3 MiB payload, so the set has thousands of input blocks and the reference moves to a 16-bit Galois field. Eight KiB of damage spans nine blocks against 64 recovery blocks.",
		Build: sequence(
			par3WritePayloads(gf16),
			par3Create(PAR3Spec{Index: "ember.vantage.s03e01.par3", Sources: []string{gf16.name},
				ECC: 1, BlockSize: 1024, RecoveryBlocks: 64}),
			par3Snapshot(gf16.name),
			zeroOutput(gf16.name, 1_500_000, 8192),
			par3ProveRepair(PAR3Proof{Index: "ember.vantage.s03e01.par3", Restored: []string{gf16.name}}),
		),
		ExpectedOutputs: par3Expected(gf16),
	})

	cohorts := prngPayload("saffron.relay.s01e04.mkv", "par3-saffron-relay", 4<<20+32768+123)
	add(Recipe{
		Slug: "par3-uneven-cohorts", ByteReproducible: true,
		Notes: "FFT with two-way interleaving (`-i2`) over a block count that does not divide evenly, so the cohorts differ in size. Three single blocks are zeroed far apart, which lands them in different cohorts.",
		Build: sequence(
			par3WritePayloads(cohorts),
			par3Create(PAR3Spec{Index: "saffron.relay.s01e04.par3", Sources: []string{cohorts.name},
				ECC: 8, Interleave: 2, BlockSize: 32768, RecoveryBlocks: 24}),
			par3Snapshot(cohorts.name),
			zeroBlocks(cohorts.name, 32768, [2]int64{10, 1}, [2]int64{71, 1}, [2]int64{120, 1}),
			par3ProveRepair(PAR3Proof{Index: "saffron.relay.s01e04.par3", Restored: []string{cohorts.name}}),
		),
		ExpectedOutputs: par3Expected(cohorts),
	})

	aligned := repeatingPayload("harbor.glyph.s01e02.mkv", "par3-harbor-glyph", 0, 32768, 16, 8)
	add(Recipe{
		Slug: "par3-aligned-dedup", ByteReproducible: true,
		Notes: "Sixteen distinct 32 KiB chunks repeated eight times on the block grid, under aligned deduplication (`-d1`). One chunk is zeroed and another scrambled; every damaged block has intact repeats elsewhere in the file.",
		Build: sequence(
			par3WritePayloads(aligned),
			par3Create(PAR3Spec{Index: "harbor.glyph.s01e02.par3", Sources: []string{aligned.name},
				ECC: 1, Dedup: 1, BlockSize: 32768, RecoveryBlocks: 8}),
			par3Snapshot(aligned.name),
			zeroBlocks(aligned.name, 32768, [2]int64{37, 1}),
			scrambleOutput(aligned.name, 90*32768+100, 64),
			par3ProveRepair(PAR3Proof{Index: "harbor.glyph.s01e02.par3", Restored: []string{aligned.name}}),
		),
		ExpectedOutputs: par3Expected(aligned),
	})

	sliding := repeatingPayload("lumen.tract.s02e08.mkv", "par3-lumen-tract", 123, 32768, 16, 8)
	add(Recipe{
		Slug: "par3-sliding-dedup", ByteReproducible: true,
		Notes: "The same repeating payload behind a 123-byte lead, so no repeat sits on the block grid and only sliding deduplication (`-d2`) can find them. Two windows are damaged.",
		Build: sequence(
			par3WritePayloads(sliding),
			par3Create(PAR3Spec{Index: "lumen.tract.s02e08.par3", Sources: []string{sliding.name},
				ECC: 1, Dedup: 2, BlockSize: 32768, RecoveryBlocks: 8}),
			par3Snapshot(sliding.name),
			zeroOutput(sliding.name, 37*32768+123, 32768),
			scrambleOutput(sliding.name, 90*32768+500, 64),
			par3ProveRepair(PAR3Proof{Index: "lumen.tract.s02e08.par3", Restored: []string{sliding.name}}),
		),
		ExpectedOutputs: par3Expected(sliding),
	})

	dataPackets := prngPayload("tidal.vesper.s01e06.mkv", "par3-tidal-vesper", 4<<20)
	add(Recipe{
		Slug: "par3-data-packets", ByteReproducible: true,
		Notes: "A set written with Data packets (`-D`), posted as its index and `.part` files only: neither the payload nor any recovery volume is in the posting, so the payload has to be rebuilt from the input the set carries.",
		Build: sequence(
			par3WritePayloads(dataPackets),
			par3Create(PAR3Spec{Index: "tidal.vesper.s01e06.par3", Sources: []string{dataPackets.name},
				ECC: 1, StoreData: true, BlockSize: par3BlockSize, RecoveryBlocks: 1}),
			par3Snapshot(dataPackets.name),
			dropOutput(dataPackets.name),
			dropOutputsMatching(func(name string) bool { return strings.Contains(name, ".vol") }),
			par3ProveRepair(PAR3Proof{Index: "tidal.vesper.s01e06.par3", Restored: []string{dataPackets.name}}),
		),
		ExpectedOutputs: par3Expected(dataPackets),
	})

	tailsMain := prngPayload("copper.meadow.s01e01.mkv", "par3-copper-meadow", 2<<20+80)
	tailsSubs := prngPayload("copper.meadow.s01e01.en.srt", "par3-copper-meadow-en", 8192+96)
	tailsForced := prngPayload("copper.meadow.s01e01.forced.srt", "par3-copper-meadow-forced", 8192+128)
	add(Recipe{
		Slug: "par3-packed-tails", ByteReproducible: true,
		Notes: "Three files whose sizes leave short tails at an 8 KiB block size, so the reference packs the tails together into shared blocks. The damage sits inside one sidecar's packed tail and inside the main payload.",
		Build: sequence(
			par3WritePayloads(tailsMain, tailsSubs, tailsForced),
			par3Create(PAR3Spec{Index: "copper.meadow.s01e01.par3", Sources: []string{tailsMain.name, tailsSubs.name, tailsForced.name},
				ECC: 1, BlockSize: 8192, RecoveryBlocks: 32}),
			par3Snapshot(tailsMain.name, tailsSubs.name),
			scrambleOutput(tailsSubs.name, 8192+40, 16),
			scrambleOutput(tailsMain.name, 1_000_000, 64),
			par3ProveRepair(PAR3Proof{Index: "copper.meadow.s01e01.par3", Restored: []string{tailsMain.name, tailsSubs.name}}),
		),
		ExpectedOutputs: par3Expected(tailsMain, tailsSubs, tailsForced),
	})

	multi := []par3Payload{
		prngPayload("garnet.isle.s01e01.mkv", "par3-garnet-isle-1", 3<<20),
		prngPayload("garnet.isle.s01e02.mkv", "par3-garnet-isle-2", 3<<20),
		prngPayload("garnet.isle.s01e03.mkv", "par3-garnet-isle-3", 3<<20),
	}
	add(Recipe{
		Slug: "par3-multi-file-repair", ByteReproducible: true,
		Notes: "One set over three payloads with two of them damaged, so a single recovery matrix restores blocks belonging to different files.",
		Build: sequence(
			par3WritePayloads(multi...),
			par3Create(PAR3Spec{Index: "garnet.isle.s01.par3", Sources: []string{multi[0].name, multi[1].name, multi[2].name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 16}),
			par3Snapshot(multi[0].name, multi[2].name),
			zeroBlocks(multi[0].name, par3BlockSize, [2]int64{12, 2}),
			scrambleOutput(multi[2].name, 30*par3BlockSize+7, 200),
			par3ProveRepair(PAR3Proof{Index: "garnet.isle.s01.par3", Restored: []string{multi[0].name, multi[2].name}}),
		),
		ExpectedOutputs: par3Expected(multi...),
	})

	withheld := []par3Payload{
		prngPayload("birch.quarry.s01e01.mkv", "par3-birch-quarry-1", 3<<20),
		prngPayload("birch.quarry.s01e02.mkv", "par3-birch-quarry-2", 3<<20),
		prngPayload("birch.quarry.s01e03.mkv", "par3-birch-quarry-3", 3<<20),
	}
	add(Recipe{
		Slug: "par3-withheld-file", ByteReproducible: true,
		Notes: "One set over three payloads, posted without the middle one. The NZB never names it, so the set is the only thing that knows the file exists, and it has to be created from recovery blocks alone.",
		Build: sequence(
			par3WritePayloads(withheld...),
			par3Create(PAR3Spec{Index: "birch.quarry.s01.par3", Sources: []string{withheld[0].name, withheld[1].name, withheld[2].name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 64}),
			par3Snapshot(withheld[1].name),
			dropOutput(withheld[1].name),
			par3ProveRepair(PAR3Proof{Index: "birch.quarry.s01.par3", Restored: []string{withheld[1].name}}),
		),
		ExpectedOutputs: par3Expected(withheld...),
	})

	insufficient := prngPayload("slate.corridor.s01e10.mkv", "par3-slate-corridor", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-insufficient", ByteReproducible: true,
		Notes: "Twelve blocks zeroed against four recovery blocks. The reference itself refuses the repair, and so must weaver: failing, never delivering damaged bytes.",
		Build: sequence(
			par3WritePayloads(insufficient),
			par3Create(PAR3Spec{Index: "slate.corridor.s01e10.par3", Sources: []string{insufficient.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 4}),
			zeroBlocks(insufficient.name, par3BlockSize, [2]int64{20, 12}),
			par3ProveUnrepairable(PAR3Proof{Index: "slate.corridor.s01e10.par3"}),
		),
	})

	heavy := prngPayload("velvet.basin.s02e02.mkv", "par3-velvet-basin", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-heavy-damage", ByteReproducible: true,
		Notes: "Forty of 92 blocks destroyed in five windows against 48 recovery blocks: most of the recovery data is needed and the decode matrix is large.",
		Build: sequence(
			par3WritePayloads(heavy),
			par3Create(PAR3Spec{Index: "velvet.basin.s02e02.par3", Sources: []string{heavy.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 48}),
			par3Snapshot(heavy.name),
			zeroBlocks(heavy.name, par3BlockSize, [2]int64{2, 8}, [2]int64{20, 8}, [2]int64{40, 8}, [2]int64{60, 8}, [2]int64{80, 8}),
			par3ProveRepair(PAR3Proof{Index: "velvet.basin.s02e02.par3", Restored: []string{heavy.name}}),
		),
		ExpectedOutputs: par3Expected(heavy),
	})

	missingArticles := prngPayload("hollow.beacon.s01e07.mkv", "par3-hollow-beacon", 8<<20)
	add(Recipe{
		Slug: "par3-missing-articles", ByteReproducible: true,
		Notes: "An 8 MiB payload whose last two articles the scenario deletes after posting, so the file is short rather than wrong. The index is named without the payload's extension so the deletion's subject match cannot touch the set.",
		Build: sequence(
			par3WritePayloads(missingArticles),
			par3Create(PAR3Spec{Index: "hollow.beacon.s01e07.par3", Sources: []string{missingArticles.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 48}),
		),
		ExpectedOutputs: par3Expected(missingArticles),
	})

	missingIndex := prngPayload("marble.causeway.s01e05.mkv", "par3-marble-causeway", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-missing-index", ByteReproducible: true,
		Notes: "A damaged payload posted with its recovery volumes but without the index file. Every recovery volume repeats the set's metadata, so the set has to be understood from a volume.",
		Build: sequence(
			par3WritePayloads(missingIndex),
			par3Create(PAR3Spec{Index: "marble.causeway.s01e05.par3", Sources: []string{missingIndex.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot(missingIndex.name),
			dropOutput("marble.causeway.s01e05.par3"),
			zeroBlocks(missingIndex.name, par3BlockSize, [2]int64{50, 2}),
			func(ctx context.Context, env *Env) error {
				volumes, err := par3SetFiles(env, "marble.causeway.s01e05")
				if err != nil || len(volumes) == 0 {
					return fmt.Errorf("the missing-index set has no recovery volumes: %v", err)
				}
				return par3ProveRepair(PAR3Proof{Index: volumes[len(volumes)-1], Restored: []string{missingIndex.name}})(ctx, env)
			},
		),
		ExpectedOutputs: par3Expected(missingIndex),
	})

	obfuscated := prngPayload("indigo.vale.s01e08.mkv", "par3-indigo-vale", par3PayloadBytes)
	const obfuscatedName = "51273aad56a8b904e96928935278a627.201"
	add(Recipe{
		Slug: "par3-obfuscated-names", ByteReproducible: true,
		Notes: "The payload posted under an obfuscated hex name with one block damaged. The name the set records is the only trustworthy one, so the delivered file must carry it.",
		Build: sequence(
			par3WritePayloads(obfuscated),
			par3Create(PAR3Spec{Index: "indigo.vale.s01e08.par3", Sources: []string{obfuscated.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot(obfuscated.name),
			scrambleOutput(obfuscated.name, 2_500_000, 32),
			renameOutput(obfuscated.name, obfuscatedName),
			par3ProveRepair(PAR3Proof{Index: "indigo.vale.s01e08.par3", Restored: []string{obfuscated.name},
				Extra: []string{obfuscatedName}}),
		),
		ExpectedOutputs: par3Expected(obfuscated),
	})

	disguised := prngPayload("russet.pylon.s01e03.mkv", "par3-russet-pylon", par3PayloadBytes)
	const disguisedCarrier = "000.metadata.bin"
	add(Recipe{
		Slug: "par3-disguised-carrier", ByteReproducible: true,
		Notes: "A single-byte-range corruption posted with exactly one PAR3 file: the first recovery volume, under a `.bin` name. Nothing about the name says PAR3, so the carrier has to be recognised by its packets.",
		Build: sequence(
			par3WritePayloads(disguised),
			par3Create(PAR3Spec{Index: "russet.pylon.s01e03.par3", Sources: []string{disguised.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot(disguised.name),
			scrambleOutput(disguised.name, 3_100_000, 16),
			par3ProveRepair(PAR3Proof{Index: "russet.pylon.s01e03.vol0+1.par3", Restored: []string{disguised.name},
				Omit: []string{"russet.pylon.s01e03.par3", "russet.pylon.s01e03.vol1+2.par3",
					"russet.pylon.s01e03.vol3+4.par3", "russet.pylon.s01e03.vol7+1.par3"}}),
			dropOutputsMatching(func(name string) bool {
				return strings.HasSuffix(name, ".par3") && name != "russet.pylon.s01e03.vol0+1.par3"
			}),
			renameOutput("russet.pylon.s01e03.vol0+1.par3", disguisedCarrier),
		),
		ExpectedOutputs: par3Expected(disguised),
	})

	primary := prngPayload("onyx.terrace.s01e01.mkv", "par3-onyx-terrace", par3PayloadBytes)
	secondary := prngPayload("onyx.terrace.s01e01.featurette.mkv", "par3-onyx-terrace-featurette", 3<<20)
	add(Recipe{
		Slug: "par3-two-sets", ByteReproducible: true,
		Notes: "Two independent PAR3 sets in one posting, one Cauchy and one FFT at different block sizes, each over its own damaged payload. Both have to be repaired before the job completes.",
		Build: sequence(
			par3WritePayloads(primary, secondary),
			par3Create(PAR3Spec{Index: "onyx.terrace.s01e01.par3", Sources: []string{primary.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Create(PAR3Spec{Index: "onyx.terrace.featurette.par3", Sources: []string{secondary.name},
				ECC: 8, BlockSize: 32768, RecoveryBlocks: 8}),
			par3Snapshot(primary.name, secondary.name),
			zeroBlocks(primary.name, par3BlockSize, [2]int64{30, 2}),
			zeroBlocks(secondary.name, 32768, [2]int64{50, 3}),
			par3ProveRepair(PAR3Proof{Index: "onyx.terrace.s01e01.par3", Restored: []string{primary.name}}),
			par3ProveRepair(PAR3Proof{Index: "onyx.terrace.featurette.par3", Restored: []string{secondary.name}}),
		),
		ExpectedOutputs: par3Expected(primary, secondary),
	})

	// ------------------------------------------------------ archives

	rarVolumes := []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar"}

	rar5 := prngPayload("carmine.hollow.s01e04.mkv", "par3-carmine-hollow", 10<<20)
	add(Recipe{
		Slug:  "par3-rar5-repair",
		Notes: "A compressed three-volume RAR5 set with an external PAR3 set over the volumes and two blocks of the interior volume zeroed, so repair has to run before extraction.",
		Build: sequence(
			par3StagePayloads(rar5),
			par3RAR(RAR5Writer, RAR5, "4m", "", rar5.name),
			par3Create(PAR3Spec{Index: "archive.par3", Sources: rarVolumes, ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 24}),
			par3Snapshot("archive.part2.rar"),
			zeroBlocks("archive.part2.rar", par3BlockSize, [2]int64{20, 2}),
			par3ProveRepair(PAR3Proof{Index: "archive.par3", Restored: []string{"archive.part2.rar"}}),
		),
		ExpectedOutputs: par3Expected(rar5),
	})

	rar5Withheld := prngPayload("sienna.gantry.s01e02.mkv", "par3-sienna-gantry", 10<<20)
	add(Recipe{
		Slug:  "par3-rar5-withheld-volume",
		Notes: "The same three-volume shape posted without its interior volume. The set carries enough recovery blocks to create a whole 4 MiB volume from nothing.",
		Build: sequence(
			par3StagePayloads(rar5Withheld),
			par3RAR(RAR5Writer, RAR5, "4m", "", rar5Withheld.name),
			par3Create(PAR3Spec{Index: "archive.par3", Sources: rarVolumes, ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 80}),
			par3Snapshot("archive.part2.rar"),
			dropOutput("archive.part2.rar"),
			par3ProveRepair(PAR3Proof{Index: "archive.par3", Restored: []string{"archive.part2.rar"}}),
		),
		ExpectedOutputs: par3Expected(rar5Withheld),
	})

	rar4 := prngPayload("fallow.ridge.s01e01.mkv", "par3-fallow-ridge", par3PayloadBytes)
	add(Recipe{
		Slug:  "par3-rar4-repair",
		Notes: "A single compressed RAR4 archive under an external PAR3 set with two blocks zeroed well past the headers, so PAR3 handling is exercised independently of the RAR generation.",
		Build: sequence(
			par3StagePayloads(rar4),
			func(ctx context.Context, env *Env) error {
				return env.RAR(ctx, RARSpec{Toolchain: RAR4Writer, Format: RAR4, Archive: "archive.rar",
					Method: "-m1", Members: []string{rar4.name}})
			},
			par3Create(PAR3Spec{Index: "archive.par3", Sources: []string{"archive.rar"}, ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot("archive.rar"),
			zeroBlocks("archive.rar", par3BlockSize, [2]int64{40, 2}),
			par3ProveRepair(PAR3Proof{Index: "archive.par3", Restored: []string{"archive.rar"}}),
		),
		ExpectedOutputs: par3Expected(rar4),
	})

	rar5Encrypted := prngPayload("garnet.spire.s01e06.mkv", "par3-garnet-spire", 10<<20)
	add(Recipe{
		Slug:  "par3-rar5-encrypted-repair",
		Notes: "A compressed three-volume RAR5 set with `-hp` header and data encryption under an external PAR3 set, the interior volume damaged. PAR3 covers ciphertext, so repair never needs the key; extraction with the password proves the restored bytes are the posted ones. Salted: RARLAB draws a fresh salt for every encrypted archive.",
		Build: sequence(
			par3StagePayloads(rar5Encrypted),
			par3RAR(RAR5Writer, RAR5, "4m", CorpusPassword, rar5Encrypted.name),
			par3Create(PAR3Spec{Index: "archive.par3", Sources: rarVolumes, ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 24}),
			par3Snapshot("archive.part2.rar"),
			zeroBlocks("archive.part2.rar", par3BlockSize, [2]int64{30, 2}),
			par3ProveRepair(PAR3Proof{Index: "archive.par3", Restored: []string{"archive.part2.rar"}}),
		),
		ExpectedOutputs: par3Expected(rar5Encrypted),
	})

	sevenZip := prngPayload("pewter.canal.s01e03.mkv", "par3-pewter-canal", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-7z-repair", ByteReproducible: true,
		Notes: "An LZMA2 7z under an external PAR3 set with two blocks zeroed well past the header, so repair runs before extraction.",
		Build: sequence(
			par3StagePayloads(sevenZip),
			func(ctx context.Context, env *Env) error {
				return env.SevenZip(ctx, SevenZipSpec{Archive: "archive.7z", Members: []string{sevenZip.name},
					Methods: []string{"-m0=LZMA2"}, Deterministic: true})
			},
			par3Create(PAR3Spec{Index: "archive.par3", Sources: []string{"archive.7z"}, ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot("archive.7z"),
			zeroBlocks("archive.7z", par3BlockSize, [2]int64{40, 2}),
			par3ProveRepair(PAR3Proof{Index: "archive.par3", Restored: []string{"archive.7z"}}),
		),
		ExpectedOutputs: par3Expected(sevenZip),
	})

	add(Recipe{
		Slug: "par3-split-7z-withheld-part", ByteReproducible: true,
		Notes: "The LZMA2 codec-matrix 7z cut into five parts with PAR3 over all five, then the third part withheld from the posting; the set has to rebuild it before extraction can run.",
		Build: sequence(
			splitSevenZipIntoParts("direct-unpack-lzma2", 5),
			par3Create(PAR3Spec{Index: "archive.7z.par3", ECC: 1, BlockSize: 16384, RecoveryBlocks: 8,
				Sources: []string{"archive.7z.001", "archive.7z.002", "archive.7z.003", "archive.7z.004", "archive.7z.005"}}),
			par3Snapshot("archive.7z.003"),
			dropOutput("archive.7z.003"),
			par3ProveRepair(PAR3Proof{Index: "archive.7z.par3", Restored: []string{"archive.7z.003"}}),
		),
		ExpectedOutputs: sevenZipCodecBySlug("lzma2").ExpectedOutputs(),
	})

	zipped := prngPayload("umber.lagoon.s01e02.mkv", "par3-umber-lagoon", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-zip-repair", ByteReproducible: true,
		Notes: "A stored ZIP under an external PAR3 set with two blocks zeroed in the member data.",
		Build: sequence(
			par3StagePayloads(zipped),
			par3Zip("archive.zip", zipped),
			par3Create(PAR3Spec{Index: "archive.par3", Sources: []string{"archive.zip"}, ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot("archive.zip"),
			zeroBlocks("archive.zip", par3BlockSize, [2]int64{40, 2}),
			par3ProveRepair(PAR3Proof{Index: "archive.par3", Restored: []string{"archive.zip"}}),
		),
		ExpectedOutputs: par3Expected(zipped),
	})

	// ------------------------------------------------------ inserted protection

	insideClean := prngPayload("azure.kiln.s01e01.mkv", "par3-azure-kiln-1", 4<<20)
	add(Recipe{
		Slug: "par3-inside-zip-clean", ByteReproducible: true,
		Notes: "A stored ZIP carrying its own PAR3 protection inserted by the reference, undamaged: the container must extract as an ordinary ZIP without the protection getting in the way.",
		Build: sequence(
			par3StagePayloads(insideClean),
			par3Zip("archive.zip", insideClean),
			par3Insert("archive.zip", 10),
		),
		ExpectedOutputs: par3Expected(insideClean),
	})

	insideRepair := prngPayload("azure.kiln.s01e02.mkv", "par3-azure-kiln-2", 4<<20)
	add(Recipe{
		Slug: "par3-inside-zip-repair", ByteReproducible: true,
		Notes: "A self-protected ZIP with 100 bytes of member data overwritten. There are no sidecars: the recovery data is inside the container being repaired.",
		Build: sequence(
			par3StagePayloads(insideRepair),
			par3Zip("archive.zip", insideRepair),
			par3Insert("archive.zip", 10),
			par3Snapshot("archive.zip"),
			scrambleOutput("archive.zip", 1_000_000, 100),
			par3ProveRepair(PAR3Proof{Index: "archive.zip", Inserted: true, Restored: []string{"archive.zip"}}),
		),
		ExpectedOutputs: par3Expected(insideRepair),
	})

	insideHeader := prngPayload("azure.kiln.s01e03.mkv", "par3-azure-kiln-3", 4<<20)
	add(Recipe{
		Slug: "par3-inside-zip-header-repair", ByteReproducible: true,
		Notes: "A self-protected ZIP whose leading local file header is zeroed, so the container does not parse as a ZIP until the inserted protection restores it.",
		Build: sequence(
			par3StagePayloads(insideHeader),
			par3Zip("archive.zip", insideHeader),
			par3Insert("archive.zip", 10),
			par3Snapshot("archive.zip"),
			zeroOutput("archive.zip", 0, 64),
			par3ProveRepair(PAR3Proof{Index: "archive.zip", Inserted: true, Restored: []string{"archive.zip"}}),
		),
		ExpectedOutputs: par3Expected(insideHeader),
	})

	insideInsufficient := prngPayload("azure.kiln.s01e04.mkv", "par3-azure-kiln-4", 4<<20)
	add(Recipe{
		Slug: "par3-inside-zip-insufficient", ByteReproducible: true,
		Notes: "A self-protected ZIP at 10% redundancy with a mebibyte of member data zeroed, more than the inserted protection can restore. The job must fail rather than deliver damaged bytes.",
		Build: sequence(
			par3StagePayloads(insideInsufficient),
			par3Zip("archive.zip", insideInsufficient),
			par3Insert("archive.zip", 10),
			zeroOutput("archive.zip", 1<<20, 1<<20),
			par3ProveUnrepairable(PAR3Proof{Index: "archive.zip", Inserted: true}),
		),
	})

	insideMissing := prngPayload("azure.kiln.s01e05.mkv", "par3-azure-kiln-5", 4<<20)
	add(Recipe{
		Slug: "par3-inside-zip-missing-article", ByteReproducible: true,
		Notes: "An undamaged self-protected ZIP posted at 16 KiB articles, with one interior article deleted by the scenario. The hole is far smaller than the inserted protection, so the container is restored from inside itself.",
		Build: sequence(
			par3StagePayloads(insideMissing),
			par3Zip("archive.zip", insideMissing),
			par3Insert("archive.zip", 10),
		),
		ExpectedOutputs: par3Expected(insideMissing),
	})

	inside7z := prngPayload("cinder.atlas.s01e04.mkv", "par3-cinder-atlas", 4<<20)
	add(Recipe{
		Slug: "par3-inside-7z-repair", ByteReproducible: true,
		Notes: "An uncompressed 7z carrying PAR3 protection inserted by the reference, with 100 bytes of packed data overwritten.",
		Build: sequence(
			par3StagePayloads(inside7z),
			func(ctx context.Context, env *Env) error {
				return env.SevenZip(ctx, SevenZipSpec{Archive: "archive.7z", Members: []string{inside7z.name},
					Store: true, Deterministic: true})
			},
			par3Insert("archive.7z", 10),
			par3Snapshot("archive.7z"),
			scrambleOutput("archive.7z", 1_000_000, 100),
			par3ProveRepair(PAR3Proof{Index: "archive.7z", Inserted: true, Restored: []string{"archive.7z"}}),
		),
		ExpectedOutputs: par3Expected(inside7z),
	})

	// ------------------------------------------------------ PAR2 beside PAR3

	fallback := prngPayload("lichen.harbor.s01e05.mkv", "par3-lichen-harbor", par3PayloadBytes)
	add(Recipe{
		Slug: "par3-par2-fallback", ByteReproducible: true,
		Notes: "A payload carrying both a PAR2 set with a single recovery block and a PAR3 set with eight, and four blocks damaged. PAR2 alone cannot repair it; PAR3 can, so the job must fall through to PAR3 rather than fail.",
		Build: sequence(
			par3WritePayloads(fallback),
			par2(PAR2Spec{Base: "lichen.harbor.s01e05.par2", SliceSize: par3BlockSize, RecoveryBlocks: 1, RecoveryFiles: 1,
				Sources: []string{fallback.name}}),
			par3Create(PAR3Spec{Index: "lichen.harbor.s01e05.par3", Sources: []string{fallback.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot(fallback.name),
			zeroBlocks(fallback.name, par3BlockSize, [2]int64{25, 4}),
			par3ProveRepair(PAR3Proof{Index: "lichen.harbor.s01e05.par3", Restored: []string{fallback.name}}),
		),
		ExpectedOutputs: par3Expected(fallback),
	})

	both := prngPayload("jasper.ferry.s01e02.mkv", "par3-jasper-ferry", par3PayloadBytes)
	add(Recipe{
		Slug: "par2-par3-both-sufficient", ByteReproducible: true,
		Notes: "A payload carrying a PAR2 set and a PAR3 set, each able to repair the two damaged blocks on its own. Either may win; exactly one repair must land and deliver the payload.",
		Build: sequence(
			par3WritePayloads(both),
			par2(PAR2Spec{Base: "jasper.ferry.s01e02.par2", SliceSize: par3BlockSize, RecoveryBlocks: 8, RecoveryFiles: 1,
				Sources: []string{both.name}}),
			par3Create(PAR3Spec{Index: "jasper.ferry.s01e02.par3", Sources: []string{both.name},
				ECC: 1, BlockSize: par3BlockSize, RecoveryBlocks: 8}),
			par3Snapshot(both.name),
			zeroBlocks(both.name, par3BlockSize, [2]int64{45, 2}),
			par3ProveRepair(PAR3Proof{Index: "jasper.ferry.s01e02.par3", Restored: []string{both.name}}),
		),
		ExpectedOutputs: par3Expected(both),
	})

	return recipes
}

// par3RAR writes a compressed RAR set over staged payloads.
func par3RAR(writer string, format RARFormat, volumeSize, headerPassword string, members ...string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		return env.RAR(ctx, RARSpec{
			Toolchain: writer, Format: format, Archive: "archive.rar",
			Method: "-m1", VolumeSize: volumeSize, HeaderPassword: headerPassword, Members: members,
		})
	}
}

// par3Zip writes a stored ZIP over staged payloads.
func par3Zip(archive string, payloads ...par3Payload) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		members := make([]Member, 0, len(payloads))
		for _, payload := range payloads {
			members = append(members, Member{Name: payload.name, Source: env.StagePath(payload.name)})
		}
		return WriteZip(env.OutputPath(archive), members, "")
	}
}
