package fixturegen

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Passwords the scenarios declare. A fixture whose scenario supplies the wrong
// password, or none at all, is written with a password that appears nowhere
// else in the corpus.
const (
	CorpusPassword         = "e2e-test-password"
	FilenamePassword       = "e2e-rar-filename-password"
	DirectStorePassword    = "weaver-e2e-direct-password"
	MissingMiddlePassword  = "weaver-e2e-repair-password"
	UndisclosedPassword    = "e2e-undisclosed-archive-password"
	UndisclosedPasswordAlt = "e2e-undisclosed-archive-password-2"
)

// Damage constants. Every "corrupted" fixture zeroes the same 1 MiB window,
// far enough into the file that the container header still parses and the
// failure surfaces as a checksum mismatch rather than an unreadable archive.
const (
	CorruptOffset = 10 << 20
	CorruptLength = 1 << 20
	TruncateBytes = 1 << 20
)

// The shapes the four recovery-gap fixtures are cut to. They are constants
// rather than literals because each one appears in two places that must agree:
// the recovery set is created at this block size, and the damage is expressed
// in whole blocks of it, so "five of the sidecar's eight blocks" stays true
// when a payload changes size.
const (
	// ignorableSliceSize keeps a 32 KiB sidecar at eight blocks while the
	// 5 MiB payload beside it stays well inside PAR2's slice ceiling.
	ignorableSliceSize    = 4096
	ignorableSidecarBytes = 32768
	// partialVolumeSliceSize puts the 5 MiB payload at eighty blocks, so
	// forty-eight recovery blocks over two volumes is 60% redundancy and
	// neither volume alone covers thirty destroyed blocks.
	partialVolumeSliceSize = 65536
	// splitPar2SliceSize and splitPar2Parts cut the same payload into three
	// equal parts; three zeroed blocks inside the middle part touch at most
	// four blocks of the joined file, well under the eight recovery blocks.
	splitPar2SliceSize = 65536
	splitPar2Parts     = 3
	// The multi-set archive fixture exercises common-refinement checkpointing:
	// its PAR2 sets deliberately use non-dividing grids. Keep the secondary
	// damage window in primary-grid units so changing the secondary grid cannot
	// silently change the recoverability shape.
	twoSetsSliceSize          = 65536
	twoSetsPrimarySliceSize   = twoSetsSliceSize
	twoSetsSecondarySliceSize = 98304
	twoSetsDamageOffset       = 30 * twoSetsPrimarySliceSize
	twoSetsDamageLength       = 2 * twoSetsPrimarySliceSize
)

// ignorableSidecarNotesText is the sidecar payload of the ignorable-deficit
// set. WriteText pads it to a fixed length, so what matters is that it is a
// deterministic run of bytes; the title is invented.
const ignorableSidecarNotesText = "Golden Meridian - season two, episode three. Release notes beside the payload; every name here is invented.\n"

// The payload paths recipes name as provenance: ledger paths that carry
// exactly the bytes the artifact cache feeds to the oracles.
const (
	samplePayloadPath  = "testdata/single-mkv/test-media.mkv"
	previewPayloadPath = "testdata/mixed-archive/sample-preview.mkv"
	sharedClipPath     = "testdata/shared/short-720p-av1.mkv"
)

// Recipe is one scenario directory: what it was built from, and the code that
// builds it. The file set is not restated here — the ledger entries for the
// slug are the contract, and the engine fails a recipe that produces anything
// else.
type Recipe struct {
	// Slug is the directory name under testdata/.
	Slug string
	// Family groups recipes that share a shape and a review argument.
	Family string
	// Notes explains the shape the scenario depends on.
	Notes string
	// Inputs are root-relative ledger paths whose bytes this fixture is built
	// from, recorded as provenance.
	Inputs []string
	// ByteReproducible is true only when nothing in the chain stamps a
	// timestamp or draws a salt.
	ByteReproducible bool
	// Build populates the scenario's output directory.
	Build func(ctx context.Context, env *Env) error
	// ExpectedOutputs maps an extracted member name to the host file whose
	// BLAKE3 the scenario pins. Only the scenarios that carry an
	// `expectedOutputBLAKE3` block declare it.
	ExpectedOutputs func(ctx context.Context, env *Env) (map[string]string, error)
}

// ScenarioOnly lists the scenario directories that legitimately carry no
// fixture bytes: they stage another scenario's assets through their
// scenario.json `fixtureAssets`, or they need no payload at all.
var ScenarioOnly = map[string]string{
	"multiserver-backup-par2-repair":          "stages par2-small-repair's archive and sidecars across two servers",
	"multiserver-primary-corrupt-direct":      "stages single-mkv's payload; the damage is injected by the fake NNTP server",
	"multiserver-primary-missing-direct":      "stages single-mkv's payload; articles are withheld at post time",
	"direct-store-post-repair-queue-liveness": "stages the small direct-store PAR2 repair corpus for the queue-liveness flow",
	"obfuscated-rar-retry-7z":                 "stages rar5-corrupted and single-7z under one obfuscated name",
	"par2-direct-late-malformed-chain-rebind": "stages a PAR2-bound direct RAR set whose swapped tail pair demotes after routed coverage exists",
	"par2-opaque-magic-rebind":                "stages obfuscated rar5-multivolume beside an extensionless PAR2 index",
	"par2-obfuscated-rar-repair":              "stages obfuscated rar5-multivolume beside par2-obfuscated-rar-rewrite's sidecars and withholds an interior tail article",
	"par2-multi-grid-late-discovery":          "stages one indexed payload and one independent late-set payload from the clean PAR2 corpus",
	"par2-optional-prefix-hole":               "stages a clean payload beside an optional recovery carrier with its leading article withheld",
	"par2-rar-placement-normalization":        "stages rar5-multivolume under swapped names beside par2-obfuscated-rar-rewrite's sidecars",
	"par2-rar-placement-stripped-recovery":    "the same staging, with the recovery volumes stripped after posting",
	"rar5-multivolume-missing-tail":           "stages the first two volumes of rar5-multivolume and omits the third",
	"single-mkv-sparse-nzb":                   "stages single-mkv's payload under a sparse NZB numbering",
	"stat-health-probe":                       "stages single-mkv's payload; articles are deleted by the harness",
}

// Recipes is the corpus, one entry per scenario directory that owns bytes.
func Recipes() []Recipe {
	recipes := make([]Recipe, 0, 128)
	add := func(recipe Recipe) { recipes = append(recipes, recipe) }

	// ------------------------------------------------------- shared clips
	add(Recipe{
		Slug: "shared", Family: "shared clips",
		Notes: "The source clips generators encode their payloads from. Nothing but FFmpeg's synthetic generators goes into them.",
		Build: func(ctx context.Context, env *Env) error { return env.PublishAll(ctx, "clip-shared") },
	})

	// ------------------------------------------------------- direct media
	for _, media := range []struct{ slug, notes string }{
		{"single-mkv", "The plain direct payload, and the clip every archive family wraps."},
		{"health-failure", "The same payload; the harness drops half its articles to force a health abort."},
		{"large-segments", "The same payload, posted at a much smaller segment size."},
	} {
		add(Recipe{
			Slug: media.slug, Family: "direct media", Notes: media.notes,
			Inputs: []string{samplePayloadPath},
			Build: func(ctx context.Context, env *Env) error {
				return env.Publish(ctx, "clip-sample", "test-media.mkv")
			},
		})
	}

	add(Recipe{
		Slug: "split-plain-mkv", Family: "direct media",
		Notes: "A short clip cut into two plain parts at 256 KiB. The split is a byte split: nothing in it understands Matroska. " +
			"The payload is the clip-small artifact, not the 1080p clip the archive families wrap, and no other scenario carries those bytes: " +
			"the two parts are the only ledger paths that hold them, so the recipe declares no inputs.",
		Build: func(ctx context.Context, env *Env) error {
			source, err := env.ArtifactPath(ctx, "clip-small")
			if err != nil {
				return err
			}
			_, err = SplitFile(source, 256<<10, func(index int) string {
				return env.OutputPath(fmt.Sprintf("test-media.mkv.%03d", index+1))
			})
			return err
		},
	})

	add(Recipe{
		Slug: "mixed-archive", Family: "mixed",
		Notes:  "A RAR5 archive posted beside a loose preview clip and an NFO, so classification has to cope with an archive that is not the whole release.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Publish(ctx, "rar5-root-sample", "archive.rar"); err != nil {
				return err
			}
			if err := env.Publish(ctx, "clip-preview", "sample-preview.mkv"); err != nil {
				return err
			}
			return WriteText(env.OutputPath("info.nfo"), "Sample NFO file for e2e testing", 32)
		},
	})

	// --------------------------------------------------------------- RAR5
	add(Recipe{
		Slug: "rar5-single", Family: "RAR5",
		Notes:  "The RAR5 baseline: one compressed member under a directory.",
		Inputs: []string{samplePayloadPath},
		Build:  publish("rar5-work-sample", "archive.rar"),
	})

	add(Recipe{
		Slug: "rar5-corrupted", Family: "RAR5",
		Notes:  "The root-member RAR5 with a 1 MiB window of payload zeroed, so extraction fails on the member checksum rather than on a broken header.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publish("rar5-root-sample", "archive.rar"),
			zeroOutput("archive.rar", CorruptOffset, CorruptLength),
		),
	})

	add(Recipe{
		Slug: "rar5-solid", Family: "RAR5",
		Notes:  "A solid RAR5 run: the clip first, then two tiny text members that are only decodable against it.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "sample.mkv"); err != nil {
				return err
			}
			if err := stageSolidText(env); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Solid: true,
				Members: []string{"sample.mkv", "file1.txt", "file2.txt"},
			})
		},
	})

	add(Recipe{
		Slug: "rar5-encrypted", Family: "RAR5",
		Notes:  "RAR5 with encrypted headers, opened with the password the scenario supplies.",
		Inputs: []string{samplePayloadPath},
		Build:  singleMemberRAR(RAR5Writer, RAR5, "work/sample.mkv", RARSpec{Method: "-m1", Dictionary: "-md32m", HeaderPassword: CorpusPassword}),
	})

	add(Recipe{
		Slug: "rar5-hp-encrypted", Family: "RAR5",
		Notes:  "The header-encrypted shape under its own slug, with the member at input/ so the two fixtures are not the same bytes.",
		Inputs: []string{samplePayloadPath},
		Build:  singleMemberRAR(RAR5Writer, RAR5, "input/sample.mkv", RARSpec{Method: "-m1", Dictionary: "-md32m", HeaderPassword: CorpusPassword}),
	})

	add(Recipe{
		Slug: "rar5-no-password-meta", Family: "RAR5",
		Notes:  "Member data is encrypted and the headers stay readable, but the scenario declares no password at all, so extraction must fail rather than guess.",
		Inputs: []string{samplePayloadPath},
		Build:  singleMemberRAR(RAR5Writer, RAR5, "sample.mkv", RARSpec{Method: "-m1", Dictionary: "-md32m", Password: UndisclosedPassword}),
	})

	add(Recipe{
		Slug: "rar5-wrong-password", Family: "RAR5",
		Notes:  "Member data encrypted with a password the scenario does not know: the scenario deliberately supplies a different one.",
		Inputs: []string{samplePayloadPath},
		Build:  singleMemberRAR(RAR5Writer, RAR5, "sample.mkv", RARSpec{Method: "-m1", Dictionary: "-md32m", Password: UndisclosedPasswordAlt}),
	})

	add(Recipe{
		Slug: "rar5-multi-member", Family: "RAR5",
		Notes:  "Three full-length episode members in one non-solid RAR5, the season-pack shape.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			members := make([]string, 0, 3)
			for index := 1; index <= 3; index++ {
				member := fmt.Sprintf("tmp/episodes/Test.Show.S01E%02d.1080p.mkv", index)
				if err := env.Stage(ctx, "clip-sample", member); err != nil {
					return err
				}
				members = append(members, member)
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Members: members,
			})
		},
	})

	add(Recipe{
		Slug: "rar5-solid-multi-member", Family: "RAR5",
		Notes:  "Three 25 MiB members in one solid run.",
		Inputs: []string{samplePayloadPath},
		Build:  episodeMembersRAR(RAR5Writer, RAR5, "ep%d.mkv", RARSpec{Method: "-m1", Dictionary: "-md32m", Solid: true}),
	})

	add(Recipe{
		Slug: "rar5-multi-member-encrypted", Family: "RAR5",
		Notes:  "Three members, each with its own encrypted data and readable headers.",
		Inputs: []string{samplePayloadPath},
		Build:  episodeMembersRAR(RAR5Writer, RAR5, "Show.S01E%02d.720p.mkv", RARSpec{Method: "-m1", Dictionary: "-md32m", Password: CorpusPassword}),
	})

	add(Recipe{
		Slug: "rar5-solid-encrypted", Family: "RAR5",
		Notes:  "A solid, data-encrypted RAR5: a 25 MiB member followed by the full clip, which is only decodable through it.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			if err := env.StageIndexed(ctx, "clip-episodes", 0, "ep1.mkv"); err != nil {
				return err
			}
			if err := env.Stage(ctx, "clip-sample", "sample.mkv"); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Solid: true, Password: CorpusPassword,
				Members: []string{"ep1.mkv", "sample.mkv"},
			})
		},
	})

	add(Recipe{
		Slug: "rar5-multivolume", Family: "RAR5",
		Notes:  "Three 30 000 KiB RAR5 volumes. Four other scenarios stage these exact bytes, so they come from the shared artifact rather than a second run of the writer.",
		Inputs: []string{samplePayloadPath},
		Build:  publishAll("rar5-multivolume"),
	})

	add(Recipe{
		Slug: "rar5-multivolume-encrypted", Family: "RAR5",
		Notes:  "Three 30 MiB RAR5 volumes with encrypted member data.",
		Inputs: []string{samplePayloadPath},
		Build:  multiVolumeRAR(RAR5Writer, RAR5, "input/sample.mkv", "30m", 3, RARSpec{Method: "-m1", Dictionary: "-md32m", Password: CorpusPassword}),
	})

	add(Recipe{
		Slug: "rar5-solid-multivolume", Family: "RAR5",
		Notes:  "A solid RAR5 run spread over four 30 MiB volumes, so a solid decode has to cross volume boundaries.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			if err := env.StageIndexed(ctx, "clip-episodes", 0, "ep1.mkv"); err != nil {
				return err
			}
			if err := env.Stage(ctx, "clip-sample", "sample.mkv"); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Solid: true, VolumeSize: "30m",
				Members: []string{"ep1.mkv", "sample.mkv"},
			}); err != nil {
				return err
			}
			return expectParts(env, "archive.part%d.rar", 4)
		},
	})

	add(Recipe{
		Slug: "rar5-filename-dedupe", Family: "RAR5",
		Notes: "Two header-encrypted RAR5 archives whose posted names collide once the illegal character is normalised, so the second has to be deduplicated rather than overwrite the first.",
		Build: func(ctx context.Context, env *Env) error {
			for _, side := range []struct{ member, text, output string }{
				{"dedupe-a-output.mkv", "silver horizon dedupe fixture a", "dedupe?archive.rar"},
				{"dedupe-b-output.mkv", "amber trail dedupe fixture b", "dedupe_archive.rar"},
			} {
				if err := WriteText(env.StagePath(side.member), side.text, 34); err != nil {
					return err
				}
				if err := env.RAR(ctx, RARSpec{
					Toolchain: RAR5Writer, Format: RAR5, Archive: "staged.rar",
					Method: "-m0", HeaderPassword: FilenamePassword, Members: []string{side.member},
				}); err != nil {
					return err
				}
				if err := os.Rename(env.OutputPath("staged.rar"), env.OutputPath(side.output)); err != nil {
					return err
				}
				if err := os.Remove(env.StagePath(side.member)); err != nil {
					return err
				}
			}
			return nil
		},
	})

	add(Recipe{
		Slug: "rar5-filename-normalization", Family: "RAR5",
		Notes: "One header-encrypted RAR5 posted under a name containing a character the filesystem cannot carry.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WriteText(env.StagePath("normalized-output.mkv"), "amber trail normalization fixture", 39); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "staged.rar",
				Method: "-m0", HeaderPassword: FilenamePassword, Members: []string{"normalized-output.mkv"},
			}); err != nil {
				return err
			}
			return os.Rename(env.OutputPath("staged.rar"), env.OutputPath("normalize?encrypted.rar"))
		},
	})

	add(Recipe{
		Slug: "unicode-filenames", Family: "RAR5",
		Notes:  "A RAR5 whose member name is a multi-script invented title, so filename decoding is exercised end to end.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			member := "tmp/uni/Ámbar-Sendëro-夜明けの地平.mkv"
			if err := env.Stage(ctx, "clip-sample", member); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Members: []string{member},
			})
		},
	})

	add(Recipe{
		Slug: "empty-rar", Family: "RAR5",
		Notes: "A valid RAR5 whose only member is a zero-length file.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WriteText(env.StagePath("empty.txt"), "", 0); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m0", Members: []string{"empty.txt"},
			})
		},
	})

	add(Recipe{
		Slug: "rar5-solid-encrypted-missing-middle-par2", Family: "RAR5",
		Notes:  "RAR 7.23 writes a solid, data-encrypted set over a shared clip with -p and never -hp, so headers stay readable; par2cmdline-turbo describes the intact set with 382 recovery blocks, and only then is the interior volume removed.",
		Inputs: []string{sharedClipPath},
		Build: func(ctx context.Context, env *Env) error {
			clip, err := env.ArtifactFile(ctx, "clip-shared", "short-720p-av1.mkv")
			if err != nil {
				return err
			}
			if err := CopyFile(clip, env.StagePath("work/payload/movie.mkv")); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: "rarlab-7.23", Format: RAR5, Archive: "archive.rar",
				Method: "-m3", Solid: true, Password: MissingMiddlePassword, VolumeSize: "22m",
				Members: []string{"work/payload/movie.mkv"},
			}); err != nil {
				return err
			}
			if err := expectParts(env, "archive.part%d.rar", 4); err != nil {
				return err
			}
			// 382 recovery blocks, not "35 percent": the ledger names the tail
			// recovery file archive.vol255+127.par2, and a percentage would
			// re-derive a different block count the moment the payload clip
			// changes size by a byte.
			if err := env.PAR2(ctx, PAR2Spec{
				Base: "archive.par2", SliceSize: 65536, RecoveryBlocks: 382,
				Sources: []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar", "archive.part4.rar"},
			}); err != nil {
				return err
			}
			return removeOutput(env, "archive.part3.rar")
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			clip, err := env.ArtifactFile(ctx, "clip-shared", "short-720p-av1.mkv")
			if err != nil {
				return nil, err
			}
			return map[string]string{"work/payload/movie.mkv": clip}, nil
		},
	})

	// --------------------------------------------------------------- RAR4
	add(Recipe{
		Slug: "rar4-single", Family: "RAR4",
		Notes:  "The RAR4 baseline, stored rather than compressed.",
		Inputs: []string{samplePayloadPath},
		Build:  publish("rar4-work-sample", "archive.rar"),
	})

	add(Recipe{
		Slug: "rar4-corrupted", Family: "RAR4",
		Notes:  "The RAR4 baseline with the same 1 MiB payload window zeroed.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publish("rar4-work-sample", "archive.rar"),
			zeroOutput("archive.rar", CorruptOffset, CorruptLength),
		),
	})

	add(Recipe{
		Slug: "rar4-solid", Family: "RAR4",
		Notes:  "A solid RAR4 run with two trailing text members.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Stage(ctx, "clip-sample", "sample.mkv"); err != nil {
				return err
			}
			if err := stageSolidText(env); err != nil {
				return err
			}
			return env.RAR(ctx, RARSpec{
				Toolchain: RAR4Writer, Format: RAR4, Archive: "archive.rar",
				Method: "-m1", Solid: true, Members: []string{"sample.mkv", "file1.txt", "file2.txt"},
			})
		},
	})

	add(Recipe{
		Slug: "rar4-encrypted", Family: "RAR4",
		Notes:  "RAR4 with encrypted headers.",
		Inputs: []string{samplePayloadPath},
		Build:  singleMemberRAR(RAR4Writer, RAR4, "work/sample.mkv", RARSpec{Method: "-m1", HeaderPassword: CorpusPassword}),
	})

	add(Recipe{
		Slug: "rar4-member-encrypted", Family: "RAR4",
		Notes:  "RAR4 with encrypted member data and readable headers.",
		Inputs: []string{samplePayloadPath},
		Build:  singleMemberRAR(RAR4Writer, RAR4, "input/sample.mkv", RARSpec{Method: "-m1", Password: CorpusPassword}),
	})

	add(Recipe{
		Slug: "rar4-multi-member", Family: "RAR4",
		Notes:  "Three stored 25 MiB members in one RAR4.",
		Inputs: []string{samplePayloadPath},
		Build:  episodeMembersRAR(RAR4Writer, RAR4, "ep%d.mkv", RARSpec{Method: "-m0"}),
	})

	add(Recipe{
		Slug: "rar4-multi-member-encrypted", Family: "RAR4",
		Notes:  "Three encrypted 25 MiB members in one RAR4.",
		Inputs: []string{samplePayloadPath},
		Build:  episodeMembersRAR(RAR4Writer, RAR4, "Show.S01E%02d.720p.mkv", RARSpec{Method: "-m1", Password: CorpusPassword}),
	})

	add(Recipe{
		Slug: "rar4-multivolume", Family: "RAR4",
		Notes:  "Three 30 MiB RAR4 volumes.",
		Inputs: []string{samplePayloadPath},
		Build:  multiVolumeRAR(RAR4Writer, RAR4, "input/sample.mkv", "30m", 3, RARSpec{Method: "-m1"}),
	})

	add(Recipe{
		Slug: "rar4-multivolume-encrypted", Family: "RAR4",
		Notes:  "Three 30 MiB RAR4 volumes with encrypted member data.",
		Inputs: []string{samplePayloadPath},
		Build:  multiVolumeRAR(RAR4Writer, RAR4, "input/sample.mkv", "30m", 3, RARSpec{Method: "-m1", Password: CorpusPassword}),
	})

	// -------------------------------------------------- RAR recovery volumes
	// RARLAB writes the data volumes and the standalone `.rev` recovery
	// volumes in one `-rv<N>` invocation; the fixture then withholds whole data
	// volumes so the product has to reconstruct them from the `.rev` files.
	// Go never touches a RAR structure here — it only removes files.
	add(Recipe{
		Slug: "rar5-recovery-volume-light", Family: "RAR recovery volumes",
		Notes:           "Four 22 MiB RAR5 volumes plus one RARLAB recovery volume; the second data volume is withheld, so extraction succeeds only after the product rebuilds it from archive.part1.rev.",
		Inputs:          []string{sharedClipPath},
		Build:           recoveryVolumeRAR(RAR5Writer, RAR5, RARSpec{Method: "-m1", Dictionary: "-md32m"}, "22m", 4, 1, "archive.part2.rar"),
		ExpectedOutputs: sharedClipExpectedOutput("work/payload/movie.mkv"),
	})

	add(Recipe{
		Slug: "rar5-recovery-volume-heavy", Family: "RAR recovery volumes",
		Notes:           "Four 22 MiB RAR5 volumes plus two RARLAB recovery volumes; the second and fourth data volumes are withheld, which is exactly the two the recovery volumes can rebuild.",
		Inputs:          []string{sharedClipPath},
		Build:           recoveryVolumeRAR(RAR5Writer, RAR5, RARSpec{Method: "-m1", Dictionary: "-md32m"}, "22m", 4, 2, "archive.part2.rar", "archive.part4.rar"),
		ExpectedOutputs: sharedClipExpectedOutput("work/payload/movie.mkv"),
	})

	add(Recipe{
		Slug: "rar5-recovery-volume-insufficient", Family: "RAR recovery volumes",
		Notes:  "Four 22 MiB RAR5 volumes with a single RARLAB recovery volume, but two data volumes withheld: one .rev cannot rebuild two volumes, so extraction must fail rather than produce a truncated member.",
		Inputs: []string{sharedClipPath},
		Build:  recoveryVolumeRAR(RAR5Writer, RAR5, RARSpec{Method: "-m1", Dictionary: "-md32m"}, "22m", 4, 1, "archive.part2.rar", "archive.part3.rar"),
	})

	add(Recipe{
		Slug: "rar4-recovery-volume-light", Family: "RAR recovery volumes",
		Notes:           "The RAR4 counterpart: four 22 MiB RAR4 volumes plus one RAR3-format recovery volume, third data volume withheld.",
		Inputs:          []string{sharedClipPath},
		Build:           recoveryVolumeRAR(RAR4Writer, RAR4, RARSpec{Method: "-m1"}, "22m", 4, 1, "archive.part3.rar"),
		ExpectedOutputs: sharedClipExpectedOutput("work/payload/movie.mkv"),
	})

	// ------------------------------------------------------------- nested
	add(Recipe{
		Slug: "nested-rar", Family: "nested",
		Notes: "One archive inside another: two levels, which extraction is expected to unwrap.", Inputs: []string{samplePayloadPath},
		Build: nestRAR(1),
	})
	add(Recipe{
		Slug: "nested-xz-rar", Family: "nested",
		Notes:  "A RAR5 containing an ordinary MKV beside an XZ-compressed NFO, so the nested pass must unpack only the sidecar and preserve its sibling.",
		Inputs: []string{previewPayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			clip, err := env.ArtifactPath(ctx, "clip-preview")
			if err != nil {
				return err
			}
			if err := CopyFile(clip, env.StagePath("test-media.mkv")); err != nil {
				return err
			}
			if err := WriteText(env.StagePath("release.nfo"), "Nested XZ sidecar beside ordinary media.", 96); err != nil {
				return err
			}
			if err := env.SevenZip(ctx, SevenZipSpec{
				Format: "xz", Archive: "release.nfo.xz", Members: []string{"release.nfo"}, Level: "-mx1",
			}); err != nil {
				return err
			}
			if err := CopyFile(env.OutputPath("release.nfo.xz"), env.StagePath("release.nfo.xz")); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
				Method: "-m1", Dictionary: "-md32m", Members: []string{"test-media.mkv", "release.nfo.xz"},
			}); err != nil {
				return err
			}
			return removeOutput(env, "release.nfo.xz")
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			clip, err := env.ArtifactPath(ctx, "clip-preview")
			if err != nil {
				return nil, err
			}
			return map[string]string{
				"test-media.mkv": clip,
				"release.nfo":    env.StagePath("release.nfo"),
			}, nil
		},
	})
	add(Recipe{
		Slug: "nested-3deep", Family: "nested",
		Notes: "Three levels of nesting, the deepest the extractor is expected to follow.", Inputs: []string{samplePayloadPath},
		Build: nestRAR(2),
	})
	add(Recipe{
		Slug: "nested-5deep", Family: "nested",
		Notes: "Five levels of nesting, one past the limit, so the job must stop with a depth error rather than recurse.", Inputs: []string{samplePayloadPath},
		Build: nestRAR(4),
	})
	add(Recipe{
		Slug: "nested-obfuscated-split-7z", Family: "nested",
		Notes:  "A stored 7z whose members are the three obfuscated split volumes, so the inner set only appears once the outer archive is opened.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			source, err := env.ArtifactPath(ctx, "sevenzip-single")
			if err != nil {
				return err
			}
			names := obfuscatedNames(10)
			if _, err := SplitFile(source, 30<<20, func(index int) string { return env.StagePath(names[index]) }); err != nil {
				return err
			}
			return env.SevenZip(ctx, SevenZipSpec{Archive: "archive.7z", Members: names, Store: true})
		},
	})

	// ----------------------------------------------------------------- 7z
	add(Recipe{
		Slug: "single-7z", Family: "7z",
		Notes: "One LZMA2 7z over the clip.", Inputs: []string{samplePayloadPath},
		Build: publish("sevenzip-single", "archive.7z"),
	})
	add(Recipe{
		Slug: "single-7z-corrupted", Family: "7z",
		Notes: "The same 7z truncated by 1 MiB, so the packed stream ends before its declared length.", Inputs: []string{samplePayloadPath},
		Build: sequence(publish("sevenzip-single", "archive.7z"), truncateOutput("archive.7z", TruncateBytes)),
	})
	add(Recipe{
		Slug: "7z-encrypted", Family: "7z",
		Notes: "Member data encrypted, headers readable.", Inputs: []string{samplePayloadPath},
		Build: publish("sevenzip-encrypted", "archive.7z"),
	})
	add(Recipe{
		Slug: "split-7z", Family: "7z",
		Notes:  "The plain 7z cut into three 30 MiB volumes. A 7z volume set is a plain byte split, so Go performs it and the volumes stay byte-identical to the whole archive.",
		Inputs: []string{samplePayloadPath},
		Build:  splitSevenZip("sevenzip-single"),
	})
	add(Recipe{
		Slug: "split-7z-encrypted", Family: "7z",
		Notes: "The encrypted 7z cut into three 30 MiB volumes.", Inputs: []string{samplePayloadPath},
		Build: splitSevenZip("sevenzip-encrypted"),
	})
	add(Recipe{
		Slug: "split-7z-corrupted", Family: "7z",
		Notes:  "A split 7z with two independent faults: a zeroed window inside the second volume and a truncated tail volume.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			splitSevenZip("sevenzip-single"),
			zeroOutput("archive.7z.002", 5<<20, CorruptLength),
			truncateOutput("archive.7z.003", TruncateBytes),
		),
	})

	// -------------------------------------------------------- obfuscation
	add(Recipe{
		Slug: "obfuscated-rar", Family: "obfuscated",
		Notes:  "The RAR5 baseline posted under a hex name with PAR2 sidecars, so the set has to be identified from its parity rather than its extension.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publish("rar5-work-sample", "a8f3b2c1d9e7.rar"),
			par2(PAR2Spec{Base: "a8f3b2c1d9e7.rar.par2", BlockCount: 2000, RecoveryBlocks: 200, RecoveryFiles: 1,
				Sources: []string{"a8f3b2c1d9e7.rar"}}),
		),
	})
	add(Recipe{
		Slug: "obfuscated-rar-unknown-numeric", Family: "obfuscated",
		Notes:  "The RAR5 volume set renamed to .10/.11/.12, the numbering an unknown-extension split uses.",
		Inputs: []string{samplePayloadPath},
		Build:  renameMultivolume("rar5-multivolume", 10),
	})
	add(Recipe{
		Slug: "obfuscated-rar-split-topology", Family: "obfuscated",
		Notes:  "The same volumes renamed to .100/.101/.102, so the topology has to be read from the volume headers rather than the suffix width.",
		Inputs: []string{samplePayloadPath},
		Build:  renameMultivolume("rar5-multivolume", 100),
	})
	add(Recipe{
		Slug: "obfuscated-split-7z", Family: "obfuscated",
		Notes:  "The split 7z renamed to .10/.11/.12.",
		Inputs: []string{samplePayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			source, err := env.ArtifactPath(ctx, "sevenzip-single")
			if err != nil {
				return err
			}
			names := obfuscatedNames(10)
			_, err = SplitFile(source, 30<<20, func(index int) string { return env.OutputPath(names[index]) })
			return err
		},
	})

	// --------------------------------------------------------------- PAR2
	add(Recipe{
		Slug: "par2-repair", Family: "PAR2",
		Notes:  "A single damaged block in a 2000-block set with 600 recovery blocks: the ordinary repair path.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publish("rar5-work-sample", "archive.rar"),
			par2(PAR2Spec{Base: "archive.rar.par2", BlockCount: 2000, RecoveryBlocks: 600, RecoveryFiles: 1,
				Sources: []string{"archive.rar"}}),
			func(ctx context.Context, env *Env) error {
				return OverwriteRange(env.OutputPath("archive.rar"), 1000, PatternBytes("par2-repair", 100))
			},
		),
	})

	for _, slug := range []string{"par2-heavy-damage", "par2-heavy-damage-a", "par2-heavy-damage-b", "par2-heavy-damage-c"} {
		add(Recipe{
			Slug: slug, Family: "PAR2",
			Notes:  "Three of 200 blocks destroyed against 30 recovery blocks. The four heavy-damage slugs are the same set, so it is built once and published four times.",
			Inputs: []string{samplePayloadPath},
			Build:  publishAll("par2-heavy-set"),
		})
	}

	add(Recipe{
		Slug: "par2-insufficient", Family: "PAR2",
		Notes:  "Sixteen of 50 blocks destroyed against a single recovery block, so repair is correctly refused rather than attempted.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publish("rar5-par2-base", "archive.rar"),
			par2(PAR2Spec{Base: "archive.rar.par2", BlockCount: 50, RecoveryBlocks: 1, Sources: []string{"archive.rar"}}),
			func(ctx context.Context, env *Env) error {
				slice, err := PAR2SliceSize(env.OutputPath("archive.rar.par2"))
				if err != nil {
					return err
				}
				return ZeroRange(env.OutputPath("archive.rar"), 6*slice, 16*slice)
			},
		),
	})

	for _, slug := range []string{"par2-small-repair", "par2-small-repair-a", "par2-small-repair-b", "par2-small-repair-c", "par2-small-repair-d"} {
		add(Recipe{
			Slug: slug, Family: "PAR2",
			Notes:  "A 5 MiB set with ten damaged blocks and 24 recovery blocks. The five small-repair slugs and the multiserver backup repair all stage these bytes.",
			Inputs: []string{previewPayloadPath},
			Build:  publishAll("par2-small-set"),
		})
	}

	add(Recipe{
		Slug: "par2-multivolume", Family: "PAR2",
		Notes:  "Parity across four RAR5 volumes with one block destroyed in the second, so the recovery set has to identify which file is short.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publishAll("rar5-par2-multivolume"),
			par2(PAR2Spec{Base: "archive.rar.par2", SliceSize: 566800, RecoveryBlocks: 15,
				Sources: []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar", "archive.part4.rar"}}),
			zeroOutput("archive.part2.rar", 10*566800, 566800),
		),
	})

	add(Recipe{
		Slug: "par2-rar4", Family: "PAR2",
		Notes:  "Parity over a RAR4 archive with one block destroyed, so PAR2 handling is exercised independently of the RAR generation.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publish("rar4-root-sample", "archive.rar"),
			par2(PAR2Spec{Base: "archive.rar.par2", BlockCount: 100, RecoveryBlocks: 10, Sources: []string{"archive.rar"}}),
			func(ctx context.Context, env *Env) error {
				slice, err := PAR2SliceSize(env.OutputPath("archive.rar.par2"))
				if err != nil {
					return err
				}
				return ZeroRange(env.OutputPath("archive.rar"), 10*slice, slice)
			},
		),
	})

	add(Recipe{
		Slug: "par2-7z-repair", Family: "PAR2",
		Notes:  "Parity over a 7z with two 64 KiB blocks zeroed well past the header, so repair runs before extraction.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publish("sevenzip-single", "archive.7z"),
			par2(PAR2Spec{Base: "archive.7z.par2", SliceSize: 65536, RecoveryBlocks: 4, Sources: []string{"archive.7z"}}),
			zeroOutput("archive.7z", 128*65536, 2*65536),
		),
	})

	add(Recipe{
		Slug: "par2-direct-repair", Family: "PAR2",
		Notes:  "Parity over a loose payload rather than an archive, with the same two-block damage.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			publish("clip-sample", "test-media.mkv"),
			par2(PAR2Spec{Base: "test-media.mkv.par2", SliceSize: 65536, RecoveryBlocks: 4, Sources: []string{"test-media.mkv"}}),
			zeroOutput("test-media.mkv", 128*65536, 2*65536),
		),
	})

	add(Recipe{
		Slug: "par2-obfuscated-rar-rewrite", Family: "PAR2",
		Notes: "Sidecars only. The recovery set is computed over rar5-multivolume's volumes and the volumes are then withheld, because three scenarios post that payload under other names and rely on PAR2 to rewrite its identity.",
		Inputs: []string{
			"testdata/rar5-multivolume/archive.part1.rar",
			"testdata/rar5-multivolume/archive.part2.rar",
			"testdata/rar5-multivolume/archive.part3.rar",
		},
		Build: func(ctx context.Context, env *Env) error {
			volumes := []string{"archive.part1.rar", "archive.part2.rar", "archive.part3.rar"}
			if err := env.PublishAll(ctx, "rar5-multivolume"); err != nil {
				return err
			}
			if err := env.PAR2(ctx, PAR2Spec{
				Base: "archive.rar.par2", SliceSize: 41856, RecoveryBlocks: 15, Sources: volumes,
			}); err != nil {
				return err
			}
			for _, volume := range volumes {
				if err := removeOutput(env, volume); err != nil {
					return err
				}
			}
			return nil
		},
	})

	add(Recipe{
		Slug: "par2-rar-placement-normalization-multi-swap", Family: "PAR2",
		Notes: "Six small RAR5 volumes staged with two swapped pairs, 2 with 3 and 5 with 6, beside canonical sidecars, so placement normalisation has to move two pairs without ever verifying.",
		Build: func(ctx context.Context, env *Env) error {
			if err := env.PublishAll(ctx, "rar5-lz-volumes"); err != nil {
				return err
			}
			names := make([]string, 0, 6)
			for index := 1; index <= 6; index++ {
				names = append(names, fmt.Sprintf("fixture_rar5_lz_plain.part%d.rar", index))
			}
			if err := env.PAR2(ctx, PAR2Spec{
				Base: "fixture_rar5_lz_plain_repair.par2", SliceSize: 65536, RecoveryBlocks: 12, RecoveryFiles: 6,
				Sources: names,
			}); err != nil {
				return err
			}
			if err := swapOutputs(env, names[1], names[2]); err != nil {
				return err
			}
			return swapOutputs(env, names[4], names[5])
		},
	})

	add(Recipe{
		Slug: "par2-ignorable-deficit", Family: "PAR2",
		Notes: "A 5 MiB payload and a 32 KiB text sidecar under one recovery set of 4 KiB blocks, with five of the sidecar's eight blocks zeroed against three recovery blocks. " +
			"Every payload block verifies, so the only damage in the set is a short metadata file the recovery data cannot rebuild: the whole question the fixture asks is what a verdict does with that alone.",
		Inputs: []string{previewPayloadPath},
		Build: sequence(
			publish("clip-preview", "test-media.mkv"),
			func(ctx context.Context, env *Env) error {
				return WriteText(env.OutputPath("info.nfo"), ignorableSidecarNotesText, ignorableSidecarBytes)
			},
			par2(PAR2Spec{Base: "release.par2", SliceSize: ignorableSliceSize, RecoveryBlocks: 3, RecoveryFiles: 1,
				Sources: []string{"test-media.mkv", "info.nfo"}}),
			// Five blocks damaged against three recovery blocks, starting one
			// block in so the sidecar's first and last blocks still verify and
			// the file reads as damaged rather than as something else entirely.
			zeroOutput("info.nfo", ignorableSliceSize, 5*ignorableSliceSize),
		),
		ExpectedOutputs: previewClipExpectedOutput("test-media.mkv"),
	})

	add(Recipe{
		Slug: "par2-partial-volume", Family: "PAR2",
		Notes: "Thirty of a 5 MiB payload's eighty blocks destroyed against forty-eight recovery blocks split evenly over two volumes, so neither volume is enough on its own. " +
			"The scenario then drops one interior article of the second volume, which leaves that volume short of its posted length while the packets on either side of the hole stay intact and self-checksummed on disk.",
		Inputs: []string{previewPayloadPath},
		Build: sequence(
			publish("clip-preview", "test-media.mkv"),
			par2(PAR2Spec{Base: "test-media.mkv.par2", SliceSize: partialVolumeSliceSize, RecoveryBlocks: 48, RecoveryFiles: 2,
				Sources: []string{"test-media.mkv"}}),
			zeroOutput("test-media.mkv", 20*partialVolumeSliceSize, 30*partialVolumeSliceSize),
		),
		ExpectedOutputs: previewClipExpectedOutput("test-media.mkv"),
	})

	add(Recipe{
		Slug: "split-plain-par2", Family: "PAR2",
		Notes: "A 5 MiB clip cut into three equal plain parts named .001/.002/.003, with a recovery set computed over the joined file rather than over the parts. " +
			"Three blocks inside the middle part are zeroed, so the set is only whole again if the parts are read as one file: the recovery data names a file nothing in the posting is called.",
		Inputs: []string{previewPayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			if err := env.Publish(ctx, "clip-preview", "test-media.mkv"); err != nil {
				return err
			}
			if err := env.PAR2(ctx, PAR2Spec{
				Base: "test-media.mkv.par2", SliceSize: splitPar2SliceSize, RecoveryBlocks: 8, RecoveryFiles: 1,
				Sources: []string{"test-media.mkv"},
			}); err != nil {
				return err
			}
			joined := env.OutputPath("test-media.mkv")
			size, err := FileSize(joined)
			if err != nil {
				return err
			}
			// An explicit part size derived from the payload keeps the part
			// count at three whatever the encoder produced, which is what makes
			// ".002" the middle part rather than a part that happens to exist.
			parts, err := SplitFile(joined, (size+splitPar2Parts-1)/splitPar2Parts, func(index int) string {
				return env.OutputPath(fmt.Sprintf("test-media.mkv.%03d", index+1))
			})
			if err != nil {
				return err
			}
			if len(parts) != splitPar2Parts {
				return fmt.Errorf("splitting %d bytes produced %d parts, want %d", size, len(parts), splitPar2Parts)
			}
			// Only the parts are posted: the joined file is the recovery set's
			// subject, not one of the release's files.
			if err := removeOutput(env, "test-media.mkv"); err != nil {
				return err
			}
			return ZeroRange(env.OutputPath("test-media.mkv.002"), splitPar2SliceSize, 3*splitPar2SliceSize)
		},
		ExpectedOutputs: previewClipExpectedOutput("test-media.mkv"),
	})

	add(Recipe{
		Slug: "par2-two-sets", Family: "PAR2",
		Notes: "One posting carrying two independent recovery sets: a 25 MiB payload with eight recovery blocks and a 5 MiB payload with four, each with two of its own blocks zeroed. " +
			"The sets describe different files and share no bytes, so both must be repaired independently before the posting can complete.",
		Inputs: []string{samplePayloadPath, previewPayloadPath},
		Build: func(ctx context.Context, env *Env) error {
			episode, err := env.ArtifactFile(ctx, "clip-episodes", "episode1.mkv")
			if err != nil {
				return err
			}
			if err := CopyFile(episode, env.OutputPath("feature.mkv")); err != nil {
				return err
			}
			if err := env.Publish(ctx, "clip-preview", "bonus.mkv"); err != nil {
				return err
			}
			for _, set := range []struct {
				base, source string
				recovery     int
			}{
				{"feature.mkv.par2", "feature.mkv", 8},
				{"bonus.mkv.par2", "bonus.mkv", 4},
			} {
				if err := env.PAR2(ctx, PAR2Spec{
					Base: set.base, SliceSize: twoSetsSliceSize, RecoveryBlocks: set.recovery, RecoveryFiles: 1,
					Sources: []string{set.source},
				}); err != nil {
					return err
				}
			}
			if err := ZeroRange(env.OutputPath("feature.mkv"), 100*twoSetsSliceSize, 2*twoSetsSliceSize); err != nil {
				return err
			}
			return ZeroRange(env.OutputPath("bonus.mkv"), 30*twoSetsSliceSize, 2*twoSetsSliceSize)
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			episode, err := env.ArtifactFile(ctx, "clip-episodes", "episode1.mkv")
			if err != nil {
				return nil, err
			}
			bonus, err := env.ArtifactPath(ctx, "clip-preview")
			if err != nil {
				return nil, err
			}
			return map[string]string{"feature.mkv": episode, "bonus.mkv": bonus}, nil
		},
	})

	add(Recipe{
		Slug: "par2-multi-set-archives", Family: "PAR2",
		Notes: "One posting carrying two independent store-method RAR5 archives and a PAR2 set for each archive. " +
			"The primary uses 64 KiB slices and the secondary 96 KiB slices; both have two damaged slices and enough recovery data to repair and extract.",
		Inputs:          []string{samplePayloadPath, previewPayloadPath},
		Build:           multiPARSetArchives(4),
		ExpectedOutputs: multiPARSetArchiveExpectedOutputs,
	})

	add(Recipe{
		Slug: "par2-multi-set-archives-clean", Family: "PAR2",
		Notes: "A clean transport counterpart carrying independent 64 KiB and 96 KiB PAR2 grids. " +
			"Both index files are staged before the archive payloads so the completion gate can settle both clean sets without recovery.",
		Inputs:          []string{samplePayloadPath, previewPayloadPath},
		Build:           multiPARSetArchivesClean(),
		ExpectedOutputs: multiPARSetArchiveExpectedOutputs,
	})

	add(Recipe{
		Slug: "par2-multi-grid-overlap-clean", Family: "PAR2",
		Notes: "One clean payload described by independent 64 KiB and 96 KiB recovery sets. " +
			"Both indexes lead the payload so one decode pass must close both slice grids without read-back verification.",
		Inputs:          []string{previewPayloadPath},
		Build:           multiPARGridOverlapClean(),
		ExpectedOutputs: previewClipExpectedOutput("payload.mkv"),
	})

	add(Recipe{
		Slug: "par2-multi-set-archives-insufficient", Family: "PAR2",
		Notes: "The 64 KiB/96 KiB mixed-grid counterpart: the primary archive has enough recovery data for its two damaged slices, " +
			"while the secondary archive has only one recovery block and must make the aggregate job fail.",
		Inputs: []string{samplePayloadPath, previewPayloadPath},
		Build:  multiPARSetArchives(1),
	})

	// ---------------------------------------------------------------- zip
	add(Recipe{
		Slug: "zip-unencrypted", Family: "zip", ByteReproducible: true,
		Notes: "One stored member, written by Go's own zip writer.", Inputs: []string{samplePayloadPath},
		Build: zipOf("archive.zip", "clip-sample", "sample.mkv", ""),
	})
	add(Recipe{
		Slug: "zip-encrypted", Family: "zip", ByteReproducible: true,
		Notes:  "The same member under the legacy PKWARE ZipCrypto cipher, whose 12-byte header is drawn from the generator's deterministic stream rather than a random source.",
		Inputs: []string{samplePayloadPath},
		Build:  zipOf("archive.zip", "clip-sample", "sample.mkv", CorpusPassword),
	})
	add(Recipe{
		Slug: "zip-corrupted", Family: "zip", ByteReproducible: true,
		Notes:  "The plain zip with the standard 1 MiB payload window zeroed, so the member CRC fails.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			zipOf("archive.zip", "clip-sample", "sample.mkv", ""),
			zeroOutput("archive.zip", CorruptOffset, CorruptLength),
		),
	})

	// ---------------------------------------------------------------- tar
	add(Recipe{
		Slug: "tar-archive", Family: "tar", ByteReproducible: true,
		Notes:  "One member, root ownership, padded to GNU tar's 10 KiB blocking factor.",
		Inputs: []string{samplePayloadPath},
		Build:  tarOf("archive.tar", "clip-sample", "sample.mkv"),
	})
	add(Recipe{
		Slug: "tar-corrupted", Family: "tar", ByteReproducible: true,
		Notes:  "The same tar truncated by 1 MiB, so the member ends short of its header's declared size.",
		Inputs: []string{samplePayloadPath},
		Build: sequence(
			tarOf("archive.tar", "clip-sample", "sample.mkv"),
			truncateOutput("archive.tar", TruncateBytes),
		),
	})
	for _, variant := range []struct{ slug, output, codec string }{
		{"tgz-archive", "archive.tgz", "gzip-anonymous"},
		{"tar-gzip-archive", "archive.tar.gzip", "gzip-anonymous"},
		{"tbz2-archive", "archive.tbz2", "bzip2"},
		{"tar-bzip2-archive", "archive.tar.bzip2", "bzip2"},
	} {
		add(Recipe{
			Slug: variant.slug, Family: "tar", ByteReproducible: true,
			Notes:  "A 5 MiB tar carrying the preview clip, wrapped in a stream codec under the extension this slug tests.",
			Inputs: []string{previewPayloadPath},
			Build: sequence(
				tarOf("archive.tar", "clip-preview", "sample-preview.mkv"),
				compressOutput(variant.codec, variant.output, "archive.tar"),
				dropOutput("archive.tar"),
			),
		})
	}
	add(Recipe{
		Slug: "targz-archive", Family: "tar", ByteReproducible: true,
		Notes:  "A season-pack tar.gz: a directory entry and three full-length episode members.",
		Inputs: []string{samplePayloadPath},
		Build:  seasonTarball,
	})
	add(Recipe{
		Slug: "targz-corrupted", Family: "tar", ByteReproducible: true,
		Notes:  "The same season pack with the standard 1 MiB window zeroed inside the DEFLATE stream.",
		Inputs: []string{samplePayloadPath},
		Build:  sequence(seasonTarball, zeroOutput("archive.tar.gz", CorruptOffset, CorruptLength)),
	})

	// ------------------------------------------------------------------ XZ
	add(Recipe{
		Slug: "xz-text", Family: "XZ",
		Notes: "A plain text member in a standalone XZ stream.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WriteText(env.StagePath("readme.txt"), "Weaver XZ text fixture", 1024); err != nil {
				return err
			}
			return env.SevenZip(ctx, SevenZipSpec{
				Format: "xz", Archive: "readme.txt.xz", Members: []string{"readme.txt"}, Level: "-mx1",
			})
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			return map[string]string{"readme.txt": env.StagePath("readme.txt")}, nil
		},
	})
	add(Recipe{
		Slug: "xz-video", Family: "XZ",
		Notes:           "The 5 MiB preview clip in a standalone XZ stream.",
		Inputs:          []string{previewPayloadPath},
		Build:           xzCodec("test-media.mkv.xz", "clip-preview", "test-media.mkv"),
		ExpectedOutputs: previewClipExpectedOutput("test-media.mkv"),
	})
	add(Recipe{
		Slug: "split-xz", Family: "XZ",
		Notes:  "The XZ preview stream split into 2 MiB numbered parts, so join must run before nested XZ extraction.",
		Inputs: []string{previewPayloadPath},
		Build: sequence(
			xzCodec("test-media.mkv.xz", "clip-preview", "test-media.mkv"),
			splitXz,
			dropOutput("test-media.mkv.xz"),
		),
		ExpectedOutputs: previewClipExpectedOutput("test-media.mkv"),
	})

	// ------------------------------------------------------ stream codecs
	add(Recipe{
		Slug: "gzip-single", Family: "stream codec", ByteReproducible: true,
		Notes: "The 1080p clip in a bare gzip stream.", Inputs: []string{samplePayloadPath},
		Build: streamCodec("gzip", "test-media.mkv.gz", "clip-sample", "test-media.mkv"),
	})
	add(Recipe{
		Slug: "gzip-corrupted", Family: "stream codec", ByteReproducible: true,
		Notes: "The same gzip with the standard 1 MiB window zeroed.", Inputs: []string{samplePayloadPath},
		Build: sequence(
			streamCodec("gzip", "test-media.mkv.gz", "clip-sample", "test-media.mkv"),
			zeroOutput("test-media.mkv.gz", CorruptOffset, CorruptLength),
		),
	})
	for _, codec := range []struct{ slug, output, codec string }{
		{"deflate-single", "test-media.mkv.deflate", "deflate"},
		{"bzip2-single", "test-media.mkv.bz2", "bzip2"},
		{"zstd-single", "test-media.mkv.zst", "zstd"},
		{"brotli-single", "test-media.mkv.br", "brotli"},
	} {
		add(Recipe{
			Slug: codec.slug, Family: "stream codec", ByteReproducible: true,
			Notes:  "The 5 MiB preview clip in a bare stream of this codec, with no container framing.",
			Inputs: []string{previewPayloadPath},
			Build:  streamCodec(codec.codec, codec.output, "clip-preview", "test-media.mkv"),
		})
	}

	// ----------------------------------------------------------- uuencode
	//
	// These are the only fixtures the harness cannot post with nyuu: it is a
	// yEnc poster and has no encoding selector, so a uu release ships its
	// article bodies pre-encoded and the seeder posts those bytes verbatim.
	// The encoding and the split across articles are UUDeview's, not this
	// package's — see uuencode.go — and every one of them is decoded back by a
	// real decoder before it is allowed into the corpus.
	add(Recipe{
		Slug: "uu-release", Family: "uuencode", ByteReproducible: true,
		Notes: "A plain uuencoded release: one multi-article payload and one single-article sidecar, canonical shape throughout, no archive and no PAR2. This is the baseline the other uu fixtures deviate from.",
		Build: func(ctx context.Context, env *Env) error {
			media, notes := env.StagePath(uuReleaseMedia), env.StagePath(uuReleaseNFO)
			if err := WritePRNG(media, "silver-horizon", uuMediaBytes); err != nil {
				return err
			}
			if err := WriteText(notes, uuReleaseNotesText, uuShortSidecarBytes); err != nil {
				return err
			}
			return EncodeUU(ctx, env, []UUSpec{
				{Source: media, Name: uuReleaseMedia, LinesPerPart: 200},
				{Source: notes, Name: uuReleaseNFO},
			})
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			return map[string]string{
				uuReleaseMedia: env.StagePath(uuReleaseMedia),
				uuReleaseNFO:   env.StagePath(uuReleaseNFO),
			}, nil
		},
	})

	add(Recipe{
		Slug: "uu-mixed-yenc", Family: "uuencode", ByteReproducible: true,
		Notes: "One NZB carrying both encodings: the media file is posted as yEnc by nyuu the way every other fixture is, and the sidecar beside it is uuencoded across three articles. A reader has to decide per file, not per job.",
		Build: func(ctx context.Context, env *Env) error {
			// The media file is a plain output: it stays a top-level file in
			// the scenario directory, which is what makes the seeder stage it
			// and hand it to nyuu.
			if err := WritePRNG(env.OutputPath(uuMixedMedia), "amber-trail", uuMediaBytes); err != nil {
				return err
			}
			notes := env.StagePath(uuMixedNFO)
			if err := WriteText(notes, uuMixedNotesText, uuLongSidecarBytes); err != nil {
				return err
			}
			return EncodeUU(ctx, env, []UUSpec{
				{Source: notes, Name: uuMixedNFO, LinesPerPart: 200},
			})
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			return map[string]string{
				uuMixedMedia: env.OutputPath(uuMixedMedia),
				uuMixedNFO:   env.StagePath(uuMixedNFO),
			}, nil
		},
	})

	add(Recipe{
		Slug: "uu-preamble-tail", Family: "uuencode", ByteReproducible: true,
		Notes: "Two tolerance probes in one release. The media file keeps uuenview's own `_=_ Part n of m` block, so every article — continuations included — opens with prose a decoder has to skip. The sidecar's last group is left unpadded, the way a class of broken encoder really did post it; the pinned decoder recovers both byte for byte, which is what the scenario's digests pin.",
		Build: func(ctx context.Context, env *Env) error {
			media, notes := env.StagePath(uuPreambleMedia), env.StagePath(uuPreambleNFO)
			if err := WritePRNG(media, "violet-cascade", uuMediaBytes); err != nil {
				return err
			}
			// uuLongSidecarBytes is not a multiple of three, so the encoding
			// ends on a partial group and there is padding for the probe to
			// strip.
			if err := WriteText(notes, uuPreambleNotesText, uuLongSidecarBytes); err != nil {
				return err
			}
			return EncodeUU(ctx, env, []UUSpec{
				{Source: media, Name: uuPreambleMedia, LinesPerPart: 200, KeepEncoderPreamble: true},
				{Source: notes, Name: uuPreambleNFO, LinesPerPart: 200, UnpadFinalGroup: true},
			})
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			return map[string]string{
				uuPreambleMedia: env.StagePath(uuPreambleMedia),
				uuPreambleNFO:   env.StagePath(uuPreambleNFO),
			}, nil
		},
	})

	add(Recipe{
		Slug: "uu-missing-middle", Family: "uuencode", ByteReproducible: true,
		Notes: "A canonical uu multipart with one interior article deleted after posting and no PAR2 to rebuild it. The scenario asserts that the job reaches a terminal state and is labelled there — not which damage verdict it lands on.",
		Build: func(ctx context.Context, env *Env) error {
			media := env.StagePath(uuMissingMedia)
			if err := WritePRNG(media, "crimson-vale", uuMediaBytes); err != nil {
				return err
			}
			return EncodeUU(ctx, env, []UUSpec{
				{Source: media, Name: uuMissingMedia, LinesPerPart: 200},
			})
		},
	})

	// ------------------------------------------------------- direct store
	recipes = append(recipes, DirectStoreRecipes()...)
	return recipes
}

// The uu fixtures' member names. They are invented titles, and they are also
// the names the `begin` lines carry, which makes them the names a decoder
// writes and therefore the keys the scenarios' output digests are held under.
const (
	uuReleaseMedia  = "silver.horizon.s01e04.mkv"
	uuReleaseNFO    = "silver.horizon.s01e04.nfo"
	uuMixedMedia    = "amber.trail.s01e02.mkv"
	uuMixedNFO      = "amber.trail.s01e02.nfo"
	uuPreambleMedia = "violet.cascade.s01e05.mkv"
	uuPreambleNFO   = "violet.cascade.s01e05.nfo"
	uuMissingMedia  = "crimson.vale.s01e06.mkv"
)

// The uu payload sizes. Unlike the rest of the corpus these are exact rather
// than encoder-determined, and deliberately so: the number of articles a file
// is split across is a function of its size, and every one of those articles
// is its own ledger path. A payload that changed size by a byte could change
// the file list, which is a corpus revision by hand rather than a digest
// refresh. Fixing the sizes here fixes the shapes:
//
//	uuMediaBytes        1,457 lines -> 8 articles at 200 lines
//	uuShortSidecarBytes    23 lines -> 1 article
//	uuLongSidecarBytes    445 lines -> 3 articles at 200 lines
//
// uuLongSidecarBytes is additionally not a multiple of three, which is what
// leaves the tail-tolerance probe a partial final group to strip.
const (
	uuMediaBytes        = 65536
	uuShortSidecarBytes = 1024
	uuLongSidecarBytes  = 20003
)

// The sidecar payloads. They are padded out to a fixed size by WriteText, so
// what matters here is only that each is a distinct, deterministic run of
// bytes; the titles are invented.
const (
	uuReleaseNotesText  = "Silver Horizon - season one, episode four. Encoded for the e2e corpus; every name here is invented.\n"
	uuMixedNotesText    = "Amber Trail - season one, episode two. The sidecar beside a yEnc-posted media file, uuencoded across three articles.\n"
	uuPreambleNotesText = "Violet Cascade - season one, episode five. The tail-tolerance probe: this file's encoding ends on an unpadded final group.\n"
)

// ---------------------------------------------------------------- helpers

func sequence(steps ...func(context.Context, *Env) error) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		for _, step := range steps {
			if err := step(ctx, env); err != nil {
				return err
			}
		}
		return nil
	}
}

func publish(artifact, name string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error { return env.Publish(ctx, artifact, name) }
}

func publishAll(artifact string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error { return env.PublishAll(ctx, artifact) }
}

func par2(spec PAR2Spec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error { return env.PAR2(ctx, spec) }
}

func zeroOutput(name string, offset, length int64) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error { return ZeroRange(env.OutputPath(name), offset, length) }
}

func truncateOutput(name string, count int64) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error { return TruncateBy(env.OutputPath(name), count) }
}

func dropOutput(name string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error { return removeOutput(env, name) }
}

func compressOutput(codec, output, source string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		return CompressFile(codec, env.OutputPath(output), env.OutputPath(source))
	}
}

func stageSolidText(env *Env) error {
	if err := WriteText(env.StagePath("file1.txt"), "solid run member one, e2e", 29); err != nil {
		return err
	}
	return WriteText(env.StagePath("file2.txt"), "solid run member two, e2e", 29)
}

func obfuscatedNames(start int) []string {
	const stem = "51273aad56a8b904e96928935278a627"
	names := make([]string, 0, 3)
	for index := 0; index < 3; index++ {
		names = append(names, fmt.Sprintf("%s.%d", stem, start+index))
	}
	return names
}

func multiPARGridOverlapClean() func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		const payload = "payload.mkv"
		if err := env.Publish(ctx, "clip-preview", payload); err != nil {
			return err
		}
		sets := []struct {
			index     string
			sliceSize int64
		}{
			{"00-grid64.par2", twoSetsPrimarySliceSize},
			{"01-grid96.par2", twoSetsSecondarySliceSize},
		}
		for _, set := range sets {
			if err := env.PAR2(ctx, PAR2Spec{
				Base:           set.index,
				SliceSize:      set.sliceSize,
				RecoveryBlocks: 1,
				RecoveryFiles:  1,
				Sources:        []string{payload},
			}); err != nil {
				return err
			}
		}
		for _, set := range sets {
			dataBlocks := int((PreviewPayloadBytes + set.sliceSize - 1) / set.sliceSize)
			if err := verifyCleanPAR2Set(ctx, env, set.index, payload, set.sliceSize, dataBlocks); err != nil {
				return err
			}
		}
		return nil
	}
}

func multiPARSetArchives(secondaryRecoveryBlocks int) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		if err := buildMultiPARSetArchives(ctx, env, secondaryRecoveryBlocks); err != nil {
			return err
		}
		if err := ZeroRange(env.OutputPath("primary.rar"), 100*twoSetsPrimarySliceSize, 2*twoSetsPrimarySliceSize); err != nil {
			return err
		}
		if err := ZeroRange(env.OutputPath("secondary.rar"), twoSetsDamageOffset, twoSetsDamageLength); err != nil {
			return err
		}
		return verifyMultiPARSetArchiveDamage(ctx, env, secondaryRecoveryBlocks)
	}
}

func multiPARSetArchivesClean() func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		if err := buildMultiPARSetArchives(ctx, env, 4); err != nil {
			return err
		}
		if err := verifyCleanMultiPARSetArchives(ctx, env); err != nil {
			return err
		}
		for _, names := range [][2]string{
			{"primary.rar.par2", "00-primary.par2"},
			{"secondary.rar.par2", "01-secondary.par2"},
		} {
			if err := os.Rename(env.OutputPath(names[0]), env.OutputPath(names[1])); err != nil {
				return err
			}
		}
		return nil
	}
}

func buildMultiPARSetArchives(ctx context.Context, env *Env, secondaryRecoveryBlocks int) error {
	sets := []struct {
		artifact, member, archive string
		recovery                  int
		sliceSize                 int64
	}{
		{"clip-sample", "feature.mkv", "primary.rar", 8, twoSetsPrimarySliceSize},
		{"clip-preview", "bonus.mkv", "secondary.rar", secondaryRecoveryBlocks, twoSetsSecondarySliceSize},
	}
	for _, set := range sets {
		if err := env.Stage(ctx, set.artifact, set.member); err != nil {
			return err
		}
		if err := env.RAR(ctx, RARSpec{
			Toolchain: RAR5Writer,
			Format:    RAR5,
			Archive:   set.archive,
			Members:   []string{set.member},
			Method:    "-m0",
		}); err != nil {
			return err
		}
		if err := env.PAR2(ctx, PAR2Spec{
			Base:           set.archive + ".par2",
			SliceSize:      set.sliceSize,
			RecoveryBlocks: set.recovery,
			RecoveryFiles:  1,
			Sources:        []string{set.archive},
		}); err != nil {
			return err
		}
	}
	return nil
}

func verifyCleanMultiPARSetArchives(ctx context.Context, env *Env) error {
	sets := []struct {
		index      string
		archive    string
		sliceSize  int64
		dataBlocks int
	}{
		{"primary.rar.par2", "primary.rar", twoSetsPrimarySliceSize, 1312},
		{"secondary.rar.par2", "secondary.rar", twoSetsSecondarySliceSize, 54},
	}
	for _, set := range sets {
		if err := verifyCleanPAR2Set(ctx, env, set.index, set.archive, set.sliceSize, set.dataBlocks); err != nil {
			return err
		}
	}
	return nil
}

func verifyCleanPAR2Set(ctx context.Context, env *Env, index, target string, sliceSize int64, dataBlocks int) error {
	report, err := env.PAR2Verify(ctx, index)
	if err != nil {
		return fmt.Errorf("verify clean mixed-grid set %s: %w", index, err)
	}
	for _, expected := range []string{
		fmt.Sprintf("The block size used was %d bytes.", sliceSize),
		fmt.Sprintf("There are a total of %d data blocks.", dataBlocks),
		fmt.Sprintf(`Target: "%s" - found.`, target),
		"All files are correct, repair is not required.",
	} {
		if !strings.Contains(report, expected) {
			return fmt.Errorf("clean mixed-grid PAR2 report for %s does not contain %q", index, expected)
		}
	}
	return nil
}

func multiPARSetArchiveExpectedOutputs(ctx context.Context, env *Env) (map[string]string, error) {
	feature, err := env.ArtifactPath(ctx, "clip-sample")
	if err != nil {
		return nil, err
	}
	bonus, err := env.ArtifactPath(ctx, "clip-preview")
	if err != nil {
		return nil, err
	}
	return map[string]string{"feature.mkv": feature, "bonus.mkv": bonus}, nil
}

// verifyMultiPARSetArchiveDamage makes the asymmetric PAR2 grids a persisted
// fixture contract rather than a recipe-only property. PAR2's nonzero status is
// expected because the generated archive has intentionally been damaged.
func verifyMultiPARSetArchiveDamage(ctx context.Context, env *Env, recoveryBlocks int) error {
	report, verifyErr := env.PAR2Verify(ctx, "secondary.rar.par2")
	if verifyErr == nil {
		return fmt.Errorf("PAR2 verification unexpectedly succeeded for damaged secondary.rar")
	}
	if err := validateMultiPARSetArchiveDamageReport(report, recoveryBlocks); err != nil {
		return fmt.Errorf("PAR2 verification did not preserve the secondary grid: %w (expected nonzero status: %v)", err, verifyErr)
	}
	if recoveryBlocks == 4 {
		if err := repairMultiPARSetArchiveSecondary(ctx, env, recoveryBlocks); err != nil {
			return err
		}
	}
	return nil
}

func repairMultiPARSetArchiveSecondary(ctx context.Context, env *Env, recoveryBlocks int) error {
	repairDir, err := os.MkdirTemp(env.Work, "multi-par2-repair-")
	if err != nil {
		return fmt.Errorf("create isolated PAR2 repair directory: %w", err)
	}
	defer os.RemoveAll(repairDir)

	for _, name := range []string{
		"secondary.rar",
		"secondary.rar.par2",
		fmt.Sprintf("secondary.rar.vol0+%d.par2", recoveryBlocks),
	} {
		if err := CopyFile(env.OutputPath(name), filepath.Join(repairDir, name)); err != nil {
			return fmt.Errorf("copy %s into isolated PAR2 repair directory: %w", name, err)
		}
	}

	toolchain, err := env.Lock.Find(PAR2Toolchain)
	if err != nil {
		return err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return err
	}
	env.usedToolchain(PAR2Toolchain)
	if err := env.Docker.Run(ctx, toolchain, env.Work, filepath.Base(repairDir), "r", "secondary.rar.par2"); err != nil {
		return fmt.Errorf("repair isolated secondary PAR2 set: %w", err)
	}

	repaired, err := os.ReadFile(filepath.Join(repairDir, "secondary.rar"))
	if err != nil {
		return fmt.Errorf("read repaired secondary archive: %w", err)
	}
	if got, want := fmt.Sprintf("%x", sha256.Sum256(repaired)), "7f4717c488c1bb71948fefc6cdd2b77d41457b7bbd7325fcac626c5d184595df"; got != want {
		return fmt.Errorf("repaired secondary archive SHA-256 = %s, want %s", got, want)
	}
	return nil
}

func validateMultiPARSetArchiveDamageReport(report string, recoveryBlocks int) error {
	want := []string{
		"The block size used was 98304 bytes.",
		"There are a total of 54 data blocks.",
		`Target: "secondary.rar" - damaged. Found 52 of 54 data blocks.`,
		fmt.Sprintf("You have %d recovery blocks available.", recoveryBlocks),
	}
	switch recoveryBlocks {
	case 4:
		want = append(want, "Repair is possible.")
	case 1:
		want = append(want,
			"Repair is not possible.",
			"You need 1 more recovery blocks to be able to repair.",
		)
	default:
		return fmt.Errorf("unexpected secondary recovery block count %d", recoveryBlocks)
	}
	for _, expected := range want {
		if !strings.Contains(report, expected) {
			return fmt.Errorf("PAR2 report does not contain %q", expected)
		}
	}
	return nil
}

func singleMemberRAR(writer string, format RARFormat, member string, spec RARSpec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		if err := env.Stage(ctx, "clip-sample", member); err != nil {
			return err
		}
		spec.Toolchain, spec.Format, spec.Archive, spec.Members = writer, format, "archive.rar", []string{member}
		return env.RAR(ctx, spec)
	}
}

func episodeMembersRAR(writer string, format RARFormat, pattern string, spec RARSpec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		members := make([]string, 0, 3)
		for index := 0; index < 3; index++ {
			member := fmt.Sprintf(pattern, index+1)
			if err := env.StageIndexed(ctx, "clip-episodes", index, member); err != nil {
				return err
			}
			members = append(members, member)
		}
		spec.Toolchain, spec.Format, spec.Archive, spec.Members = writer, format, "archive.rar", members
		return env.RAR(ctx, spec)
	}
}

func multiVolumeRAR(writer string, format RARFormat, member, volumeSize string, parts int, spec RARSpec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		if err := env.Stage(ctx, "clip-sample", member); err != nil {
			return err
		}
		spec.Toolchain, spec.Format, spec.Archive, spec.Members = writer, format, "archive.rar", []string{member}
		spec.VolumeSize = volumeSize
		if err := env.RAR(ctx, spec); err != nil {
			return err
		}
		return expectParts(env, "archive.part%d.rar", parts)
	}
}

// recoveryVolumeRAR stages the shared clip as work/payload/movie.mkv, has
// RARLAB write `parts` data volumes of volumeSize plus `recoveryVolumes`
// standalone .rev files, and then withholds the named data volumes. Only
// RARLAB writes RAR bytes; the withholding is a plain file removal.
func recoveryVolumeRAR(writer string, format RARFormat, spec RARSpec, volumeSize string, parts, recoveryVolumes int, withhold ...string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		clip, err := env.ArtifactFile(ctx, "clip-shared", "short-720p-av1.mkv")
		if err != nil {
			return err
		}
		if err := CopyFile(clip, env.StagePath("work/payload/movie.mkv")); err != nil {
			return err
		}
		spec.Toolchain, spec.Format, spec.Archive = writer, format, "archive.rar"
		spec.Members = []string{"work/payload/movie.mkv"}
		spec.VolumeSize, spec.RecoveryVolumes = volumeSize, recoveryVolumes
		if err := env.RAR(ctx, spec); err != nil {
			return err
		}
		expected := make([]string, 0, parts+recoveryVolumes)
		for index := 1; index <= parts; index++ {
			expected = append(expected, fmt.Sprintf("archive.part%d.rar", index))
		}
		for index := 1; index <= recoveryVolumes; index++ {
			expected = append(expected, fmt.Sprintf("archive.part%d.rev", index))
		}
		if err := expectOutputs(env, expected); err != nil {
			return err
		}
		for _, name := range withhold {
			if err := removeOutput(env, name); err != nil {
				return err
			}
		}
		return nil
	}
}

// expectOutputs requires the output directory to hold exactly the named files.
func expectOutputs(env *Env, names []string) error {
	produced, err := env.Outputs()
	if err != nil {
		return err
	}
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[name] = true
	}
	for _, name := range produced {
		if !wanted[name] {
			return fmt.Errorf("unexpected output %s (wanted exactly %s)", name, strings.Join(names, ", "))
		}
		delete(wanted, name)
	}
	if len(wanted) != 0 {
		missing := make([]string, 0, len(wanted))
		for name := range wanted {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return fmt.Errorf("outputs missing: %s", strings.Join(missing, ", "))
	}
	return nil
}

// sharedClipExpectedOutput pins the extracted member to the shared clip's
// bytes, which is what a successful reconstruction must reproduce exactly.
func sharedClipExpectedOutput(member string) func(context.Context, *Env) (map[string]string, error) {
	return func(ctx context.Context, env *Env) (map[string]string, error) {
		clip, err := env.ArtifactFile(ctx, "clip-shared", "short-720p-av1.mkv")
		if err != nil {
			return nil, err
		}
		return map[string]string{member: clip}, nil
	}
}

// previewClipExpectedOutput pins a delivered member to the preview clip's
// bytes: the payload of these fixtures is damaged on disk, so the oracle is the
// artifact the damage was applied to, not the fixture file.
func previewClipExpectedOutput(member string) func(context.Context, *Env) (map[string]string, error) {
	return func(ctx context.Context, env *Env) (map[string]string, error) {
		clip, err := env.ArtifactPath(ctx, "clip-preview")
		if err != nil {
			return nil, err
		}
		return map[string]string{member: clip}, nil
	}
}

// nestRAR wraps the root-member RAR5 in `levels` further stored RAR5 archives.
// Every level is written by RARLAB; Go only moves the previous level's file
// into the next stage.
func nestRAR(levels int) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		inner, err := env.ArtifactPath(ctx, "rar5-root-sample")
		if err != nil {
			return err
		}
		names := []string{"inner.rar", "middle.rar", "level3.rar", "level4.rar"}
		intermediates := make([]string, 0, levels)
		current := inner
		for level := 0; level < levels; level++ {
			member := names[level]
			if err := CopyFile(current, env.StagePath(member)); err != nil {
				return err
			}
			archive := "archive.rar"
			if level < levels-1 {
				archive = names[level+1]
				intermediates = append(intermediates, archive)
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: archive,
				Method: "-m0", Members: []string{member},
			}); err != nil {
				return err
			}
			if err := os.Remove(env.StagePath(member)); err != nil {
				return err
			}
			current = env.OutputPath(archive)
		}
		for _, name := range intermediates {
			if err := removeOutput(env, name); err != nil {
				return err
			}
		}
		return nil
	}
}

func splitSevenZip(artifact string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		source, err := env.ArtifactPath(ctx, artifact)
		if err != nil {
			return err
		}
		_, err = SplitFile(source, 30<<20, func(index int) string {
			return env.OutputPath(fmt.Sprintf("archive.7z.%03d", index+1))
		})
		return err
	}
}

func splitXz(_ context.Context, env *Env) error {
	_, err := SplitFile(env.OutputPath("test-media.mkv.xz"), 2<<20, func(index int) string {
		return env.OutputPath(fmt.Sprintf("test-media.mkv.xz.%03d", index+1))
	})
	return err
}

func renameMultivolume(artifact string, start int) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		files, err := env.Artifacts.Files(ctx, env, artifact)
		if err != nil {
			return err
		}
		names := obfuscatedNames(start)
		for index, file := range files {
			if err := CopyFile(file, env.OutputPath(names[index])); err != nil {
				return err
			}
		}
		return nil
	}
}

func tarOf(output, artifact, member string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		source, err := env.ArtifactPath(ctx, artifact)
		if err != nil {
			return err
		}
		return WriteTar(env.OutputPath(output), nil, []Member{{Name: member, Source: source}})
	}
}

func zipOf(output, artifact, member, password string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		source, err := env.ArtifactPath(ctx, artifact)
		if err != nil {
			return err
		}
		return WriteZip(env.OutputPath(output), []Member{{Name: member, Source: source}}, password)
	}
}

func seasonTarball(ctx context.Context, env *Env) error {
	source, err := env.ArtifactPath(ctx, "clip-sample")
	if err != nil {
		return err
	}
	members := make([]Member, 0, 3)
	for index := 1; index <= 3; index++ {
		members = append(members, Member{Name: fmt.Sprintf("./Test.Show.S01E%02d.1080p.mkv", index), Source: source})
	}
	if err := WriteTar(env.OutputPath("archive.tar"), []string{"./"}, members); err != nil {
		return err
	}
	if err := WriteGzip(env.OutputPath("archive.tar.gz"), env.OutputPath("archive.tar"), ""); err != nil {
		return err
	}
	return removeOutput(env, "archive.tar")
}

func streamCodec(codec, output, artifact, innerName string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		source, err := env.ArtifactPath(ctx, artifact)
		if err != nil {
			return err
		}
		staged := env.StagePath(innerName)
		if err := CopyFile(source, staged); err != nil {
			return err
		}
		return CompressFile(codec, env.OutputPath(output), staged)
	}
}

func xzCodec(output, artifact, innerName string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		source, err := env.ArtifactPath(ctx, artifact)
		if err != nil {
			return err
		}
		if err := CopyFile(source, env.StagePath(innerName)); err != nil {
			return err
		}
		return env.SevenZip(ctx, SevenZipSpec{
			Format: "xz", Archive: output, Members: []string{innerName}, Level: "-mx1",
		})
	}
}

func swapOutputs(env *Env, left, right string) error {
	leftPath, rightPath := env.OutputPath(left), env.OutputPath(right)
	temporary := filepath.Join(filepath.Dir(leftPath), ".swap.tmp")
	if err := os.Rename(leftPath, temporary); err != nil {
		return err
	}
	if err := os.Rename(rightPath, leftPath); err != nil {
		return err
	}
	return os.Rename(temporary, rightPath)
}
