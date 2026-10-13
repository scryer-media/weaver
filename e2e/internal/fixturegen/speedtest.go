package fixturegen

import "context"

// The Usenet speed test's reference posting. The public reference set for
// this purpose is a 100 MB download published for speed testing: a
// 100,000,000-byte member and a one-line text member in four 25 MiB RAR5
// volumes under normal compression, covered by a 10% PAR2 set in
// par2cmdline's default doubling layout. This recipe reproduces that shape
// from the generator's own bytes under the same file names, so the private
// NNTP serves the equivalent set and a Usenet run against it measures what a
// run against the public set would.
const (
	SpeedTestSlug = "speed-test-100mb"
	// speedTestPayloadSize is the reference member's exact size.
	speedTestPayloadSize = 100_000_000
	// speedTestSliceSize is the reference set's PAR2 block size, which puts
	// the four volumes at about two thousand source blocks.
	speedTestSliceSize = 50124
	// speedTestRecoveryBlocks is the reference set's 10% recovery count,
	// which par2cmdline lays out as 1, 2, 4, 8, 16, 32, 64 and 73 blocks.
	speedTestRecoveryBlocks = 200
	speedTestVolumes        = 4
)

// SpeedTestRecipes is the speed-test family: one posting the speed-test
// release phase seeds.
func SpeedTestRecipes() []Recipe {
	const payload = "100MB.bin"
	const note = "100MB-explanation.txt"
	const seed = "speed-test-100mb"
	return []Recipe{{
		Slug: SpeedTestSlug, Family: "speed test",
		Notes: "Four 25 MiB RAR5 volumes (normal compression over an incompressible member, quick-open records kept) " +
			"with a 200-block PAR2 set at a 50124-byte slice: the shape of the public 100 MB speed-test posting under its file names.",
		Build: func(ctx context.Context, env *Env) error {
			if err := WritePRNG(env.StagePath(payload), seed, speedTestPayloadSize); err != nil {
				return err
			}
			if err := WriteText(env.StagePath(note), "this is a testfile", 19); err != nil {
				return err
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: RAR5Writer, Format: RAR5, Archive: "rar-files.rar",
				Method: "-m3", Dictionary: "-md32m", VolumeSize: "25m", Extra: []string{"-qo+"},
				Members: []string{payload, note},
			}); err != nil {
				return err
			}
			if err := expectParts(env, "rar-files.part%d.rar", speedTestVolumes); err != nil {
				return err
			}
			volumes, err := env.Outputs()
			if err != nil {
				return err
			}
			return env.PAR2(ctx, PAR2Spec{
				Base: "par-files.par2", SliceSize: speedTestSliceSize, RecoveryBlocks: speedTestRecoveryBlocks, Sources: volumes,
			})
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			// Both members are re-derived from the recipe rather than read
			// back out of the archive.
			expected := env.StagePath("expected-" + payload)
			if err := WritePRNG(expected, seed, speedTestPayloadSize); err != nil {
				return nil, err
			}
			return map[string]string{payload: expected, note: env.StagePath(note)}, nil
		},
	}}
}
