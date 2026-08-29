package fixturegen

import (
	"context"
	"fmt"
)

// The direct-store writers. RAR direct-store routing writes a set's members
// straight to their destinations without materialising the volumes, and its
// output is byte-identical to the conventional path — so no output assertion
// can tell the two apart. The corpus is the instrument instead: every fixture
// here is one Weaver should route direct end to end, and a demotion is a
// failure signal rather than expected noise. These recipes keep the older
// RARLAB pair the family has always been written with.
const (
	DirectStoreRAR5Writer = "rarlab-5.00"
	DirectStoreRAR4Writer = "rarlab-4.20"
)

// directStore describes one direct-store set. The shape constraints are all
// easy to get wrong by hand:
//
//  1. Members must be STORE (`-m0`). Any compressed part makes the member
//     ineligible and demotes the set.
//  2. Sets must be NON-SOLID (`-s-`). A solid member is only decodable against
//     the rest of the run, which the router cannot do per member.
//  3. Volumes must exceed the router's 4 MiB header-prefix ceiling, or every
//     volume parses from its first article and the interesting ordering
//     behaviour never happens. 8 MiB volumes at the harness's article size
//     give about eleven articles each, so articles genuinely arrive out of
//     order.
//  4. Payloads must be incompressible, otherwise `-m0` is a lie about what the
//     format would do in the field. They are a deterministic counter-fed
//     SHA-256 stream, seeded per fixture.
type directStore struct {
	slug       string
	notes      string
	writer     string
	format     RARFormat
	volumeSize string
	password   string
	// members maps the member path inside the archive to its payload size and
	// PRNG seed.
	members []directStoreMember
}

type directStoreMember struct {
	path string
	size int64
	seed string
}

// extra adds `-qo-` — no quick-open record — on the RAR5 writer only. The 4.x
// writer predates the switch and rejects it.
func (set directStore) extra() []string {
	if set.format == RAR5 {
		return []string{"-qo-"}
	}
	return nil
}

// DirectStoreRecipes ports the direct-store generator, including the parity
// step, onto the pinned images and into Go.
func DirectStoreRecipes() []Recipe {
	sets := []directStore{
		{
			slug:   "direct-store-single",
			notes:  "One volume, one member, nothing else in play. If this demotes, nothing else in the family is worth reading.",
			writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "64m",
			members: []directStoreMember{{"silver.horizon.s01e01.mkv", 8 << 20, "silver-horizon"}},
		},
		{
			slug:   "direct-store-multivolume",
			notes:  "One member spread over 8 MiB volumes, so the router's per-volume mapping has to agree with a member that crosses boundaries.",
			writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "8m",
			members: []directStoreMember{{"amber.trail.s01e02.mkv", 24 << 20, "amber-trail"}},
		},
		{
			slug: "direct-store-rar4",
			notes: "RAR4 across two volumes with the member under a directory, so the RAR4 backslash path separator is in play. " +
				"The format is read from the volume signature rather than assumed, so RAR4 has to route direct as RAR5 does. " +
				"A flat RAR4 fixture passes even when the separator handling is wrong, which is why the subdirectory stays.",
			writer: DirectStoreRAR4Writer, format: RAR4, volumeSize: "8m",
			members: []directStoreMember{{"work/crimson.vector.s01e03.mkv", 16 << 20, "crimson-vector"}},
		},
		{
			slug:   "direct-store-multi-member",
			notes:  "Two members in one set: each maps independently, and the set finalises only once both have been placed.",
			writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "64m",
			members: []directStoreMember{
				{"neon.meridian.s01e04.mkv", 6 << 20, "neon-meridian-a"},
				{"neon.meridian.s01e05.mkv", 6 << 20, "neon-meridian-b"},
			},
		},
		{
			slug: "direct-store-encrypted",
			notes: "`-p` encrypts member data but leaves headers readable, which is the one encrypted shape the router can carry: " +
				"all parts stored, uniform key material. Header encryption is a different path and deliberately is not here.",
			writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "8m", password: DirectStorePassword,
			members: []directStoreMember{{"quartz.harbor.s01e06.mkv", 12 << 20, "quartz-harbor"}},
		},
	}

	recipes := make([]Recipe, 0, len(sets)+1)
	for _, set := range sets {
		recipes = append(recipes, set.recipe(nil))
	}

	// The repair fixture is the gap the family exists to close: a set that is
	// repaired without ever leaving direct mode. The damage is applied by the
	// harness deleting the last few articles of an interior volume, not by
	// corrupting bytes here — corrupt bytes are caught at routing time by the
	// part and member checksum layers, which demote the set, and the repair
	// then happens conventionally. A few missing articles instead leave holes,
	// which is the shape the per-slice damage accounting was built for.
	repair := directStore{
		slug:   "direct-store-par2-repair",
		notes:  "Four stored volumes with PAR2 recovery at 20% over four recovery files. The scenario deletes the tail articles of an interior volume, never its head: every volume carries its signature in its first article, and a volume whose header is gone cannot be mapped at all.",
		writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "8m",
		members: []directStoreMember{{"violet.cascade.s01e07.mkv", 24 << 20, "violet-cascade"}},
	}
	recipes = append(recipes, repair.recipe(func(ctx context.Context, env *Env) error {
		volumes, err := env.Outputs()
		if err != nil {
			return err
		}
		return env.PAR2(ctx, PAR2Spec{
			Base: "archive.par2", RedundancyPercent: 20, RecoveryFiles: 4, Sources: volumes,
		})
	}))
	return recipes
}

func (set directStore) recipe(after func(context.Context, *Env) error) Recipe {
	return Recipe{
		Slug: set.slug, Family: "direct store", Notes: set.notes,
		Build: func(ctx context.Context, env *Env) error {
			members := make([]string, 0, len(set.members))
			for _, member := range set.members {
				if err := WritePRNG(env.StagePath(member.path), member.seed, member.size); err != nil {
					return err
				}
				members = append(members, member.path)
			}
			if err := env.RAR(ctx, RARSpec{
				Toolchain: set.writer, Format: set.format, Archive: "archive.rar",
				Method: "-m0", Password: set.password, VolumeSize: set.volumeSize,
				Extra: set.extra(), Members: members,
			}); err != nil {
				return err
			}
			if after != nil {
				return after(ctx, env)
			}
			return nil
		},
		ExpectedOutputs: func(ctx context.Context, env *Env) (map[string]string, error) {
			// The scenario pins the BLAKE3 of the extracted member, so a run
			// that produced the right bytes by the wrong route still has to
			// produce the right bytes. The payload is re-derived from its seed
			// rather than read back out of the archive.
			digests := make(map[string]string, 1)
			member := set.members[0]
			path := env.StagePath(fmt.Sprintf("expected-%s", sanitizeName(member.path)))
			if err := WritePRNG(path, member.seed, member.size); err != nil {
				return nil, err
			}
			digests[member.path] = path
			return digests, nil
		},
	}
}

func sanitizeName(name string) string {
	out := make([]rune, 0, len(name))
	for _, character := range name {
		if character == '/' {
			character = '-'
		}
		out = append(out, character)
	}
	return string(out)
}
