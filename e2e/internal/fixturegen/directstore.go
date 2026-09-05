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
	// headerPassword encrypts the headers as well (`-hp`), RAR5 only: the
	// router proves a candidate against the archive's own check and keys the
	// header walk from it. RAR4 header encryption is refused by design.
	headerPassword string
	// quickOpen keeps the RAR5 writer's quick-open records (`-qo+`), one per
	// file header. Every other RAR5 set suppresses them, so this is the only
	// shape on which the router's quick-open cross-check — the copy of the
	// headers behind the main header against the physical header walk — has
	// anything to compare.
	quickOpen bool
	// members maps the member path inside the archive to its payload size and
	// PRNG seed.
	members []directStoreMember
}

type directStoreMember struct {
	path string
	size int64
	seed string
}

// extra controls the quick-open record on the RAR5 writer only: `-qo-` (none)
// by default, `-qo+` (one per file header) for the quick-open set. The 4.x
// writer predates the switch and rejects it.
func (set directStore) extra() []string {
	if set.format != RAR5 {
		return nil
	}
	if set.quickOpen {
		return []string{"-qo+"}
	}
	return []string{"-qo-"}
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
			slug: "direct-store-quick-open",
			notes: "RAR5 with the writer's quick-open records kept (`-qo+`) across three 8 MiB volumes. Every other RAR5 set passes `-qo-`, " +
				"so this is the only shape on which the router's quick-open cross-check has a second copy of the file headers to compare " +
				"against the physical header walk; a demotion here means that cross-check, not the routing, regressed.",
			writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "8m", quickOpen: true,
			members: []directStoreMember{{"cobalt.lantern.s01e07.mkv", 20 << 20, "cobalt-lantern"}},
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
		{
			slug: "direct-store-rar4-encrypted",
			notes: "RAR4 `-p` across three volumes: member data encrypted under the header's 8-byte file salt, headers readable. " +
				"The router derives RAR4 keys off that salt and routes the member as an encrypted store member exactly as it does for RAR5, " +
				"so this is the RAR4 twin of direct-store-encrypted and demotes only if the RAR4 keying path regresses.",
			writer: DirectStoreRAR4Writer, format: RAR4, volumeSize: "8m", password: DirectStorePassword,
			members: []directStoreMember{{"basalt.meridian.s01e09.mkv", 16 << 20, "basalt-meridian"}},
		},
		{
			slug: "direct-store-hp",
			notes: "RAR5 `-hp`: headers and member data both encrypted. The router proves the job's password against the archive's own " +
				"check before a header is decrypted, then keys the header walk from it and routes the stored member direct. " +
				"The header-encrypted shape direct-store-encrypted deliberately leaves out.",
			writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "8m", headerPassword: DirectStorePassword,
			members: []directStoreMember{{"ivory.signal.s01e11.mkv", 12 << 20, "ivory-signal"}},
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
	directStorePar2 := func(ctx context.Context, env *Env) error {
		volumes, err := env.Outputs()
		if err != nil {
			return err
		}
		return env.PAR2(ctx, PAR2Spec{
			Base: "archive.par2", RedundancyPercent: 20, RecoveryFiles: 4, Sources: volumes,
		})
	}
	recipes = append(recipes, repair.recipe(directStorePar2))

	// Repair and encryption together, which neither fixture above exercises.
	// PAR2 covers the archive bytes as posted — the *encrypted* bytes — so
	// repair reconstructs ciphertext and never touches a key: the store hands
	// it clean-volume bytes verbatim and the rewritten volume is ciphertext
	// again. Nothing in that chain should re-encrypt, and this fixture is what
	// proves it: if the store served anything but byte-exact archive data, the
	// repaired set would fail extraction only when a password is in play.
	encryptedRepair := directStore{
		slug:   "direct-store-encrypted-par2-repair",
		notes:  "Four stored volumes with `-p` data encryption and PAR2 recovery at 20%. The scenario deletes the tail articles of an interior volume; repair rewrites ciphertext in place while the clean volumes stay virtual, and extraction with the password is the proof the repaired bytes are the posted bytes.",
		writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "8m", password: DirectStorePassword,
		members: []directStoreMember{{"obsidian.current.s01e08.mkv", 24 << 20, "obsidian-current"}},
	}
	recipes = append(recipes, encryptedRepair.recipe(directStorePar2))

	// The same repair under RAR4 data encryption. Parity covers the posted
	// bytes — RAR4 ciphertext — and the edge cipher blocks on either side of
	// the hole are held exactly as they are for RAR5, so this is where a RAR4
	// keying difference in the repair path would show.
	rar4EncryptedRepair := directStore{
		slug:   "direct-store-rar4-encrypted-par2-repair",
		notes:  "Four stored RAR4 volumes with `-p` data encryption and PAR2 recovery at 20%. The scenario deletes the tail articles of an interior volume; repair rewrites RAR4 ciphertext in place while the clean volumes stay virtual.",
		writer: DirectStoreRAR4Writer, format: RAR4, volumeSize: "8m", password: DirectStorePassword,
		members: []directStoreMember{{"cobalt.lantern.s01e10.mkv", 24 << 20, "cobalt-lantern"}},
	}
	recipes = append(recipes, rar4EncryptedRepair.recipe(directStorePar2))

	// And under encrypted headers: the header walk over the repaired volume
	// runs keyed, and the recovery set describes volumes whose headers the
	// conventional reader could not even name without the password.
	hpRepair := directStore{
		slug:   "direct-store-hp-par2-repair",
		notes:  "Four stored RAR5 volumes with `-hp` header and data encryption and PAR2 recovery at 20%. The scenario deletes the tail articles of an interior volume; repair rewrites ciphertext in place under encrypted headers while the clean volumes stay virtual.",
		writer: DirectStoreRAR5Writer, format: RAR5, volumeSize: "8m", headerPassword: DirectStorePassword,
		members: []directStoreMember{{"umber.tideline.s01e12.mkv", 24 << 20, "umber-tideline"}},
	}
	recipes = append(recipes, hpRepair.recipe(directStorePar2))
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
				Method: "-m0", Password: set.password, HeaderPassword: set.headerPassword, VolumeSize: set.volumeSize,
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
