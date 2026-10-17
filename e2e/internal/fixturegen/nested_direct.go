package fixturegen

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// The two passwords of the nested fixtures whose levels are keyed differently.
// A job's password candidates are the password it was submitted with, first,
// then the one its NZB file name carries in `{{password}}` braces; a scenario
// hands over two keys by putting one in each.
const (
	NestedOuterPassword = "weaver-e2e-nested-outer"
	NestedInnerPassword = "weaver-e2e-nested-inner"
)

// nestedArchive is one archive in a nested-direct fixture. Its entries are
// written into the stage first; child archives are built into the output
// directory, then moved into the stage under the name the parent stores them
// as, so every level is written by its own pinned writer and Go only moves
// finished files.
type nestedArchive struct {
	// name is what the writer is asked to produce. A RAR volume set writes
	// `.partN.rar` beside it.
	name string
	// storedAs renames a finished single-file child before its parent stores
	// it, for a child archive the parent names without an archive extension.
	storedAs string
	format   string // "rar5" or "7z"
	// method is the RAR compression selector; empty stores (`-m0`). 7z is
	// always the Copy chain unless compressed is set.
	method     string
	compressed bool
	solid      bool
	// password encrypts member data; headerPassword (RAR) or encryptHeaders
	// (7z) encrypts the headers too.
	password       string
	headerPassword string
	encryptHeaders bool
	volumeSize     string
	// split cuts the finished 7z into `.7z.NNN` parts of this many bytes.
	split   int64
	entries []nestedEntry
}

// nestedEntry is a leaf payload, an empty file, an empty directory, or a child
// archive. Leaf paths are stage-relative and unique across the whole tree.
type nestedEntry struct {
	leaf      *directStoreMember
	emptyFile string
	emptyDir  string
	child     *nestedArchive
	// member overrides the name the parent stores this entry under. It is
	// only for a directory holding other entries (`pack` for `pack/...`).
	member string
}

func leafEntry(path string, size int64, seed string) nestedEntry {
	return nestedEntry{leaf: &directStoreMember{path: path, size: size, seed: seed}}
}

func childEntry(child nestedArchive) nestedEntry { return nestedEntry{child: &child} }

// build writes the archive into the output directory and returns the names it
// produced there.
func (archive nestedArchive) build(ctx context.Context, env *Env) ([]string, error) {
	members := make([]string, 0, len(archive.entries))
	addMember := func(name string) {
		for _, existing := range members {
			if existing == name {
				return
			}
		}
		members = append(members, name)
	}
	for _, entry := range archive.entries {
		switch {
		case entry.leaf != nil:
			if err := WritePRNG(env.StagePath(entry.leaf.path), entry.leaf.seed, entry.leaf.size); err != nil {
				return nil, err
			}
			if entry.member == "" {
				addMember(entry.leaf.path)
			}
		case entry.emptyFile != "":
			if err := os.WriteFile(env.StagePath(entry.emptyFile), nil, 0o644); err != nil {
				return nil, err
			}
			if entry.member == "" {
				addMember(entry.emptyFile)
			}
		case entry.emptyDir != "":
			if err := os.MkdirAll(env.StagePath(entry.emptyDir), 0o755); err != nil {
				return nil, err
			}
			if entry.member == "" {
				addMember(entry.emptyDir)
			}
		case entry.child != nil:
			produced, err := entry.child.build(ctx, env)
			if err != nil {
				return nil, err
			}
			if entry.child.storedAs != "" {
				if len(produced) != 1 {
					return nil, fmt.Errorf("%s: only a single-file child can be renamed, got %v", entry.child.name, produced)
				}
				produced = []string{entry.child.storedAs}
				if err := os.Rename(env.OutputPath(entry.child.name), env.StagePath(entry.child.storedAs)); err != nil {
					return nil, err
				}
			} else {
				for _, name := range produced {
					if err := os.Rename(env.OutputPath(name), env.StagePath(name)); err != nil {
						return nil, err
					}
				}
			}
			for _, name := range produced {
				addMember(name)
			}
			continue
		}
		if entry.member != "" {
			addMember(entry.member)
		}
	}
	// The writers record every entry's mtime. A child archive moved in from
	// the output directory carries the time its own writer ran, and a
	// directory's mtime moves whenever an entry is created in it, so this
	// archive's members are stamped once all of them are in place rather than
	// as each one lands.
	for _, name := range members {
		if err := stampCanonicalTree(env.StagePath(name)); err != nil {
			return nil, err
		}
	}

	before, err := env.Outputs()
	if err != nil {
		return nil, err
	}
	switch archive.format {
	case "rar5":
		method := archive.method
		if method == "" {
			method = "-m0"
		}
		if err := env.RAR(ctx, RARSpec{
			Toolchain: DirectStoreRAR5Writer, Format: RAR5, Archive: archive.name,
			Method: method, Solid: archive.solid, Password: archive.password, HeaderPassword: archive.headerPassword,
			VolumeSize: archive.volumeSize, Extra: []string{"-qo-"}, Members: members,
		}); err != nil {
			return nil, err
		}
	case "7z":
		spec := SevenZipSpec{
			Archive: archive.name, Members: members, Store: !archive.compressed, Solid: archive.solid,
			Password: archive.password, EncryptHeaders: archive.encryptHeaders,
		}
		if err := env.SevenZip(ctx, spec); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%s: unknown nested format %q", archive.name, archive.format)
	}
	if archive.split > 0 {
		if _, err := SplitFile(env.OutputPath(archive.name), archive.split, func(index int) string {
			return env.OutputPath(fmt.Sprintf("%s.%03d", archive.name, index+1))
		}); err != nil {
			return nil, err
		}
		if err := removeOutput(env, archive.name); err != nil {
			return nil, err
		}
	}
	after, err := env.Outputs()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(before))
	for _, name := range before {
		seen[name] = true
	}
	var produced []string
	for _, name := range after {
		if !seen[name] {
			produced = append(produced, name)
		}
	}
	sort.Strings(produced)
	if len(produced) == 0 {
		return nil, fmt.Errorf("%s: the writer produced nothing", archive.name)
	}
	return produced, nil
}

// stampCanonicalTree sets CanonicalTime on a stage path and, for a directory,
// on everything under it. Stamping an entry never moves its parent's mtime,
// so the walk order does not matter.
func stampCanonicalTree(root string) error {
	return filepath.WalkDir(root, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(path, CanonicalTime, CanonicalTime)
	})
}

// leaves lists every payload in the tree, which is what the product delivers.
func (archive nestedArchive) leaves() []directStoreMember {
	var out []directStoreMember
	for _, entry := range archive.entries {
		switch {
		case entry.leaf != nil:
			out = append(out, *entry.leaf)
		case entry.emptyFile != "":
			out = append(out, directStoreMember{path: entry.emptyFile})
		case entry.child != nil:
			out = append(out, entry.child.leaves()...)
		}
	}
	return out
}

type nestedDirect struct {
	slug  string
	notes string
	root  nestedArchive
	after func(context.Context, *Env) error
	// undeliverable marks a fixture built so that no password candidate opens
	// one of its levels. The job fails and delivers nothing, so there is no
	// output to pin.
	undeliverable bool
}

func (set nestedDirect) recipe() Recipe {
	recipe := Recipe{
		Slug: set.slug, Family: "nested", Notes: set.notes,
		Build: func(ctx context.Context, env *Env) error {
			if _, err := set.root.build(ctx, env); err != nil {
				return err
			}
			if set.after != nil {
				return set.after(ctx, env)
			}
			return nil
		},
	}
	if set.undeliverable {
		return recipe
	}
	// Every payload, nested or not, is re-derived from its seed and pinned
	// under the path it has inside its own archive, so the run has to deliver
	// each one, byte for byte, beside its siblings.
	recipe.ExpectedOutputs = func(ctx context.Context, env *Env) (map[string]string, error) {
		digests := map[string]string{}
		for _, leaf := range set.root.leaves() {
			path := env.StagePath("expected-" + sanitizeName(leaf.path))
			if err := WritePRNG(path, leaf.seed, leaf.size); err != nil {
				return nil, err
			}
			digests[leaf.path] = path
		}
		return digests, nil
	}
	return recipe
}

// NestedDirectRecipes are the nested-archive direct-store shapes: archives
// inside a posted RAR5 or 7z set, across container pairings, volume and split
// topologies, encryption at either level, depth, and the shapes that fall back
// to a file or to the conventional path. Every non-empty payload is a seeded,
// incompressible stream of at least 2 MiB, so a stored member really is stored
// and no video artifact is involved.
func NestedDirectRecipes() []Recipe {
	sets := []nestedDirect{
		{
			slug: "nested-direct-7z-in-7z",
			notes: "A stored 7z holding a stored 7z beside an ordinary member. The scenario pins the outer set finalizing direct " +
				"and its one child streamed into a live child engine rather than written out as a file.",
			root: nestedArchive{name: "archive.7z", format: "7z", entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.7z", format: "7z", entries: []nestedEntry{
					leafEntry("lumen.vale.s01e01.mkv", 6<<20, "lumen-vale-inner"),
				}}),
				leafEntry("lumen.vale.s01e01.extras.bin", 3<<20, "lumen-vale-sibling"),
			}},
		},
		{
			slug: "nested-direct-7z-in-rar5",
			notes: "A stored RAR5 holding a stored 7z beside an ordinary member: the RAR outer routes direct and streams the 7z " +
				"child into a 7z engine, the first of the two cross-format pairings.",
			root: nestedArchive{name: "archive.rar", format: "rar5", entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.7z", format: "7z", entries: []nestedEntry{
					leafEntry("cinder.reach.s01e02.mkv", 6<<20, "cinder-reach-inner"),
				}}),
				leafEntry("cinder.reach.s01e02.extras.bin", 2<<20, "cinder-reach-sibling"),
			}},
		},
		{
			slug: "nested-direct-aes-7z-inner",
			notes: "A stored RAR5 holding a 7z whose data is AES-encrypted under clear headers, beside an ordinary member. The job " +
				"has no password of its own; the only candidate is the one in the NZB file name's braces, and the scenario pins " +
				"the outer set finalizing direct and the encrypted child streamed into a live child engine.",
			root: nestedArchive{name: "archive.rar", format: "rar5", entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.7z", format: "7z", password: NestedInnerPassword, entries: []nestedEntry{
					leafEntry("sable.ford.s01e15.mkv", 5<<20, "sable-ford-inner"),
				}}),
				leafEntry("sable.ford.s01e15.extras.bin", 2<<20, "sable-ford-sibling"),
			}},
		},
		{
			slug: "nested-direct-rar5-in-7z",
			notes: "A stored 7z holding a stored RAR5 beside an ordinary member: the 7z outer routes direct and streams the RAR " +
				"child into a RAR engine, the second cross-format pairing.",
			root: nestedArchive{name: "archive.7z", format: "7z", entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.rar", format: "rar5", entries: []nestedEntry{
					leafEntry("frost.arbor.s01e03.mkv", 6<<20, "frost-arbor-inner"),
				}}),
				leafEntry("frost.arbor.s01e03.extras.bin", 2<<20, "frost-arbor-sibling"),
			}},
		},
		{
			slug: "nested-direct-multivolume-rar5-7z",
			notes: "A stored RAR5 across three 8 MiB volumes whose second member, a stored 7z, starts inside the first volume and " +
				"crosses both volume boundaries, so the child engine is fed one archive from three volumes' routed bytes.",
			root: nestedArchive{name: "archive.rar", format: "rar5", volumeSize: "8m", entries: []nestedEntry{
				leafEntry("glass.meadow.s01e04.extras.bin", 2<<20, "glass-meadow-sibling"),
				childEntry(nestedArchive{name: "inner.7z", format: "7z", entries: []nestedEntry{
					leafEntry("glass.meadow.s01e04.mkv", 18<<20, "glass-meadow-inner"),
				}}),
			}},
		},
		{
			slug: "nested-direct-7z-aes-pair",
			notes: "An LZMA2 7z with AES data encryption holding a stored 7z AES inner under a different password. The job " +
				"carries the outer password and the NZB file name's braces the inner one, so each level opens only with its own " +
				"candidate. AES over a compressing coder is a by-design direct-store refusal, so the set demotes with " +
				"7z_encrypted_content and the conventional extraction opens both levels.",
			root: nestedArchive{name: "archive.7z", format: "7z", compressed: true, password: NestedOuterPassword, entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.7z", format: "7z", password: NestedInnerPassword, entries: []nestedEntry{
					leafEntry("hollow.crest.s01e05.mkv", 4<<20, "hollow-crest-inner"),
				}}),
				leafEntry("hollow.crest.s01e05.extras.bin", 2<<20, "hollow-crest-sibling"),
			}},
		},
		{
			slug: "nested-direct-hp-rar5-aes-7z",
			notes: "A stored RAR5 with `-hp` header and data encryption holding a 7z AES inner under the same password. The outer " +
				"routes direct and decrypts as it places; the encrypted parent member is not a routable child, so the 7z is " +
				"opened from its placed file afterwards.",
			root: nestedArchive{name: "archive.rar", format: "rar5", headerPassword: DirectStorePassword, entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.7z", format: "7z", password: DirectStorePassword, entries: []nestedEntry{
					leafEntry("iron.tide.s01e06.mkv", 5<<20, "iron-tide-inner"),
				}}),
			}},
		},
		{
			slug: "nested-direct-mhe-7z-rar5-p",
			notes: "An LZMA2 7z with `-mhe=on` header encryption holding a RAR5 `-p` inner under the same password. The job's " +
				"password opens the headers, and AES over a compressing coder is a by-design direct-store refusal, so the set " +
				"demotes with 7z_encrypted_content and the conventional extraction opens the encrypted RAR child.",
			root: nestedArchive{name: "archive.7z", format: "7z", compressed: true, password: DirectStorePassword, encryptHeaders: true, entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.rar", format: "rar5", password: DirectStorePassword, entries: []nestedEntry{
					leafEntry("jade.harbor.s01e07.mkv", 5<<20, "jade-harbor-inner"),
				}}),
			}},
		},
		{
			slug: "nested-direct-7z-mhe-first-password",
			notes: "An LZMA2 7z with `-mhe=on` under the outer password, holding an ordinary member and a stored, unencrypted 7z. " +
				"The job's own password is the right one; nested-direct-7z-mhe-second-password is the same shape with the right " +
				"candidate second. The set demotes with 7z_encrypted_content, and the scenario pins the direct-unpack chase " +
				"arming and its members being installed.",
			root: nestedArchive{name: "archive.7z", format: "7z", compressed: true, password: NestedOuterPassword, encryptHeaders: true, entries: []nestedEntry{
				leafEntry("umber.lake.s01e17.extras.bin", 4<<20, "umber-lake-sibling"),
				childEntry(nestedArchive{name: "inner.7z", format: "7z", entries: []nestedEntry{
					leafEntry("umber.lake.s01e17.mkv", 5<<20, "umber-lake-inner"),
				}}),
			}},
		},
		{
			slug: "nested-direct-7z-mhe-second-password",
			notes: "nested-direct-7z-mhe-first-password with the candidates the other way round: the job's own password is wrong " +
				"and the right one is in the NZB file name's braces. The scenario pins the direct-unpack chase arming and its " +
				"members being installed all the same.",
			root: nestedArchive{name: "archive.7z", format: "7z", compressed: true, password: NestedOuterPassword, encryptHeaders: true, entries: []nestedEntry{
				leafEntry("tarn.ridge.s01e16.extras.bin", 4<<20, "tarn-ridge-sibling"),
				childEntry(nestedArchive{name: "inner.7z", format: "7z", entries: []nestedEntry{
					leafEntry("tarn.ridge.s01e16.mkv", 5<<20, "tarn-ridge-inner"),
				}}),
			}},
		},
		{
			slug: "nested-direct-7z-aes-all-wrong",
			notes: "A stored RAR5 holding a 7z whose data is AES-encrypted under clear headers. The job's one candidate is the " +
				"wrong key, so the outer set finalizes direct and streams the child, and the child's extraction fails the job.",
			undeliverable: true,
			root: nestedArchive{name: "archive.rar", format: "rar5", entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.7z", format: "7z", password: NestedInnerPassword, entries: []nestedEntry{
					leafEntry("vesper.glen.s01e18.mkv", 4<<20, "vesper-glen-inner"),
				}}),
			}},
		},
		{
			slug: "nested-direct-3deep",
			notes: "A RAR5 holding a 7z holding a RAR5 holding the payload, all stored: three archive levels, with the outer set " +
				"finalizing direct and both nested archives streamed into live child engines.",
			root: nestedArchive{name: "archive.rar", format: "rar5", entries: []nestedEntry{
				childEntry(nestedArchive{name: "middle.7z", format: "7z", entries: []nestedEntry{
					childEntry(nestedArchive{name: "core.rar", format: "rar5", entries: []nestedEntry{
						leafEntry("kestrel.line.s01e08.mkv", 6<<20, "kestrel-line-core"),
					}}),
				}}),
			}},
		},
		{
			slug: "nested-direct-solid-outer-demotes",
			notes: "A solid, compressed RAR5 holding a 7z beside an ordinary member. RARLAB orders a solid run by extension, so the " +
				"7z opens the run and the sibling is the solid member: a by-design direct-store refusal, so the set demotes with " +
				"member_solid and the conventional extraction opens the child archive.",
			root: nestedArchive{name: "archive.rar", format: "rar5", method: "-m3", solid: true, entries: []nestedEntry{
				leafEntry("linen.pass.s01e09.extras.bin", 2<<20, "linen-pass-sibling"),
				childEntry(nestedArchive{name: "inner.7z", format: "7z", entries: []nestedEntry{
					leafEntry("linen.pass.s01e09.mkv", 4<<20, "linen-pass-inner"),
				}}),
			}},
		},
		{
			slug: "nested-direct-split-7z-inner",
			notes: "A stored RAR5 holding a stored 7z cut into `.7z.001`/`.7z.002` parts, so the child streamed into the child " +
				"engine is one archive assembled from two parent members in part order.",
			root: nestedArchive{name: "archive.rar", format: "rar5", entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.7z", format: "7z", split: 3 << 20, entries: []nestedEntry{
					leafEntry("marrow.field.s01e10.mkv", 5<<20, "marrow-field-inner"),
				}}),
			}},
		},
		{
			slug: "nested-direct-extensionless-child",
			notes: "A stored RAR5 holding a stored 7z under a name with no archive extension. Routing is by name, so the child " +
				"is placed as an ordinary file; the scenario pins its payload delivered and the file itself gone from the output.",
			root: nestedArchive{name: "archive.rar", format: "rar5", entries: []nestedEntry{
				childEntry(nestedArchive{name: "bundle.7z", storedAs: "bundle", format: "7z", entries: []nestedEntry{
					leafEntry("north.quay.s01e11.mkv", 4<<20, "north-quay-inner"),
				}}),
				leafEntry("north.quay.s01e11.extras.bin", 2<<20, "north-quay-sibling"),
			}},
		},
		{
			slug: "nested-direct-fake-archive-member",
			notes: "A stored RAR5 holding an ordinary payload named `.7z` that is not an archive, beside a real payload. The name " +
				"routes it to a child engine; the scenario pins it written out as a file instead and delivered unchanged.",
			root: nestedArchive{name: "archive.rar", format: "rar5", entries: []nestedEntry{
				leafEntry("notes.7z", 2<<20, "notes-not-an-archive"),
				leafEntry("opal.drift.s01e12.mkv", 4<<20, "opal-drift-sibling"),
			}},
		},
		{
			slug: "nested-direct-7z-empty-and-dirs",
			notes: "A stored RAR5 holding a stored 7z whose tree carries a payload under a directory, a zero-length file and an " +
				"empty directory. The scenario pins the child streamed, the payload and the zero-length file by digest, and the " +
				"empty directory by its presence.",
			root: nestedArchive{name: "archive.rar", format: "rar5", entries: []nestedEntry{
				childEntry(nestedArchive{name: "pack.7z", format: "7z", entries: []nestedEntry{
					{leaf: &directStoreMember{path: "pack/pine.gate.s01e13.mkv", size: 4 << 20, seed: "pine-gate-inner"}, member: "pack"},
					{emptyFile: "pack/empty.marker", member: "pack"},
					{emptyDir: "pack/hollow", member: "pack"},
				}}),
			}},
		},
		{
			slug: "nested-direct-par2-repaired-outer",
			notes: "A stored RAR5 across four 8 MiB volumes with PAR2 at 20%, whose only member is a stored 7z. The scenario deletes " +
				"tail articles of an interior volume, so the outer is repaired without demoting and the child it streams still " +
				"delivers the payload byte for byte.",
			root: nestedArchive{name: "archive.rar", format: "rar5", volumeSize: "8m", entries: []nestedEntry{
				childEntry(nestedArchive{name: "inner.7z", format: "7z", entries: []nestedEntry{
					leafEntry("quill.marsh.s01e14.mkv", 24<<20, "quill-marsh-inner"),
				}}),
			}},
			after: func(ctx context.Context, env *Env) error {
				volumes, err := env.Outputs()
				if err != nil {
					return err
				}
				return env.PAR2(ctx, PAR2Spec{
					Base: "archive.par2", RedundancyPercent: 20, RecoveryFiles: 4, Sources: volumes,
				})
			},
		},
	}
	recipes := make([]Recipe, 0, len(sets))
	for _, set := range sets {
		recipes = append(recipes, set.recipe())
	}
	return recipes
}
