package fixturegen

import "context"

// The three invented member names the solid and non-solid fixtures pack, so a
// multi-member 7z is distinguishable from a single-member one by listing alone.
const sevenZipSingleMember = "silver_horizon.mkv"

var sevenZipMatrixMembers = []string{
	"silver_horizon_part1.mkv",
	"silver_horizon_part2.mkv",
	"silver_horizon_part3.mkv",
}

// sevenZipCodec is one cell of the direct-unpack codec matrix.
type sevenZipCodec struct {
	// Slug names the artifact and the scenarios derived from it.
	Slug string
	// Notes is the artifact's ledger note.
	Notes string
	// Password, when the scenario has to supply one.
	Password string
	// Build writes the archive.
	Build func(context.Context, *Env) error
	// Members are the entry names extraction must produce, in archive order.
	Members []string
}

// ExpectedOutputs maps every member of a codec-matrix fixture to the clip whose
// bytes it carries, so fixturegen can fill the scenario's expected digests.
func (codec sevenZipCodec) ExpectedOutputs() func(context.Context, *Env) (map[string]string, error) {
	members := codec.Members
	return func(ctx context.Context, env *Env) (map[string]string, error) {
		clip, err := env.ArtifactFile(ctx, "clip-small", "small.mkv")
		if err != nil {
			return nil, err
		}
		outputs := make(map[string]string, len(members))
		for _, member := range members {
			outputs[member] = clip
		}
		return outputs, nil
	}
}

// oneMember stages the short clip under an invented name and writes one 7z
// over it with the given chain.
func oneMember(spec SevenZipSpec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		if err := env.Stage(ctx, "clip-small", sevenZipSingleMember); err != nil {
			return err
		}
		spec.Archive = "archive.7z"
		spec.Members = []string{sevenZipSingleMember}
		return env.SevenZip(ctx, spec)
	}
}

// threeMembers stages the short clip three times under invented names, so
// solidity is observable.
func threeMembers(spec SevenZipSpec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		for _, member := range sevenZipMatrixMembers {
			if err := env.Stage(ctx, "clip-small", member); err != nil {
				return err
			}
		}
		spec.Archive = "archive.7z"
		spec.Members = sevenZipMatrixMembers
		return env.SevenZip(ctx, spec)
	}
}

// sevenZipCodecMatrix is the writable-by-the-oracle, decodable-by-weaver
// intersection, measured from `7zz i` on the pinned 26.02 build.
func sevenZipCodecMatrix() []sevenZipCodec {
	return []sevenZipCodec{
		{
			Slug:    "copy",
			Notes:   "Stored 7z: no compression, so the packed stream is the payload verbatim.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Store: true}),
		},
		{
			Slug:    "lzma",
			Notes:   "LZMA1, the original 7z coder, still the default for small dictionaries.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Methods: []string{"-m0=LZMA"}}),
		},
		{
			Slug:    "lzma2",
			Notes:   "LZMA2, the modern default and the chain most real 7z posts use.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Methods: []string{"-m0=LZMA2"}}),
		},
		{
			Slug:    "bzip2",
			Notes:   "BZip2 inside a 7z container, a block coder rather than a stream one.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Methods: []string{"-m0=BZip2"}}),
		},
		{
			Slug:    "ppmd",
			Notes:   "PPMd, the context-modelling coder; its reads are the finest-grained of the matrix.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Methods: []string{"-m0=PPMd"}}),
		},
		{
			Slug:    "deflate",
			Notes:   "Deflate inside a 7z container. Deflate64 is deliberately excluded: 7-Zip writes it, weaver has no decoder for it.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Methods: []string{"-m0=Deflate"}}),
		},
		{
			Slug:    "delta-lzma2",
			Notes:   "A Delta filter ahead of LZMA2 — a two-coder chain, so the reader crosses a filter boundary.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Methods: []string{"-m0=Delta:4", "-m1=LZMA2"}}),
		},
		{
			Slug:    "bcj-lzma2",
			Notes:   "The x86 BCJ branch filter ahead of LZMA2. The payload is video rather than code, so the filter earns nothing — the point is the chain's decode path, not its ratio.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Methods: []string{"-m0=BCJ", "-m1=LZMA2"}}),
		},
		{
			Slug: "bcj2",
			Notes: "BCJ2, the format's only multi-input coder: four pack streams read through concurrent cursors. " +
				"Measured to read as one ascending sweep after a header probe, so a chase overlaps it like any other chain.",
			Members: []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{Methods: []string{
				"-m0=BCJ2", "-m1=LZMA2", "-m2=LZMA2", "-m3=LZMA2",
				"-mb0:1", "-mb0s1:2", "-mb0s2:3",
			}}),
		},
		{
			Slug:     "aes256",
			Notes:    "AES256 over the member data with the headers left readable, so the entry table lists without a password.",
			Password: CorpusPassword,
			Build:    oneMember(SevenZipSpec{Methods: []string{"-m0=LZMA2"}, Password: CorpusPassword}),
		},
		{
			Slug:     "aes256-header",
			Notes:    "AES256 with `-mhe=on`, so the end header is itself an encrypted packed stream and nothing lists without the password.",
			Password: CorpusPassword,
			Members:  []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{
				Methods: []string{"-m0=LZMA2"}, Password: CorpusPassword, EncryptHeaders: true,
			}),
		},
		{
			Slug:    "solid",
			Notes:   "Three members packed into one solid LZMA2 block, so no member decodes without the ones before it.",
			Members: sevenZipMatrixMembers, Build: threeMembers(SevenZipSpec{Methods: []string{"-m0=LZMA2"}, Solid: true}),
		},
		{
			Slug:    "nonsolid",
			Notes:   "The same three members in their own blocks, the counterpart to the solid fixture.",
			Members: sevenZipMatrixMembers, Build: threeMembers(SevenZipSpec{Methods: []string{"-m0=LZMA2"}}),
		},
	}
}
