package fixturegen

import (
	"context"
	"os"
)

// Payload size for the codec matrix. Big enough that every coder does real
// work and a three-way split has substance, small enough that thirteen chains
// build in seconds.
const sevenZipMatrixPayloadBytes = 384 * 1024

// deterministicPayload writes `len` bytes of a fixed pseudo-random stream.
//
// Derived rather than encoded, on purpose. The corpus's clips come out of
// ffmpeg, which is not bit-reproducible across machines — an x264 encode of the
// same filter graph gives different bytes — so a fixture built on one would
// regenerate to a different digest than the ledger records. These bytes are a
// pure function of the seed, so the same recipe gives the same archive
// anywhere, and the ledger can be a contract rather than a snapshot.
//
// Incompressible-ish by construction, which is also what a codec matrix wants:
// every coder has to move real volume rather than emit a run-length token.
func deterministicPayload(path string, length int, seed uint64) error {
	state := seed | 1
	buf := make([]byte, length)
	for i := range buf {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		buf[i] = byte(state >> 24)
	}
	return os.WriteFile(path, buf, 0o644)
}

// The three invented member names the solid and non-solid fixtures pack, so a
// multi-member 7z is distinguishable from a single-member one by listing alone.
const sevenZipSingleMember = "silver_horizon.mkv"

// Payload for the repair-resume fixture.
//
// Big enough that decoding it takes far longer than downloading it locally,
// which is the property the whole scenario rests on — see the invariant on the
// `direct-unpack-repair` recipe.
const directUnpackRepairPayloadBytes = 8 * 1024 * 1024

// DirectUnpackRepairPayload writes the repair fixture's payload. Exported for
// the recipe's expected-output oracle, which re-derives it rather than keeping
// a copy.
func DirectUnpackRepairPayload(path string) error {
	return deterministicPayload(path, directUnpackRepairPayloadBytes, 7)
}

// BuildDirectUnpackRepairSource writes the PPMd archive the repair fixture
// splits.
//
// PPMd on purpose: it is the slowest decoder in the matrix by a wide margin, and
// the scenario needs the chase to still be working through the early parts when
// the later ones land and their verdicts arrive.
func BuildDirectUnpackRepairSource(ctx context.Context, env *Env) error {
	if err := DirectUnpackRepairPayload(env.StagePath(sevenZipSingleMember)); err != nil {
		return err
	}
	return env.SevenZip(ctx, SevenZipSpec{
		Archive:       "archive.7z",
		Members:       []string{sevenZipSingleMember},
		Methods:       []string{"-m0=PPMd"},
		Deterministic: true,
	})
}

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
	// SaltsItsOwnKey is true for the AES chains. 7-Zip draws a fresh random
	// salt and IV for every encrypted archive and offers no switch to fix
	// them, so those fixtures cannot be byte-reproducible however deterministic
	// their payload is — which is exactly the case `ByteReproducible` is
	// defined to exclude. They are reproducible in *shape*, and the published
	// corpus is what keeps their bytes stable across machines.
	SaltsItsOwnKey bool
	// Build writes the archive.
	Build func(context.Context, *Env) error
	// Members are the entry names extraction must produce, in archive order.
	Members []string
}

// ExpectedOutputs maps every member of a codec-matrix fixture to the clip whose
// bytes it carries, so fixturegen can fill the scenario's expected digests.
func (codec sevenZipCodec) ExpectedOutputs() func(context.Context, *Env) (map[string]string, error) {
	members := codec.Members
	return func(_ context.Context, env *Env) (map[string]string, error) {
		// Re-derive the payload rather than reach for the staged copy: a
		// scenario that only publishes or splits an artifact has its own empty
		// stage, and the bytes are a pure function of the seed anyway, so
		// writing them again is both cheaper and more honest than depending on
		// which scenario happened to stage them.
		outputs := make(map[string]string, len(members))
		for index, member := range members {
			path := env.StagePath(member)
			seed := uint64(1)
			if len(members) > 1 {
				seed = uint64(index) + 1
			}
			if err := deterministicPayload(path, sevenZipMatrixPayloadBytes, seed); err != nil {
				return nil, err
			}
			outputs[member] = path
		}
		return outputs, nil
	}
}

// oneMember stages the short clip under an invented name and writes one 7z
// over it with the given chain.
func oneMember(spec SevenZipSpec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		if err := deterministicPayload(
			env.StagePath(sevenZipSingleMember),
			sevenZipMatrixPayloadBytes,
			1,
		); err != nil {
			return err
		}
		spec.Archive = "archive.7z"
		spec.Members = []string{sevenZipSingleMember}
		spec.Deterministic = true
		return env.SevenZip(ctx, spec)
	}
}

// threeMembers stages the short clip three times under invented names, so
// solidity is observable.
func threeMembers(spec SevenZipSpec) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		for index, member := range sevenZipMatrixMembers {
			if err := deterministicPayload(
				env.StagePath(member),
				sevenZipMatrixPayloadBytes,
				uint64(index)+1,
			); err != nil {
				return err
			}
		}
		spec.Archive = "archive.7z"
		spec.Members = sevenZipMatrixMembers
		spec.Deterministic = true
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
			Slug:           "aes256",
			Notes:          "AES256 over the member data with the headers left readable, so the entry table lists without a password.",
			Password:       CorpusPassword,
			SaltsItsOwnKey: true,
			Build:          oneMember(SevenZipSpec{Methods: []string{"-m0=LZMA2"}, Password: CorpusPassword}),
		},
		{
			Slug:           "aes256-header",
			Notes:          "AES256 with `-mhe=on`, so the end header is itself an encrypted packed stream and nothing lists without the password.",
			Password:       CorpusPassword,
			SaltsItsOwnKey: true,
			Members:        []string{sevenZipSingleMember}, Build: oneMember(SevenZipSpec{
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
