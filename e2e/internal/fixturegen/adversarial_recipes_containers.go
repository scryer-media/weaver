// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
)

// The adversarial containers: ZIP, 7z, tar and the stream codecs, the
// extraction budget's levels, and the names a job's members ask for. Go
// writes the tar, gzip and zstd bytes; every 7z comes from the pinned 7-Zip
// and Go only copies it or rewrites the signature header's two size fields
// and the checksum over them.

// The extraction budget's nest: one level of a .tar.gz expands its post
// about seventy times, under the ratio of a hundred, and the two levels
// together about a hundred and forty times, over it.
const (
	ratioLevelsNoise = 64 << 10
	ratioLevelsZeros = 4400 << 10
	// The zeros after the end blocks of a tar a gzip holds: the tar alone,
	// which is never delivered, expands its post a thousand times.
	ratioInnerPadding = 16 << 20
	// Each of two members under the tight group's 32 MiB member cap; the tar
	// that holds them is past it.
	memberCapMember = 24 << 20
)

// The name chain: this many members of inner tars all want one name, and a
// shallower member of the outer tar wants it too.
const (
	nameChainMembers = 2000
	nameChainName    = "beacon.txt"
)

// NameChainDelivered is the name a member of the chain is delivered under:
// the shallow member keeps the name, the others take its suffixes.
func NameChainDelivered(suffix int) string {
	if suffix == 0 {
		return nameChainName
	}
	return fmt.Sprintf("beacon (%d).txt", suffix)
}

// deepNameParts is how many one-letter directories the deep tar member's
// GNU long name runs through: a name of about 512 KiB, under the tar
// object's 1 MiB extension limit.
const deepNameParts = 262_000

// DeepName is the deep tar member's name.
var DeepName = strings.Repeat("a/", deepNameParts) + "deep.txt"

// The folded copies: a stored 7z of two members copied under distinct names
// into one tar. Over the tight group's entry cap of 1000, and well under the
// default cap.
const (
	foldedCopiesOverCap = 1200
	foldedCopiesHonest  = 50
)

// FoldedMembers are the members of the copied 7z.
var FoldedMembers = []string{"ember-a.txt", "ember-b.txt"}

// heldPiecesPayload is the stored 7z body the held-pieces case posts in
// 8 KiB articles, about two thousand of them before the end header.
const heldPiecesPayload = 16 << 20

// AdversarialContainerRecipes is the adversarial family's container cases.
func AdversarialContainerRecipes() []Recipe {
	return []Recipe{
		{
			Slug: "adv-zstd-later-frame-wide", Family: "adversarial", ByteReproducible: true,
			Notes: "Two zstd frames: the first names a 1 MiB window, the second a 2 TiB one, past any window a decoder is given. " +
				"Whatever the first frame reserved, the second is refused at its header and the job fails naming decode memory.",
			Build: writeZstdFrames("harbor-wide.mkv.zst", 20, 41),
		},
		{
			Slug: "adv-zstd-later-frame-honest", Family: "adversarial", ByteReproducible: true,
			Notes: "Two zstd frames: the first names a 1 MiB window, the second 256 MiB, wider than the decoder's own default but within every profile's window. " +
				"Multi-frame streams are honest, so the job completes with both frames' bytes.",
			Build: writeZstdFrames("harbor-light.mkv.zst", 20, 28),
		},
		{
			Slug: "adv-targz-inner-ratio", Family: "adversarial", ByteReproducible: true,
			Notes: "A .tar.gz whose tar holds one small text member and then 16 MiB of zeros after its end blocks. The tar is output the tar object reads and is never " +
				"delivered, but it alone expands the post a thousand times, so under the ratio group's 1 MiB floor the budget refuses it naming the ratio.",
			Build: writeInnerRatioBomb("beacon-bomb.tar.gz"),
		},
		{
			Slug: "adv-targz-levels-under-ratio", Family: "adversarial", ByteReproducible: true,
			Notes: "A .tar.gz whose tar and whose one member each expand the post about seventy times: together past the ratio of a hundred, alone under it. " +
				"Levels are judged one at a time, so under the ratio group's 1 MiB floor the job completes.",
			Build: writeRatioLevels("harbor-reel.tar.gz"),
		},
		{
			Slug: "adv-targz-inner-over-member-cap", Family: "adversarial", ByteReproducible: true,
			Notes: "A .tar.gz whose 48 MiB tar holds two members of 24 MiB. Under the tight group's 32 MiB member cap the tar is past it and each member under it; " +
				"only a delivered file is held to the member cap, so the job completes.",
			Build: writeMemberCapNest("harbor-pair.tar.gz"),
		},
		{
			Slug: "adv-tar-one-name-chain", Family: "adversarial", ByteReproducible: true,
			Notes: "A tar of two thousand inner tars, each holding beacon.txt, then beacon.txt itself one level up. The shallow member takes the name and every " +
				"deeper one moves on a suffix, a chain two thousand moves long; every file must be delivered under its own name.",
			Build: writeNameChain("harbor-crates.tar"),
		},
		{
			Slug: "adv-tar-deep-long-name", Family: "adversarial", ByteReproducible: true,
			Notes: "A tar member whose GNU long name runs through 262,000 one-letter directories, about 512 KiB, beside an ordinary member. " +
				"No disk can hold the path; the job must settle without the name's parts costing memory or time by its depth.",
			Build: writeDeepLongName("deep-name.tar"),
		},
		{
			Slug: "adv-tar-yenc-size-lie", Family: "adversarial", ByteReproducible: true,
			Notes: "A tar of one 12 KiB member posted in 8 KiB articles whose =ybegin headers state a size of 2^63-1 bytes. " +
				"The tar ends at its end blocks whatever its volume is said to hold, so the member is delivered.",
			Build: writeAdversarialTar("ember-lie.tar", []rawTarEntry{
				{Name: YencLieMember, Type: '0', Body: append(adversarialPayload(YencLieMember), PatternBytes(YencLieMember, 12<<10)...)},
			}),
		},
		{
			Slug: "adv-7z-folded-copies-over-cap", Family: "adversarial", ByteReproducible: true,
			Notes: "A tar of 1,200 identical copies of one stored 7z under distinct names. The copies fold into the first 7z but still count as the entries they are, " +
				"so under the tight group's entry cap of 1000 the budget refuses the job.",
			Build: writeFoldedCopies("ember-copies.tar", foldedCopiesOverCap),
		},
		{
			Slug: "adv-7z-folded-copies", Family: "adversarial", ByteReproducible: true,
			Notes: "A tar of fifty identical copies of one stored 7z under distinct names, under the default entry cap. The copies fold into the first 7z and its two " +
				"members are delivered once.",
			Build: writeFoldedCopies("ember-fifty.tar", foldedCopiesHonest),
		},
		{
			Slug: "adv-7z-end-header-2-63", Family: "adversarial", ByteReproducible: true,
			Notes: "A stored 7z whose signature header states an end header of 2^63 bytes at offset zero, with the start header checksum written again over it. " +
				"The store must hold only the bytes posted and the job must settle with nothing extracted.",
			Build: writeRestated7z("lantern-header.7z"),
		},
		{
			Slug: "adv-7z-held-pieces", Family: "adversarial", ByteReproducible: true,
			Notes: "One stored 7z of a 16 MiB member, posted in 8 KiB articles, so about two thousand pieces of body arrive and are held before the end header names them. " +
				"The held pieces must be released in a time that grows with the pieces, not their square, and the member delivered.",
			Build: writeHeldPieces7z("harbor-pieces.7z"),
		},
	}
}

// ------------------------------------------------------------------ zstd

// zstdFrame is one zstd frame of one raw block holding body, whose header
// names a window of 1 << log and no content size.
func zstdFrame(log int, body []byte) []byte {
	frame := []byte{0x28, 0xB5, 0x2F, 0xFD, 0x00, byte((log - 10) << 3)}
	// Last_Block set, Block_Type raw, Block_Size the body's length.
	header := uint32(1) | uint32(len(body))<<3
	frame = append(frame, byte(header), byte(header>>8), byte(header>>16))
	return append(frame, body...)
}

// ZstdFramePayload is what frame index of a two-frame case holds.
func ZstdFramePayload(output string, index int) []byte {
	return adversarialPayload(fmt.Sprintf("%s frame %d", output, index))
}

func writeZstdFrames(output string, first, second int) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		data := append(zstdFrame(first, ZstdFramePayload(output, 0)), zstdFrame(second, ZstdFramePayload(output, 1))...)
		return os.WriteFile(env.OutputPath(output), data, 0o644)
	}
}

// ------------------------------------------------------------ tar + gzip

// tarStreamMember is one regular member of a streamed ustar archive.
type tarStreamMember struct {
	Name  string
	Size  int64
	Write func(io.Writer) error
}

// writeTarStream writes members as a ustar archive into writer, end blocks
// included, without holding any body.
func writeTarStream(writer io.Writer, members []tarStreamMember) error {
	archive := tar.NewWriter(writer)
	for _, member := range members {
		header := &tar.Header{
			Name: member.Name, Mode: 0o644, Size: member.Size, ModTime: CanonicalTime,
			Typeflag: tar.TypeReg, Uname: "root", Gname: "root", Format: tar.FormatUSTAR,
		}
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		if err := member.Write(archive); err != nil {
			return err
		}
	}
	return archive.Close()
}

// writeGzipped writes one anonymous gzip member at the best level, its body
// whatever write puts into it.
func writeGzipped(path string, write func(io.Writer) error) error {
	return writeFile(path, func(writer io.Writer) error {
		compressor, err := gzip.NewWriterLevel(writer, gzip.BestCompression)
		if err != nil {
			return err
		}
		compressor.ModTime = CanonicalTime
		if err := write(compressor); err != nil {
			return err
		}
		return compressor.Close()
	})
}

func writeZeros(writer io.Writer, count int64) error {
	chunk := make([]byte, 1<<20)
	for count > 0 {
		step := min(count, int64(len(chunk)))
		if _, err := writer.Write(chunk[:step]); err != nil {
			return err
		}
		count -= step
	}
	return nil
}

func bytesMember(name string, body []byte) tarStreamMember {
	return tarStreamMember{Name: name, Size: int64(len(body)), Write: func(writer io.Writer) error {
		_, err := writer.Write(body)
		return err
	}}
}

// paddedMember is a member of size bytes: its sentinel payload, noise bytes
// of the seed, then zeros.
func paddedMember(name string, noise int, size int64) tarStreamMember {
	head := append(adversarialPayload(name), PatternBytes(name, noise)...)
	return tarStreamMember{Name: name, Size: size, Write: func(writer io.Writer) error {
		if _, err := writer.Write(head); err != nil {
			return err
		}
		return writeZeros(writer, size-int64(len(head)))
	}}
}

// RatioLevelsMember is the one member of adv-targz-levels-under-ratio.
const RatioLevelsMember = "harbor-reel.mkv"

func ratioLevelsMembers() []tarStreamMember {
	size := int64(len(adversarialPayload(RatioLevelsMember))) + ratioLevelsNoise + ratioLevelsZeros
	return []tarStreamMember{paddedMember(RatioLevelsMember, ratioLevelsNoise, size)}
}

func writeRatioLevels(output string) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		return writeGzipped(env.OutputPath(output), func(writer io.Writer) error {
			return writeTarStream(writer, ratioLevelsMembers())
		})
	}
}

// InnerRatioMember is the small member of adv-targz-inner-ratio's tar.
const InnerRatioMember = "ember.txt"

func writeInnerRatioBomb(output string) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		return writeGzipped(env.OutputPath(output), func(writer io.Writer) error {
			if err := writeTarStream(writer, []tarStreamMember{bytesMember(InnerRatioMember, adversarialPayload("ember inner ratio"))}); err != nil {
				return err
			}
			return writeZeros(writer, ratioInnerPadding)
		})
	}
}

// MemberCapMembers are the two members of adv-targz-inner-over-member-cap.
var MemberCapMembers = []string{"reel-a.mkv", "reel-b.mkv"}

func memberCapMembers() []tarStreamMember {
	members := make([]tarStreamMember, 0, len(MemberCapMembers))
	for _, name := range MemberCapMembers {
		members = append(members, paddedMember(name, 0, memberCapMember))
	}
	return members
}

func writeMemberCapNest(output string) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		return writeGzipped(env.OutputPath(output), func(writer io.Writer) error {
			return writeTarStream(writer, memberCapMembers())
		})
	}
}

// ------------------------------------------------------------ tar names

func nameChainEntries() ([]rawTarEntry, error) {
	entries := make([]rawTarEntry, 0, nameChainMembers+1)
	for index := 1; index <= nameChainMembers; index++ {
		inner, err := encodeRawTar([]rawTarEntry{
			{Name: nameChainName, Type: '0', Body: adversarialPayload(fmt.Sprintf("beacon crate %04d", index))},
		})
		if err != nil {
			return nil, err
		}
		entries = append(entries, rawTarEntry{Name: fmt.Sprintf("crate-%04d.tar", index), Type: '0', Body: inner})
	}
	entries = append(entries, rawTarEntry{Name: nameChainName, Type: '0', Body: adversarialPayload("beacon top")})
	return entries, nil
}

func writeNameChain(output string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		entries, err := nameChainEntries()
		if err != nil {
			return err
		}
		return writeAdversarialTar(output, entries)(ctx, env)
	}
}

// DeepSibling is the ordinary member beside the deep one.
const DeepSibling = "beacon.txt"

func deepLongNameEntries() []rawTarEntry {
	return []rawTarEntry{
		{Name: "././@LongLink", Type: 'L', Body: append([]byte(DeepName), 0)},
		{Name: DeepName[:100], Type: '0', Body: adversarialPayload("deep")},
		{Name: DeepSibling, Type: '0', Body: adversarialPayload("deep sibling")},
	}
}

func writeDeepLongName(output string) func(context.Context, *Env) error {
	return writeAdversarialTar(output, deepLongNameEntries())
}

// YencLieMember is the member of adv-tar-yenc-size-lie.
const YencLieMember = "ember.mkv"

// ------------------------------------------------------------------- 7z

// stagedSevenZip has 7-Zip store the named members, each staged with its
// sentinel payload, in one 7z, and moves it to the stage so it is no output
// of its own.
func stagedSevenZip(ctx context.Context, env *Env, archive string, names []string) ([]byte, error) {
	for _, name := range names {
		if err := os.WriteFile(env.StagePath(name), adversarialPayload(name), 0o644); err != nil {
			return nil, err
		}
	}
	if err := env.SevenZip(ctx, SevenZipSpec{Archive: archive, Members: names, Store: true, Deterministic: true}); err != nil {
		return nil, err
	}
	staged := env.StagePath(archive)
	if err := os.Rename(env.OutputPath(archive), staged); err != nil {
		return nil, err
	}
	return os.ReadFile(staged)
}

// foldedCopyEntries is a tar of copies of one 7z under distinct names.
func foldedCopyEntries(archive []byte, copies int) []rawTarEntry {
	entries := make([]rawTarEntry, 0, copies)
	for index := 1; index <= copies; index++ {
		entries = append(entries, rawTarEntry{Name: fmt.Sprintf("ember-%04d.7z", index), Type: '0', Body: archive})
	}
	return entries
}

func writeFoldedCopies(output string, copies int) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		archive, err := stagedSevenZip(ctx, env, "ember.7z", FoldedMembers)
		if err != nil {
			return err
		}
		return writeAdversarialTar(output, foldedCopyEntries(archive, copies))(ctx, env)
	}
}

// restateSevenZip rewrites a 7z signature header to state its end header at
// offset and length, with the start header checksum written again over the
// two fields, the way the 7z store's own tests forge one.
func restateSevenZip(data []byte, offset, length uint64) ([]byte, error) {
	if len(data) < 32 || string(data[:6]) != "7z\xbc\xaf\x27\x1c" {
		return nil, fmt.Errorf("not a 7z signature header")
	}
	forged := append([]byte(nil), data...)
	binary.LittleEndian.PutUint64(forged[12:20], offset)
	binary.LittleEndian.PutUint64(forged[20:28], length)
	binary.LittleEndian.PutUint32(forged[8:12], crc32.ChecksumIEEE(forged[12:32]))
	return forged, nil
}

func writeRestated7z(output string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		archive, err := stagedSevenZip(ctx, env, "lantern.7z", []string{"lantern.txt"})
		if err != nil {
			return err
		}
		forged, err := restateSevenZip(archive, 0, 1<<63)
		if err != nil {
			return err
		}
		return os.WriteFile(env.OutputPath(output), forged, 0o644)
	}
}

// HeldPiecesMember is the member of adv-7z-held-pieces.
const HeldPiecesMember = "harbor-pieces.mkv"

func writeHeldPieces7z(output string) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		payload := append(adversarialPayload(HeldPiecesMember), PatternBytes(HeldPiecesMember, heldPiecesPayload)...)
		if err := os.WriteFile(env.StagePath(HeldPiecesMember), payload, 0o644); err != nil {
			return err
		}
		return env.SevenZip(ctx, SevenZipSpec{Archive: output, Members: []string{HeldPiecesMember}, Store: true, Deterministic: true})
	}
}
