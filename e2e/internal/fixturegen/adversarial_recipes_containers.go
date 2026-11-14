// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// The adversarial containers: tar and the stream codecs, the extraction
// budget's levels, and the names a job's members ask for. Go writes the tar,
// gzip and zstd bytes.

// The extraction budget's nest: one level of a .tar.gz expands its post
// about seventy times, under the ratio of a hundred, and the two levels
// together about a hundred and forty times, over it.
const (
	ratioLevelsNoise = 64 << 10
	ratioLevelsZeros = 4400 << 10
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
