// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// The ZIP fixtures behind the graph engine's ZIP rulings: a byte lost inside
// one entry of a compressed zip fails that entry alone, and a zip's empty
// directories and code page 437 names come out as the entries declare them.
// Both are written by Go from the generator's own pseudorandom payloads, so
// they carry no inputs and are byte-reproducible.

const (
	// The member that closes before the lost article and is delivered whole.
	lostArticleClosedMember = "Ember.Part1.mkv"
	// The member the lost article falls inside.
	lostArticleDamagedMember = "Quill.Part2.mkv"
	// The name a code page 437 entry decodes to. The entry records it as the
	// single byte 0x82 for the accented letter, with the UTF-8 flag off.
	cp437DecodedName = "Extras/Café.txt"
	cp437RawName     = "Extras/Caf\x82.txt"
	// A UTF-8 name recorded with the UTF-8 flag off: it decodes as UTF-8
	// because it is valid UTF-8, whatever the flag says.
	unflaggedUTF8Name = "Subs/Ñandú.srt"
	cp437Feature      = "Feature.mkv"
)

// zipEntry is one record of a Go-written zip: a file whose bytes come from
// Source, or a directory when Source is empty. RawName is the name exactly as
// recorded; UTF8Flag sets the general-purpose bit 11.
type zipEntry struct {
	RawName  string
	Source   string
	Deflate  bool
	UTF8Flag bool
}

// GraphGapRecipes exercise ZIP delivery and PAR2 content identity.
func GraphGapRecipes() []Recipe {
	return []Recipe{
		{
			Slug: "zip-deflate-lost-article", Family: "zip", ByteReproducible: true,
			Notes: "Two deflated members, the first 2 MiB and the second 4 MiB, of the generator's incompressible stream. " +
				"The scenario withholds one article inside the second member's data, so the first closes whole and the second never can.",
			Build: func(_ context.Context, env *Env) error {
				first, second := env.StagePath(lostArticleClosedMember), env.StagePath(lostArticleDamagedMember)
				if err := WritePRNG(first, "zip-deflate-lost-article/ember", 2<<20); err != nil {
					return err
				}
				if err := WritePRNG(second, "zip-deflate-lost-article/quill", 4<<20); err != nil {
					return err
				}
				return writeZipEntries(env.OutputPath("archive.zip"), []zipEntry{
					{RawName: lostArticleClosedMember, Source: first, Deflate: true},
					{RawName: lostArticleDamagedMember, Source: second, Deflate: true},
				})
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				return map[string]string{lostArticleClosedMember: env.StagePath(lostArticleClosedMember)}, nil
			},
		},
		{
			Slug: "zip-cp437-empty-dirs", Family: "zip", ByteReproducible: true,
			Notes: "Deflated entries under three directory records, two of them empty; one name recorded in code page 437 " +
				"and one valid UTF-8 name, both with the UTF-8 flag off.",
			Build: func(_ context.Context, env *Env) error {
				feature, notes, subs := env.StagePath(cp437Feature), env.StagePath("cafe.txt"), env.StagePath("nandu.srt")
				if err := WritePRNG(feature, "zip-cp437-empty-dirs/feature", 3<<20); err != nil {
					return err
				}
				if err := WriteText(notes, "Notes for the extras directory.", 4096); err != nil {
					return err
				}
				if err := WriteText(subs, "1\n00:00:01,000 --> 00:00:02,000\nSubtitle line.\n", 2048); err != nil {
					return err
				}
				return writeZipEntries(env.OutputPath("archive.zip"), []zipEntry{
					{RawName: "Extras/"},
					{RawName: "Extras/Empty/"},
					{RawName: "Subs/"},
					{RawName: "Empty/"},
					{RawName: cp437Feature, Source: feature, Deflate: true},
					{RawName: cp437RawName, Source: notes, Deflate: true},
					{RawName: unflaggedUTF8Name, Source: subs, Deflate: true},
				})
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				return map[string]string{
					cp437Feature:      env.StagePath(cp437Feature),
					cp437DecodedName:  env.StagePath("cafe.txt"),
					unflaggedUTF8Name: env.StagePath("nandu.srt"),
				}, nil
			},
		},
		{
			Slug: "zip-unicode-collisions", Family: "zip", ByteReproducible: true,
			Notes: "Three UTF-8 ZIP members collide under Unicode normalization and case folding. Every distinct payload must survive under the existing namespace suffix policy.",
			Build: func(_ context.Context, env *Env) error {
				names := []string{"Café.txt", "Cafe\u0301.txt", "CAFÉ.TXT"}
				var entries []zipEntry
				for index, name := range names {
					source := env.StagePath(fmt.Sprintf("member-%d", index))
					if err := WritePRNG(source, "zip-unicode-collisions/"+name, 4096); err != nil {
						return err
					}
					entries = append(entries, zipEntry{RawName: name, Source: source, Deflate: true, UTF8Flag: true})
				}
				return writeZipEntries(env.OutputPath("archive.zip"), entries)
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				return map[string]string{
					"Café.txt":           env.StagePath("member-0"),
					"Cafe\u0301 (1).txt": env.StagePath("member-1"),
					"CAFÉ (2).TXT":       env.StagePath("member-2"),
				}, nil
			},
		},
		{
			Slug: "par2-exact-name-content-match", Family: "PAR2", ByteReproducible: true,
			Notes: "A valid PAR2 set describes a.bin and b.bin. Only a.bin's bytes are posted, under b.bin's exact name; content must bind them to a.bin and recovery must reconstruct b.bin.",
			Build: func(ctx context.Context, env *Env) error {
				for _, member := range []struct {
					name string
					size int64
				}{{"a.bin", 8192}, {"b.bin", 16384}} {
					if err := WritePRNG(env.StagePath(member.name), "par2-exact-name-content-match/"+member.name, member.size); err != nil {
						return err
					}
					if err := CopyFile(env.StagePath(member.name), env.OutputPath(member.name)); err != nil {
						return err
					}
				}
				if err := env.PAR2(ctx, PAR2Spec{Base: "set.par2", SliceSize: 4096, RecoveryBlocks: 4, RecoveryFiles: 1, Sources: []string{"a.bin", "b.bin"}}); err != nil {
					return err
				}
				if err := os.Remove(env.OutputPath("a.bin")); err != nil {
					return err
				}
				return CopyFile(env.StagePath("a.bin"), env.OutputPath("b.bin"))
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				return map[string]string{"a.bin": env.StagePath("a.bin"), "b.bin": env.StagePath("b.bin")}, nil
			},
		},
	}
}

// writeZipEntries writes each entry with CreateRaw, so the name bytes, the
// UTF-8 flag and the compressed bytes are exactly what the recipe says and
// no data descriptor follows an entry.
func writeZipEntries(destination string, entries []zipEntry) error {
	return writeFile(destination, func(writer io.Writer) error {
		archive := zip.NewWriter(writer)
		for _, entry := range entries {
			header := &zip.FileHeader{Name: entry.RawName, Modified: CanonicalTime, Method: zip.Store}
			header.ModifiedDate, header.ModifiedTime = msdosTime(CanonicalTime) //lint:ignore SA1019 CreateRaw copies these fields verbatim
			header.NonUTF8 = !entry.UTF8Flag
			if entry.UTF8Flag {
				header.Flags |= 0x800
			}
			var payload []byte
			if entry.Source == "" {
				header.SetMode(os.ModeDir | 0o755)
			} else {
				contents, err := os.ReadFile(entry.Source)
				if err != nil {
					return err
				}
				header.SetMode(0o644)
				header.CRC32 = crc32.ChecksumIEEE(contents)
				header.UncompressedSize64 = uint64(len(contents))
				payload = contents
				if entry.Deflate {
					var compressed bytes.Buffer
					compressor, err := flate.NewWriter(&compressed, flate.DefaultCompression)
					if err != nil {
						return err
					}
					if _, err := compressor.Write(contents); err != nil {
						return err
					}
					if err := compressor.Close(); err != nil {
						return err
					}
					header.Method = zip.Deflate
					payload = compressed.Bytes()
				}
			}
			header.CompressedSize64 = uint64(len(payload))
			record, err := archive.CreateRaw(header)
			if err != nil {
				return err
			}
			if _, err := record.Write(payload); err != nil {
				return err
			}
		}
		return archive.Close()
	})
}
