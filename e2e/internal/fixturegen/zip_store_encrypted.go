// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Uneven sizes put cipher state, article boundaries and member boundaries at
// different offsets. Every nonempty member spans several posted articles.
const ZipStoreArticleSize = 768000

type encryptedZipShape struct {
	slug         string
	multi        bool
	descriptor   bool
	nested       bool
	outerDeflate bool
	lyingLength  bool
}

var encryptedZipShapes = []encryptedZipShape{
	{slug: "zip-store-encrypted-single"},
	{slug: "zip-store-encrypted-multi-member", multi: true},
	{slug: "zip-store-encrypted-descriptor", descriptor: true},
	{slug: "zip-store-encrypted-lying-header", lyingLength: true},
	{slug: "zip-store-encrypted-nested-seek", descriptor: true, nested: true},
	{slug: "zip-store-encrypted-nested-no-seek", descriptor: true, nested: true, outerDeflate: true},
}

func (shape encryptedZipShape) members(env *Env) []Member {
	if shape.multi {
		return []Member{
			{Name: "feature.mkv", Source: env.StagePath("feature.mkv")},
			{Name: "extras/bonus.mkv", Source: env.StagePath("bonus.mkv")},
			{Name: "empty.bin", Source: env.StagePath("empty.bin")},
		}
	}
	return []Member{{Name: "feature.mkv", Source: env.StagePath("feature.mkv")}}
}

// ZipStoreEncryptedRecipes uses the Go writer and its independent ZipCrypto
// implementation. The nested pair carries byte-identical inner archives;
// only the outer member's storage method changes whether it can be sought.
func ZipStoreEncryptedRecipes() []Recipe {
	var recipes []Recipe
	for _, shape := range encryptedZipShapes {
		recipes = append(recipes, Recipe{
			Slug: shape.slug, Family: "zip", ByteReproducible: true,
			Notes: "Stored ZipCrypto over deterministic incompressible multi-article payloads, with exact output digests and archive-mode assertions.",
			Build: func(_ context.Context, env *Env) error {
				if err := WritePRNG(env.StagePath("feature.mkv"), "encrypted-zip/feature", 6<<20+123); err != nil {
					return err
				}
				if shape.multi {
					if err := WritePRNG(env.StagePath("bonus.mkv"), "encrypted-zip/bonus", 3<<20+71); err != nil {
						return err
					}
					if err := WritePRNG(env.StagePath("empty.bin"), "encrypted-zip/empty", 0); err != nil {
						return err
					}
				}
				destination := env.OutputPath("archive.zip")
				if shape.nested {
					destination = env.StagePath("inner.zip")
				}
				members := shape.members(env)
				var err error
				if shape.descriptor {
					err = writeEncryptedDescriptorZip(destination, members, !shape.nested)
				} else {
					err = WriteZip(destination, members, CorpusPassword)
				}
				if err != nil {
					return err
				}
				if shape.lyingLength {
					data, err := os.ReadFile(destination)
					if err != nil {
						return err
					}
					// The central directory remains authoritative and correct;
					// the local header lies about the plaintext length.
					length := binary.LittleEndian.Uint32(data[22:26])
					binary.LittleEndian.PutUint32(data[22:26], length+ZipStoreArticleSize)
					if err := os.WriteFile(destination, data, 0o644); err != nil {
						return err
					}
				}
				if shape.nested {
					return writeZipEntries(env.OutputPath("outer.zip"), []zipEntry{{
						RawName: "inner.zip", Source: destination, Deflate: shape.outerDeflate, UTF8Flag: true,
					}})
				}
				return nil
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				outputs := make(map[string]string)
				for _, member := range shape.members(env) {
					outputs[member.Name] = member.Source
				}
				return outputs, nil
			},
		})
	}
	return recipes
}

// A descriptor uses the high byte of the DOS time for its password verifier.
// CreateRaw writes zero local sizes with bit 3; the bounded variant fills
// them afterward, retaining the descriptor and the same encrypted payload.
func writeEncryptedDescriptorZip(destination string, members []Member, bounded bool) error {
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	for _, member := range members {
		plain, err := os.ReadFile(member.Source)
		if err != nil {
			return err
		}
		header := &zip.FileHeader{
			Name: member.Name, Method: zip.Store, Flags: 9,
			CRC32:              crc32.ChecksumIEEE(plain),
			UncompressedSize64: uint64(len(plain)), CompressedSize64: uint64(len(plain) + 12),
		}
		header.ModifiedDate, header.ModifiedTime = msdosTime(CanonicalTime) //lint:ignore SA1019 CreateRaw copies these fields verbatim
		header.SetMode(0o644)
		verifier := uint32(header.ModifiedTime>>8) << 24 //lint:ignore SA1019 the descriptor verifier is the DOS-time high byte
		cipher := zipCryptoEncrypt(plain, CorpusPassword, verifier, member.Name)
		output, err := archive.CreateRaw(header)
		if err != nil {
			return err
		}
		if _, err := output.Write(cipher); err != nil {
			return err
		}
	}
	if err := archive.Close(); err != nil {
		return err
	}
	data := buffer.Bytes()
	if bounded {
		reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return err
		}
		for _, member := range reader.File {
			offset, err := member.DataOffset()
			if err != nil {
				return err
			}
			local := int(offset) - 30 - len(member.Name) - len(member.Extra)
			if local < 0 || binary.LittleEndian.Uint32(data[local:]) != 0x04034b50 {
				return fmt.Errorf("cannot locate local header for %q", member.Name)
			}
			binary.LittleEndian.PutUint32(data[local+14:], member.CRC32)
			binary.LittleEndian.PutUint32(data[local+18:], uint32(member.CompressedSize64))
			binary.LittleEndian.PutUint32(data[local+22:], uint32(member.UncompressedSize64))
		}
	}
	return writeFile(destination, func(writer io.Writer) error { _, err := writer.Write(data); return err })
}
