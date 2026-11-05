// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"testing"
)

func TestEncryptedZipStoreRecipesCarryRealMultiArticleCiphertext(t *testing.T) {
	var nested [][]byte
	for _, recipe := range ZipStoreEncryptedRecipes() {
		t.Run(recipe.Slug, func(t *testing.T) {
			env := &Env{Work: t.TempDir(), Slug: recipe.Slug}
			if err := recipe.Build(t.Context(), env); err != nil {
				t.Fatal(err)
			}
			outputs, err := recipe.ExpectedOutputs(t.Context(), env)
			if err != nil {
				t.Fatal(err)
			}
			filename := "archive.zip"
			isNested := recipe.Slug == "zip-store-encrypted-nested-seek" || recipe.Slug == "zip-store-encrypted-nested-no-seek"
			if isNested {
				filename = "outer.zip"
			}
			archive, err := os.ReadFile(env.OutputPath(filename))
			if err != nil {
				t.Fatal(err)
			}
			if len(archive) <= 8*ZipStoreArticleSize {
				t.Fatal("archive must span more than eight articles")
			}
			if isNested {
				outer, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
				if err != nil {
					t.Fatal(err)
				}
				if len(outer.File) != 1 || outer.File[0].Name != "inner.zip" {
					t.Fatal("wrong nested shape")
				}
				want := uint16(zip.Store)
				if recipe.Slug == "zip-store-encrypted-nested-no-seek" {
					want = zip.Deflate
				}
				if outer.File[0].Method != want {
					t.Fatalf("outer method=%d want=%d", outer.File[0].Method, want)
				}
				reader, err := outer.File[0].Open()
				if err != nil {
					t.Fatal(err)
				}
				archive, err = io.ReadAll(reader)
				reader.Close()
				if err != nil {
					t.Fatal(err)
				}
				nested = append(nested, archive)
			}
			reader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
			if err != nil {
				t.Fatal(err)
			}
			if len(reader.File) != len(outputs) {
				t.Fatalf("members=%d expected outputs=%d", len(reader.File), len(outputs))
			}
			for _, member := range reader.File {
				if member.Method != zip.Store || member.Flags&1 == 0 {
					t.Fatalf("%s is not encrypted STORE", member.Name)
				}
				if member.CompressedSize64 != member.UncompressedSize64+12 {
					t.Fatal("missing cipher header")
				}
				raw, err := member.OpenRaw()
				if err != nil {
					t.Fatal(err)
				}
				encrypted, err := io.ReadAll(raw)
				if err != nil {
					t.Fatal(err)
				}
				decoded := zipCryptoDecryptForTest(encrypted, CorpusPassword)
				check := byte(member.CRC32 >> 24)
				if member.Flags&8 != 0 {
					check = byte(member.ModifiedTime >> 8)
				} //lint:ignore SA1019 ZIP password verifier
				if decoded[11] != check {
					t.Fatal("wrong encryption verifier")
				}
				plain := decoded[12:]
				expected, err := os.ReadFile(outputs[member.Name])
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(plain, expected) || crc32.ChecksumIEEE(plain) != member.CRC32 {
					t.Fatalf("%s plaintext/CRC differs", member.Name)
				}
				if len(plain) > 0 {
					if len(plain) <= 4*ZipStoreArticleSize {
						t.Fatal("member does not cross enough articles")
					}
					var compressed bytes.Buffer
					writer, err := flate.NewWriter(&compressed, flate.BestSpeed)
					if err != nil {
						t.Fatal(err)
					}
					writer.Write(plain)
					writer.Close()
					if compressed.Len()*100 < len(plain)*99 {
						t.Fatal("payload is compressible")
					}
					if bytes.Equal(encrypted[12:], plain) {
						t.Fatal("payload is plaintext")
					}
				}
				offset, err := member.DataOffset()
				if err != nil {
					t.Fatal(err)
				}
				local := int(offset) - 30 - len(member.Name) - len(member.Extra)
				length := binary.LittleEndian.Uint32(archive[local+22:])
				switch {
				case isNested:
					if member.Flags&8 == 0 || length != 0 {
						t.Fatal("nested archive must require directory lengths")
					}
				case recipe.Slug == "zip-store-encrypted-lying-header":
					if uint64(length) != member.UncompressedSize64+ZipStoreArticleSize {
						t.Fatal("local length does not lie")
					}
				default:
					if uint64(length) != member.UncompressedSize64 {
						t.Fatal("local length must be bounded and correct")
					}
				}
			}
		})
	}
	if len(nested) != 2 || !bytes.Equal(nested[0], nested[1]) {
		t.Fatal("seek/no-seek archives must carry identical encrypted inner bytes")
	}
}
