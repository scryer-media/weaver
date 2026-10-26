// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/flate"
	"crypto/md5"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"testing"
)

func readZip(t *testing.T, encoded []byte) *zip.Reader {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(encoded), int64(len(encoded)))
	if err != nil {
		t.Fatalf("archive/zip refuses the encoding: %v", err)
	}
	return reader
}

func TestRawZipIsReadableWhereItTellsTheTruth(t *testing.T) {
	encoded, err := encodeRawZip(traversalZipEntries())
	if err != nil {
		t.Fatal(err)
	}
	reader := readZip(t, encoded)
	if len(reader.File) != len(TraversalNames) {
		t.Fatalf("%d members, want %d", len(reader.File), len(TraversalNames))
	}
	for index, file := range reader.File {
		if file.Name != TraversalNames[index] {
			t.Errorf("member %d is %q, want %q", index, file.Name, TraversalNames[index])
		}
		opened, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(opened)
		if err != nil {
			t.Fatalf("%s: %v", file.Name, err)
		}
		if !bytes.Equal(body, adversarialPayload(file.Name)) {
			t.Errorf("%s holds %q", file.Name, body)
		}
	}
}

func TestTraversalDeliveredCoversEveryName(t *testing.T) {
	sources := map[string]bool{}
	for _, source := range TraversalDelivered {
		sources[source] = true
	}
	for _, name := range TraversalNames {
		if !sources[name] {
			t.Errorf("%q has no delivered name", name)
		}
	}
	if len(sources) != len(TraversalNames) {
		t.Errorf("%d delivered names for %d archive names", len(sources), len(TraversalNames))
	}
}

func TestRawZipDuplicatesAndSharedHeadersAreTwoRecords(t *testing.T) {
	encoded, err := encodeRawZip([]rawZipEntry{
		storedEntry("dup.txt", []byte("one")),
		storedEntry("dup.txt", []byte("two")),
		{CentralName: "shared.txt", SharesLocalOf: 1, Body: []byte("one"), Size: 3, CRC: crc32.ChecksumIEEE([]byte("one"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := readZip(t, encoded)
	if len(reader.File) != 3 {
		t.Fatalf("%d records, want 3", len(reader.File))
	}
	first, _ := reader.File[0].DataOffset()
	shared, _ := reader.File[2].DataOffset()
	if first != shared {
		t.Errorf("the shared record reads data at %d, the first at %d", shared, first)
	}
	if reader.File[0].Name != reader.File[1].Name {
		t.Errorf("the duplicate names differ: %q, %q", reader.File[0].Name, reader.File[1].Name)
	}
}

func TestRawZipDeclaredSizesLie(t *testing.T) {
	entry := storedEntry("declared.bin", []byte("tiny"))
	entry.DeclaredSize = 0xFFFFFFF0
	encoded, err := encodeRawZip([]rawZipEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(encoded[22:26]); got != 0xFFFFFFF0 {
		t.Errorf("local header declares %d bytes", got)
	}
	disagree := storedEntry("local.txt", []byte("body"))
	disagree.CentralName = "central.txt"
	disagree.CentralSize = 8
	encoded, err = encodeRawZip([]rawZipEntry{disagree})
	if err != nil {
		t.Fatal(err)
	}
	reader := readZip(t, encoded)
	if reader.File[0].Name != "central.txt" || reader.File[0].UncompressedSize64 != 8 {
		t.Errorf("central record is %q of %d bytes", reader.File[0].Name, reader.File[0].UncompressedSize64)
	}
	if !bytes.Contains(encoded, []byte("local.txt")) {
		t.Error("the local header lost its own name")
	}
}

func TestDeflatedZerosInflateToTheirSize(t *testing.T) {
	const size = 3<<20 + 17
	body, checksum, err := deflatedZeros(size)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > size/500 {
		t.Errorf("%d zeros deflated to %d bytes", size, len(body))
	}
	inflated, err := io.ReadAll(flate.NewReader(bytes.NewReader(body)))
	if err != nil {
		t.Fatal(err)
	}
	if len(inflated) != size || crc32.ChecksumIEEE(inflated) != checksum || bytes.ContainsFunc(inflated, func(r rune) bool { return r != 0 }) {
		t.Errorf("inflated %d bytes, crc %08x want %08x", len(inflated), crc32.ChecksumIEEE(inflated), checksum)
	}
}

func TestNestedZipsNestToTheirDepth(t *testing.T) {
	env := &Env{Work: t.TempDir()}
	if err := writeNestedZips("nest.zip", 17)(t.Context(), env); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(env.OutputPath("nest.zip"))
	if err != nil {
		t.Fatal(err)
	}
	depth := 0
	for {
		depth++
		reader := readZip(t, encoded)
		if len(reader.File) != 1 {
			t.Fatalf("level %d holds %d members", depth, len(reader.File))
		}
		opened, err := reader.File[0].Open()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err = io.ReadAll(opened)
		if err != nil {
			t.Fatal(err)
		}
		if reader.File[0].Name == "deepest.txt" {
			break
		}
	}
	if depth != 17 {
		t.Errorf("nested %d deep, want 17", depth)
	}
}

func TestRawTarCarriesLinksAndHostileNames(t *testing.T) {
	encoded, err := encodeRawTar(append(traversalTarEntries(), linkTarEntries()...))
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(bytes.NewReader(encoded))
	var names []string
	links := map[string]string{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("archive/tar refuses the encoding: %v", err)
		}
		names = append(names, header.Name)
		if header.Typeflag == tar.TypeSymlink || header.Typeflag == tar.TypeLink {
			links[header.Name] = header.Linkname
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg && !bytes.Equal(body, adversarialPayload(header.Name)) {
			t.Errorf("%s holds %q", header.Name, body)
		}
	}
	if len(names) != len(TraversalNames)+len(linkTarEntries()) {
		t.Errorf("%d records: %v", len(names), names)
	}
	if links["links/hard-link"] != "/etc/hosts" || links["links/dir-link"] != "/tmp" || !strings.HasPrefix(links["links/file-link"], "../") {
		t.Errorf("links lost their targets: %v", links)
	}
}

// par2Packet seals one packet the way par2cmdline does.
func par2Packet(setID []byte, kind []byte, body []byte) []byte {
	packet := make([]byte, 64+len(body))
	copy(packet, par2Magic)
	binary.LittleEndian.PutUint64(packet[8:], uint64(len(packet)))
	copy(packet[32:48], setID)
	copy(packet[48:64], kind)
	copy(packet[64:], body)
	sum := md5.Sum(packet[32:])
	copy(packet[16:32], sum[:])
	return packet
}

func lyingSet() []byte {
	setID := bytes.Repeat([]byte{7}, 16)
	main := make([]byte, 12+16)
	binary.LittleEndian.PutUint64(main, 65536)
	binary.LittleEndian.PutUint32(main[8:], 1)
	description := make([]byte, 56+16)
	for index := 16; index < 48; index++ {
		description[index] = byte(index)
	}
	binary.LittleEndian.PutUint64(description[48:], 3*65536)
	copy(description[56:], "lie-payload.bin")
	check := make([]byte, 16+3*20)
	check[16] = 0x55
	var set []byte
	set = append(set, par2Packet(setID, par2MainType, main)...)
	set = append(set, par2Packet(setID, par2TypeFileDesc, description)...)
	set = append(set, par2Packet(setID, par2TypeSliceCheck, check)...)
	return set
}

func packetsSealed(t *testing.T, data []byte) [][]byte {
	t.Helper()
	var bodies [][]byte
	for offset := 0; offset < len(data); {
		length := int(binary.LittleEndian.Uint64(data[offset+8:]))
		sum := md5.Sum(data[offset+32 : offset+length])
		if !bytes.Equal(sum[:], data[offset+16:offset+32]) {
			t.Fatalf("packet at %d is not sealed", offset)
		}
		bodies = append(bodies, data[offset+64:offset+length])
		offset += length
	}
	return bodies
}

func TestEveryPAR2LieRewritesItsFieldAndReseals(t *testing.T) {
	truth := packetsSealed(t, lyingSet())
	for _, lie := range adversarialPAR2Lies {
		set := lyingSet()
		rewritten, err := TellPAR2Lie(set, lie)
		if err != nil {
			t.Fatalf("%s: %v", lie, err)
		}
		if rewritten != 1 {
			t.Errorf("%s rewrote %d packets", lie, rewritten)
		}
		told := packetsSealed(t, set)
		changed := -1
		for index := range told {
			if !bytes.Equal(told[index], truth[index]) {
				if changed >= 0 {
					t.Errorf("%s changed packets %d and %d", lie, changed, index)
				}
				changed = index
			}
		}
		want := map[string]int{"slice-size": 0, "slice-hash": 2}[lie]
		if lie != "slice-size" && lie != "slice-hash" {
			want = 1
		}
		if changed != want {
			t.Errorf("%s changed packet %d, want %d", lie, changed, want)
		}
		if bytes.Equal(set[32:48], bytes.Repeat([]byte{7}, 16)) == false {
			t.Errorf("%s moved the set ID", lie)
		}
	}
	if _, err := TellPAR2Lie(lyingSet(), "no-such-lie"); err == nil {
		t.Error("an unknown lie was told")
	}
}
