// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The adversarial family: archives that lie about themselves or ask to be
// written somewhere they must not be. No archiver writes most of these shapes
// on purpose, so Go writes the zip and tar bytes itself, header by header,
// rather than through archive/zip and archive/tar, which refuse to. The PAR2
// lies start from a set the pinned par2cmdline build writes and then rewrite
// one field the way the graph engine's breadth campaign does, recomputing
// each packet's checksum so the lie is told by a well-formed packet.
//
// Every payload carries AdversarialSentinel, so a run can find any copy of it
// written outside the job's output directory by content alone.

// AdversarialSentinel opens every payload the family writes.
const AdversarialSentinel = "weaver-adversarial-sentinel"

// adversarialPayload is a short text payload unique to its name.
func adversarialPayload(name string) []byte {
	return []byte(AdversarialSentinel + " " + name + "\n")
}

// The PAR2 lies, named as the scenario slugs name them. Each is one field of
// one packet type, rewritten in every file of the set that carries it.
var adversarialPAR2Lies = []string{
	"file-hash", "hash16k", "length-long", "length-short",
	"slice-hash", "name-case", "name-other", "slice-size",
}

// adversarialPAR2Slice is the lying sets' block size; the payload is a few
// blocks long and ends inside one.
const (
	adversarialPAR2Slice   = 65536
	adversarialPAR2Payload = 2<<20 + 12345
)

// AdversarialRecipes is the adversarial family.
func AdversarialRecipes() []Recipe {
	recipes := []Recipe{
		{
			Slug: "adv-zip-traversal", Family: "adversarial", ByteReproducible: true,
			Notes: "A stored zip whose members name parent directories, absolute paths, backslash parents and a drive letter. " +
				"The graph confines every name under the job's output directory; nothing may land above it.",
			Build:           writeAdversarialZip("traversal.zip", traversalZipEntries()),
			ExpectedOutputs: expectPayloads(TraversalDelivered),
		},
		{
			Slug: "adv-tar-traversal", Family: "adversarial", ByteReproducible: true,
			Notes:           "The same hostile names as adv-zip-traversal in a ustar archive, so the tar object's confinement is held to the same contract.",
			Build:           writeAdversarialTar("traversal.tar", traversalTarEntries()),
			ExpectedOutputs: expectPayloads(TraversalDelivered),
		},
		{
			Slug: "adv-tar-links", Family: "adversarial", ByteReproducible: true,
			Notes: "A ustar archive with a symlink and a hardlink pointing outside the root, a directory symlink, and a regular file written through " +
				"that symlink's name. Links are skipped, so the file must land in a real directory under the output.",
			Build: writeAdversarialTar("links.tar", linkTarEntries()),
			ExpectedOutputs: expectPayloads(map[string]string{
				LinkThroughName: LinkThroughName,
				LinkPayloadName: LinkPayloadName,
			}),
		},
		{
			Slug: "adv-zip-bomb-member", Family: "adversarial", ByteReproducible: true,
			Notes: "One deflated member of 256 MiB of zeros in a zip of a few hundred KiB. The lane runs it with the member cap at 32 MiB, " +
				"so the extraction budget refuses it before scratch fills.",
			Build: writeAdversarialZipBomb("bomb.zip", "zeros.bin", 256<<20),
		},
		{
			Slug: "adv-zip-bomb-reserve", Family: "adversarial", ByteReproducible: true,
			Notes: "One deflated member of 3 GiB of zeros. The lane runs it with the free-space reserve set just under what the disk holds, " +
				"so the disk reserve is what refuses it.",
			Build: writeAdversarialZipBomb("reserve.zip", "zeros.bin", 3<<30),
		},
		{
			Slug: "adv-zip-nest-17", Family: "adversarial", ByteReproducible: true,
			Notes: "Seventeen stored zips, each holding the next, the innermost holding one text member: one level past the graph's fixed nesting limit " +
				"of sixteen. nested-5deep covers RAR nesting inside the limit; this one crosses it.",
			Build: writeNestedZips("nest.zip", 17),
		},
		{
			Slug: "adv-zip-duplicate-names", Family: "adversarial", ByteReproducible: true,
			Notes: "Two stored members with the same name and different bytes. Both must be delivered, the second under a suffixed name.",
			Build: writeAdversarialZip("duplicates.zip", []rawZipEntry{
				storedEntry("dup.txt", adversarialPayload("dup-first")),
				storedEntry("dup.txt", adversarialPayload("dup-second")),
			}),
			ExpectedOutputs: expectPayloads(map[string]string{
				"dup.txt":     "dup-first",
				"dup (1).txt": "dup-second",
			}),
		},
		{
			Slug: "adv-zip-overlapping-headers", Family: "adversarial", ByteReproducible: true,
			Notes: "Two central directory records that point at the same local header, so two members claim one range of bytes.",
			Build: writeAdversarialZip("overlap.zip", []rawZipEntry{
				storedEntry("overlap-a.txt", adversarialPayload("overlap")),
				{CentralName: "overlap-b.txt", SharesLocalOf: 1, Body: adversarialPayload("overlap"), Size: uint64(len(adversarialPayload("overlap"))), CRC: crc32.ChecksumIEEE(adversarialPayload("overlap"))},
			}),
		},
		{
			Slug: "adv-zip-directory-disagrees", Family: "adversarial", ByteReproducible: true,
			Notes: "A member whose central directory record names a different file and declares twice the size its local header and data carry.",
			Build: writeAdversarialZip("disagree.zip", []rawZipEntry{
				func() rawZipEntry {
					entry := storedEntry("local-name.txt", adversarialPayload("disagree"))
					entry.CentralName = "central-name.txt"
					entry.CentralSize = entry.Size * 2
					return entry
				}(),
			}),
		},
		{
			Slug: "adv-zip-declared-huge", Family: "adversarial", ByteReproducible: true,
			Notes: "A stored member of a few bytes whose headers declare an uncompressed size of almost 4 GiB, so the declared and posted sizes disagree by six orders of magnitude.",
			Build: writeAdversarialZip("declared.zip", []rawZipEntry{
				func() rawZipEntry {
					entry := storedEntry("declared.bin", adversarialPayload("declared"))
					entry.DeclaredSize = 0xFFFFFFF0
					return entry
				}(),
			}),
		},
		{
			Slug: "adv-zip-truncated-local", Family: "adversarial", ByteReproducible: true,
			Notes: "A zip cut twenty bytes into its first local header: no name, no data, no central directory.",
			Build: sequence(
				writeAdversarialZip("truncated-local.zip", []rawZipEntry{storedEntry("cut.txt", adversarialPayload("cut-local"))}),
				truncateOutputTo("truncated-local.zip", 20),
			),
		},
		{
			Slug: "adv-zip-truncated-directory", Family: "adversarial", ByteReproducible: true,
			Notes: "A whole zip with the last ten bytes of its end-of-central-directory record cut off.",
			Build: sequence(
				writeAdversarialZip("truncated-directory.zip", []rawZipEntry{storedEntry("cut.txt", adversarialPayload("cut-directory"))}),
				truncateOutput("truncated-directory.zip", 10),
			),
		},
		{
			Slug: "adv-zip-hostile-names", Family: "adversarial", ByteReproducible: true,
			Notes: "Members named with control characters, an embedded NUL, an overlong UTF-8 encoding of '/', reserved Windows device names, " +
				"an over-long run of two-byte characters and one 4 KiB name. The UTF-8 flag is set on every record. unicode-filenames covers well-formed CJK names.",
			Build: writeAdversarialZip("names.zip", hostileNameEntries()),
		},
		{
			Slug: "adv-zip-many-members", Family: "adversarial", ByteReproducible: true,
			Notes: "Thirty thousand stored members of a few dozen bytes each, inside the default entry cap; every one must be delivered.",
			Build: writeAdversarialZip("many.zip", manyMemberEntries(30000)),
		},
		{
			Slug: "adv-zip-entries-over-cap", Family: "adversarial", ByteReproducible: true,
			Notes: "Fifteen hundred stored members. The lane runs it with the entry cap at a thousand, so the extraction budget refuses it.",
			Build: writeAdversarialZip("entries.zip", manyMemberEntries(1500)),
		},
	}
	for _, lie := range adversarialPAR2Lies {
		recipes = append(recipes, Recipe{
			Slug: "adv-par2-lie-" + lie, Family: "adversarial",
			Notes: "A loose payload beside a par2cmdline set with eight recovery blocks, every file of the set rewritten to tell the " + lie +
				" lie from the graph engine's breadth campaign. The payload is intact, so the job either completes with its exact bytes or fails named with none of them.",
			Build: sequence(
				func(_ context.Context, env *Env) error {
					return WritePRNG(env.OutputPath("lie-payload.bin"), "adversarial/par2-lie", adversarialPAR2Payload)
				},
				par2(PAR2Spec{
					Base: "lie-payload.par2", Sources: []string{"lie-payload.bin"},
					SliceSize: adversarialPAR2Slice, RecoveryBlocks: 8, RecoveryFiles: 1,
				}),
				tellPAR2LieInOutputs(lie),
			),
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				name := "lie-payload.bin"
				if lie == "name-other" {
					name = "mie-payload.bin"
				}
				return map[string]string{name: env.OutputPath("lie-payload.bin")}, nil
			},
		})
	}
	return recipes
}

// ------------------------------------------------------------------ zip

// rawZipEntry is one member as the headers describe it, which need not be
// what its bytes are.
type rawZipEntry struct {
	// Name is the local header's name. CentralName, when set, is the name
	// the central directory record gives instead.
	Name        string
	CentralName string
	// Method is 0 for stored, 8 for deflated; Body is the bytes after the
	// local header, already compressed when Method says so.
	Method uint16
	Body   []byte
	// Size and CRC describe the uncompressed member.
	Size uint64
	CRC  uint32
	// DeclaredSize, when set, replaces Size in both headers; CentralSize
	// replaces it in the central directory alone.
	DeclaredSize uint64
	CentralSize  uint64
	// Flags is the general purpose bit field of both records.
	Flags uint16
	// SharesLocalOf, when set, is the one-based index of an earlier entry
	// whose local header this record points at; the entry writes no local
	// header of its own.
	SharesLocalOf int
}

// zipUTF8Flag is general purpose bit 11: the names are UTF-8.
const zipUTF8Flag = 1 << 11

func storedEntry(name string, body []byte) rawZipEntry {
	return rawZipEntry{Name: name, Body: body, Size: uint64(len(body)), CRC: crc32.ChecksumIEEE(body)}
}

// encodeRawZip lays the entries out as local headers and bodies, then the
// central directory, then the end record. Sizes past 32 bits are not
// representable here; no recipe needs zip64.
func encodeRawZip(entries []rawZipEntry) ([]byte, error) {
	date, clock := msdosTime(CanonicalTime)
	var out bytes.Buffer
	offsets := make([]uint32, len(entries))
	put16 := func(value uint16) { _ = binary.Write(&out, binary.LittleEndian, value) }
	put32 := func(value uint32) { _ = binary.Write(&out, binary.LittleEndian, value) }
	size32 := func(value uint64) (uint32, error) {
		if value > 0xFFFFFFFF {
			return 0, fmt.Errorf("zip size %d needs zip64", value)
		}
		return uint32(value), nil
	}
	for index, entry := range entries {
		if entry.SharesLocalOf > 0 {
			if entry.SharesLocalOf > index {
				return nil, fmt.Errorf("entry %d shares the local header of a later entry", index+1)
			}
			offsets[index] = offsets[entry.SharesLocalOf-1]
			continue
		}
		declared := entry.Size
		if entry.DeclaredSize != 0 {
			declared = entry.DeclaredSize
		}
		size, err := size32(declared)
		if err != nil {
			return nil, err
		}
		compressed, err := size32(uint64(len(entry.Body)))
		if err != nil {
			return nil, err
		}
		if entry.Method == 0 && entry.DeclaredSize != 0 {
			compressed = size
		}
		offsets[index] = uint32(out.Len())
		put32(0x04034b50)
		put16(20)
		put16(entry.Flags)
		put16(entry.Method)
		put16(clock)
		put16(date)
		put32(entry.CRC)
		put32(compressed)
		put32(size)
		put16(uint16(len(entry.Name)))
		put16(0)
		out.WriteString(entry.Name)
		out.Write(entry.Body)
	}
	directory := out.Len()
	for index, entry := range entries {
		name := entry.Name
		if entry.CentralName != "" {
			name = entry.CentralName
		}
		declared := entry.Size
		if entry.DeclaredSize != 0 {
			declared = entry.DeclaredSize
		}
		if entry.CentralSize != 0 {
			declared = entry.CentralSize
		}
		size, err := size32(declared)
		if err != nil {
			return nil, err
		}
		compressed, err := size32(uint64(len(entry.Body)))
		if err != nil {
			return nil, err
		}
		if entry.Method == 0 && (entry.DeclaredSize != 0 || entry.CentralSize != 0) {
			compressed = size
		}
		put32(0x02014b50)
		put16(0x031e)
		put16(20)
		put16(entry.Flags)
		put16(entry.Method)
		put16(clock)
		put16(date)
		put32(entry.CRC)
		put32(compressed)
		put32(size)
		put16(uint16(len(name)))
		put16(0)
		put16(0)
		put16(0)
		put16(0)
		put32(0o100644 << 16)
		put32(offsets[index])
		out.WriteString(name)
	}
	if len(entries) > 0xFFFF {
		return nil, fmt.Errorf("%d entries need zip64", len(entries))
	}
	directorySize := out.Len() - directory
	put32(0x06054b50)
	put16(0)
	put16(0)
	put16(uint16(len(entries)))
	put16(uint16(len(entries)))
	put32(uint32(directorySize))
	put32(uint32(directory))
	put16(0)
	return out.Bytes(), nil
}

func writeAdversarialZip(output string, entries []rawZipEntry) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		encoded, err := encodeRawZip(entries)
		if err != nil {
			return fmt.Errorf("%s: %w", output, err)
		}
		return os.WriteFile(env.OutputPath(output), encoded, 0o644)
	}
}

// deflatedZeros compresses size zero bytes at the best level deflate has,
// without ever holding them, and returns the stream with the zeros' CRC.
func deflatedZeros(size int64) ([]byte, uint32, error) {
	var compressed bytes.Buffer
	writer, err := flate.NewWriter(&compressed, flate.BestCompression)
	if err != nil {
		return nil, 0, err
	}
	chunk := make([]byte, 1<<20)
	checksum := uint32(0)
	for remaining := size; remaining > 0; {
		step := int64(len(chunk))
		if remaining < step {
			step = remaining
		}
		if _, err := writer.Write(chunk[:step]); err != nil {
			return nil, 0, err
		}
		checksum = crc32.Update(checksum, crc32.IEEETable, chunk[:step])
		remaining -= step
	}
	if err := writer.Close(); err != nil {
		return nil, 0, err
	}
	return compressed.Bytes(), checksum, nil
}

func writeAdversarialZipBomb(output, member string, size int64) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		body, checksum, err := deflatedZeros(size)
		if err != nil {
			return err
		}
		entry := rawZipEntry{Name: member, Method: 8, Body: body, Size: uint64(size), CRC: checksum}
		return writeAdversarialZip(output, []rawZipEntry{entry})(ctx, env)
	}
}

// writeNestedZips writes levels stored zips, each the only member of the one
// outside it, the innermost holding one text member.
func writeNestedZips(output string, levels int) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		inner, err := encodeRawZip([]rawZipEntry{storedEntry("deepest.txt", adversarialPayload("deepest"))})
		if err != nil {
			return err
		}
		for level := levels - 1; level >= 1; level-- {
			name := "level-" + strconv.Itoa(level+1) + ".zip"
			inner, err = encodeRawZip([]rawZipEntry{storedEntry(name, inner)})
			if err != nil {
				return err
			}
		}
		return os.WriteFile(env.OutputPath(output), inner, 0o644)
	}
}

// TraversalNames are the hostile member names of the traversal archives, in
// archive order. Each payload is adversarialPayload of its name.
var TraversalNames = []string{
	"../../adv-escape-parent.txt",
	"/tmp/adv-escape-absolute.txt",
	`..\..\adv-escape-backslash.txt`,
	`C:\adv-escape-drive.txt`,
	"inside/../../adv-escape-middle.txt",
	"plain.txt",
}

// TraversalDelivered is where each traversal name lands once the graph
// confines it: separators of either kind split it, and empty, "." and ".."
// parts are dropped. Keyed by the delivered name, valued by the archive name.
var TraversalDelivered = map[string]string{
	"adv-escape-parent.txt":        "../../adv-escape-parent.txt",
	"tmp/adv-escape-absolute.txt":  "/tmp/adv-escape-absolute.txt",
	"adv-escape-backslash.txt":     `..\..\adv-escape-backslash.txt`,
	"C:/adv-escape-drive.txt":      `C:\adv-escape-drive.txt`,
	"inside/adv-escape-middle.txt": "inside/../../adv-escape-middle.txt",
	"plain.txt":                    "plain.txt",
}

// expectPayloads pins each delivered name to the payload adversarialPayload
// writes for its source name, staged so the generator can digest it.
func expectPayloads(delivered map[string]string) func(context.Context, *Env) (map[string]string, error) {
	return func(_ context.Context, env *Env) (map[string]string, error) {
		paths := make(map[string]string, len(delivered))
		index := 0
		for name, source := range delivered {
			index++
			path := env.StagePath(filepath.Join("expected", strconv.Itoa(index)))
			if err := os.WriteFile(path, adversarialPayload(source), 0o644); err != nil {
				return nil, err
			}
			paths[name] = path
		}
		return paths, nil
	}
}

func traversalZipEntries() []rawZipEntry {
	entries := make([]rawZipEntry, 0, len(TraversalNames))
	for _, name := range TraversalNames {
		entries = append(entries, storedEntry(name, adversarialPayload(name)))
	}
	return entries
}

func hostileNameEntries() []rawZipEntry {
	names := []string{
		"control-\x01\x07\x1b[31m-name.txt",
		"nul-\x00-hidden.txt",
		"..\xc0\xaf..\xc0\xafadv-overlong-escape.txt",
		"CON.txt",
		"nul",
		"COM1.mkv",
		"aux.srt",
		"LPT1",
		strings.Repeat("\u00e9", 200) + ".txt",
		strings.Repeat("a", 4096-len(".txt")) + ".txt",
		"ordinary.txt",
	}
	entries := make([]rawZipEntry, 0, len(names))
	for _, name := range names {
		entry := storedEntry(name, adversarialPayload(strconv.Quote(name)))
		entry.Flags = zipUTF8Flag
		entries = append(entries, entry)
	}
	return entries
}

func manyMemberEntries(count int) []rawZipEntry {
	entries := make([]rawZipEntry, 0, count)
	for index := 0; index < count; index++ {
		name := fmt.Sprintf("many/%03d/m%05d.txt", index/1000, index)
		entries = append(entries, storedEntry(name, adversarialPayload(name)))
	}
	return entries
}

// truncateOutputTo cuts a written output down to its first size bytes.
func truncateOutputTo(name string, size int64) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		return os.Truncate(env.OutputPath(name), size)
	}
}

// ------------------------------------------------------------------ tar

// rawTarEntry is one ustar record: a type flag, a name, a link target and a
// body, none of them checked.
type rawTarEntry struct {
	Name string
	Type byte
	Link string
	Body []byte
}

func encodeRawTar(entries []rawTarEntry) ([]byte, error) {
	var out bytes.Buffer
	octal := func(field []byte, value int64) {
		text := strconv.FormatInt(value, 8)
		pad := len(field) - 1 - len(text)
		for index := range field {
			field[index] = '0'
		}
		copy(field[pad:], text)
		field[len(field)-1] = 0
	}
	for _, entry := range entries {
		if len(entry.Name) > 100 || len(entry.Link) > 100 {
			return nil, fmt.Errorf("tar name %q or link %q is past the ustar field", entry.Name, entry.Link)
		}
		header := make([]byte, 512)
		copy(header[0:100], entry.Name)
		mode := int64(0o644)
		if entry.Type == '5' {
			mode = 0o755
		}
		octal(header[100:108], mode)
		octal(header[108:116], 0)
		octal(header[116:124], 0)
		octal(header[124:136], int64(len(entry.Body)))
		octal(header[136:148], CanonicalTime.Unix())
		copy(header[148:156], "        ")
		header[156] = entry.Type
		copy(header[157:257], entry.Link)
		copy(header[257:263], "ustar\x00")
		copy(header[263:265], "00")
		copy(header[265:297], "root")
		copy(header[297:329], "root")
		var sum int64
		for _, value := range header {
			sum += int64(value)
		}
		checksum := fmt.Sprintf("%06o\x00 ", sum)
		copy(header[148:156], checksum)
		out.Write(header)
		out.Write(entry.Body)
		if pad := len(entry.Body) % 512; pad != 0 {
			out.Write(make([]byte, 512-pad))
		}
	}
	out.Write(make([]byte, 1024))
	return out.Bytes(), nil
}

func writeAdversarialTar(output string, entries []rawTarEntry) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		encoded, err := encodeRawTar(entries)
		if err != nil {
			return fmt.Errorf("%s: %w", output, err)
		}
		return os.WriteFile(env.OutputPath(output), encoded, 0o644)
	}
}

func traversalTarEntries() []rawTarEntry {
	entries := make([]rawTarEntry, 0, len(TraversalNames))
	for _, name := range TraversalNames {
		entries = append(entries, rawTarEntry{Name: name, Type: '0', Body: adversarialPayload(name)})
	}
	return entries
}

// LinkThroughName is the regular member the links archive writes through its
// directory symlink's name, and LinkPayloadName the ordinary member beside it.
const (
	LinkThroughName = "links/dir-link/adv-through-link.txt"
	LinkPayloadName = "links/payload.txt"
)

func linkTarEntries() []rawTarEntry {
	return []rawTarEntry{
		{Name: "links/", Type: '5'},
		{Name: "links/file-link", Type: '2', Link: "../../../../adv-symlink-target.txt"},
		{Name: "links/hard-link", Type: '1', Link: "/etc/hosts"},
		{Name: "links/dir-link", Type: '2', Link: "/tmp"},
		{Name: LinkThroughName, Type: '0', Body: adversarialPayload(LinkThroughName)},
		{Name: LinkPayloadName, Type: '0', Body: adversarialPayload(LinkPayloadName)},
	}
}

// ------------------------------------------------------------------ par2

// The packet types a lie rewrites besides the main packet par2packet.go
// already names.
var (
	par2TypeFileDesc   = []byte("PAR 2.0\x00FileDesc")
	par2TypeSliceCheck = []byte("PAR 2.0\x00IFSC\x00\x00\x00\x00")
)

// TellPAR2Lie rewrites, in place, the one field of one packet type that the
// lie names, in every packet of that type the file holds, and reseals each
// rewritten packet's MD5. The set ID is left as it was, so a lie about the
// main packet is told under the set's original identity. It returns how many
// packets it rewrote.
func TellPAR2Lie(data []byte, lie string) (int, error) {
	rewritten := 0
	for offset := 0; offset+64 <= len(data); {
		if !bytes.Equal(data[offset:offset+8], par2Magic) {
			return rewritten, fmt.Errorf("no PAR2 packet at offset %d", offset)
		}
		length := binary.LittleEndian.Uint64(data[offset+8 : offset+16])
		if length < 64 || length%4 != 0 || uint64(offset)+length > uint64(len(data)) {
			return rewritten, fmt.Errorf("PAR2 packet at offset %d declares length %d", offset, length)
		}
		end := offset + int(length)
		kind := data[offset+48 : offset+64]
		body := data[offset+64 : end]
		changed, err := tellLieInBody(lie, kind, body)
		if err != nil {
			return rewritten, err
		}
		if changed {
			sum := md5.Sum(data[offset+32 : end])
			copy(data[offset+16:offset+32], sum[:])
			rewritten++
		}
		offset = end
	}
	return rewritten, nil
}

func tellLieInBody(lie string, kind, body []byte) (bool, error) {
	switch lie {
	case "file-hash", "hash16k", "length-long", "length-short", "name-case", "name-other":
		if !bytes.Equal(kind, par2TypeFileDesc) {
			return false, nil
		}
		if len(body) < 57 {
			return false, fmt.Errorf("file description packet of %d bytes", len(body))
		}
	case "slice-hash":
		if !bytes.Equal(kind, par2TypeSliceCheck) {
			return false, nil
		}
	case "slice-size":
		if !bytes.Equal(kind, par2MainType) {
			return false, nil
		}
	default:
		return false, fmt.Errorf("unknown PAR2 lie %q", lie)
	}
	switch lie {
	case "file-hash":
		body[16] ^= 0xff
	case "hash16k":
		body[32] ^= 0xff
	case "length-long", "length-short":
		length := binary.LittleEndian.Uint64(body[48:56])
		if lie == "length-long" {
			length += adversarialPAR2Slice
		} else if length > adversarialPAR2Slice {
			length -= adversarialPAR2Slice
		} else {
			length = 0
		}
		binary.LittleEndian.PutUint64(body[48:56], length)
	case "name-case":
		copy(body[56:], bytes.ToUpper(body[56:]))
	case "name-other":
		if body[56] != 0 {
			if body[56] == 'z' {
				body[56] = 'a'
			} else {
				body[56]++
			}
		}
	case "slice-hash":
		if len(body) < 36 {
			return true, nil
		}
		body[16] ^= 0xff
	case "slice-size":
		slice := binary.LittleEndian.Uint64(body[:8])
		binary.LittleEndian.PutUint64(body[:8], slice*2)
	}
	return true, nil
}

// tellPAR2LieInOutputs tells the lie in every .par2 file the recipe wrote and
// fails when the set never carried the packet the lie rewrites.
func tellPAR2LieInOutputs(lie string) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		outputs, err := env.Outputs()
		if err != nil {
			return err
		}
		total := 0
		for _, name := range outputs {
			if !strings.HasSuffix(name, ".par2") {
				continue
			}
			path := env.OutputPath(filepath.FromSlash(name))
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rewritten, err := TellPAR2Lie(data, lie)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			total += rewritten
			if err := os.WriteFile(path, data, 0o644); err != nil {
				return err
			}
		}
		if total == 0 {
			return fmt.Errorf("the set carries no packet the %s lie rewrites", lie)
		}
		return nil
	}
}
