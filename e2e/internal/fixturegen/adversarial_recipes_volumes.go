// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// The adversarial volume cases: recovery sets that lie about what they hold
// or describe more than any honest set, and RAR archives whose passwords cost
// a key per member. The cases that rename, withhold or repeat RAR volumes stage
// another scenario's RARLAB output through their scenario.json and own no
// bytes; they are listed in ScenarioOnly. No RAR byte is written or edited
// here: RAR comes only from RARLAB's writer.
//
// PAR2 index packets are written by Go, packet by packet, each sealed with its
// MD5, so a case can hold a thousand sets or a packet repeated thousands of
// times without a thousand runs of par2cmdline. No case here holds a recovery
// block.

// volumePAR2Slice is the block size of every set below that names no other.
const volumePAR2Slice = 65536

// volumeSaltedPassword is the password of the many-salted-entries archive.
const volumeSaltedPassword = "adversarial-salted-password"

// AdversarialVolumeRecipes is the volume half of the adversarial family.
func AdversarialVolumeRecipes() []Recipe {
	return []Recipe{
		{
			Slug: "adv-par2-nested-false-packets", Family: "adversarial", ByteReproducible: true,
			Notes: "An intact payload and its PAR2 index beside a recovery volume of 8,192 false packet headers, each nested in the one before " +
				"and claiming the rest of the volume, none sealed. Every false packet is read again from the volume, so the replay must not nest " +
				"and the bytes read again must stay within the volume's re-read budget. The payload completes with its exact bytes.",
			Build: sequence(
				writeVolumePRNG("nested-payload.mkv", "adversarial/nested-false-packets", 1<<20+777),
				func(_ context.Context, env *Env) error {
					setID, err := writePAR2Index(env, "nested-payload.par2", volumePAR2Slice, "nested-payload.mkv")
					if err != nil {
						return err
					}
					return os.WriteFile(env.OutputPath("nested-payload.vol00+01.par2"), nestedFalsePackets(setID, 8192), 0o644)
				},
			),
			ExpectedOutputs: expectPostedOutputs("nested-payload.mkv"),
		},
		{
			Slug: "adv-par2-many-sets-below-root", Family: "adversarial", ByteReproducible: true,
			Notes: "A stored zip holding one 16 KiB member, beside one PAR2 file holding 1,024 index-only sets that each describe that member " +
				"under its own slice size, from 4 bytes up in steps of 4. Each set has its own identity. A delivery below the root is cut only " +
				"where some set's slices meet, each size named once, and binding does not grow with the square of the sets.",
			Build: func(_ context.Context, env *Env) error {
				member := env.StagePath("many-sets/many-sets.mkv")
				if err := WritePRNG(member, "adversarial/many-sets-below-root", 16<<10); err != nil {
					return err
				}
				body, err := os.ReadFile(member)
				if err != nil {
					return err
				}
				encoded, err := encodeRawZip([]rawZipEntry{storedEntry("many-sets.mkv", body)})
				if err != nil {
					return err
				}
				if err := os.WriteFile(env.OutputPath("many-sets.zip"), encoded, 0o644); err != nil {
					return err
				}
				var bundle bytes.Buffer
				for step := int64(1); step <= 1024; step++ {
					description, err := describeForPAR2(member, "many-sets.mkv", 4*step)
					if err != nil {
						return err
					}
					index, _ := encodePAR2Index(4*step, []par2Description{description})
					bundle.Write(index)
				}
				return os.WriteFile(env.OutputPath("many-sets.par2"), bundle.Bytes(), 0o644)
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				return map[string]string{"many-sets.mkv": env.StagePath("many-sets/many-sets.mkv")}, nil
			},
		},
		{
			Slug: "adv-par2-zero-twin-copy", Family: "adversarial", ByteReproducible: true,
			Notes: "A PAR2 index with 16 MiB slices describing a posted 4 KiB file of zeros and an unposted 128 MiB file of zeros. Every slice " +
				"of the large file has the checksums of the small file's one padded slice, so the large file is rebuilt by copying with no recovery " +
				"block at all. The copy goes a piece at a time within the memory granted, and the job completes with both files.",
			Build: func(_ context.Context, env *Env) error {
				if err := writeZeroFile(env.OutputPath("seed.mkv"), 4<<10); err != nil {
					return err
				}
				hollow := env.StagePath("hollow/hollow.mkv")
				if err := writeZeroFile(hollow, 128<<20); err != nil {
					return err
				}
				seed, err := describeForPAR2(env.OutputPath("seed.mkv"), "seed.mkv", 16<<20)
				if err != nil {
					return err
				}
				large, err := describeForPAR2(hollow, "hollow.mkv", 16<<20)
				if err != nil {
					return err
				}
				index, _ := encodePAR2Index(16<<20, []par2Description{seed, large})
				return os.WriteFile(env.OutputPath("zero-twin.par2"), index, 0o644)
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				return map[string]string{
					"seed.mkv":   env.OutputPath("seed.mkv"),
					"hollow.mkv": env.StagePath("hollow/hollow.mkv"),
				}, nil
			},
		},
		{
			Slug: "adv-rev-distinct-stems", Family: "adversarial", ByteReproducible: true,
			Notes: "rar5-single's archive beside sixty-four RAR recovery volumes of noise, each under a stem of its own. Every recovery volume " +
				"founds its own set, one after another rather than each from inside the last, and none takes the archive. The archive's member is delivered.",
			Build: func(_ context.Context, env *Env) error {
				for index := 0; index < 64; index++ {
					name := fmt.Sprintf("stem-%02d.rev", index)
					if err := WritePRNG(env.OutputPath(name), "adversarial/rev-distinct-stems/"+name, 1<<10); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			Slug: "adv-rar-far-part-with-rev", Family: "adversarial", ByteReproducible: true,
			Notes: "A file of noise named as volume four billion of a RAR set, a recovery volume of noise for the same stem, and an intact " +
				"payload. Nothing walks the volume numbers below the named one: a recovery set counts what it misses, and a name without the " +
				"RAR signature is never a volume. The payload is delivered.",
			Build: sequence(
				writeVolumePRNG("payload.part4000000000.rar", "adversarial/far-part/volume", 64<<10),
				writeVolumePRNG("payload.rev", "adversarial/far-part/recovery", 4<<10),
				writeVolumePRNG("far-part.mkv", "adversarial/far-part/payload", 1<<20),
			),
			ExpectedOutputs: expectPostedOutputs("far-part.mkv"),
		},
		{
			Slug: "adv-par2-duplicate-metadata", Family: "adversarial", ByteReproducible: true,
			Notes: "An intact payload and its PAR2 index beside a second PAR2 file that repeats the same index packets 4,096 times. " +
				"A packet already held changes nothing and folds nothing, and the payload completes with its exact bytes.",
			Build: sequence(
				writeVolumePRNG("duplicate-meta.mkv", "adversarial/duplicate-metadata", 1<<20+4321),
				func(_ context.Context, env *Env) error {
					if _, err := writePAR2Index(env, "duplicate-meta.par2", volumePAR2Slice, "duplicate-meta.mkv"); err != nil {
						return err
					}
					index, err := os.ReadFile(env.OutputPath("duplicate-meta.par2"))
					if err != nil {
						return err
					}
					return os.WriteFile(env.OutputPath("duplicate-meta.vol00+01.par2"), bytes.Repeat(index, 4096), 0o644)
				},
			),
			ExpectedOutputs: expectPostedOutputs("duplicate-meta.mkv"),
		},
		{
			Slug: "adv-rar5-many-salted-entries", Family: "adversarial",
			Notes: "512 stored members of 4 KiB in a RAR5 set written with `-p` across three 1 MiB volumes. RARLAB draws a salt per member, " +
				"so every member needs a key of its own, and every member announcement crosses volumes. File keys are derived on the pool, " +
				"not on the pipeline turn, and announcing a member replays no volume. With the right password every member is delivered.",
			Build: func(ctx context.Context, env *Env) error {
				members := make([]string, 0, 512)
				for index := 0; index < 512; index++ {
					name := fmt.Sprintf("salted/entry-%03d.bin", index)
					if err := WritePRNG(env.StagePath(name), "adversarial/many-salted-entries/"+name, 4<<10); err != nil {
						return err
					}
					members = append(members, name)
				}
				if err := env.RAR(ctx, RARSpec{
					Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar", Members: members,
					Method: "-m0", Password: volumeSaltedPassword, VolumeSize: "1m",
				}); err != nil {
					return err
				}
				return expectParts(env, "archive.part%d.rar", 3)
			},
		},
	}
}

// writeVolumePRNG writes a seeded payload to the output directory.
func writeVolumePRNG(name, seed string, size int64) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		return WritePRNG(env.OutputPath(name), seed, size)
	}
}

// expectPostedOutputs pins posted outputs that the job delivers unchanged.
func expectPostedOutputs(names ...string) func(context.Context, *Env) (map[string]string, error) {
	return func(_ context.Context, env *Env) (map[string]string, error) {
		expected := make(map[string]string, len(names))
		for _, name := range names {
			expected[name] = env.OutputPath(name)
		}
		return expected, nil
	}
}

// writeZeroFile writes a file of zeros as a hole, which reads back as zeros.
func writeZeroFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

// ------------------------------------------------------------------ PAR2 index

var (
	par2TypeCreator  = []byte("PAR 2.0\x00Creator\x00")
	par2TypeRecovery = []byte("PAR 2.0\x00RecvSlic")
)

// par2Description is what a PAR2 index says of one file.
type par2Description struct {
	name   string
	length uint64
	full   [16]byte
	head   [16]byte
	// slices holds each slice's MD5 followed by its CRC32, both over the
	// slice padded with zeros to the slice size.
	slices [][20]byte
}

// fileID is the PAR2 file identity: the MD5 of the first 16 KiB's MD5, the
// length and the name.
func (description par2Description) fileID() [16]byte {
	buffer := make([]byte, 0, 24+len(description.name))
	buffer = append(buffer, description.head[:]...)
	buffer = binary.LittleEndian.AppendUint64(buffer, description.length)
	buffer = append(buffer, description.name...)
	return md5.Sum(buffer)
}

// describeForPAR2 reads a file a slice at a time and describes it under name.
func describeForPAR2(path, name string, sliceSize int64) (par2Description, error) {
	if sliceSize <= 0 || sliceSize%4 != 0 {
		return par2Description{}, fmt.Errorf("PAR2 slice size %d is not a positive multiple of 4", sliceSize)
	}
	file, err := os.Open(path)
	if err != nil {
		return par2Description{}, err
	}
	defer file.Close()
	description := par2Description{name: name}
	whole := md5.New()
	head := md5.New()
	slice := make([]byte, sliceSize)
	for {
		read, err := io.ReadFull(file, slice)
		if read > 0 {
			whole.Write(slice[:read])
			if description.length < 16<<10 {
				room := int(min(int64(16<<10)-int64(description.length), int64(read)))
				head.Write(slice[:room])
			}
			description.length += uint64(read)
			clear(slice[read:])
			var entry [20]byte
			sum := md5.Sum(slice)
			copy(entry[:16], sum[:])
			binary.LittleEndian.PutUint32(entry[16:], crc32.ChecksumIEEE(slice))
			description.slices = append(description.slices, entry)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return par2Description{}, err
		}
	}
	copy(description.full[:], whole.Sum(nil))
	copy(description.head[:], head.Sum(nil))
	return description, nil
}

// encodePAR2Index writes one set's index packets in par2cmdline's order: the
// main packet, each file's description and slice checksums, then the creator.
// It returns the bytes and the set identity, the MD5 of the main packet body.
func encodePAR2Index(sliceSize int64, files []par2Description) ([]byte, [16]byte) {
	ids := make([][16]byte, len(files))
	for index, file := range files {
		ids[index] = file.fileID()
	}
	order := make([]int, len(files))
	for index := range order {
		order[index] = index
	}
	// par2cmdline orders file identities as 128-bit little-endian numbers.
	sort.Slice(order, func(a, b int) bool {
		left, right := ids[order[a]], ids[order[b]]
		for at := 15; at >= 0; at-- {
			if left[at] != right[at] {
				return left[at] < right[at]
			}
		}
		return false
	})
	main := binary.LittleEndian.AppendUint64(nil, uint64(sliceSize))
	main = binary.LittleEndian.AppendUint32(main, uint32(len(files)))
	for _, index := range order {
		main = append(main, ids[index][:]...)
	}
	setID := md5.Sum(main)

	var out bytes.Buffer
	out.Write(sealPAR2Packet(setID, par2MainType, main))
	for _, index := range order {
		file := files[index]
		described := append([]byte(nil), ids[index][:]...)
		described = append(described, file.full[:]...)
		described = append(described, file.head[:]...)
		described = binary.LittleEndian.AppendUint64(described, file.length)
		described = append(described, padToFour([]byte(file.name))...)
		out.Write(sealPAR2Packet(setID, par2TypeFileDesc, described))
		checks := append([]byte(nil), ids[index][:]...)
		for _, entry := range file.slices {
			checks = append(checks, entry[:]...)
		}
		out.Write(sealPAR2Packet(setID, par2TypeSliceCheck, checks))
	}
	out.Write(sealPAR2Packet(setID, par2TypeCreator, padToFour([]byte("weaver e2e fixturegen"))))
	return out.Bytes(), setID
}

// writePAR2Index describes posted outputs under their own names in one set.
func writePAR2Index(env *Env, index string, sliceSize int64, sources ...string) ([16]byte, error) {
	files := make([]par2Description, 0, len(sources))
	for _, source := range sources {
		description, err := describeForPAR2(env.OutputPath(source), filepath.Base(source), sliceSize)
		if err != nil {
			return [16]byte{}, err
		}
		files = append(files, description)
	}
	encoded, setID := encodePAR2Index(sliceSize, files)
	return setID, os.WriteFile(env.OutputPath(index), encoded, 0o644)
}

// sealPAR2Packet frames a body as a PAR2 packet of the set and seals its MD5.
func sealPAR2Packet(setID [16]byte, kind, body []byte) []byte {
	packet := make([]byte, 64+len(body))
	copy(packet, par2Magic)
	binary.LittleEndian.PutUint64(packet[8:], uint64(len(packet)))
	copy(packet[32:], setID[:])
	copy(packet[48:], kind)
	copy(packet[64:], body)
	sum := md5.Sum(packet[32:])
	copy(packet[16:32], sum[:])
	return packet
}

// padToFour pads with zeros to a multiple of four bytes, as PAR2 strings are.
func padToFour(data []byte) []byte {
	for len(data)%4 != 0 {
		data = append(data, 0)
	}
	return data
}

// nestedFalsePackets is a recovery volume of count packet headers 64 bytes
// apart, each claiming the rest of the volume for the set and none sealed:
// every packet MD5 is left as zeros, which no packet hashes to.
func nestedFalsePackets(setID [16]byte, count int) []byte {
	total := count * 64
	data := make([]byte, total)
	for index := 0; index < count; index++ {
		at := index * 64
		copy(data[at:], par2Magic)
		binary.LittleEndian.PutUint64(data[at+8:], uint64(total-at))
		copy(data[at+32:], setID[:])
		copy(data[at+48:], par2TypeRecovery)
	}
	return data
}
