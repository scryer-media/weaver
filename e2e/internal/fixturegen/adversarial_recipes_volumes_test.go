// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// volumePackets splits a PAR2 file into its packets and checks each seal.
func volumePackets(t *testing.T, data []byte) [][]byte {
	t.Helper()
	var packets [][]byte
	for at := 0; at < len(data); {
		if !bytes.Equal(data[at:at+8], par2Magic) {
			t.Fatalf("offset %d does not open a packet", at)
		}
		length := int(binary.LittleEndian.Uint64(data[at+8:]))
		if length < 64 || length%4 != 0 || at+length > len(data) {
			t.Fatalf("packet at %d claims %d bytes", at, length)
		}
		packet := data[at : at+length]
		sum := md5.Sum(packet[32:])
		if !bytes.Equal(sum[:], packet[16:32]) {
			t.Fatalf("packet at %d is not sealed", at)
		}
		packets = append(packets, packet)
		at += length
	}
	return packets
}

func TestVolumePAR2IndexIsSealedAndOrdered(t *testing.T) {
	dir := t.TempDir()
	names := []string{"alpha.mkv", "beta.mkv", "gamma.mkv"}
	var files []par2Description
	for index, name := range names {
		path := filepath.Join(dir, name)
		if err := WritePRNG(path, "adversarial/test/"+name, int64(20000+index*70001)); err != nil {
			t.Fatal(err)
		}
		description, err := describeForPAR2(path, name, volumePAR2Slice)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, description)
	}
	encoded, setID := encodePAR2Index(volumePAR2Slice, files)
	packets := volumePackets(t, encoded)
	if len(packets) != 2+2*len(files) {
		t.Fatalf("index holds %d packets, want %d", len(packets), 2+2*len(files))
	}
	for _, packet := range packets {
		if !bytes.Equal(packet[32:48], setID[:]) {
			t.Fatal("a packet names another set")
		}
	}
	main := packets[0]
	if !bytes.Equal(main[48:64], par2MainType) {
		t.Fatal("the first packet is not the main packet")
	}
	if sum := md5.Sum(main[64:]); sum != setID {
		t.Fatal("the set identity is not the MD5 of the main packet body")
	}
	if !bytes.Equal(packets[len(packets)-1][48:64], par2TypeCreator) {
		t.Fatal("the last packet is not the creator")
	}
	if count := binary.LittleEndian.Uint32(main[72:]); int(count) != len(files) {
		t.Fatalf("main packet lists %d files, want %d", count, len(files))
	}
	var previous []byte
	for index := range files {
		id := main[76+16*index : 92+16*index]
		if previous != nil && !lessLittleEndian(previous, id) {
			t.Fatal("file identities are not in ascending little-endian order")
		}
		previous = id
		description := packets[1+2*index]
		checks := packets[2+2*index]
		if !bytes.Equal(description[48:64], par2TypeFileDesc) || !bytes.Equal(description[64:80], id) {
			t.Fatalf("file %d description does not follow the main packet's order", index)
		}
		if !bytes.Equal(checks[48:64], par2TypeSliceCheck) || !bytes.Equal(checks[64:80], id) {
			t.Fatalf("file %d slice checksums do not follow its description", index)
		}
	}
	for _, file := range files {
		head := md5.Sum(mustRead(t, filepath.Join(dir, file.name))[:16<<10])
		if head != file.head {
			t.Fatalf("%s head MD5 is not the MD5 of its first 16 KiB", file.name)
		}
		buffer := append(append(append([]byte(nil), head[:]...), binary.LittleEndian.AppendUint64(nil, file.length)...), file.name...)
		if md5.Sum(buffer) != file.fileID() {
			t.Fatalf("%s file identity is not MD5(head, length, name)", file.name)
		}
		wantSlices := (int64(file.length) + volumePAR2Slice - 1) / volumePAR2Slice
		if int64(len(file.slices)) != wantSlices {
			t.Fatalf("%s has %d slice checksums, want %d", file.name, len(file.slices), wantSlices)
		}
	}
	index := filepath.Join(dir, "set.par2")
	if err := os.WriteFile(index, encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	if size, err := PAR2SliceSize(index); err != nil || size != volumePAR2Slice {
		t.Fatalf("PAR2SliceSize = %d, %v; want %d", size, err, volumePAR2Slice)
	}
}

func TestNestedFalsePacketsNestAndNoneIsSealed(t *testing.T) {
	setID := [16]byte{1, 2, 3}
	const count = 32
	data := nestedFalsePackets(setID, count)
	if len(data) != count*64 {
		t.Fatalf("volume is %d bytes, want %d", len(data), count*64)
	}
	for index := 0; index < count; index++ {
		at := index * 64
		if !bytes.Equal(data[at:at+8], par2Magic) || !bytes.Equal(data[at+48:at+64], par2TypeRecovery) {
			t.Fatalf("header %d is not a recovery packet header", index)
		}
		if claimed := binary.LittleEndian.Uint64(data[at+8:]); claimed != uint64(len(data)-at) {
			t.Fatalf("header %d claims %d bytes, not the rest of the volume", index, claimed)
		}
		if sum := md5.Sum(data[at+32:]); bytes.Equal(sum[:], data[at+16:at+32]) {
			t.Fatalf("header %d is sealed", index)
		}
	}
}

// gf16Mul multiplies in par2's GF(2^16), generator polynomial 0x1100B.
func gf16Mul(a, b uint32) uint32 {
	var product uint32
	for b != 0 {
		if b&1 != 0 {
			product ^= a
		}
		b >>= 1
		a <<= 1
		if a&0x10000 != 0 {
			a ^= 0x1100B
		}
	}
	return product
}

func gf16Pow(base uint32, exponent int) uint32 {
	result := uint32(1)
	for ; exponent > 0; exponent-- {
		result = gf16Mul(result, base)
	}
	return result
}

// par2Base is par2cmdline's base for input slice index: 2 raised to the
// index-th integer coprime to 65535.
func par2Base(index int) uint32 {
	found := -1
	for n := 1; ; n++ {
		if n%3 == 0 || n%5 == 0 || n%17 == 0 || n%257 == 0 {
			continue
		}
		found++
		if found == index {
			return gf16Pow(2, n)
		}
	}
}

func TestDependentExponentsWeighTheDamagedSlicesAlike(t *testing.T) {
	a, b := par2Base(dependentSliceA), par2Base(dependentSliceB)
	if a == b {
		t.Fatal("the two damaged slices share a base")
	}
	for step := 0; step < dependentBlocks; step++ {
		exponent := step * dependentStride
		if gf16Pow(a, exponent) != gf16Pow(b, exponent) {
			t.Fatalf("exponent %d tells the damaged slices apart", exponent)
		}
	}
	// The honest twin's exponents 0 and 1 already tell them apart, so two
	// of its blocks rebuild both slices.
	if gf16Pow(a, 1) == gf16Pow(b, 1) {
		t.Fatal("exponent 1 does not tell the damaged slices apart")
	}
	if dependentSliceB >= dependentSlices {
		t.Fatal("the second damaged slice is past the payload")
	}
}

func TestVolumeScenarioDigestsMatchTheirSeeds(t *testing.T) {
	cases := []struct {
		slug, name, seed string
		size             int64
	}{
		{"adv-par2-independent-recovery", independentPosted, "adversarial/dependent-recovery", dependentPayload},
		{"adv-recovery-sets-disagree", "contested.mkv", "adversarial/sets-disagree/posted", 1 << 20},
		{"adv-recovery-sets-disagree-damaged", "contested.mkv", "adversarial/sets-disagree/posted", 1 << 20},
		{"adv-par2-nested-false-packets", "nested-payload.mkv", "adversarial/nested-false-packets", 1<<20 + 777},
	}
	for _, c := range cases {
		path := filepath.Join(t.TempDir(), c.name)
		if err := WritePRNG(path, c.seed, c.size); err != nil {
			t.Fatal(err)
		}
		digest, err := corpus.DigestFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var scenario struct {
			Expected map[string]string `json:"expectedOutputBLAKE3"`
		}
		if err := json.Unmarshal(mustRead(t, filepath.Join(repoRoot, "testdata", c.slug, corpus.ScenarioFile)), &scenario); err != nil {
			t.Fatal(err)
		}
		if scenario.Expected[c.name] != digest.BLAKE3 {
			t.Errorf("%s: %s pins %q, the seed gives %s", c.slug, c.name, scenario.Expected[c.name], digest.BLAKE3)
		}
	}
}

func TestVolumeRecipesAreAdversarialAndNotScenarioOnly(t *testing.T) {
	for _, recipe := range AdversarialVolumeRecipes() {
		if recipe.Family != "adversarial" {
			t.Errorf("%s is in family %q", recipe.Slug, recipe.Family)
		}
		if _, listed := ScenarioOnly[recipe.Slug]; listed {
			t.Errorf("%s has a recipe and is also listed as scenario only", recipe.Slug)
		}
		if _, err := os.Stat(filepath.Join(repoRoot, "testdata", recipe.Slug, corpus.ScenarioFile)); err != nil {
			t.Errorf("%s has no scenario: %v", recipe.Slug, err)
		}
	}
}

func lessLittleEndian(left, right []byte) bool {
	for at := len(left) - 1; at >= 0; at-- {
		if left[at] != right[at] {
			return left[at] < right[at]
		}
	}
	return false
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
