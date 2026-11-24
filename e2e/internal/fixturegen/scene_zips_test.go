// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

func sceneZipsSlugs() map[string]bool {
	return map[string]bool{"scene-zips-outer-rar": true, "scene-zips-root": false}
}

// The end files are seeded payloads, so the digests the scenarios pin can be
// checked without running a single oracle.
func TestSceneZipScenariosPinTheirEndFiles(t *testing.T) {
	for slug := range sceneZipsSlugs() {
		var scenario struct {
			ExpectedOutcome      string            `json:"expected_outcome"`
			ExpectedOutputBLAKE3 map[string]string `json:"expectedOutputBLAKE3"`
			ForbiddenOutputPaths []string          `json:"forbiddenOutputPaths"`
		}
		if err := json.Unmarshal(read(t, filepath.Join(repoRoot, "testdata", slug, corpus.ScenarioFile)), &scenario); err != nil {
			t.Fatal(err)
		}
		if scenario.ExpectedOutcome != "success" {
			t.Errorf("%s: expected_outcome %q, want success", slug, scenario.ExpectedOutcome)
		}
		want := map[string]string{}
		dir := t.TempDir()
		for _, file := range sceneZipsEndFiles {
			payload := filepath.Join(dir, file.Name)
			if err := WritePRNG(payload, sceneZipsSeed(slug, file.Name), file.Size); err != nil {
				t.Fatal(err)
			}
			digest, err := corpus.DigestFile(payload)
			if err != nil {
				t.Fatal(err)
			}
			want[file.Name] = digest.BLAKE3
		}
		if !reflect.DeepEqual(scenario.ExpectedOutputBLAKE3, want) {
			t.Errorf("%s: expectedOutputBLAKE3 = %v, want %v", slug, scenario.ExpectedOutputBLAKE3, want)
		}
		for _, pattern := range []string{"*.zip", "*.rar", "*.r[0-9][0-9]"} {
			found := false
			for _, forbidden := range scenario.ForbiddenOutputPaths {
				found = found || forbidden == pattern
			}
			if !found {
				t.Errorf("%s: forbiddenOutputPaths does not hold %q", slug, pattern)
			}
		}
	}
}

// The ledger names the files before the recipe runs, so it has to agree with
// the names the recipe requires of RARLAB. Like every other plain RAR and zip
// in the corpus, the entries are hashed, not salted: no writer here draws a
// salt, so the generator pins whatever bytes it last wrote.
func TestSceneZipLedgerNamesEveryPostedFile(t *testing.T) {
	ledger, _, err := corpus.LoadLedger(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	for slug, outer := range sceneZipsSlugs() {
		var got []string
		for _, file := range ledger.Files {
			name, ok := strings.CutPrefix(file.Path, "testdata/"+slug+"/")
			if !ok {
				continue
			}
			got = append(got, name)
			if file.Salted {
				t.Errorf("%s is salted, but nothing that writes it draws a salt", file.Path)
			}
		}
		want := append([]string(nil), sceneZipsOutputs(outer)...)
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: ledger holds %v, recipe posts %v", slug, got, want)
		}
	}
	for _, recipe := range SceneZipRecipes() {
		if recipe.ByteReproducible || recipe.ExpectedOutputs == nil {
			t.Errorf("%s must allow variable archive bytes and pin its end files", recipe.Slug)
		}
	}
}

// Every member is stored, so the volume counts follow from the sizes: the
// bytes must overrun one volume fewer and leave room in the last for headers.
func TestSceneZipVolumeCountsFollowFromTheSizes(t *testing.T) {
	volume := func(selector string) int64 {
		size, err := strconv.ParseInt(strings.TrimSuffix(selector, "b"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return size
	}
	const headroom = 16 << 10
	var inner int64
	for _, file := range sceneZipsEndFiles {
		inner += file.Size
	}
	if size := volume(sceneZipsInnerVolume); inner <= size*(sceneZipsInnerVolumes-1)+headroom || inner >= size*sceneZipsInnerVolumes-headroom {
		t.Errorf("%d end-file bytes in %d-byte volumes is not %d volumes with headroom", inner, size, sceneZipsInnerVolumes)
	}
	// Each zip adds the diz, the nfo and its own records to its volume.
	const zipOverhead = sceneZipsDIZSize + sceneZipsNFOSize + 512
	outer := inner + sceneZipsInnerVolumes*zipOverhead
	if size := volume(sceneZipsOuterVolume); outer <= size*(sceneZipsOuterVolumes-1)+headroom || outer >= size*sceneZipsOuterVolumes-headroom {
		t.Errorf("%d zip bytes in %d-byte volumes is not %d volumes with headroom", outer, size, sceneZipsOuterVolumes)
	}
	names := sceneZipsInnerVolumeNames()
	if names[0] != "lbcn0101.rar" || names[1] != "lbcn0101.r00" || names[len(names)-1] != "lbcn0101.r03" {
		t.Errorf("inner volume names %v", names)
	}
	if zips := sceneZipsZipNames(); zips[0] != "lbcn0101a.zip" || zips[len(zips)-1] != "lbcn0101e.zip" {
		t.Errorf("zip names %v", zips)
	}
}

// Each zip stores, in order, the diz, the nfo and one volume, uncompressed.
func TestSceneZipHoldsTheDizTheNfoAndOneVolumeStored(t *testing.T) {
	dir := t.TempDir()
	volume := filepath.Join(dir, "lbcn0101.r01")
	if err := WritePRNG(volume, "scene-zip-test", 70_000); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "lbcn0101c.zip")
	if err := writeSceneZip(destination, volume, "lbcn0101.r01", filepath.Join(dir, "scratch", sceneZipsDIZ), filepath.Join(dir, "scratch", sceneZipsNFO), 2); err != nil {
		t.Fatal(err)
	}
	body := read(t, destination)
	reader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	contents := map[string][]byte{}
	for _, entry := range reader.File {
		names = append(names, entry.Name)
		if entry.Method != zip.Store {
			t.Errorf("%s is not stored", entry.Name)
		}
		opened, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened)
		opened.Close()
		if err != nil {
			t.Fatal(err)
		}
		contents[entry.Name] = data
	}
	if want := []string{sceneZipsDIZ, sceneZipsNFO, "lbcn0101.r01"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("members %v, want %v", names, want)
	}
	if !bytes.Equal(contents["lbcn0101.r01"], read(t, volume)) {
		t.Error("the volume is not stored byte for byte")
	}
	if len(contents[sceneZipsDIZ]) != sceneZipsDIZSize || !bytes.Contains(contents[sceneZipsDIZ], []byte("[03/05]")) {
		t.Errorf("diz %q", contents[sceneZipsDIZ])
	}
	if len(contents[sceneZipsNFO]) != sceneZipsNFOSize {
		t.Errorf("nfo is %d bytes, want %d", len(contents[sceneZipsNFO]), sceneZipsNFOSize)
	}
}
