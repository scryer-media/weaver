// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// buildContainerCase runs one Go-only container recipe into a temporary
// work directory and returns its one output.
func buildContainerCase(t *testing.T, slug, output string) []byte {
	t.Helper()
	for _, recipe := range AdversarialContainerRecipes() {
		if recipe.Slug != slug {
			continue
		}
		env := &Env{Work: t.TempDir(), Slug: slug}
		if err := recipe.Build(context.Background(), env); err != nil {
			t.Fatalf("%s: %v", slug, err)
		}
		produced, err := env.Outputs()
		if err != nil {
			t.Fatal(err)
		}
		if len(produced) != 1 || produced[0] != output {
			t.Fatalf("%s produced %v, want only %s", slug, produced, output)
		}
		data, err := os.ReadFile(env.OutputPath(output))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	t.Fatalf("no container recipe %s", slug)
	return nil
}

// zstdFrames reads the frames of a stream whose every frame is one raw
// last block after a header of no flags but its window descriptor: each
// frame's window log and body.
func zstdFrames(t *testing.T, data []byte) ([]int, [][]byte) {
	t.Helper()
	var logs []int
	var bodies [][]byte
	for len(data) > 0 {
		if len(data) < 9 || !bytes.Equal(data[:4], []byte{0x28, 0xB5, 0x2F, 0xFD}) || data[4] != 0 {
			t.Fatalf("not a frame of the expected shape: % x", data[:min(len(data), 9)])
		}
		descriptor := data[5]
		if descriptor&7 != 0 {
			t.Fatalf("window descriptor %#x carries a mantissa", descriptor)
		}
		logs = append(logs, 10+int(descriptor>>3))
		header := uint32(data[6]) | uint32(data[7])<<8 | uint32(data[8])<<16
		if header&1 != 1 || (header>>1)&3 != 0 {
			t.Fatalf("block header %#x is not a raw last block", header)
		}
		size := int(header >> 3)
		bodies = append(bodies, data[9:9+size])
		data = data[9+size:]
	}
	return logs, bodies
}

func TestZstdCasesPutTheWideWindowInTheLaterFrame(t *testing.T) {
	for _, tc := range []struct {
		slug, output string
		logs         []int
	}{
		{"adv-zstd-later-frame-wide", "harbor-wide.mkv.zst", []int{20, 41}},
		{"adv-zstd-later-frame-honest", "harbor-light.mkv.zst", []int{20, 28}},
	} {
		logs, bodies := zstdFrames(t, buildContainerCase(t, tc.slug, tc.output))
		if len(logs) != 2 || logs[0] != tc.logs[0] || logs[1] != tc.logs[1] {
			t.Errorf("%s: window logs %v, want %v", tc.slug, logs, tc.logs)
		}
		for index, body := range bodies {
			if !bytes.Equal(body, ZstdFramePayload(tc.output, index)) {
				t.Errorf("%s: frame %d holds %q", tc.slug, index, body)
			}
		}
	}
}

// gunzip inflates one gzip member whole.
func gunzip(t *testing.T, data []byte) []byte {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	inflated, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return inflated
}

// tarMembers reads a tar with archive/tar: each member's size by name, and
// the names in archive order.
func tarMembers(t *testing.T, data []byte) (map[string]int64, []string) {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(data))
	sizes := map[string]int64{}
	var order []string
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return sizes, order
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(body)) != header.Size {
			t.Fatalf("%s: read %d bytes of %d", header.Name, len(body), header.Size)
		}
		sizes[header.Name] = header.Size
		order = append(order, header.Name)
	}
}

// The honest nest: the tar and the member each expand the post well under
// the ratio of a hundred, and the two together well past it. The margins
// leave room for the posted bytes the NZB declares running a few percent
// over the file.
func TestRatioLevelsAreUnderTheRatioAloneAndOverItTogether(t *testing.T) {
	post := buildContainerCase(t, "adv-targz-levels-under-ratio", "harbor-reel.tar.gz")
	inner := gunzip(t, post)
	sizes, _ := tarMembers(t, inner)
	member, ok := sizes[RatioLevelsMember]
	if !ok || len(sizes) != 1 {
		t.Fatalf("members %v, want only %s", sizes, RatioLevelsMember)
	}
	posted := float64(len(post))
	tarRatio, memberRatio := float64(len(inner))/posted, float64(member)/posted
	if tarRatio > 80 || memberRatio > 80 {
		t.Errorf("a level alone expands %.1fx (tar) and %.1fx (member); each must stay well under 100x", tarRatio, memberRatio)
	}
	if tarRatio+memberRatio < 120 {
		t.Errorf("the levels together expand %.1fx; they must pass 100x by a margin", tarRatio+memberRatio)
	}
	if limit := posted * 100; limit < 1<<20 {
		t.Errorf("the ratio limit %.0f is under the ratio group's 1 MiB floor, so the floor would judge the case", limit)
	}
}

// The member cap nest: the tar is past the tight group's 32 MiB member cap,
// each member under it.
func TestMemberCapNestPutsOnlyTheTarOverTheCap(t *testing.T) {
	const tightMemberCap = 32 << 20
	post := buildContainerCase(t, "adv-targz-inner-over-member-cap", "harbor-pair.tar.gz")
	inner := gunzip(t, post)
	if len(inner) <= tightMemberCap {
		t.Errorf("the tar is %d bytes; it must pass the %d member cap", len(inner), tightMemberCap)
	}
	sizes, _ := tarMembers(t, inner)
	if len(sizes) != len(MemberCapMembers) {
		t.Fatalf("members %v, want %v", sizes, MemberCapMembers)
	}
	for _, name := range MemberCapMembers {
		if size, ok := sizes[name]; !ok || size >= tightMemberCap {
			t.Errorf("%s is %d bytes; it must be present and under the member cap", name, size)
		}
	}
}

// The name chain: every inner tar holds the one name, the outer member
// asking for it comes last, and the scenario's required outputs are the
// names the chain settles on.
func TestNameChainAsksForOneNameAtTwoDepths(t *testing.T) {
	outer := buildContainerCase(t, "adv-tar-one-name-chain", "harbor-crates.tar")
	_, order := tarMembers(t, outer)
	if len(order) != nameChainMembers+1 || order[len(order)-1] != nameChainName {
		t.Fatalf("%d members ending %q, want %d inner tars then %s", len(order), order[len(order)-1], nameChainMembers, nameChainName)
	}
	reader := tar.NewReader(bytes.NewReader(outer))
	for index := 0; index < nameChainMembers; index++ {
		if _, err := reader.Next(); err != nil {
			t.Fatal(err)
		}
		inner, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		_, names := tarMembers(t, inner)
		if len(names) != 1 || names[0] != nameChainName {
			t.Fatalf("inner tar %d holds %v, want only %s", index+1, names, nameChainName)
		}
	}

	var scenario struct {
		Adversarial struct {
			MinOutputFiles  int      `json:"minOutputFiles"`
			RequiredOutputs []string `json:"requiredOutputs"`
		} `json:"adversarial"`
	}
	if err := json.Unmarshal(read(t, filepath.Join(repoRoot, "testdata", "adv-tar-one-name-chain", "scenario.json")), &scenario); err != nil {
		t.Fatal(err)
	}
	if scenario.Adversarial.MinOutputFiles != nameChainMembers+1 {
		t.Errorf("minOutputFiles %d, want %d", scenario.Adversarial.MinOutputFiles, nameChainMembers+1)
	}
	want := map[string]bool{
		NameChainDelivered(0): true, NameChainDelivered(1): true,
		NameChainDelivered(nameChainMembers / 2): true, NameChainDelivered(nameChainMembers): true,
	}
	for _, name := range scenario.Adversarial.RequiredOutputs {
		if !want[name] {
			t.Errorf("required output %q is not a name the chain settles on", name)
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("the scenario does not require %q", name)
	}
}

// The deep name is a GNU long name a tar reader takes whole, under the tar
// object's 1 MiB extension limit, and the sibling beside it is ordinary.
func TestDeepLongNameIsReadBackWhole(t *testing.T) {
	data := buildContainerCase(t, "adv-tar-deep-long-name", "deep-name.tar")
	if len(DeepName)+1 > 1<<20 {
		t.Fatalf("the long name's %d bytes pass the 1 MiB extension limit", len(DeepName)+1)
	}
	_, order := tarMembers(t, data)
	if len(order) != 2 || order[0] != DeepName || order[1] != DeepSibling {
		t.Fatalf("members of lengths %d, want the deep name then %s", len(order), DeepSibling)
	}
}

// The yEnc lie case's member runs past one 8 KiB article, so the post is
// multipart and every =ybegin header carries the lied size.
func TestYencLieTarSpansTwoArticles(t *testing.T) {
	data := buildContainerCase(t, "adv-tar-yenc-size-lie", "ember-lie.tar")
	if len(data) <= 8192 || len(data) > 2*8192 {
		t.Errorf("the tar is %d bytes; it must fill two 8 KiB articles", len(data))
	}
	sizes, _ := tarMembers(t, data)
	if _, ok := sizes[YencLieMember]; !ok || len(sizes) != 1 {
		t.Fatalf("members %v, want only %s", sizes, YencLieMember)
	}
}
