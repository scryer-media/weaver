// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func yencLieArticle(ybegin, ypart string) []byte {
	lines := []string{"Message-ID: <a@e2e-test>", "", ybegin}
	if ypart != "" {
		lines = append(lines, ypart)
	}
	lines = append(lines, "body", "=yend size=768000 part=14 pcrc32=0badf00d", ".", "")
	return []byte(strings.Join(lines, "\r\n"))
}

func TestYencHeaderLieRewritesOnlyTheNamedField(t *testing.T) {
	const ybegin = "=ybegin part=14 total=28 line=128 size=21504000 name=size=9.mkv"
	const ypart = "=ypart begin=9984001 end=10752000"
	cases := []struct {
		name         string
		lie          ScenarioYencHeaderLie
		ybegin, part string
	}{
		{
			name:   "size",
			lie:    ScenarioYencHeaderLie{Size: 1536000},
			ybegin: "=ybegin part=14 total=28 line=128 size=1536000 name=size=9.mkv",
			part:   ypart,
		},
		{
			name:   "end below begin",
			lie:    ScenarioYencHeaderLie{EndBelowBegin: true},
			ybegin: ybegin,
			part:   "=ypart begin=9984001 end=9984000",
		},
		{
			name:   "shift down",
			lie:    ScenarioYencHeaderLie{Shift: -384000},
			ybegin: ybegin,
			part:   "=ypart begin=9600001 end=10368000",
		},
		{
			name:   "size and shift",
			lie:    ScenarioYencHeaderLie{Size: 32256000, Shift: 384000},
			ybegin: "=ybegin part=14 total=28 line=128 size=32256000 name=size=9.mkv",
			part:   "=ypart begin=10368001 end=11136000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			told, err := tellYencHeaderLie(yencLieArticle(ybegin, ypart), tc.lie)
			if err != nil {
				t.Fatal(err)
			}
			if want := string(yencLieArticle(tc.ybegin, tc.part)); string(told) != want {
				t.Fatalf("told\n%q\nwant\n%q", told, want)
			}
		})
	}
}

func TestYencHeaderLieRefusesAnArticleWithoutTheLine(t *testing.T) {
	single := yencLieArticle("=ybegin line=128 size=42 name=a.bin", "")
	if _, err := tellYencHeaderLie(single, ScenarioYencHeaderLie{EndBelowBegin: true}); err == nil {
		t.Fatal("a single-part article has no =ypart line to rewrite")
	}
	if _, err := tellYencHeaderLie(yencLieArticle("=ybegin part=1 total=2 line=128 size=10 name=a", "=ypart begin=1 end=5"),
		ScenarioYencHeaderLie{Shift: -1}); err == nil {
		t.Fatal("a shift below the first byte must be refused")
	}
}

func TestYencHeaderLieReachesTheNamedSegments(t *testing.T) {
	nzb := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
	<file subject="[1/2] - &quot;copper.mesa.s01e02.mkv&quot; yEnc (1/3) 2304000">
		<segments>
			<segment bytes="1" number="1">m1@x</segment>
			<segment bytes="1" number="2">m2@x</segment>
			<segment bytes="1" number="3">m3@x</segment>
		</segments>
	</file>
	<file subject="[2/2] - &quot;copper.mesa.s01e02.mkv.par2&quot; yEnc (1/1) 900">
		<segments>
			<segment bytes="1" number="1">p1@x</segment>
		</segments>
	</file>
</nzb>`)
	every, err := yencHeaderLieMessageIDs(nzb, ScenarioYencHeaderLie{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(every)
	if want := []string{"m1@x", "m2@x", "m3@x", "p1@x"}; !slices.Equal(every, want) {
		t.Fatalf("every segment: %v, want %v", every, want)
	}
	one, err := yencHeaderLieMessageIDs(nzb, ScenarioYencHeaderLie{
		SubjectContains: []string{`copper.mesa.s01e02.mkv"`}, Segments: []int{3}, Shift: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"m3@x"}; !slices.Equal(one, want) {
		t.Fatalf("one segment: %v, want %v", one, want)
	}
}

// Every lying-header scenario tells a lie, and every lie names something to
// rewrite.
func TestYencHeaderLieScenariosEachTellALie(t *testing.T) {
	dirs, err := filepath.Glob(filepath.Join(testdataDir(), "yenc-lie-*", "scenario.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 5 {
		t.Fatalf("found %d lying-header scenarios, want 5", len(dirs))
	}
	for _, path := range dirs {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var scenario Scenario
		if err := json.Unmarshal(raw, &scenario); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if !slices.Contains(canonicalFixtureSlugs, scenario.Slug) {
			t.Errorf("%s is not in the canonical run", scenario.Slug)
		}
		if scenario.SegmentSize != 768000 {
			t.Errorf("%s posts %d-byte articles, want the field post's 768000", scenario.Slug, scenario.SegmentSize)
		}
		if len(scenario.YencHeaderLies) == 0 {
			t.Errorf("%s tells no lie", scenario.Slug)
		}
		for _, lie := range scenario.YencHeaderLies {
			if lie.Size == 0 && !lie.EndBelowBegin && lie.Shift == 0 {
				t.Errorf("%s has a lie that changes nothing", scenario.Slug)
			}
		}
	}
}
