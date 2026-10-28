// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"reflect"
	"testing"
)

func TestArticleDecodedRangeReadsTheYEncHeader(t *testing.T) {
	part := []byte("220 0 <a@x> article\r\nSubject: x\r\n\r\n=ybegin part=2 total=3 line=128 size=300 name=a.rar\r\n=ypart begin=101 end=200\r\ndata\r\n=yend size=100 part=2\r\n.\r\n")
	if got, err := articleDecodedRange(part); err != nil || got != (decodedRange{Start: 100, End: 200}) {
		t.Fatalf("multipart range = %+v, %v", got, err)
	}
	single := []byte("=ybegin line=128 size=42 name=a.bin\r\ndata\r\n=yend size=42\r\n")
	if got, err := articleDecodedRange(single); err != nil || got != (decodedRange{Start: 0, End: 42}) {
		t.Fatalf("single-part range = %+v, %v", got, err)
	}
	if _, err := articleDecodedRange([]byte("=ybegin part=1 size=10 name=a\r\n=ypart begin=0 end=5\r\n")); err == nil {
		t.Fatal("a part beginning at 0 was read as a range")
	}
	if _, err := articleDecodedRange([]byte("not yEnc\r\n")); err == nil {
		t.Fatal("an article with no yEnc header was read as a range")
	}
}

func TestCoveredArticlesAreOnlyThoseTheCheckpointHoldsWhole(t *testing.T) {
	segments := []nzbSegmentRef{
		{FileIndex: 0, MessageID: "a1"},
		{FileIndex: 0, MessageID: "a2"},
		{FileIndex: 0, MessageID: "a3"},
		{FileIndex: 1, MessageID: "b1"},
		{FileIndex: 1, MessageID: "unread"},
	}
	ranges := map[string]decodedRange{
		"a1": {0, 100}, "a2": {100, 200}, "a3": {200, 300},
		"b1": {0, 100},
	}
	rows := []fileCoverageRow{
		{FileIndex: 0, Coverage: fileCoverage{Runs: []fileCoverageRun{{0, 150, "fresh"}, {200, 300, "fresh"}}}},
		{FileIndex: 2, Coverage: fileCoverage{Runs: []fileCoverageRun{{0, 100, "fresh"}}}},
	}
	covered := coveredArticles(segments, ranges, rows)
	if want := map[string]bool{"a1": true, "a3": true}; !reflect.DeepEqual(covered, want) {
		t.Fatalf("covered = %v, want %v", covered, want)
	}

	before := restartNntpMetrics{BodyCounts: map[string]int{"a1": 1, "a2": 1, "a3": 1}}
	after := restartNntpMetrics{BodyCounts: map[string]int{"a1": 1, "a2": 2, "a3": 2, "b1": 1}}
	if got := refetchedArticles(covered, before, after); !reflect.DeepEqual(got, []string{"a3"}) {
		t.Fatalf("refetched = %v, want only the covered article served again", got)
	}
}
