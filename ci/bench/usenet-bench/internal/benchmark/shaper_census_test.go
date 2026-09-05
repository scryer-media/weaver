package benchmark

import (
	"strings"
	"testing"
	"time"
)

func TestShaperArticleCensusForBracketsOneRun(t *testing.T) {
	acquired := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	before := ShaperSnapshot{SchemaVersion: 3, Status: "ok", StartedAt: acquired, ExecutionLeaseID: strings.Repeat("a", 64), ExecutionLeaseAcquiredAt: &acquired,
		ArticleRequests: 1000, RepeatedArticleRequests: 7}
	after := before
	after.ArticleRequests = 1130
	after.DistinctArticleRequests = 125
	after.RepeatedArticleRequests = 12
	census, err := ShaperArticleCensusFor(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if census == nil || census.ArticleRequests != 130 || census.DistinctArticleRequests != 125 || census.RepeatedArticleRequests != 5 {
		t.Fatalf("census=%+v", census)
	}

	// A schema-2 shaper counted nothing; the artifact carries no census.
	legacyBefore, legacyAfter := before, after
	legacyBefore.SchemaVersion, legacyAfter.SchemaVersion = 2, 2
	if census, err := ShaperArticleCensusFor(legacyBefore, legacyAfter); err != nil || census != nil {
		t.Fatalf("legacy census=%+v err=%v, want nil, nil", census, err)
	}

	// The lease acquisition must have reset the distinct set.
	stale := before
	stale.DistinctArticleRequests = 3
	if _, err := ShaperArticleCensusFor(stale, after); err == nil {
		t.Fatal("expected a stale distinct set to be refused")
	}

	// Counters that go backwards or do not add up are refused.
	backwards := after
	backwards.ArticleRequests = 999
	if _, err := ShaperArticleCensusFor(before, backwards); err == nil {
		t.Fatal("expected a backwards counter to be refused")
	}
	inconsistent := after
	inconsistent.DistinctArticleRequests = 200
	if _, err := ShaperArticleCensusFor(before, inconsistent); err == nil {
		t.Fatal("expected distinct+repeated > requests to be refused")
	}
}

func TestShaperSnapshotValidateAcceptsCensusSchema(t *testing.T) {
	acquired := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	link := ServerLinkProfile{ID: "1gbit", EgressBitsPerSecond: 1_000_000_000, BurstBytes: 1 << 20}
	for _, version := range []int{2, 3} {
		snapshot := ShaperSnapshot{SchemaVersion: version, Status: "ok", StartedAt: acquired,
			ConfiguredEgressBitsPerSecond: link.EgressBitsPerSecond, ConfiguredBurstBytes: link.BurstBytes,
			DownstreamSourceConnections: map[string]uint64{}, DownstreamSourceBytes: map[string]uint64{},
			ExecutionLeaseID: strings.Repeat("a", 64), ExecutionLeaseAcquiredAt: &acquired,
			Build: ShaperBuildIdentity{ExecutableSHA256: strings.Repeat("0", 64)}}
		if err := snapshot.ValidateFor(link); err != nil {
			t.Errorf("schema %d: %v", version, err)
		}
		snapshot.SchemaVersion = 4
		if err := snapshot.ValidateFor(link); err == nil {
			t.Errorf("schema 4 accepted")
		}
	}
}
