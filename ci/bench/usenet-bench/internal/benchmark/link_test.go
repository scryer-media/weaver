package benchmark

import "testing"

func TestNamedServerLinkAcceptsItsSerializedValues(t *testing.T) {
	profile, err := ResolveServerLinkProfile(Link10Gbit, 10_000_000_000, 1<<20)
	if err != nil {
		t.Fatalf("serialized 10gbit profile rejected: %v", err)
	}
	if profile.ID != Link10Gbit || profile.EgressBitsPerSecond != 10_000_000_000 || profile.BurstBytes != 1<<20 {
		t.Fatalf("unexpected resolved profile: %#v", profile)
	}
	if _, err := ResolveServerLinkProfile(Link10Gbit, 10_000_000_001, 1<<20); err == nil {
		t.Fatal("named profile must reject a mismatched explicit rate")
	}
}
