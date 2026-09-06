package benchmark

import (
	"testing"
	"time"
)

func TestNamedServerLinkAcceptsItsSerializedValues(t *testing.T) {
	profile, err := ResolveServerLinkProfile(Link10Gbit, 10_000_000_000, 1<<20, 0)
	if err != nil {
		t.Fatalf("serialized 10gbit profile rejected: %v", err)
	}
	if profile.ID != Link10Gbit || profile.EgressBitsPerSecond != 10_000_000_000 || profile.BurstBytes != 1<<20 || profile.RTTMicros != 0 {
		t.Fatalf("unexpected resolved profile: %#v", profile)
	}
	if _, err := ResolveServerLinkProfile(Link10Gbit, 10_000_000_001, 1<<20, 0); err == nil {
		t.Fatal("named profile must reject a mismatched explicit rate")
	}
}

func TestServerLinkRoundTripIsDeclaredAlongsideAnyProfile(t *testing.T) {
	for _, id := range []string{LinkUnlimited, Link1Gbit, Link10Gbit} {
		profile, err := ResolveServerLinkProfile(id, 0, 0, 250_000)
		if err != nil {
			t.Fatalf("%s at 250ms rejected: %v", id, err)
		}
		if profile.RTTMicros != 250_000 || profile.RTT() != 250*time.Millisecond || !profile.Shaped() {
			t.Fatalf("%s: unexpected round trip on %#v", id, profile)
		}
		if err := profile.Validate(); err != nil {
			t.Fatalf("%s at 250ms does not validate: %v", id, err)
		}
	}
	custom, err := ResolveServerLinkProfile(LinkCustom, 100_000_000, 1<<20, 500_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := custom.Validate(); err != nil {
		t.Fatalf("custom at 500ms does not validate: %v", err)
	}
	unshaped := DefaultServerLinkProfile()
	if unshaped.Shaped() || unshaped.RTTMicros != 0 {
		t.Fatalf("default profile must be unshaped: %#v", unshaped)
	}
}

func TestServerLinkRoundTripBounds(t *testing.T) {
	for _, micros := range []uint64{500, 250_500, 250_001, 5_000_001, 60_000_000} {
		if _, err := ResolveServerLinkProfile(Link1Gbit, 0, 0, micros); err == nil {
			t.Fatalf("%dus round trip must be rejected", micros)
		}
	}
	for _, micros := range []uint64{1_000, 250_000, 500_000, 5_000_000} {
		if _, err := ResolveServerLinkProfile(Link1Gbit, 0, 0, micros); err != nil {
			t.Fatalf("%dus round trip rejected: %v", micros, err)
		}
	}
	// A profile whose serialized round trip was edited by hand fails the
	// same way an edited rate does.
	edited := ServerLinkProfile{ID: Link1Gbit, Scope: serverAggregateEgressScope, EgressBitsPerSecond: 1_000_000_000, BurstBytes: 1 << 20, RTTMicros: 250_500}
	if err := edited.Validate(); err == nil {
		t.Fatal("hand-edited round trip must not validate")
	}
	if err := ValidateServerRTT(-time.Millisecond); err == nil {
		t.Fatal("negative round trip must be rejected")
	}
}
