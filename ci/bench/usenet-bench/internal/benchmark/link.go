package benchmark

import (
	"fmt"
	"time"
)

const serverAggregateEgressScope = "server_aggregate_egress"

// ServerLinkProfile is a physical-link simulation imposed before the NNTP
// response reaches a client. It is deliberately server aggregate, never a
// per-client throttle: every connection competes for the same declared link.
//
// RTTMicros is the fixed round trip the shaper adds between the client and
// the server, split evenly across the two directions with zero jitter. It is
// orthogonal to the rate: a 1gbit link at 0 and at 250 ms are different
// strata, and neither is pooled with the other. Zero means the shaper adds no
// delay, which is what every plan declared before the field existed.
type ServerLinkProfile struct {
	ID                  string `json:"id"`
	Scope               string `json:"scope"`
	EgressBitsPerSecond uint64 `json:"egress_bits_per_second"`
	BurstBytes          uint64 `json:"burst_bytes"`
	RTTMicros           uint64 `json:"rtt_micros"`
}

// Server RTT bounds. The floor keeps a plan from declaring a delay netem
// cannot render distinctly from none; the ceiling keeps a typo in the unit
// (seconds for milliseconds) from producing a suite that never finishes.
const (
	MinServerRTT = 1 * time.Millisecond
	MaxServerRTT = 5 * time.Second
)

// ValidateServerRTT accepts zero (no added delay) or a whole number of
// milliseconds within the bounds, so each direction's one-way delay is a
// whole number of microseconds that tc prints back exactly.
func ValidateServerRTT(rtt time.Duration) error {
	if rtt == 0 {
		return nil
	}
	if rtt < MinServerRTT || rtt > MaxServerRTT {
		return fmt.Errorf("server RTT %s must be zero or between %s and %s", rtt, MinServerRTT, MaxServerRTT)
	}
	if rtt%time.Millisecond != 0 {
		return fmt.Errorf("server RTT %s must be a whole number of milliseconds", rtt)
	}
	return nil
}

const (
	LinkUnlimited = "unlimited"
	Link1Gbit     = "1gbit"
	Link10Gbit    = "10gbit"
	LinkCustom    = "custom"
)

func DefaultServerLinkProfile() ServerLinkProfile {
	return ServerLinkProfile{ID: LinkUnlimited, Scope: serverAggregateEgressScope}
}

// ResolveServerLinkProfile returns a complete, serializable server-link
// contract. Custom is intentionally explicit: no benchmark silently changes a
// named 1/10 Gbit profile's rate or burst. The RTT is declared alongside any
// profile, including unlimited, and is validated the same way for all of them.
func ResolveServerLinkProfile(id string, egressBitsPerSecond, burstBytes, rttMicros uint64) (ServerLinkProfile, error) {
	profile, err := resolveServerLinkRate(id, egressBitsPerSecond, burstBytes)
	if err != nil {
		return ServerLinkProfile{}, err
	}
	if rttMicros > uint64(MaxServerRTT/time.Microsecond) {
		return ServerLinkProfile{}, fmt.Errorf("server RTT %dus exceeds %s", rttMicros, MaxServerRTT)
	}
	if err := ValidateServerRTT(time.Duration(rttMicros) * time.Microsecond); err != nil {
		return ServerLinkProfile{}, err
	}
	profile.RTTMicros = rttMicros
	return profile, nil
}

func resolveServerLinkRate(id string, egressBitsPerSecond, burstBytes uint64) (ServerLinkProfile, error) {
	switch id {
	case "", LinkUnlimited:
		if egressBitsPerSecond != 0 || burstBytes != 0 {
			return ServerLinkProfile{}, fmt.Errorf("unlimited link profile cannot set a rate or burst")
		}
		return DefaultServerLinkProfile(), nil
	case Link1Gbit:
		profile := ServerLinkProfile{ID: Link1Gbit, Scope: serverAggregateEgressScope, EgressBitsPerSecond: 1_000_000_000, BurstBytes: 1 << 20}
		if (egressBitsPerSecond != 0 || burstBytes != 0) && (egressBitsPerSecond != profile.EgressBitsPerSecond || burstBytes != profile.BurstBytes) {
			return ServerLinkProfile{}, fmt.Errorf("1gbit link profile has fixed rate and burst; use custom to override")
		}
		return profile, nil
	case Link10Gbit:
		profile := ServerLinkProfile{ID: Link10Gbit, Scope: serverAggregateEgressScope, EgressBitsPerSecond: 10_000_000_000, BurstBytes: 1 << 20}
		if (egressBitsPerSecond != 0 || burstBytes != 0) && (egressBitsPerSecond != profile.EgressBitsPerSecond || burstBytes != profile.BurstBytes) {
			return ServerLinkProfile{}, fmt.Errorf("10gbit link profile has fixed rate and burst; use custom to override")
		}
		return profile, nil
	case LinkCustom:
		if egressBitsPerSecond == 0 || burstBytes == 0 {
			return ServerLinkProfile{}, fmt.Errorf("custom link profile requires positive egress bits per second and burst bytes")
		}
		return ServerLinkProfile{ID: LinkCustom, Scope: serverAggregateEgressScope, EgressBitsPerSecond: egressBitsPerSecond, BurstBytes: burstBytes}, nil
	default:
		return ServerLinkProfile{}, fmt.Errorf("unsupported server link profile %q", id)
	}
}

// Shaped reports whether a run under this profile needs the transparent
// shaper at all: either a rate cap or an added round trip makes the shaper's
// attestation part of the run's evidence.
func (p ServerLinkProfile) Shaped() bool {
	return p.EgressBitsPerSecond > 0 || p.RTTMicros > 0
}

// RTT is the declared round trip as a duration.
func (p ServerLinkProfile) RTT() time.Duration {
	return time.Duration(p.RTTMicros) * time.Microsecond
}

func (p ServerLinkProfile) Validate() error {
	if p.Scope != serverAggregateEgressScope {
		return fmt.Errorf("server link profile %q must use %q scope", p.ID, serverAggregateEgressScope)
	}
	resolved, err := ResolveServerLinkProfile(p.ID, 0, 0, p.RTTMicros)
	if p.ID == LinkCustom {
		resolved, err = ResolveServerLinkProfile(p.ID, p.EgressBitsPerSecond, p.BurstBytes, p.RTTMicros)
	}
	if err != nil {
		return err
	}
	if p != resolved {
		return fmt.Errorf("server link profile %q does not match its declared fixed values", p.ID)
	}
	return nil
}
