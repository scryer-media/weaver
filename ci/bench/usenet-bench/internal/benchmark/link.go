package benchmark

import "fmt"

const serverAggregateEgressScope = "server_aggregate_egress"

// ServerLinkProfile is a physical-link simulation imposed before the NNTP
// response reaches a client. It is deliberately server aggregate, never a
// per-client throttle: every connection competes for the same declared link.
type ServerLinkProfile struct {
	ID                  string `json:"id"`
	Scope               string `json:"scope"`
	EgressBitsPerSecond uint64 `json:"egress_bits_per_second"`
	BurstBytes          uint64 `json:"burst_bytes"`
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
// named 1/10 Gbit profile's rate or burst.
func ResolveServerLinkProfile(id string, egressBitsPerSecond, burstBytes uint64) (ServerLinkProfile, error) {
	switch id {
	case "", LinkUnlimited:
		if egressBitsPerSecond != 0 || burstBytes != 0 {
			return ServerLinkProfile{}, fmt.Errorf("unlimited link profile cannot set a rate or burst")
		}
		return DefaultServerLinkProfile(), nil
	case Link1Gbit:
		if egressBitsPerSecond != 0 || burstBytes != 0 {
			return ServerLinkProfile{}, fmt.Errorf("1gbit link profile has fixed rate and burst; use custom to override")
		}
		return ServerLinkProfile{ID: Link1Gbit, Scope: serverAggregateEgressScope, EgressBitsPerSecond: 1_000_000_000, BurstBytes: 1 << 20}, nil
	case Link10Gbit:
		if egressBitsPerSecond != 0 || burstBytes != 0 {
			return ServerLinkProfile{}, fmt.Errorf("10gbit link profile has fixed rate and burst; use custom to override")
		}
		return ServerLinkProfile{ID: Link10Gbit, Scope: serverAggregateEgressScope, EgressBitsPerSecond: 10_000_000_000, BurstBytes: 1 << 20}, nil
	case LinkCustom:
		if egressBitsPerSecond == 0 || burstBytes == 0 {
			return ServerLinkProfile{}, fmt.Errorf("custom link profile requires positive egress bits per second and burst bytes")
		}
		return ServerLinkProfile{ID: LinkCustom, Scope: serverAggregateEgressScope, EgressBitsPerSecond: egressBitsPerSecond, BurstBytes: burstBytes}, nil
	default:
		return ServerLinkProfile{}, fmt.Errorf("unsupported server link profile %q", id)
	}
}

func (p ServerLinkProfile) Validate() error {
	if p.Scope != serverAggregateEgressScope {
		return fmt.Errorf("server link profile %q must use %q scope", p.ID, serverAggregateEgressScope)
	}
	resolved, err := ResolveServerLinkProfile(p.ID, 0, 0)
	if p.ID == LinkCustom {
		resolved, err = ResolveServerLinkProfile(p.ID, p.EgressBitsPerSecond, p.BurstBytes)
	}
	if err != nil {
		return err
	}
	if p != resolved {
		return fmt.Errorf("server link profile %q does not match its declared fixed values", p.ID)
	}
	return nil
}
