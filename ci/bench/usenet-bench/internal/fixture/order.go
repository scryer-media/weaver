package fixture

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"path"
	"sort"
	"strings"
)

// NZBOrderSeed derives the permutation seed from the fixture id alone, so the
// same corpus reseeded on another host produces the same posting order.
func NZBOrderSeed(fixtureID string) uint64 {
	digest := sha256.Sum256([]byte(fixtureID))
	return binary.BigEndian.Uint64(digest[:8])
}

// OrderedNZBFiles returns the posting order for a fixture's archive files.
//
// sequential keeps the sorted volume order, which leaves any repair material
// trailing the volumes. scattered applies a deterministic permutation seeded
// from the fixture id: no volume is guaranteed to arrive first and repair
// material is interleaved among the volumes, so a client that downloads
// strictly in NZB order cannot rely on receiving the first volume first.
//
// The returned slice is a copy; files is never mutated.
func OrderedNZBFiles(order NZBOrder, fixtureID string, files []string) ([]string, uint64, error) {
	if order == "" {
		order = SequentialNZBOrder
	}
	if !order.Valid() {
		return nil, 0, fmt.Errorf("unsupported nzb_order %q", order)
	}
	if strings.TrimSpace(fixtureID) == "" {
		return nil, 0, fmt.Errorf("nzb order requires a fixture id")
	}
	ordered := append([]string(nil), files...)
	sort.Strings(ordered)
	if len(ordered) == 0 {
		return nil, 0, fmt.Errorf("fixture %q has no files to order", fixtureID)
	}
	if order == SequentialNZBOrder {
		return ordered, 0, nil
	}
	seed := NZBOrderSeed(fixtureID)
	// A uniform permutation can still land on an order that defeats the point
	// of the axis, so redraw deterministically until it does not. The redraw
	// counter is part of the recorded seed derivation, not a random retry.
	for attempt := uint64(0); attempt < 1024; attempt++ {
		candidate := append([]string(nil), ordered...)
		shuffle(candidate, seed+attempt)
		if scatteredEnough(ordered, candidate) {
			return candidate, seed + attempt, nil
		}
	}
	return nil, 0, fmt.Errorf("fixture %q has no scattered posting order distinct from its volume order", fixtureID)
}

// scatteredEnough rejects the degenerate draws: an order that still leads with
// the first sorted volume, an order identical to the sorted one, and — when
// repair material exists — an order that still leaves every repair file behind
// every archive volume.
func scatteredEnough(sorted, candidate []string) bool {
	if len(candidate) < 2 {
		return false
	}
	if candidate[0] == sorted[0] {
		return false
	}
	identical := true
	for index := range sorted {
		if sorted[index] != candidate[index] {
			identical = false
			break
		}
	}
	if identical {
		return false
	}
	lastVolume := -1
	firstRepair := -1
	for index, name := range candidate {
		if isRepairMaterial(name) {
			if firstRepair < 0 {
				firstRepair = index
			}
			continue
		}
		lastVolume = index
	}
	if firstRepair < 0 || lastVolume < 0 {
		return true
	}
	return firstRepair < lastVolume
}

func isRepairMaterial(name string) bool {
	lowered := strings.ToLower(path.Base(name))
	return strings.HasSuffix(lowered, ".par2") || strings.HasSuffix(lowered, ".rev")
}

// shuffle is an explicit Fisher-Yates over a SplitMix64 stream. The standard
// library makes no promise that its shuffle or generator algorithms stay
// byte-stable across Go releases, and a published corpus has to reproduce its
// posting order years later, so the algorithm is spelled out here.
func shuffle(values []string, seed uint64) {
	state := seed
	next := func() uint64 {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
	for index := len(values) - 1; index > 0; index-- {
		pick := int(next() % uint64(index+1))
		values[index], values[pick] = values[pick], values[index]
	}
}
