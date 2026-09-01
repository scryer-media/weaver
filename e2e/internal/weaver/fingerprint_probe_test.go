package weaver

import "testing"

// The corpus fingerprint decides which NNTP image a phase runs against, so it
// has to be a function of committed bytes and nothing else. A seeding pass that
// rewrites a fixture, a ledger entry or a scenario digest moves it — and a moved
// fingerprint silently misses the image this run captured.
func TestFunctionalSeedFingerprintIsStableAcrossReads(t *testing.T) {
	slugs := fixtureSlugsForSeedProfile("functional")
	first, err := nntpSeedCorpusFingerprint("functional", slugs)
	if err != nil {
		t.Skipf("fingerprint unavailable in this checkout: %v", err)
	}
	second, err := nntpSeedCorpusFingerprint("functional", slugs)
	if err != nil {
		t.Fatalf("second fingerprint: %v", err)
	}
	if first != second {
		t.Fatalf("fingerprint moved between reads: %s vs %s", first, second)
	}
}
