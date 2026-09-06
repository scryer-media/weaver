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

// A seeded image is one server build plus one corpus, so the fake-server pin
// is part of the fingerprint: bumping E2E_NNTP_MODULE_VERSION must miss the
// images captured on the previous server rather than reuse them.
func TestFunctionalSeedFingerprintFollowsTheServerPin(t *testing.T) {
	slugs := fixtureSlugsForSeedProfile("functional")
	t.Setenv("E2E_NNTP_SOURCE_DIR", "")
	t.Setenv("E2E_NNTP_IMAGE", "")
	t.Setenv("E2E_NNTP_MODULE_VERSION", "v0.1.0")
	before, err := nntpSeedCorpusFingerprint("functional", slugs)
	if err != nil {
		t.Skipf("fingerprint unavailable in this checkout: %v", err)
	}
	t.Setenv("E2E_NNTP_MODULE_VERSION", "v0.1.1")
	after, err := nntpSeedCorpusFingerprint("functional", slugs)
	if err != nil {
		t.Fatalf("fingerprint after pin bump: %v", err)
	}
	if before == after {
		t.Fatalf("fingerprint ignored the server pin bump: %s", before)
	}
	// A parent phase hands children the seeded tags through E2E_NNTP_IMAGE;
	// those must not feed back into the fingerprint they were derived from.
	t.Setenv("E2E_NNTP_IMAGE", nntpSeedImageTag("functional", "primary", after))
	again, err := nntpSeedCorpusFingerprint("functional", slugs)
	if err != nil {
		t.Fatalf("fingerprint with seeded tag applied: %v", err)
	}
	if again != after {
		t.Fatalf("seeded tag in E2E_NNTP_IMAGE moved the fingerprint: %s vs %s", after, again)
	}
}
