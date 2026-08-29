package corpus

import (
	"os"
	"strings"
	"testing"
)

func verifyFixture(t *testing.T) (string, *Ledger, *Profiles, ToolchainLock) {
	t.Helper()
	root := writeTree(t, map[string]string{
		"testdata/one/a.rar":           "the first fixture",
		"testdata/one/scenario.json":   `{"slug":"one"}`,
		"testdata/two/b.par2":          "the second fixture",
		"testdata/shared/clip.mkv":     "the shared clip",
		"testdata/two/scenario.json":   `{"slug":"two"}`,
		"testdata/shared/.keep-me-out": "hidden entries are not fixtures",
	})
	ledger := newLedger(
		entry("testdata/one/a.rar", "the first fixture", generatedSource()),
		entry("testdata/two/b.par2", "the second fixture", blockedSource("generator pending: par2 set")),
		entry("testdata/shared/clip.mkv", "the shared clip", blockedSource("generator pending: shared clip")),
	)
	profiles := newProfiles(map[string][]string{"all": {"testdata/**"}})
	return root, ledger, profiles, loadToolchains(t, root)
}

func TestVerifyTreeAcceptsAMatchingTree(t *testing.T) {
	root, ledger, _, _ := verifyFixture(t)
	options := VerifyOptions{AllPresent: true}
	report, err := VerifyTree(root, ledger, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := report.Err(options); err != nil {
		t.Fatal(err)
	}
	if report.Present != 3 {
		t.Fatalf("%d present, want 3", report.Present)
	}
	// scenario.json is tracked in git and never hydrated, so it is not a
	// corpus member and must not be reported as unledgered.
	if len(report.Unledgered) != 0 {
		t.Fatalf("unledgered: %v", report.Unledgered)
	}
	if len(report.Blocked) != 2 {
		t.Fatalf("%d blocked, want 2", len(report.Blocked))
	}
}

func TestVerifyTreeFailsOnADigestMismatch(t *testing.T) {
	root, ledger, _, _ := verifyFixture(t)
	writeFixture(t, root, "testdata/one/a.rar", "the first fixture!")
	options := VerifyOptions{}
	report, err := VerifyTree(root, ledger, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mismatched) != 1 {
		t.Fatalf("mismatched = %v", report.Mismatched)
	}
	if err := report.Err(options); err == nil {
		t.Fatal("a mismatch is always a failure, with or without --all-present")
	}
}

func TestVerifyTreeFlagsUnledgeredFixtures(t *testing.T) {
	root, ledger, _, _ := verifyFixture(t)
	writeFixture(t, root, "testdata/one/surprise.par2", "nobody listed me")
	options := VerifyOptions{}
	report, err := VerifyTree(root, ledger, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Unledgered) != 1 || report.Unledgered[0] != "testdata/one/surprise.par2" {
		t.Fatalf("unledgered = %v", report.Unledgered)
	}
	err = report.Err(options)
	if err == nil || !strings.Contains(err.Error(), "unledgered fixture") {
		t.Fatalf("every fixture the harness can read must be listed: %v", err)
	}
}

func TestVerifyTreeMissingIsOnlyAFailureWithAllPresent(t *testing.T) {
	root, ledger, _, _ := verifyFixture(t)
	if err := os.Remove(HostPath(root, "testdata/two/b.par2")); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyTree(root, ledger, VerifyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Missing) != 1 {
		t.Fatalf("missing = %v", report.Missing)
	}
	if err := report.Err(VerifyOptions{}); err != nil {
		t.Fatalf("a developer with one profile hydrated is not a failure: %v", err)
	}
	if err := report.Err(VerifyOptions{AllPresent: true}); err == nil {
		t.Fatal("--all-present must fail on a missing fixture")
	}
}

// The lock check is what makes a ledger edit without a republication fail
// closed.
func TestVerifyLockFailsClosedOnALedgerEdit(t *testing.T) {
	root, ledger, profiles, toolchains := verifyFixture(t)
	manifest, err := BuildManifest(ledger, profiles, toolchains)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := manifest.Encode()
	if err != nil {
		t.Fatal(err)
	}
	lock := LockEntry(DigestBytes(encoded), provenanceDigest, "https://corpus.example.net", testCommit, testRun)
	if err := VerifyLock(root, ledger, profiles, &lock, toolchains); err != nil {
		t.Fatal(err)
	}
	ledger.Files[0].Format = "rar5"
	err = VerifyLock(root, ledger, profiles, &lock, toolchains)
	if err == nil || !strings.Contains(err.Error(), "without a republication") {
		t.Fatalf("a ledger edit must fail against a pinned lock, got %v", err)
	}

	unpinned := Lock{SchemaVersion: SchemaVersion}
	if err := VerifyLock(root, ledger, profiles, &unpinned, toolchains); err != nil {
		t.Fatalf("an unpinned lock has nothing to check: %v", err)
	}
}

func TestFixtureRootsAlwaysIncludesTestdata(t *testing.T) {
	roots := FixtureRoots(newLedger(entry("other/x.bin", "x", blockedSource("pending"))))
	if len(roots) != 2 || roots[0] != "other" || roots[1] != "testdata" {
		t.Fatalf("FixtureRoots = %v", roots)
	}
}
