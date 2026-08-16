package corpus

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

const (
	manifestDigest   = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	provenanceDigest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	testCommit       = "0123456789abcdef0123456789abcdef01234567"
	testRun          = "https://github.com/scryer-media/weaver/actions/runs/1"
)

func TestLockEntryDerivesAndChecksItsURLs(t *testing.T) {
	lock := LockEntry(manifestDigest, provenanceDigest, "https://corpus.example.net/", testCommit, testRun)
	if err := lock.Validate(); err != nil {
		t.Fatal(err)
	}
	if !lock.Pinned() {
		t.Fatal("a lock with a manifest digest is pinned")
	}
	want := "https://corpus.example.net/test-corpus/manifests/blake3/" + manifestDigest + ".json"
	if lock.Manifest.URL != want {
		t.Fatalf("manifest url %s, want %s", lock.Manifest.URL, want)
	}
	if lock.Signature.BundleURL != want+".sigstore.json" {
		t.Fatalf("bundle url %s", lock.Signature.BundleURL)
	}
	if got := lock.ProvenanceURL(); !strings.HasSuffix(got, manifestDigest+".provenance.json") {
		t.Fatalf("provenance url %s", got)
	}
	if got := lock.ObjectURL(provenanceDigest); got != "https://corpus.example.net/test-corpus/objects/blake3/"+provenanceDigest {
		t.Fatalf("object url %s", got)
	}
	// The rendered entry is what an operator pastes; it must round-trip.
	rendered, err := lock.Render()
	if err != nil {
		t.Fatal(err)
	}
	var reparsed Lock
	if err := json.Unmarshal(rendered, &reparsed); err != nil {
		t.Fatal(err)
	}
	if reparsed != lock {
		t.Fatalf("round trip changed the lock:\n%+v\n%+v", reparsed, lock)
	}
}

func TestLockValidateRejectsTampering(t *testing.T) {
	good := LockEntry(manifestDigest, provenanceDigest, "https://corpus.example.net", testCommit, testRun)
	cases := []struct {
		name   string
		mutate func(*Lock)
	}{
		{"a manifest URL off the base", func(lock *Lock) {
			lock.Manifest.URL = "https://elsewhere.example.net/manifest.json"
		}},
		{"another signer identity", func(lock *Lock) {
			lock.Signature.CertificateIdentity = "https://github.com/someone/else/.github/workflows/x.yml@refs/heads/main"
		}},
		{"another issuer", func(lock *Lock) {
			lock.Signature.CertificateOIDCIssuer = "https://accounts.example.com"
		}},
		{"a bundle URL off the manifest", func(lock *Lock) {
			lock.Signature.BundleURL = "https://corpus.example.net/other.sigstore.json"
		}},
		{"plain http", func(lock *Lock) {
			lock.BaseURL = "http://corpus.example.net"
			lock.Manifest.URL = lock.ManifestURL()
			lock.Signature.BundleURL = lock.BundleURL()
			lock.Provenance.URL = lock.ProvenanceURL()
		}},
		{"a trailing slash on the base", func(lock *Lock) {
			lock.BaseURL = "https://corpus.example.net/"
		}},
		{"a short commit", func(lock *Lock) { lock.PublishedFrom.Commit = "0123456" }},
		{"a run that is not a URL", func(lock *Lock) { lock.PublishedFrom.Run = "run 1" }},
		{"a digest that is not blake3 hex", func(lock *Lock) { lock.Manifest.BLAKE3 = "nope" }},
		{"the wrong schema", func(lock *Lock) { lock.SchemaVersion = 2 }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			tampered := good
			testCase.mutate(&tampered)
			if err := tampered.Validate(); err == nil {
				t.Fatalf("%s should be rejected", testCase.name)
			}
		})
	}
}

func TestUnpinnedLockIsEmptyAndOnlyEmpty(t *testing.T) {
	root := t.TempDir()
	empty := Lock{
		SchemaVersion: SchemaVersion,
		Signature: LockSignature{
			CertificateIdentity:   PublishWorkflowIdentity,
			CertificateOIDCIssuer: GitHubOIDCIssuer,
		},
	}
	writeJSON(t, filepath.Join(root, LockFile), empty)
	loaded, err := LoadLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Pinned() {
		t.Fatal("an empty manifest digest means nothing is published")
	}
	// A half-filled lock is how a stale URL gets past review.
	half := empty
	half.BaseURL = "https://corpus.example.net"
	if err := half.Validate(); err == nil {
		t.Fatal("an unpinned lock carrying a base URL must be rejected")
	}
	half = empty
	half.PublishedFrom.Commit = testCommit
	if err := half.Validate(); err == nil {
		t.Fatal("an unpinned lock carrying a commit must be rejected")
	}
}

// The checked-in lock is the one every developer and CI lane loads.
func TestCheckedInLockLoads(t *testing.T) {
	root := harnessRootForTest(t)
	lock, err := LoadLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if lock.Signature.CertificateIdentity != PublishWorkflowIdentity {
		t.Fatalf("the checked-in lock pins %q", lock.Signature.CertificateIdentity)
	}
}

// harnessRootForTest points at the checkout these tests run inside.
func harnessRootForTest(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
