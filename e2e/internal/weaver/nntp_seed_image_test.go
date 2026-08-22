package weaver

import (
	"crypto/sha256"
	"testing"
)

func TestNntpSeedImageTagIncludesProfileRoleAndCorpusFingerprint(t *testing.T) {
	fingerprint := "0123456789abcdef"
	if got, want := nntpSeedImageTag("tcp-chaos", "backup", fingerprint), "weaver-e2e-nntp:corpus-tcp-chaos-backup-"+fingerprint; got != want {
		t.Fatalf("tag = %q, want %q", got, want)
	}
}

func TestNntpSeedFingerprintInputsAreLengthDelimited(t *testing.T) {
	first := sha256.New()
	writeNntpSeedFingerprintInput(first, "a", []byte("bc"))

	second := sha256.New()
	writeNntpSeedFingerprintInput(second, "ab", []byte("c"))

	if string(first.Sum(nil)) == string(second.Sum(nil)) {
		t.Fatal("distinct labeled inputs produced the same fingerprint input hash")
	}
}
