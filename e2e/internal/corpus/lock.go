package corpus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	// PublishWorkflowIdentity is the exact Sigstore certificate identity a
	// corpus manifest must have been signed under: the publish workflow file,
	// on main, in this repository. It is matched literally — never as a
	// regexp — so a fork or a branch can never satisfy it.
	PublishWorkflowIdentity = "https://github.com/scryer-media/weaver/.github/workflows/e2e-corpus-publish.yml@refs/heads/main"

	// GitHubOIDCIssuer is the only OIDC issuer accepted for that identity.
	GitHubOIDCIssuer = "https://token.actions.githubusercontent.com"
)

// Lock is test-corpus/lock.json: the one published manifest a checkout
// hydrates from, the identity that must have signed it, and where it came
// from. An empty manifest digest means nothing has been published yet.
type Lock struct {
	SchemaVersion int           `json:"schema_version"`
	BaseURL       string        `json:"base_url"`
	Manifest      LockRef       `json:"manifest"`
	Signature     LockSignature `json:"signature"`
	Provenance    LockRef       `json:"provenance"`
	PublishedFrom LockOrigin    `json:"published_from"`
}

// LockRef pins one published document by digest and public URL.
type LockRef struct {
	BLAKE3 string `json:"blake3"`
	URL    string `json:"url"`
}

// LockSignature pins the Sigstore bundle and the identity cosign must find in
// the signing certificate.
type LockSignature struct {
	BundleURL             string `json:"bundle_url"`
	CertificateIdentity   string `json:"certificate_identity"`
	CertificateOIDCIssuer string `json:"certificate_oidc_issuer"`
}

// LockOrigin records the commit and workflow run a publication came from.
type LockOrigin struct {
	Commit string `json:"commit"`
	Run    string `json:"run"`
}

// LoadLock reads and validates the lock under root.
func LoadLock(root string) (*Lock, error) {
	contents, err := os.ReadFile(HostPath(root, LockFile))
	if err != nil {
		return nil, fmt.Errorf("read lock: %w", err)
	}
	var lock Lock
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lock); err != nil {
		return nil, fmt.Errorf("decode %s: %w", LockFile, err)
	}
	if err := lock.Validate(); err != nil {
		return nil, err
	}
	return &lock, nil
}

// Pinned reports whether the lock names a published manifest. While it does
// not, fetch/hydrate refuse: there is nothing to hydrate from.
func (lock *Lock) Pinned() bool { return lock.Manifest.BLAKE3 != "" }

// Validate enforces the whole shape: the signer identity and issuer are fixed
// constants, an unpinned lock carries nothing else, and a pinned lock's URLs
// are exactly the ones its base URL and manifest digest derive.
func (lock *Lock) Validate() error {
	var problems []string
	problem := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if lock.SchemaVersion != SchemaVersion {
		problem("schema_version %d is not %d", lock.SchemaVersion, SchemaVersion)
	}
	if lock.Signature.CertificateIdentity != PublishWorkflowIdentity {
		problem("certificate_identity must be %s, got %q", PublishWorkflowIdentity, lock.Signature.CertificateIdentity)
	}
	if lock.Signature.CertificateOIDCIssuer != GitHubOIDCIssuer {
		problem("certificate_oidc_issuer must be %s, got %q", GitHubOIDCIssuer, lock.Signature.CertificateOIDCIssuer)
	}
	if !lock.Pinned() {
		// Nothing else may be set: a half-filled lock is how a stale URL gets
		// past review.
		if lock.BaseURL != "" || lock.Manifest.URL != "" || lock.Provenance.BLAKE3 != "" ||
			lock.Provenance.URL != "" || lock.Signature.BundleURL != "" ||
			lock.PublishedFrom.Commit != "" || lock.PublishedFrom.Run != "" {
			problem("lock pins no manifest digest but carries other publication fields")
		}
		return joinProblems("lock is invalid", problems)
	}
	if !validBaseURL(lock.BaseURL) {
		problem("base_url must be an https URL without a trailing slash, got %q", lock.BaseURL)
	}
	if !IsDigest(lock.Manifest.BLAKE3) {
		problem("manifest blake3 %q is not a lowercase 64-hex digest", lock.Manifest.BLAKE3)
	}
	if !IsDigest(lock.Provenance.BLAKE3) {
		problem("provenance blake3 %q is not a lowercase 64-hex digest", lock.Provenance.BLAKE3)
	}
	if len(problems) == 0 {
		if lock.Manifest.URL != lock.ManifestURL() {
			problem("manifest url must be %s, got %q", lock.ManifestURL(), lock.Manifest.URL)
		}
		if lock.Signature.BundleURL != lock.BundleURL() {
			problem("signature bundle_url must be %s, got %q", lock.BundleURL(), lock.Signature.BundleURL)
		}
		if lock.Provenance.URL != lock.ProvenanceURL() {
			problem("provenance url must be %s, got %q", lock.ProvenanceURL(), lock.Provenance.URL)
		}
	}
	if !isCommitSHA(lock.PublishedFrom.Commit) {
		problem("published_from.commit must be a full 40-hex commit sha, got %q", lock.PublishedFrom.Commit)
	}
	if !strings.HasPrefix(lock.PublishedFrom.Run, "https://") {
		problem("published_from.run must be the https workflow run URL, got %q", lock.PublishedFrom.Run)
	}
	return joinProblems("lock is invalid", problems)
}

// ManifestURL, BundleURL, ProvenanceURL and ObjectURL derive every published
// address from the base URL and the manifest digest, so a lock can never point
// half of its documents somewhere else.
func (lock *Lock) ManifestURL() string {
	return lock.BaseURL + "/" + ManifestsPrefix + lock.Manifest.BLAKE3 + ".json"
}

// BundleURL is the Sigstore bundle beside the manifest.
func (lock *Lock) BundleURL() string { return lock.ManifestURL() + ".sigstore.json" }

// ProvenanceURL is the build-metadata document beside the manifest.
func (lock *Lock) ProvenanceURL() string {
	return lock.BaseURL + "/" + ManifestsPrefix + lock.Manifest.BLAKE3 + ".provenance.json"
}

// ObjectURL is where one fixture's bytes live.
func (lock *Lock) ObjectURL(digest string) string {
	return lock.BaseURL + "/" + ObjectsPrefix + digest
}

// LockEntry is the lock a publication produces, ready for an operator to paste
// into test-corpus/lock.json through a reviewed PR. The workflow never commits
// it; pinning is always a review.
func LockEntry(manifestDigest, provenanceDigest, baseURL, commit, run string) Lock {
	lock := Lock{
		SchemaVersion: SchemaVersion,
		BaseURL:       strings.TrimRight(baseURL, "/"),
		Manifest:      LockRef{BLAKE3: manifestDigest},
		Signature: LockSignature{
			CertificateIdentity:   PublishWorkflowIdentity,
			CertificateOIDCIssuer: GitHubOIDCIssuer,
		},
		Provenance:    LockRef{BLAKE3: provenanceDigest},
		PublishedFrom: LockOrigin{Commit: commit, Run: run},
	}
	lock.Manifest.URL = lock.ManifestURL()
	lock.Signature.BundleURL = lock.BundleURL()
	lock.Provenance.URL = lock.ProvenanceURL()
	return lock
}

// Render is the pasteable form: indented JSON with a trailing newline.
func (lock *Lock) Render() ([]byte, error) {
	contents, err := json.MarshalIndent(lock, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(contents, '\n'), nil
}

// validBaseURL accepts https, plus loopback http so the fetch path can be
// exercised end to end against a local test server without network exposure.
func validBaseURL(base string) bool {
	if strings.HasSuffix(base, "/") {
		return false
	}
	loopback := strings.HasPrefix(base, "http://127.0.0.1:") || strings.HasPrefix(base, "http://localhost:")
	return strings.HasPrefix(base, "https://") || loopback
}

func isCommitSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func joinProblems(subject string, problems []string) error {
	if len(problems) == 0 {
		return nil
	}
	return errors.New(subject + ":\n  " + strings.Join(problems, "\n  "))
}
