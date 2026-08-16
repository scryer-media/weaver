package corpus

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// VerifyOptions controls how strict a tree check is.
type VerifyOptions struct {
	// AllPresent turns an absent ledger path into a failure. CI reconstructs
	// the whole corpus first and runs with it; a developer with one profile
	// hydrated does not.
	AllPresent bool
}

// Report is what one tree check found.
type Report struct {
	// Present is the number of ledger paths found on disk with the right size
	// and digest.
	Present int
	// Missing are ledger paths with no file on disk. Only a failure under
	// AllPresent.
	Missing []string
	// Mismatched are files whose size or digest disagrees with the ledger,
	// one human-readable line each. Always a failure.
	Mismatched []string
	// Unledgered are fixture files on disk that the ledger does not list.
	// Always a failure: every fixture the harness can read must be described.
	Unledgered []string
	// Blocked are the ledger paths whose generator does not exist yet. They
	// verify like any other file and are reported, not failed — they are the
	// generator backlog, and they block publication rather than verification.
	Blocked []string
}

// Failed reports whether this check should exit non-zero.
func (report Report) Failed(opts VerifyOptions) bool {
	return len(report.Mismatched) > 0 || len(report.Unledgered) > 0 || (opts.AllPresent && len(report.Missing) > 0)
}

// Err renders the failures as one error, or nil.
func (report Report) Err(opts VerifyOptions) error {
	if !report.Failed(opts) {
		return nil
	}
	var problems []string
	problems = append(problems, report.Mismatched...)
	for _, path := range report.Unledgered {
		problems = append(problems, fmt.Sprintf("%s: unledgered fixture (add it to %s or delete it)", path, LedgerFile))
	}
	if opts.AllPresent {
		for _, path := range report.Missing {
			problems = append(problems, fmt.Sprintf("%s: missing from the tree", path))
		}
	}
	sort.Strings(problems)
	return joinProblems("corpus does not match its ledger", problems)
}

// VerifyTree checks the working tree against the ledger in both directions:
// every listed path that exists must have the recorded size and BLAKE3, and
// every fixture file that exists must be listed.
func VerifyTree(root string, ledger *Ledger, opts VerifyOptions) (Report, error) {
	var report Report
	listed := make(map[string]struct{}, len(ledger.Files))
	for _, entry := range ledger.Files {
		listed[entry.Path] = struct{}{}
		if entry.Source.Kind == SourceBlocked {
			report.Blocked = append(report.Blocked, entry.Path)
		}
		info, err := os.Stat(HostPath(root, entry.Path))
		if os.IsNotExist(err) {
			report.Missing = append(report.Missing, entry.Path)
			continue
		}
		if err != nil {
			return report, fmt.Errorf("stat %s: %w", entry.Path, err)
		}
		if !info.Mode().IsRegular() {
			report.Mismatched = append(report.Mismatched, fmt.Sprintf("%s: not a regular file", entry.Path))
			continue
		}
		if info.Size() != entry.Size {
			// Size disagreement is enough; do not spend a digest pass on it.
			report.Mismatched = append(report.Mismatched,
				fmt.Sprintf("%s: size %d on disk, ledger says %d", entry.Path, info.Size(), entry.Size))
			continue
		}
		digest, err := DigestFile(HostPath(root, entry.Path))
		if err != nil {
			return report, err
		}
		if digest.BLAKE3 != entry.BLAKE3 {
			report.Mismatched = append(report.Mismatched,
				fmt.Sprintf("%s: blake3 %s on disk, ledger says %s", entry.Path, digest.BLAKE3, entry.BLAKE3))
			continue
		}
		report.Present++
	}
	unledgered, err := scanFixtureRoots(root, ledger, listed)
	if err != nil {
		return report, err
	}
	report.Unledgered = unledgered
	sort.Strings(report.Missing)
	sort.Strings(report.Mismatched)
	sort.Strings(report.Blocked)
	return report, nil
}

// FixtureRoots are the tree roots the harness reads fixtures from. testdata is
// always scanned; any other top-level directory the ledger names is scanned
// too, so adding a root to the ledger automatically brings it under the
// "nothing unlisted" rule.
func FixtureRoots(ledger *Ledger) []string {
	roots := map[string]struct{}{"testdata": {}}
	for _, entry := range ledger.Files {
		if index := strings.Index(entry.Path, "/"); index > 0 {
			roots[entry.Path[:index]] = struct{}{}
		}
	}
	names := make([]string, 0, len(roots))
	for name := range roots {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// scanFixtureRoots walks the fixture roots and returns everything the ledger
// does not list. scenario.json files are the scenario definitions, tracked in
// git and never hydrated, so they are not corpus members.
func scanFixtureRoots(root string, ledger *Ledger, listed map[string]struct{}) ([]string, error) {
	var unledgered []string
	for _, fixtureRoot := range FixtureRoots(ledger) {
		base := HostPath(root, fixtureRoot)
		if _, err := os.Stat(base); os.IsNotExist(err) {
			continue
		}
		err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := entry.Name()
			// Dot-entries are editor droppings and this package's own
			// ".<name>.partial-*" hydration temporaries, never fixtures.
			if strings.HasPrefix(name, ".") {
				if entry.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			if entry.IsDir() || name == ScenarioFile {
				return nil
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			if _, ok := listed[relative]; !ok {
				unledgered = append(unledgered, relative)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", fixtureRoot, err)
		}
	}
	sort.Strings(unledgered)
	return unledgered, nil
}

// PublishedOptions controls the online half of a verification.
type PublishedOptions struct {
	// Client is the HTTP client; nil means a default.
	Client *http.Client
	// RequireSignature turns a missing cosign into a failure. Without it the
	// signature is checked whenever cosign happens to be installed, so a
	// developer machine that has it gets the check for free.
	RequireSignature bool
	// Progress, when set, receives one line per step.
	Progress func(string)
}

// VerifyPublished walks the rest of the chain: the manifest the lock names is
// downloaded and must hash to the pinned digest, and its Sigstore bundle must
// verify under the exact publish-workflow identity and issuer the lock pins.
func VerifyPublished(ctx context.Context, lock *Lock, opts PublishedOptions) error {
	if !lock.Pinned() {
		return nil
	}
	progress := opts.Progress
	if progress == nil {
		progress = func(string) {}
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	manifestBytes, err := getBytes(ctx, client, lock.Manifest.URL)
	if err != nil {
		return fmt.Errorf("download the published manifest: %w", err)
	}
	if digest := DigestBytes(manifestBytes); digest != lock.Manifest.BLAKE3 {
		return fmt.Errorf("the manifest at %s has blake3 %s, %s pins %s",
			lock.Manifest.URL, digest, LockFile, lock.Manifest.BLAKE3)
	}
	if _, err := DecodeManifest(manifestBytes); err != nil {
		return err
	}
	progress("published manifest downloaded and its digest matches the lock")

	if !CosignAvailable() {
		if opts.RequireSignature {
			return errors.New("--require-signature was given but cosign is not on PATH; cosign is the only verifier")
		}
		progress("cosign is not installed: the Sigstore bundle was not checked (pass --require-signature to make that a failure)")
		return nil
	}
	bundle, err := getBytes(ctx, client, lock.Signature.BundleURL)
	if err != nil {
		return fmt.Errorf("download the Sigstore bundle: %w", err)
	}
	scratch, err := os.MkdirTemp("", "corpus-verify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	manifestPath := filepath.Join(scratch, BuildManifestFile)
	bundlePath := manifestPath + BundleSuffix
	if err := os.WriteFile(manifestPath, manifestBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(bundlePath, bundle, 0o644); err != nil {
		return err
	}
	if err := VerifyBlob(ctx, manifestPath, bundlePath, lock.Signature.CertificateIdentity, lock.Signature.CertificateOIDCIssuer); err != nil {
		return err
	}
	progress("Sigstore bundle verifies under " + lock.Signature.CertificateIdentity)
	return nil
}

// VerifyLock recomputes the manifest from the checkout and requires it to
// match the pinned digest, so editing the ledger without republishing fails
// closed. An unpinned lock has nothing to check. Signature verification is in
// sign.go; this is the offline half of the chain.
func VerifyLock(root string, ledger *Ledger, profiles *Profiles, lock *Lock, toolchains ToolchainLock) error {
	if !lock.Pinned() {
		return nil
	}
	manifest, err := BuildManifest(ledger, profiles, toolchains)
	if err != nil {
		return err
	}
	contents, err := manifest.Encode()
	if err != nil {
		return err
	}
	digest := DigestBytes(contents)
	if digest != lock.Manifest.BLAKE3 {
		return fmt.Errorf(
			"the manifest this checkout produces (%s) is not the one %s pins (%s): %s, %s or %s changed without a republication",
			digest, LockFile, lock.Manifest.BLAKE3, LedgerFile, ProfilesFile, ledger.Toolchains)
	}
	return nil
}
