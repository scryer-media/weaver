// Package corpus implements the signed, content-addressed fixture corpus for
// the Weaver e2e harness: a checked-in ledger (test-corpus/sources.json)
// describes every fixture path, its size, its BLAKE3 digest and the generator
// that produces it; a profile table (test-corpus/profiles.json) names hydration
// subsets; a lock (test-corpus/lock.json) pins the one published manifest that
// developers and CI hydrate from. Objects live on R2 under content-addressed
// keys and are never rewritten.
//
// The mechanism mirrors the rarpar test corpus with two deliberate
// differences: every digest is BLAKE3 (these are large media-shaped files), and
// the only fixture sources are `generated` and `blocked` — nothing is imported
// from another repository. Nothing here implements crypto: digests come from
// the blake3 package, signatures from cosign (the only external tool), and
// uploads are signed with the AWS SigV4 signer over net/http.
package corpus

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/zeebo/blake3"
)

const (
	// LedgerFile, ProfilesFile, LockFile and ToolchainsFile are relative to
	// the harness root (the directory that holds go.mod).
	LedgerFile     = "test-corpus/sources.json"
	ProfilesFile   = "test-corpus/profiles.json"
	LockFile       = "test-corpus/lock.json"
	ToolchainsFile = "test-corpus/toolchains.json"

	// ScenarioFile is the per-fixture scenario definition. It is tracked in
	// git, edited by hand and read by the harness directly, so it is never a
	// corpus object: not ledgered, not published, not hydrated.
	ScenarioFile = "scenario.json"

	// Bucket key prefixes. Content-addressed and immutable.
	ObjectsPrefix   = "test-corpus/objects/blake3/"
	ManifestsPrefix = "test-corpus/manifests/blake3/"

	DigestAlgorithm = "blake3"
	SchemaVersion   = 1

	// SourceGenerated marks a fixture produced by a declared generator on the
	// pinned toolchain; SourceBlocked marks one whose generator does not exist
	// yet. Blocked entries verify like any other file but block publication.
	SourceGenerated = "generated"
	SourceBlocked   = "blocked"
)

// ErrNotPinned is returned by fetch/hydrate while lock.json pins no manifest.
var ErrNotPinned = errors.New("test-corpus/lock.json pins no published manifest; publish a corpus revision and pin it before hydrating")

// FileDigest is the size and BLAKE3 digest of one file.
type FileDigest struct {
	BLAKE3 string
	Size   int64
}

// DigestFile streams a file through BLAKE3. It never holds the file in memory.
func DigestFile(path string) (FileDigest, error) {
	file, err := os.Open(path)
	if err != nil {
		return FileDigest{}, err
	}
	defer file.Close()
	hasher := blake3.New()
	size, err := io.Copy(hasher, bufio.NewReaderSize(file, 1<<20))
	if err != nil {
		return FileDigest{}, fmt.Errorf("read %s: %w", path, err)
	}
	return FileDigest{BLAKE3: hex.EncodeToString(hasher.Sum(nil)), Size: size}, nil
}

// DigestBytes is the BLAKE3 digest of an in-memory document (manifests,
// provenance, the toolchain lock).
func DigestBytes(contents []byte) string {
	sum := blake3.Sum256(contents)
	return hex.EncodeToString(sum[:])
}

// IsDigest reports whether value is a lowercase 64-character hex BLAKE3.
func IsDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

// ValidRelativePath accepts the `/`-separated, root-relative form every ledger
// path, profile glob and manifest entry uses regardless of host OS.
func ValidRelativePath(path string) bool {
	if path == "" || strings.HasPrefix(path, "/") || strings.Contains(path, "\\") || strings.Contains(path, "//") {
		return false
	}
	for _, component := range strings.Split(path, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}

// HostPath maps a ledger path onto the host filesystem under root.
func HostPath(root, relative string) string {
	return filepath.Join(root, filepath.FromSlash(relative))
}

// WriteFileAtomic writes contents to path via a temporary sibling and rename so
// a hydrated fixture is either whole or absent.
func WriteFileAtomic(path string, write func(io.Writer) error, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".partial-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	cleanup := func(cause error) error {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
		return cause
	}
	writer := bufio.NewWriterSize(temporary, 1<<20)
	if err := write(writer); err != nil {
		return cleanup(err)
	}
	if err := writer.Flush(); err != nil {
		return cleanup(err)
	}
	if err := temporary.Sync(); err != nil {
		return cleanup(err)
	}
	if err := temporary.Close(); err != nil {
		return cleanup(err)
	}
	if err := os.Chmod(temporaryPath, mode); err != nil {
		return cleanup(err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return cleanup(err)
	}
	return nil
}
